"""
Backtest Engine for Momentum Dashboard
=======================================
Two-phase design:
  Phase 1 — Pre-compute: Calculate indicators at every historical bar for all
            tickers and timeframes, storing results in a SQLite table.
  Phase 2 — Query: Run backtests by scanning the pre-computed table for matching
            entry signals, then computing forward returns from CSV price data.

Pre-computed data lives in the same DB (momentum_dashboard.db), table:
  backtest_indicators — one row per (ticker, date, timeframe, div_adj)
"""

import os
import sys
import time
import sqlite3
import json
import hashlib
import traceback
from datetime import datetime, timedelta
from collections import defaultdict

import pandas as pd
import numpy as np

# ── Local imports ──────────────────────────────────────────────────
from indicators import (
    calc_momentum, calc_squeeze, calc_band, calc_acceleration,
    calc_deviation_signal, calc_hvc, calc_30wma, sma,
    linreg_scalar, calc_momentum_pivot, calc_consecutive_streaks, calc_pivot_wick
)
from data_fetcher import build_timeframe_data, clean_ohlc
from config import DB_PATH, TIMEFRAMES, WATCHLISTS

# ── Constants ──────────────────────────────────────────────────────
CACHE_DIR = "cache"
MAX_LOOKBACK = 110  # must match indicators.py

# Timeframes we pre-compute (skip hourly — too granular for backtesting)
BT_TIMEFRAMES = ["1D", "2D", "3D", "5D", "1W", "2W", "1M", "6W", "3M", "6M", "12M"]

# ── In-memory caches (persist across backtest runs) ──────────────
_CACHE_VERSION = 4           # bump when signal-matching logic changes to invalidate caches
_signal_cache = {}           # signal_hash → {"precompute_ts": str, "matches": [...]}
_price_cache = {}            # ticker → {"mtime": float, "data": [(date, close, open), ...]}
_cache_precompute_ts = None  # tracks last known precompute_ts for invalidation
_PRICE_CACHE_MAX = 3000      # max tickers before eviction
_universe_cache = {"ts": None, "tickers": []}  # cached "all" universe tickers

# ══════════════════════════════════════════════════════════════════
# SCHEMA
# ══════════════════════════════════════════════════════════════════

CREATE_BT_TABLE = """
CREATE TABLE IF NOT EXISTS backtest_indicators (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    date TEXT NOT NULL,          -- YYYY-MM-DD (the bar's date)
    timeframe TEXT NOT NULL,
    div_adj INTEGER DEFAULT 0,
    close_price REAL,
    open_price REAL,
    -- Momentum
    mom_color TEXT DEFAULT '',
    mom_rising INTEGER,
    -- Squeeze
    sqz_state TEXT DEFAULT '',
    -- Band
    band_pos TEXT DEFAULT '',
    band_flip INTEGER DEFAULT 0,
    -- Acceleration
    acc_state TEXT DEFAULT '',
    acc_impulse TEXT DEFAULT '',
    -- Deviation
    dev_signal TEXT DEFAULT '',
    -- Meta (only populated for 1D timeframe)
    above_wma30 INTEGER,
    wma30_cross TEXT DEFAULT '',
    hvc_triggered INTEGER DEFAULT 0,
    hvc_volume_ratio REAL,
    hvc_candle_dir TEXT DEFAULT '',
    hvc_gap_type TEXT DEFAULT '',
    hvc_gap_closed INTEGER DEFAULT 0,
    -- Pivot / Streak (populated for all timeframes)
    mom_pivot_price REAL,
    consec_mom_falling INTEGER DEFAULT 0,
    consec_mom_rising INTEGER DEFAULT 0,
    consec_price_green INTEGER DEFAULT 0,
    consec_price_red INTEGER DEFAULT 0,
    pivot_wick_above INTEGER DEFAULT 0,
    pivot_wick_below INTEGER DEFAULT 0,
    UNIQUE(ticker, date, timeframe, div_adj)
)
"""

CREATE_BT_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_bt_ticker_tf_date ON backtest_indicators(ticker, timeframe, date)",
    "CREATE INDEX IF NOT EXISTS idx_bt_date ON backtest_indicators(date)",
    "CREATE INDEX IF NOT EXISTS idx_bt_divadj ON backtest_indicators(div_adj)",
    "CREATE INDEX IF NOT EXISTS idx_bt_sqz ON backtest_indicators(timeframe, sqz_state, date)",
    "CREATE INDEX IF NOT EXISTS idx_bt_mom ON backtest_indicators(timeframe, mom_color, date)",
]

CREATE_BT_META = """
CREATE TABLE IF NOT EXISTS backtest_meta (
    key TEXT PRIMARY KEY,
    value TEXT
)
"""

CREATE_BT_CACHE = """
CREATE TABLE IF NOT EXISTS backtest_cache (
    payload_hash TEXT PRIMARY KEY,
    precompute_ts TEXT,
    result_json TEXT,
    created_at TEXT
)
"""


def init_db(conn):
    """Create tables and indexes if they don't exist."""
    c = conn.cursor()
    c.execute(CREATE_BT_TABLE)
    for idx_sql in CREATE_BT_INDEXES:
        c.execute(idx_sql)
    c.execute(CREATE_BT_META)
    c.execute(CREATE_BT_CACHE)
    # Migrations: add columns if missing (for existing DBs)
    existing = {row[1] for row in c.execute("PRAGMA table_info(backtest_indicators)").fetchall()}
    if "hvc_gap_type" not in existing:
        c.execute("ALTER TABLE backtest_indicators ADD COLUMN hvc_gap_type TEXT DEFAULT ''")
    if "hvc_gap_closed" not in existing:
        c.execute("ALTER TABLE backtest_indicators ADD COLUMN hvc_gap_closed INTEGER DEFAULT 0")
    if "open_price" not in existing:
        c.execute("ALTER TABLE backtest_indicators ADD COLUMN open_price REAL")
    # Pivot / streak columns (added 2026-02-26)
    for col, typedef in [
        ("mom_pivot_price", "REAL"),
        ("consec_mom_falling", "INTEGER DEFAULT 0"),
        ("consec_mom_rising", "INTEGER DEFAULT 0"),
        ("consec_price_green", "INTEGER DEFAULT 0"),
        ("consec_price_red", "INTEGER DEFAULT 0"),
        ("pivot_wick_above", "INTEGER DEFAULT 0"),
        ("pivot_wick_below", "INTEGER DEFAULT 0"),
    ]:
        if col not in existing:
            c.execute(f"ALTER TABLE backtest_indicators ADD COLUMN {col} {typedef}")
    conn.commit()


# ══════════════════════════════════════════════════════════════════
# PHASE 1: PRE-COMPUTE HISTORICAL INDICATORS
# ══════════════════════════════════════════════════════════════════

def _load_daily_csv(ticker):
    """Load daily OHLCV from cache CSV. Returns DataFrame or None."""
    safe = ticker.replace("/", "_").replace(":", "_")
    path = os.path.join(CACHE_DIR, f"{safe}_day.csv")
    if not os.path.isfile(path):
        return None
    try:
        df = pd.read_csv(path, parse_dates=["timestamp"])
        for col in ("open", "high", "low", "close", "volume"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["close"])
        if df.empty:
            return None
        df = df.sort_values("timestamp").reset_index(drop=True)
        return df
    except Exception:
        return None


def _compute_indicator_series(daily_df):
    """
    Given a full daily DataFrame, build all timeframes and compute
    indicator values at every bar. Returns dict:
      { timeframe: DataFrame with columns [date, close, mom_color, sqz_state, ...] }
    """
    # Build timeframe data using the same function the live system uses
    raw_data = {"daily": daily_df, "hourly": pd.DataFrame(), "weekly": pd.DataFrame()}
    tf_data = build_timeframe_data(raw_data)

    results = {}

    for tf in BT_TIMEFRAMES:
        if tf not in tf_data or tf_data[tf].empty:
            continue

        df = tf_data[tf].copy()
        if len(df) < 21:
            continue

        # Build date column from timestamp
        df["date"] = pd.to_datetime(df["timestamp"]).dt.strftime("%Y-%m-%d")

        # Compute each indicator (full series)
        row_count = len(df)
        mom_color = pd.Series("", index=df.index)
        mom_rising = pd.Series(np.nan, index=df.index)
        sqz_state = pd.Series("", index=df.index)
        band_pos = pd.Series("", index=df.index)
        band_flip = pd.Series(0, index=df.index)
        acc_state = pd.Series("", index=df.index)
        acc_impulse = pd.Series("", index=df.index)
        dev_signal = pd.Series("", index=df.index)

        # Momentum (needs ~40 bars)
        mom_data = None
        if row_count >= 40:
            try:
                mom_data = calc_momentum(df)
                mom_color = mom_data["mom_color"]
                mom_rising = mom_data["mom_rising"].astype(float)
            except Exception:
                pass

        # Squeeze (needs ~21 bars)
        if row_count >= 21:
            try:
                sqz = calc_squeeze(df)
                sqz_state = sqz["sqz_state"]
            except Exception:
                pass

        # Band (needs ~21 bars)
        if row_count >= 21:
            try:
                band = calc_band(df)
                band_pos = band["band_pos"]
                band_flip = band["band_flip"].astype(int)
            except Exception:
                pass

        # Acceleration (needs ~90 bars)
        if row_count >= 90:
            try:
                acc = calc_acceleration(df, precomputed_mom=mom_data)
                acc_state = acc["acc_state"]
                acc_impulse = acc["acc_impulse"]
            except Exception:
                pass

        # Deviation (needs ~21 bars)
        if row_count >= 21:
            try:
                dev = calc_deviation_signal(df)
                dev_signal = dev["dev_signal"]
            except Exception:
                pass

        # Consecutive streaks (needs momentum, ~40 bars)
        consec_mom_falling = pd.Series(0, index=df.index)
        consec_mom_rising = pd.Series(0, index=df.index)
        consec_price_green = pd.Series(0, index=df.index)
        consec_price_red = pd.Series(0, index=df.index)
        if row_count >= 40 and mom_data is not None:
            try:
                streaks = calc_consecutive_streaks(df, precomputed_mom=mom_data)
                consec_mom_falling = streaks["consec_mom_falling"]
                consec_mom_rising = streaks["consec_mom_rising"]
                consec_price_green = streaks["consec_price_green"]
                consec_price_red = streaks["consec_price_red"]
            except Exception:
                pass

        # Momentum pivot price + wick detection (needs 2*20+1 = 41 bars)
        mom_pivot_price = pd.Series(np.nan, index=df.index)
        pivot_wick_above = pd.Series(0, index=df.index)
        pivot_wick_below = pd.Series(0, index=df.index)
        if row_count >= 41:
            try:
                pivot_data = calc_momentum_pivot(df)
                mom_pivot_price = pivot_data["mom_pivot_price"]
                wick_data = calc_pivot_wick(df, mom_pivot_price)
                pivot_wick_above = wick_data["pivot_wick_above"]
                pivot_wick_below = wick_data["pivot_wick_below"]
            except Exception:
                pass

        out = pd.DataFrame({
            "date": df["date"],
            "close_price": df["close"],
            "open_price": df["open"],
            "mom_color": mom_color,
            "mom_rising": mom_rising,
            "sqz_state": sqz_state,
            "band_pos": band_pos,
            "band_flip": band_flip,
            "acc_state": acc_state,
            "acc_impulse": acc_impulse,
            "dev_signal": dev_signal,
            "mom_pivot_price": mom_pivot_price,
            "consec_mom_falling": consec_mom_falling,
            "consec_mom_rising": consec_mom_rising,
            "consec_price_green": consec_price_green,
            "consec_price_red": consec_price_red,
            "pivot_wick_above": pivot_wick_above,
            "pivot_wick_below": pivot_wick_below,
        })

        # Drop rows where all indicators are empty/NaN (warm-up period)
        indicator_cols = ["mom_color", "sqz_state", "band_pos", "acc_state", "dev_signal"]
        has_data = out[indicator_cols].apply(lambda c: c != "").any(axis=1)
        out = out[has_data].copy()

        results[tf] = out

    return results, tf_data


def _compute_meta_series(daily_df, tf_data):
    """
    Compute meta-level indicators (30WMA, HVC) for each daily bar.
    Returns DataFrame with [date, above_wma30, wma30_cross, hvc_triggered, ...].
    """
    weekly_df = tf_data.get("1W")

    # 30WMA series — compute over weekly data
    above_wma30 = pd.Series(np.nan, index=daily_df.index)
    wma30_cross = pd.Series("", index=daily_df.index)

    if weekly_df is not None and not weekly_df.empty and len(weekly_df) >= 30:
        wma30 = sma(weekly_df["close"], 30)
        weekly_dates = pd.to_datetime(weekly_df["timestamp"]).dt.strftime("%Y-%m-%d")

        # Build a lookup: for each daily date, find the corresponding weekly WMA30
        daily_dates = pd.to_datetime(daily_df["timestamp"])
        weekly_ts = pd.to_datetime(weekly_df["timestamp"])

        for i in range(len(daily_df)):
            d = daily_dates.iloc[i]
            # Find latest weekly bar on or before this date
            mask = weekly_ts <= d
            if not mask.any():
                continue
            widx = mask.values.nonzero()[0][-1]
            if pd.isna(wma30.iloc[widx]):
                continue

            close = daily_df["close"].iloc[i]
            above = bool(close > wma30.iloc[widx])
            above_wma30.iloc[i] = int(above)

            # Cross detection: compare to previous week
            if widx > 0 and not pd.isna(wma30.iloc[widx - 1]):
                prev_above = bool(weekly_df["close"].iloc[widx - 1] > wma30.iloc[widx - 1])
                # Only mark cross on the first daily bar of the new weekly period
                if i > 0:
                    prev_d = daily_dates.iloc[i - 1]
                    prev_mask = weekly_ts <= prev_d
                    if prev_mask.any():
                        prev_widx = prev_mask.values.nonzero()[0][-1]
                        if prev_widx != widx:
                            # New weekly bar
                            if above and not prev_above:
                                wma30_cross.iloc[i] = "crossed_above"
                            elif not above and prev_above:
                                wma30_cross.iloc[i] = "crossed_below"

    # HVC series
    hvc_triggered = pd.Series(0, index=daily_df.index)
    hvc_volume_ratio = pd.Series(np.nan, index=daily_df.index)
    hvc_candle_dir = pd.Series("", index=daily_df.index)
    hvc_gap_type = pd.Series("", index=daily_df.index)
    hvc_gap_closed = pd.Series(0, index=daily_df.index)

    if len(daily_df) >= 21:
        try:
            hvc = calc_hvc(daily_df)
            hvc_triggered = hvc["hvc_triggered"].astype(int)
            hvc_volume_ratio = hvc["hvc_volume_ratio"]
            hvc_candle_dir = hvc["hvc_candle_dir"]
            hvc_gap_type = hvc["hvc_gap_type"]
            hvc_gap_closed = hvc["hvc_gap_closed"].astype(int)
        except Exception:
            pass

    return pd.DataFrame({
        "above_wma30": above_wma30,
        "wma30_cross": wma30_cross,
        "hvc_triggered": hvc_triggered,
        "hvc_volume_ratio": hvc_volume_ratio,
        "hvc_candle_dir": hvc_candle_dir,
        "hvc_gap_type": hvc_gap_type,
        "hvc_gap_closed": hvc_gap_closed,
    }, index=daily_df.index)


def precompute_ticker(conn, ticker, div_adj=0, force=False):
    """
    Pre-compute all indicators for a single ticker across all timeframes.
    Stores results in backtest_indicators table.
    Returns number of rows inserted, or -1 on error.
    """
    daily_df = _load_daily_csv(ticker)
    if daily_df is None:
        return -1

    c = conn.cursor()

    # Check if already computed (skip unless force)
    if not force:
        c.execute(
            "SELECT COUNT(*) FROM backtest_indicators WHERE ticker=? AND div_adj=?",
            (ticker, div_adj)
        )
        if c.fetchone()[0] > 0:
            return 0  # already done

    # Delete existing rows for this ticker+adj (if force recompute)
    c.execute("DELETE FROM backtest_indicators WHERE ticker=? AND div_adj=?", (ticker, div_adj))

    try:
        tf_results, tf_data = _compute_indicator_series(daily_df)
    except Exception as e:
        return -1

    # Compute meta indicators
    try:
        meta = _compute_meta_series(daily_df, tf_data)
        daily_dates = pd.to_datetime(daily_df["timestamp"]).dt.strftime("%Y-%m-%d")
        meta["date"] = daily_dates.values
    except Exception:
        meta = None

    total_inserted = 0

    for tf, df in tf_results.items():
        rows = []
        for _, row in df.iterrows():
            date_str = row["date"]

            # Merge meta data for 1D timeframe
            above_wma30 = None
            wma30_cross_val = ""
            hvc_trig = 0
            hvc_ratio = None
            hvc_dir = ""
            hvc_gap = ""
            hvc_gap_cl = 0

            if tf == "1D" and meta is not None:
                meta_match = meta[meta["date"] == date_str]
                if not meta_match.empty:
                    m = meta_match.iloc[0]
                    above_wma30 = None if pd.isna(m["above_wma30"]) else int(m["above_wma30"])
                    wma30_cross_val = m["wma30_cross"] if m["wma30_cross"] else ""
                    hvc_trig = int(m["hvc_triggered"]) if not pd.isna(m["hvc_triggered"]) else 0
                    hvc_ratio = None if pd.isna(m["hvc_volume_ratio"]) else round(float(m["hvc_volume_ratio"]), 2)
                    hvc_dir = m["hvc_candle_dir"] if m["hvc_candle_dir"] else ""
                    hvc_gap = m["hvc_gap_type"] if m["hvc_gap_type"] else ""
                    hvc_gap_cl = int(m["hvc_gap_closed"]) if not pd.isna(m["hvc_gap_closed"]) else 0

            rows.append((
                ticker, date_str, tf, div_adj,
                round(float(row["close_price"]), 4) if not pd.isna(row["close_price"]) else None,
                round(float(row["open_price"]), 4) if not pd.isna(row["open_price"]) else None,
                row["mom_color"] or "",
                None if pd.isna(row["mom_rising"]) else int(row["mom_rising"]),
                row["sqz_state"] or "",
                row["band_pos"] or "",
                int(row["band_flip"]) if not pd.isna(row["band_flip"]) else 0,
                row["acc_state"] or "",
                row["acc_impulse"] or "",
                row["dev_signal"] or "",
                above_wma30,
                wma30_cross_val,
                hvc_trig,
                hvc_ratio,
                hvc_dir,
                hvc_gap,
                hvc_gap_cl,
                round(float(row["mom_pivot_price"]), 4) if not pd.isna(row["mom_pivot_price"]) else None,
                int(row["consec_mom_falling"]) if not pd.isna(row["consec_mom_falling"]) else 0,
                int(row["consec_mom_rising"]) if not pd.isna(row["consec_mom_rising"]) else 0,
                int(row["consec_price_green"]) if not pd.isna(row["consec_price_green"]) else 0,
                int(row["consec_price_red"]) if not pd.isna(row["consec_price_red"]) else 0,
                int(row["pivot_wick_above"]) if not pd.isna(row["pivot_wick_above"]) else 0,
                int(row["pivot_wick_below"]) if not pd.isna(row["pivot_wick_below"]) else 0,
            ))

        if rows:
            c.executemany("""
                INSERT OR REPLACE INTO backtest_indicators
                (ticker, date, timeframe, div_adj, close_price, open_price,
                 mom_color, mom_rising, sqz_state, band_pos, band_flip,
                 acc_state, acc_impulse, dev_signal,
                 above_wma30, wma30_cross, hvc_triggered, hvc_volume_ratio, hvc_candle_dir,
                 hvc_gap_type, hvc_gap_closed,
                 mom_pivot_price, consec_mom_falling, consec_mom_rising,
                 consec_price_green, consec_price_red,
                 pivot_wick_above, pivot_wick_below)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, rows)
            total_inserted += len(rows)

    conn.commit()
    return total_inserted


def precompute_all(div_adj=0, force=False, watchlist=None, progress_callback=None):
    """
    Pre-compute indicators for all tickers (or a specific watchlist).
    Returns dict with stats.
    """
    conn = sqlite3.connect(DB_PATH, timeout=30)
    init_db(conn)

    # Build ticker list
    if watchlist and watchlist in WATCHLISTS:
        tickers = []
        for group_name, group_tickers in WATCHLISTS[watchlist]:
            for display, api_ticker, *_ in group_tickers:
                tickers.append(api_ticker)
    else:
        # All tickers from CSV files
        tickers = set()
        for f in os.listdir(CACHE_DIR):
            if f.endswith("_day.csv"):
                t = f.replace("_day.csv", "").replace("_", "/", 1) if "_" in f and not f.startswith("_") else f.replace("_day.csv", "")
                # Simple: just use the filename prefix
                tickers.add(f.replace("_day.csv", ""))
        tickers = sorted(tickers)

    total = len(tickers)
    done = 0
    errors = 0
    skipped = 0
    inserted = 0
    t0 = time.time()

    for ticker in tickers:
        try:
            n = precompute_ticker(conn, ticker, div_adj=div_adj, force=force)
            if n == 0:
                skipped += 1
            elif n < 0:
                errors += 1
            else:
                inserted += n
        except Exception as e:
            errors += 1

        done += 1
        if progress_callback and done % 50 == 0:
            progress_callback(done, total, errors, time.time() - t0)

    elapsed = time.time() - t0

    # Update meta
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO backtest_meta (key, value) VALUES (?, ?)",
              ("last_precompute", datetime.now().isoformat()))
    c.execute("INSERT OR REPLACE INTO backtest_meta (key, value) VALUES (?, ?)",
              ("precompute_stats", json.dumps({
                  "total": total, "done": done, "errors": errors,
                  "skipped": skipped, "inserted": inserted,
                  "elapsed_sec": round(elapsed, 1)
              })))
    conn.commit()
    conn.close()

    return {
        "total": total, "done": done, "errors": errors,
        "skipped": skipped, "inserted": inserted,
        "elapsed_sec": round(elapsed, 1)
    }


def backfill_clusterbomb_returns():
    """
    Populate return_1w and return_1m columns on clusterbomb_events
    using daily CSV price data. 1W = 5 trading days, 1M = 21 trading days.
    Only updates rows where the columns are NULL.
    Returns count of rows updated.
    """
    conn = sqlite3.connect(DB_PATH, timeout=30)
    c = conn.cursor()

    # Ensure columns exist
    existing = {row[1] for row in c.execute("PRAGMA table_info(clusterbomb_events)").fetchall()}
    if "return_1w" not in existing:
        c.execute("ALTER TABLE clusterbomb_events ADD COLUMN return_1w REAL")
    if "return_1m" not in existing:
        c.execute("ALTER TABLE clusterbomb_events ADD COLUMN return_1m REAL")

    # Get events that need backfilling
    rows = c.execute(
        "SELECT id, ticker, event_date FROM clusterbomb_events "
        "WHERE return_1w IS NULL OR return_1m IS NULL"
    ).fetchall()

    if not rows:
        conn.close()
        return 0

    # Group by ticker to load each CSV once
    by_ticker = defaultdict(list)
    for row_id, ticker, event_date in rows:
        by_ticker[ticker].append((row_id, event_date))

    updated = 0
    for ticker, events in by_ticker.items():
        daily = _load_daily_prices(ticker)
        if not daily:
            continue
        date_idx = {d: i for i, (d, cl, op) in enumerate(daily)}

        for row_id, event_date in events:
            idx = date_idx.get(event_date)
            if idx is None:
                continue
            event_close = daily[idx][1]
            if event_close <= 0:
                continue

            ret_1w = None
            ret_1m = None
            if idx + 5 < len(daily):
                ret_1w = round(((daily[idx + 5][1] - event_close) / event_close) * 100, 2)
            if idx + 21 < len(daily):
                ret_1m = round(((daily[idx + 21][1] - event_close) / event_close) * 100, 2)

            c.execute(
                "UPDATE clusterbomb_events SET return_1w=?, return_1m=? WHERE id=?",
                (ret_1w, ret_1m, row_id)
            )
            updated += 1

    conn.commit()
    conn.close()
    return updated


# ══════════════════════════════════════════════════════════════════
# PHASE 2: BACKTEST QUERY ENGINE
# ══════════════════════════════════════════════════════════════════

def _get_tickers_for_universe(universe, conn, precompute_ts=None):
    """Get list of tickers for a given watchlist/universe filter.
    For 'all' universe, caches result keyed by precompute_ts to avoid
    expensive SELECT DISTINCT on every run."""
    global _universe_cache
    if not universe or universe == "all":
        # Return cached if precompute_ts hasn't changed
        if precompute_ts and _universe_cache["ts"] == precompute_ts and _universe_cache["tickers"]:
            return _universe_cache["tickers"]
        c = conn.cursor()
        c.execute("SELECT DISTINCT ticker FROM backtest_indicators")
        tickers = [r[0] for r in c.fetchall()]
        _universe_cache = {"ts": precompute_ts, "tickers": tickers}
        return tickers

    if universe in WATCHLISTS:
        tickers = []
        for group_name, group_tickers in WATCHLISTS[universe]:
            for entry in group_tickers:
                tickers.append(entry[1])  # api_ticker
        return tickers

    return []


def _tf_to_calendar_days(tf):
    """Convert a timeframe code to approximate calendar days per bar.
    Used for the 'within N bars' lookback calculation."""
    mapping = {
        "1D": 1.5,   # 1 trading day ≈ 1.5 calendar days (weekends)
        "2D": 3,
        "3D": 4.5,
        "5D": 7,
        "1W": 7,
        "2W": 14,
        "1M": 30,
        "6W": 42,
        "3M": 91,
        "6M": 182,
        "12M": 365,
    }
    return mapping.get(tf, 1.5)


def _build_signal_cache_key(entry_conditions, universe_tickers, div_adj, lookback_date, end_date, dedup_setting, confirmed_only=True):
    """Build a deterministic hash key for signal-affecting parameters only.
    Exit rules, maintain, direction, stop_loss_pct are intentionally excluded."""
    key_data = json.dumps({
        "v": _CACHE_VERSION,
        "entry": entry_conditions,
        "tickers": sorted(universe_tickers) if universe_tickers else [],
        "div_adj": div_adj,
        "lookback": lookback_date,
        "end": end_date,
        "dedup": dedup_setting,
        "confirmed_only": confirmed_only,
    }, sort_keys=True, default=str)
    return hashlib.sha256(key_data.encode()).hexdigest()[:20]


def _check_cache_validity(precompute_ts):
    """Invalidate in-memory caches if precompute_ts has changed."""
    global _signal_cache, _price_cache, _cache_precompute_ts, _universe_cache

    if _cache_precompute_ts != precompute_ts:
        _signal_cache.clear()
        _price_cache.clear()
        _universe_cache = {"ts": None, "tickers": []}
        _cache_precompute_ts = precompute_ts
        return

    # Enforce price cache size limit
    if len(_price_cache) > _PRICE_CACHE_MAX:
        _price_cache.clear()


def _resolve_bar_end_date(cursor, ticker, div_adj, tf, bar_date):
    """
    Given a higher-TF bar's start date, find the last daily trading date
    within that bar period (i.e. when the signal is actually confirmed at close).

    For 1D, bar_date IS the end date — return it unchanged.
    For 1W/1M/etc, find the next bar's start date in that TF, then find the
    last daily bar BEFORE that next start date.

    Returns: end_date string (YYYY-MM-DD) or None if no data available.

    The caller can then feed this into _compute_forward_returns (which shifts
    to next-day open), giving realistic execution.
    """
    if tf == "1D":
        return bar_date

    # Find the next bar in the same timeframe
    cursor.execute(
        "SELECT date FROM backtest_indicators "
        "WHERE ticker=? AND div_adj=? AND timeframe=? AND date>? "
        "ORDER BY date ASC LIMIT 1",
        (ticker, div_adj, tf, bar_date)
    )
    nxt = cursor.fetchone()
    if nxt:
        next_bar_start = nxt[0]
        # Last daily bar before the next period starts
        cursor.execute(
            "SELECT date FROM backtest_indicators "
            "WHERE ticker=? AND div_adj=? AND timeframe='1D' AND date>=? AND date<? "
            "ORDER BY date DESC LIMIT 1",
            (ticker, div_adj, bar_date, next_bar_start)
        )
        last_daily = cursor.fetchone()
        return last_daily[0] if last_daily else bar_date
    else:
        # No next bar — this is the most recent period.
        # Use the last daily bar available from bar_date onwards.
        cursor.execute(
            "SELECT date FROM backtest_indicators "
            "WHERE ticker=? AND div_adj=? AND timeframe='1D' AND date>=? "
            "ORDER BY date DESC LIMIT 1",
            (ticker, div_adj, bar_date)
        )
        last_daily = cursor.fetchone()
        return last_daily[0] if last_daily else bar_date


def _match_entry_conditions(conn, entry_conditions, universe_tickers, div_adj, lookback_date, end_date=None, confirmed_only=True):
    """
    Find all (ticker, date) pairs where ALL entry conditions are met.
    Uses the "within N bars" flexibility.

    IMPORTANT — ordering matters:
      The FIRST condition is the primary signal and determines the entry dates.
      Additional conditions are confirmations that must have occurred on or
      before the primary date, within the specified bar window.

    entry_conditions: list of {indicator, timeframe, value, within_bars}

    confirmed_only: if True (default), cross-timeframe matching only uses the
        PREVIOUS completed higher-TF bar — never the bar whose period contains
        the primary date.  This prevents look-ahead bias where the backtester
        enters mid-month using a monthly signal that won't confirm until month-end.
        Set to False to match any higher-TF bar whose period contains the primary
        date (legacy behaviour — useful for research but results are biased).

    Returns list of (ticker, date, close_price) tuples.
    """
    if not entry_conditions:
        return []

    # Strategy: for each condition, find matching (ticker, date) sets,
    # then intersect them (AND logic) with "within_bars" tolerance.

    # First condition determines the base set (entry date)
    # Additional conditions narrow it down (must have fired within N bars before)

    c = conn.cursor()
    ticker_filter = ""
    params_base = [div_adj]
    if universe_tickers:
        placeholders = ",".join(["?"] * len(universe_tickers))
        ticker_filter = f" AND ticker IN ({placeholders})"
        params_base.extend(universe_tickers)

    date_filter = ""
    if lookback_date:
        date_filter = " AND date >= ?"
        params_base.append(lookback_date)

    # For the first condition (the primary signal), get all matches
    primary = entry_conditions[0]
    matches = _query_condition_matches(c, primary, div_adj, universe_tickers, lookback_date, end_date)

    if not matches:
        return []

    # For each additional condition, filter matches using within_bars tolerance
    for cond in entry_conditions[1:]:
        if not matches:
            break
        cond_matches = _query_condition_matches(c, cond, div_adj, universe_tickers, lookback_date, end_date)

        # Build a lookup: ticker -> set of dates
        cond_by_ticker = defaultdict(set)
        for ticker, date, _ in cond_matches:
            cond_by_ticker[ticker].add(date)

        within = cond.get("within_bars", 0)
        # Convert "within N bars" to calendar days using the condition's timeframe
        cond_tf = cond.get("timeframe", "1D")
        cal_days_per_bar = _tf_to_calendar_days(cond_tf)

        # For cross-timeframe "current" matching, we need to check whether
        # the primary date falls within the higher-TF bar's period.
        # E.g. a 1D primary date of 2025-03-20 should match a 1W bar dated
        # 2025-03-17 (which covers Mar 17-21).
        # The condition bar date is the period start; the next bar's date is
        # the start of the next period.  Primary date is "current" if it falls
        # in [bar_date, next_bar_date).
        # For same-timeframe or within>0, use the existing calendar-day approach.

        filtered = []
        for ticker, date, price in matches:
            if within == 0:
                cond_dates = cond_by_ticker.get(ticker, set())
                if date in cond_dates:
                    # Exact date match (same TF or happens to align)
                    filtered.append((ticker, date, price))
                elif cond_dates:
                    # Cross-timeframe: find condition bars ≤ primary date,
                    # sorted descending so we can check the most recent first.
                    target_dt = datetime.strptime(date, "%Y-%m-%d")
                    sorted_cds = sorted(
                        [datetime.strptime(s, "%Y-%m-%d") for s in cond_dates],
                        reverse=True
                    )

                    for cd in sorted_cds:
                        if cd > target_dt:
                            continue
                        diff_days = (target_dt - cd).days

                        if diff_days < cal_days_per_bar:
                            # This bar's period contains the primary date —
                            # i.e. the bar is still forming / not yet confirmed.
                            if confirmed_only:
                                # Skip — look for the previous completed bar
                                continue
                            else:
                                # Legacy mode: match the forming bar
                                filtered.append((ticker, date, price))
                                break
                        else:
                            # This bar closed before the primary date — it's
                            # a confirmed, completed bar.  Match it.
                            # Only accept if it's reasonably recent (within
                            # 2x the bar period so we don't match ancient bars).
                            if diff_days < cal_days_per_bar * 2:
                                filtered.append((ticker, date, price))
                            break
            else:
                # Within N bars: check if condition was true in [date - N bars, date]
                max_lookback_days = int(within * cal_days_per_bar) + 1
                target_date = datetime.strptime(date, "%Y-%m-%d")
                found = False
                for cond_date_str in cond_by_ticker.get(ticker, set()):
                    cond_date = datetime.strptime(cond_date_str, "%Y-%m-%d")
                    # Condition must be on or before entry date, within N bars
                    diff = (target_date - cond_date).days
                    if 0 <= diff <= max_lookback_days:
                        found = True
                        break
                if found:
                    filtered.append((ticker, date, price))

        matches = filtered

    return matches


def _deduplicate_signals(matches, cooldown_days=30):
    """
    Remove duplicate signals for the same ticker within a cooldown period.
    If a weekly orange squeeze persists for 4 weeks, only keep the FIRST bar.
    A new signal is only counted after `cooldown_days` of no signal.

    matches: list of (ticker, date, close_price) sorted by date
    Returns filtered list.
    """
    if not matches:
        return matches

    by_ticker = defaultdict(list)
    for ticker, date, price in matches:
        by_ticker[ticker].append((date, price))

    deduped = []
    for ticker, signals in by_ticker.items():
        signals.sort(key=lambda x: x[0])
        last_entry_date = None
        for date_str, price in signals:
            d = datetime.strptime(date_str, "%Y-%m-%d")
            if last_entry_date is None or (d - last_entry_date).days >= cooldown_days:
                deduped.append((ticker, date_str, price))
                last_entry_date = d

    # Sort by date
    deduped.sort(key=lambda x: x[1])
    return deduped


def _parse_bias_params(cond):
    """Parse bias direction and days from a sweep entry condition.

    Supports new flexible values (buy_bias, sell_bias with explicit bias_days)
    and legacy hardcoded values (1w_buy, 1m_sell, etc.) for backward compat.

    Returns (bias_direction, bias_days, use_precomputed).
      bias_direction: 'buy', 'sell', or None
      bias_days: int trading days (0 = no bias)
      use_precomputed: True if we can use return_1w/return_1m SQL columns
    """
    value = cond.get("value", "any")
    explicit_days = cond.get("bias_days")

    # Strip rare_ prefix to get the bias component
    bias_part = value.replace("rare_", "") if value.startswith("rare_") else value

    # Determine direction
    if bias_part in ("buy_bias", "1w_buy", "1m_buy"):
        direction = "buy"
    elif bias_part in ("sell_bias", "1w_sell", "1m_sell"):
        direction = "sell"
    else:
        return None, 0, False

    # Determine days — explicit bias_days overrides legacy strings
    if explicit_days is not None:
        days = int(explicit_days)
    elif "1w_" in bias_part:
        days = 5
    elif "1m_" in bias_part:
        days = 21
    else:
        days = 5  # default for buy_bias/sell_bias without explicit days

    # Fast path: N=5 or N=21 can use pre-computed columns
    use_precomputed = (days == 5 or days == 21) and explicit_days is None

    return direction, days, use_precomputed


def _apply_bias_confirmation(raw_matches, bias_days, bias_direction):
    """Filter matches by N-day price direction, then shift entry to confirmation date.

    For custom N (not pre-filtered in SQL), computes forward return from CSV prices
    and keeps only matches where price moved in the expected direction.

    Then shifts all remaining matches forward by bias_days trading days.

    Returns: [(ticker, confirm_date, confirm_price), ...]
    """
    if bias_days <= 0 or not bias_direction:
        return raw_matches

    max_calendar_gap = bias_days * 3

    # Group by ticker to load each CSV once
    by_ticker = defaultdict(list)
    for ticker, event_date, event_price in raw_matches:
        by_ticker[ticker].append((event_date, event_price))

    # Diagnostic counters (printed to server console)
    _diag = {"raw": len(raw_matches), "tickers": len(by_ticker),
             "no_csv": 0, "no_date": 0, "too_short": 0, "gap": 0,
             "wrong_dir": 0, "passed": 0}

    shifted = []
    for ticker, events in by_ticker.items():
        daily = _load_daily_prices(ticker)
        if not daily:
            _diag["no_csv"] += len(events)
            continue
        date_idx = {d: i for i, (d, c, o) in enumerate(daily)}

        for event_date, event_price in events:
            idx = date_idx.get(event_date)
            if idx is None:
                _diag["no_date"] += 1
                continue
            confirm_idx = idx + bias_days
            if confirm_idx >= len(daily):
                _diag["too_short"] += 1
                continue

            event_close = daily[idx][1]
            confirm_date = daily[confirm_idx][0]
            confirm_price = daily[confirm_idx][1]  # close at confirmation

            # Sanity: confirmation must be near the event (not years later)
            gap = (datetime.strptime(confirm_date, "%Y-%m-%d") -
                   datetime.strptime(event_date, "%Y-%m-%d")).days
            if gap > max_calendar_gap:
                _diag["gap"] += 1
                continue

            # Filter by price direction (skip if already pre-filtered in SQL)
            if event_close > 0:
                ret = (confirm_price - event_close) / event_close
                if bias_direction == "buy" and ret <= 0:
                    _diag["wrong_dir"] += 1
                    continue
                elif bias_direction == "sell" and ret >= 0:
                    continue

            _diag["passed"] += 1
            shifted.append((ticker, confirm_date, confirm_price))

    print(f"[BIAS DIAG] {bias_direction} {bias_days}d: {_diag}", flush=True)
    shifted.sort(key=lambda x: x[1])
    return shifted


def _query_clusterbomb_matches(cursor, cond, universe_tickers, lookback_date, end_date):
    """
    Query clusterbomb_events table for backtest entry condition matching.

    Values:
      "any"            — any clusterbomb (enter day after event)
      "buy_bias"       — price up after N days → enter at confirmation
      "sell_bias"      — price down after N days → enter at confirmation
      "rare"           — rare clusterbombs only
      "rare_buy_bias"  — rare + buy bias
      "rare_sell_bias" — rare + sell bias
      Legacy: "1w_buy", "1w_sell", "1m_buy", "1m_sell" and rare_ variants

    Optional filters: min_notional, min_sweeps, bias_days (trading days for confirmation).

    Returns list of (ticker, date, close_price) matching standard backtest format.
    """
    value = cond.get("value", "any")
    where_parts = []
    params = []

    if "rare" in value:
        where_parts.append("is_rare = 1")

    # Parse bias parameters
    bias_direction, bias_days, use_precomputed = _parse_bias_params(cond)

    # Fast path: use pre-computed return columns for 1W/1M
    if use_precomputed and bias_direction:
        if bias_days == 5:
            col = "return_1w"
        else:
            col = "return_1m"
        op = ">" if bias_direction == "buy" else "<"
        where_parts.append(f"{col} {op} 0")

    # Optional numeric thresholds
    min_notional = cond.get("min_notional")
    if min_notional is not None:
        try:
            min_notional = float(min_notional)
            where_parts.append("total_notional >= ?")
            params.append(min_notional)
        except (ValueError, TypeError):
            pass

    min_sweeps = cond.get("min_sweeps")
    if min_sweeps is not None:
        try:
            min_sweeps = int(min_sweeps)
            where_parts.append("sweep_count >= ?")
            params.append(min_sweeps)
        except (ValueError, TypeError):
            pass

    if universe_tickers:
        placeholders = ",".join(["?"] * len(universe_tickers))
        where_parts.append(f"ticker IN ({placeholders})")
        params.extend(universe_tickers)

    if lookback_date:
        where_parts.append("event_date >= ?")
        params.append(lookback_date)

    if end_date:
        where_parts.append("event_date <= ?")
        params.append(end_date)

    where_sql = " AND ".join(where_parts) if where_parts else "1=1"

    try:
        cursor.execute(f"""
            SELECT ticker, event_date, avg_price
            FROM clusterbomb_events
            WHERE {where_sql}
            ORDER BY event_date
        """, params)
        raw_matches = [(r[0], r[1], r[2]) for r in cursor.fetchall()]
    except Exception:
        # Table might not exist yet
        return []

    if bias_days == 0:
        return raw_matches

    # For pre-computed fast path, matches are already bias-filtered in SQL —
    # just need to shift entry date forward. _apply_bias_confirmation will
    # skip the return check when SQL already filtered.
    if use_precomputed:
        # Shift only (no re-filtering needed)
        return _apply_bias_shift_only(raw_matches, bias_days)

    # Custom N: filter by return direction + shift in one pass
    return _apply_bias_confirmation(raw_matches, bias_days, bias_direction)


def _apply_bias_shift_only(raw_matches, bias_days):
    """Shift entry dates forward by bias_days trading days (no return filtering).
    Used when SQL WHERE already filtered by pre-computed return columns."""
    if bias_days <= 0:
        return raw_matches

    max_calendar_gap = bias_days * 3
    by_ticker = defaultdict(list)
    for ticker, event_date, event_price in raw_matches:
        by_ticker[ticker].append((event_date, event_price))

    shifted = []
    for ticker, events in by_ticker.items():
        daily = _load_daily_prices(ticker)
        if not daily:
            continue
        date_idx = {d: i for i, (d, c, o) in enumerate(daily)}

        for event_date, event_price in events:
            idx = date_idx.get(event_date)
            if idx is None:
                continue
            confirm_idx = idx + bias_days
            if confirm_idx >= len(daily):
                continue
            confirm_date = daily[confirm_idx][0]
            confirm_price = daily[confirm_idx][1]

            gap = (datetime.strptime(confirm_date, "%Y-%m-%d") -
                   datetime.strptime(event_date, "%Y-%m-%d")).days
            if gap > max_calendar_gap:
                continue

            shifted.append((ticker, confirm_date, confirm_price))

    shifted.sort(key=lambda x: x[1])
    return shifted


def _query_monster_matches(cursor, cond, universe_tickers, lookback_date, end_date):
    """
    Query clusterbomb_events for monster sweep matches.

    Targets standalone monsters (event_type='monster_sweep') and clusterbombs
    that are also monsters (is_monster=1).

    Values:
      "any"        — any monster (enter day after event)
      "buy_bias"   — price up after N days → enter at confirmation
      "sell_bias"  — price down after N days → enter at confirmation

    Optional filters: min_notional, bias_days.
    No min_sweeps (monsters are about single sweep size, not count).

    Returns list of (ticker, date, close_price) matching standard backtest format.
    """
    where_parts = ["(event_type = 'monster_sweep' OR is_monster = 1)"]
    params = []

    # Parse bias parameters
    bias_direction, bias_days, use_precomputed = _parse_bias_params(cond)

    # Fast path: use pre-computed return columns for 1W/1M
    if use_precomputed and bias_direction:
        col = "return_1w" if bias_days == 5 else "return_1m"
        op = ">" if bias_direction == "buy" else "<"
        where_parts.append(f"{col} {op} 0")

    # Optional notional threshold
    min_notional = cond.get("min_notional")
    if min_notional is not None:
        try:
            min_notional = float(min_notional)
            where_parts.append("total_notional >= ?")
            params.append(min_notional)
        except (ValueError, TypeError):
            pass

    if universe_tickers:
        placeholders = ",".join(["?"] * len(universe_tickers))
        where_parts.append(f"ticker IN ({placeholders})")
        params.extend(universe_tickers)

    if lookback_date:
        where_parts.append("event_date >= ?")
        params.append(lookback_date)

    if end_date:
        where_parts.append("event_date <= ?")
        params.append(end_date)

    where_sql = " AND ".join(where_parts)

    try:
        cursor.execute(f"""
            SELECT ticker, event_date, avg_price
            FROM clusterbomb_events
            WHERE {where_sql}
            ORDER BY event_date
        """, params)
        raw_matches = [(r[0], r[1], r[2]) for r in cursor.fetchall()]
    except Exception:
        return []

    if bias_days == 0:
        return raw_matches

    if use_precomputed:
        return _apply_bias_shift_only(raw_matches, bias_days)

    return _apply_bias_confirmation(raw_matches, bias_days, bias_direction)


def _query_close_vs_prev_matches(cursor, div_adj, tf, value,
                                 universe_tickers, lookback_date, end_date):
    """Query for bars where close is lower/higher than previous bar's close.

    'lower'  = close < prev bar's close (bearish continuation)
    'higher' = close > prev bar's close (bullish continuation)
    """
    params_q = [div_adj, tf]
    where_q = ["div_adj = ?", "timeframe = ?"]

    if universe_tickers:
        placeholders = ",".join(["?"] * len(universe_tickers))
        where_q.append(f"ticker IN ({placeholders})")
        params_q.extend(universe_tickers)

    sql = f"""SELECT ticker, date, close_price
              FROM backtest_indicators
              WHERE {' AND '.join(where_q)}
              ORDER BY ticker, date"""
    cursor.execute(sql, params_q)
    all_rows = cursor.fetchall()

    matches = []
    prev_ticker = None
    prev_close = None

    for ticker, d, close_p in all_rows:
        if ticker != prev_ticker:
            prev_ticker = ticker
            prev_close = close_p
            continue

        if close_p is None or prev_close is None:
            prev_close = close_p
            continue

        # Apply date filters on result
        if lookback_date and d < lookback_date:
            prev_close = close_p
            continue
        if end_date and d > end_date:
            prev_close = close_p
            continue

        if value == "lower" and close_p < prev_close:
            matches.append((ticker, d, close_p))
        elif value == "higher" and close_p > prev_close:
            matches.append((ticker, d, close_p))

        prev_close = close_p

    return matches


def _query_price_streak_matches(cursor, cond, div_adj, tf, value,
                                universe_tickers, lookback_date, end_date):
    """Query for price run matches using fixed-window lookback.

    Checks if close[bar] > open[bar - N] for any N in [min_count, max_count].
    'green' = ascending (overall gain over N-bar window).
    'red'   = descending (overall loss over N-bar window).
    """
    min_count = int(cond.get("min_count", 1))
    max_count = int(cond.get("max_count", 99))

    # Fetch all bars (ticker, date, close_price, open_price) for the universe/TF
    params_q = [div_adj, tf]
    where_q = ["div_adj = ?", "timeframe = ?"]

    if universe_tickers:
        placeholders = ",".join(["?"] * len(universe_tickers))
        where_q.append(f"ticker IN ({placeholders})")
        params_q.extend(universe_tickers)

    # Extend lookback to get enough prior bars for the comparison window
    # (we need max_count bars before the earliest result date)
    # Don't filter by lookback_date here — we'll filter results after
    sql = f"""SELECT ticker, date, close_price, open_price
              FROM backtest_indicators
              WHERE {' AND '.join(where_q)}
              ORDER BY ticker, date"""
    cursor.execute(sql, params_q)
    all_rows = cursor.fetchall()

    # Group by ticker, then check window condition
    matches = []
    prev_ticker = None
    ticker_bars = []

    def _process_ticker(bars):
        for i in range(len(bars)):
            _, d, close_p, _ = bars[i]
            if close_p is None:
                continue
            # Apply date range filters on the result
            if lookback_date and d < lookback_date:
                continue
            if end_date and d > end_date:
                continue
            # Check if any N in [min_count, max_count] satisfies the condition
            for n in range(min_count, min(max_count + 1, i + 2)):
                lookback_idx = i - n
                if lookback_idx < 0:
                    break
                lookback_open = bars[lookback_idx][3]  # open_price of bar N bars back
                if lookback_open is None:
                    continue
                if value == "green" and close_p > lookback_open:
                    matches.append((bars[i][0], d, close_p))
                    break
                elif value == "red" and close_p < lookback_open:
                    matches.append((bars[i][0], d, close_p))
                    break

    for row in all_rows:
        if row[0] != prev_ticker:
            if ticker_bars:
                _process_ticker(ticker_bars)
            ticker_bars = []
            prev_ticker = row[0]
        ticker_bars.append(row)
    if ticker_bars:
        _process_ticker(ticker_bars)

    return matches


def _query_condition_matches(cursor, cond, div_adj, universe_tickers, lookback_date, end_date=None):
    """Query backtest_indicators for a single condition. Returns list of (ticker, date, close_price)."""
    indicator = cond.get("indicator", "")
    tf = cond.get("timeframe", "1D")
    value = cond.get("value", "")

    if not indicator or not value:
        return []

    # Sweep events: query clusterbomb_events table (not backtest_indicators)
    if indicator == "clusterbomb":
        return _query_clusterbomb_matches(cursor, cond, universe_tickers, lookback_date, end_date)
    if indicator == "monster":
        return _query_monster_matches(cursor, cond, universe_tickers, lookback_date, end_date)

    # Meta indicators (no timeframe, stored on 1D rows)
    meta_indicators = {"above_wma30", "wma30_cross", "hvc_triggered"}
    is_meta = indicator in meta_indicators

    if is_meta:
        tf = "1D"

    # Build query
    params = [div_adj, tf]
    where_parts = ["div_adj = ?", "timeframe = ?"]

    # Handle special cases
    if indicator == "above_wma30":
        col_value = 1 if value in ("true", "1", True) else 0
        where_parts.append("above_wma30 = ?")
        params.append(col_value)
    elif indicator == "hvc_triggered":
        where_parts.append("hvc_triggered = 1")
        # Filter by candle direction
        if value in ("bull", "bear"):
            where_parts.append("hvc_candle_dir = ?")
            params.append(value)
        # Filter by gap type
        elif value == "gap_up":
            where_parts.append("hvc_gap_type = 'up'")
        elif value == "gap_up_defended":
            where_parts.append("hvc_gap_type = 'up'")
            where_parts.append("hvc_gap_closed = 0")
        elif value == "gap_down":
            where_parts.append("hvc_gap_type = 'down'")
        elif value == "gap_down_closed":
            where_parts.append("hvc_gap_type = 'down'")
            where_parts.append("hvc_gap_closed = 1")
    elif indicator == "band_flip":
        where_parts.append("band_flip = 1")
    elif indicator == "mom_rising":
        col_value = 1 if value in ("true", "1", True) else 0
        where_parts.append("mom_rising = ?")
        params.append(col_value)
    elif indicator == "price_above":
        # Skip - this is a runtime comparison, not pre-computed
        return []
    elif indicator == "rs_rating":
        # Skip - RS data in different table
        return []
    elif indicator == "mom_trend_duration":
        # Consecutive momentum direction: "falling" or "rising"
        min_count = int(cond.get("min_count", 2))
        max_count = int(cond.get("max_count", 99))
        if value == "falling":
            where_parts.append("consec_mom_falling >= ?")
            params.append(min_count)
            where_parts.append("consec_mom_falling <= ?")
            params.append(max_count)
        else:  # rising
            where_parts.append("consec_mom_rising >= ?")
            params.append(min_count)
            where_parts.append("consec_mom_rising <= ?")
            params.append(max_count)
    elif indicator == "price_streak":
        # Fixed-window lookback: "over the last N bars, did price gain/lose overall?"
        # Uses open_price for cross-bar comparison — NOT a running counter.
        return _query_price_streak_matches(
            cursor, cond, div_adj, tf, value,
            universe_tickers, lookback_date, end_date
        )
    elif indicator == "pivot_reclaim_failed":
        # Wick rejection at momentum pivot: "bearish" or "bullish"
        if value == "bearish":
            where_parts.append("pivot_wick_above = 1")
        else:  # bullish
            where_parts.append("pivot_wick_below = 1")
    elif indicator == "close_vs_prev":
        # Cross-bar comparison: close vs previous bar's close
        return _query_close_vs_prev_matches(
            cursor, div_adj, tf, value,
            universe_tickers, lookback_date, end_date
        )
    else:
        # Standard column match
        col = indicator  # mom_color, sqz_state, band_pos, acc_state, acc_impulse, dev_signal, wma30_cross
        multi = _expand_multi_value(indicator, value)
        if multi:
            placeholders_v = ",".join(["?"] * len(multi))
            where_parts.append(f"{col} IN ({placeholders_v})")
            params.extend(multi)
        else:
            where_parts.append(f"{col} = ?")
            params.append(value)

    if universe_tickers:
        placeholders = ",".join(["?"] * len(universe_tickers))
        where_parts.append(f"ticker IN ({placeholders})")
        params.extend(universe_tickers)

    if lookback_date:
        where_parts.append("date >= ?")
        params.append(lookback_date)

    if end_date:
        where_parts.append("date <= ?")
        params.append(end_date)

    sql = f"SELECT ticker, date, close_price FROM backtest_indicators WHERE {' AND '.join(where_parts)} ORDER BY date"
    cursor.execute(sql, params)
    return cursor.fetchall()


def _load_daily_prices(ticker):
    """Load full daily prices for forward returns.
    Uses module-level _price_cache with file mtime validation.
    Returns list of (date_str, close, open) tuples."""
    global _price_cache

    safe = ticker.replace("/", "_").replace(":", "_")
    path = os.path.join(CACHE_DIR, f"{safe}_day.csv")

    # Check file mtime for staleness (single stat call, no disk I/O)
    try:
        current_mtime = os.path.getmtime(path)
    except OSError:
        return []

    cached = _price_cache.get(ticker)
    if cached and cached["mtime"] == current_mtime:
        return cached["data"]

    # Cache miss — load from disk
    df = _load_daily_csv(ticker)
    if df is None:
        return []
    dates = pd.to_datetime(df["timestamp"]).dt.strftime("%Y-%m-%d").tolist()
    closes = df["close"].tolist()
    opens = df["open"].tolist() if "open" in df.columns else closes
    result = list(zip(dates, closes, opens))
    _price_cache[ticker] = {"mtime": current_mtime, "data": result}
    return result


def _check_maintain_indicator(conn, ticker, review_date, indicator, timeframe, value, div_adj):
    """Check if an indicator matches a value at a specific date for maintain review.

    For higher-TF indicators (1W, 1M, etc.) with a daily review_date, we find
    the most recent TF bar whose date is <= the review_date (i.e. the bar that
    covers the review_date).

    Returns True if the condition passes (stay in trade), False otherwise."""
    if not indicator or not value:
        return False

    c = conn.cursor()
    meta_indicators = {"above_wma30", "wma30_cross", "hvc_triggered"}
    tf = "1D" if indicator in meta_indicators else timeframe

    # For higher-TF indicators, find the bar that covers the review_date.
    # The bar's date is the period start; the review_date must fall within it.
    check_date = review_date
    if tf != "1D":
        c.execute(
            "SELECT date FROM backtest_indicators "
            "WHERE ticker=? AND div_adj=? AND timeframe=? AND date<=? "
            "ORDER BY date DESC LIMIT 1",
            (ticker, div_adj, tf, review_date)
        )
        row = c.fetchone()
        if row:
            check_date = row[0]
        else:
            return False  # no covering bar found

    if indicator == "above_wma30":
        col_val = 1 if value in ("true", "1", True) else 0
        c.execute(
            "SELECT 1 FROM backtest_indicators WHERE ticker=? AND date=? AND timeframe=? AND div_adj=? AND above_wma30=?",
            (ticker, check_date, tf, div_adj, col_val)
        )
    elif indicator == "hvc_triggered":
        if value in ("bull", "bear"):
            c.execute(
                "SELECT 1 FROM backtest_indicators WHERE ticker=? AND date=? AND timeframe=? AND div_adj=? AND hvc_triggered=1 AND hvc_candle_dir=?",
                (ticker, check_date, tf, div_adj, value)
            )
        elif value == "gap_up":
            c.execute(
                "SELECT 1 FROM backtest_indicators WHERE ticker=? AND date=? AND timeframe=? AND div_adj=? AND hvc_triggered=1 AND hvc_gap_type='up'",
                (ticker, check_date, tf, div_adj)
            )
        elif value == "gap_up_defended":
            c.execute(
                "SELECT 1 FROM backtest_indicators WHERE ticker=? AND date=? AND timeframe=? AND div_adj=? AND hvc_triggered=1 AND hvc_gap_type='up' AND hvc_gap_closed=0",
                (ticker, check_date, tf, div_adj)
            )
        elif value == "gap_down":
            c.execute(
                "SELECT 1 FROM backtest_indicators WHERE ticker=? AND date=? AND timeframe=? AND div_adj=? AND hvc_triggered=1 AND hvc_gap_type='down'",
                (ticker, check_date, tf, div_adj)
            )
        elif value == "gap_down_closed":
            c.execute(
                "SELECT 1 FROM backtest_indicators WHERE ticker=? AND date=? AND timeframe=? AND div_adj=? AND hvc_triggered=1 AND hvc_gap_type='down' AND hvc_gap_closed=1",
                (ticker, check_date, tf, div_adj)
            )
        else:
            c.execute(
                "SELECT 1 FROM backtest_indicators WHERE ticker=? AND date=? AND timeframe=? AND div_adj=? AND hvc_triggered=1",
                (ticker, check_date, tf, div_adj)
            )
    elif indicator == "band_flip":
        c.execute(
            "SELECT 1 FROM backtest_indicators WHERE ticker=? AND date=? AND timeframe=? AND div_adj=? AND band_flip=1",
            (ticker, check_date, tf, div_adj)
        )
    else:
        # Handle momentum color progression values (yellow+, blue+)
        multi = _expand_multi_value(indicator, value)
        if multi:
            placeholders = ",".join(["?"] * len(multi))
            c.execute(
                f"SELECT 1 FROM backtest_indicators WHERE ticker=? AND date=? AND timeframe=? AND div_adj=? AND {indicator} IN ({placeholders})",
                (ticker, check_date, tf, div_adj, *multi)
            )
        else:
            c.execute(
                f"SELECT 1 FROM backtest_indicators WHERE ticker=? AND date=? AND timeframe=? AND div_adj=? AND {indicator}=?",
                (ticker, check_date, tf, div_adj, value)
            )

    return c.fetchone() is not None


def _expand_multi_value(indicator, value):
    """Expand combined values into a list of matching DB values.
    Returns a list if expansion applies, or None for exact match."""
    if indicator == "mom_color":
        if value in ("descending", "blue+"):
            return ["blue", "red"]       # momentum falling (positive or negative)
        elif value in ("ascending_mom", "yellow+"):
            return ["yellow", "aqua"]    # momentum rising (negative or positive)
    return None


def _compute_forward_returns(entry_date, entry_price, daily_prices, exit_rules, stop_loss_pct=None, maintain=None, direction="long", entry_mode="next_open"):
    """
    Given an entry point and exit rules, compute the trade outcome.

    entry_mode: "next_open" (default) — enter at next trading day's open after signal.
                "signal_close" — enter at signal bar's close (e.g. Friday close for weekly).

    daily_prices: list of (date_str, close, open) tuples.

    exit_rules: list of rule dicts. First rule that triggers wins (OR logic).
      {type: 'signal', indicator, timeframe, value}  — needs indicator lookup
      {type: 'max_bars', timeframe, bars}             — fixed holding period
      {type: 'trailing_stop', pct}                    — trail from peak/trough
      {type: 'take_profit', pct}                      — exit at % gain from entry
      {type: 'none'}                                  — hold to present

    stop_loss_pct: hard stop from entry (e.g. 15 = exit if price moves 15% against).
                   Overrides all other exit rules.
    direction: "long" or "short". Controls P&L, stop, TP, and trailing math.

    Returns dict with trade details.
    """
    # Find signal index in daily prices
    signal_idx = None
    for i in range(len(daily_prices)):
        if daily_prices[i][0] == entry_date:
            signal_idx = i
            break

    if signal_idx is None:
        return None

    if entry_mode == "signal_close":
        # Enter at signal bar's close price (same day)
        entry_idx = signal_idx
        actual_entry_date = daily_prices[signal_idx][0]
        entry_price = daily_prices[signal_idx][1]  # close price of signal day
    else:
        # Default: enter at next trading day's open (signal seen at close, execute next morning)
        entry_idx = signal_idx + 1
        if entry_idx >= len(daily_prices):
            return None
        actual_entry_date = daily_prices[entry_idx][0]
        entry_price = daily_prices[entry_idx][2]  # open price of next day

    # Default: hold to end (no exit)
    exit_date = None
    exit_price = daily_prices[-1][1] if daily_prices else entry_price
    exit_reason = "Open"
    holding_days = 0
    max_drawdown_pct = 0.0
    peak_price = entry_price
    prices_after_entry = []

    # Collect prices from entry day onwards (use closes for daily tracking)
    for i in range(entry_idx, len(daily_prices)):
        d, c, o = daily_prices[i]
        prices_after_entry.append((d, c))

    if len(prices_after_entry) < 2:
        return None

    # ── Prepare exit mechanisms ──────────────────────────────────
    is_short = direction == "short"

    # Hard stop loss (above entry for shorts, below for longs)
    if stop_loss_pct and stop_loss_pct > 0:
        stop_price = entry_price * (1 + stop_loss_pct / 100.0) if is_short else entry_price * (1 - stop_loss_pct / 100.0)
    else:
        stop_price = None

    # Trailing stop — track favorable extreme (trough for shorts, peak for longs)
    trail_rule = next((r for r in exit_rules if r.get("type") == "trailing_stop"), None)
    trail_pct = float(trail_rule["pct"]) / 100.0 if trail_rule else None
    local_extreme = entry_price  # peak for longs, trough for shorts

    # Take profit (below entry for shorts, above for longs)
    tp_rule = next((r for r in exit_rules if r.get("type") == "take_profit"), None)
    if tp_rule:
        tp_price = entry_price * (1 - float(tp_rule["pct"]) / 100.0) if is_short else entry_price * (1 + float(tp_rule["pct"]) / 100.0)
    else:
        tp_price = None

    # Max hold (convert to trading days)
    max_hold_days = None
    max_hold_label = ""
    for rule in exit_rules:
        if rule.get("type") == "max_bars":
            bars = int(rule.get("bars", 20))
            tf = rule.get("timeframe", "1D")
            if tf in ("1W",):
                max_hold_days = bars * 5
            elif tf in ("1M",):
                max_hold_days = bars * 21
            else:
                multiplier = {"1D": 1, "2D": 2, "3D": 3, "5D": 5}.get(tf, 1)
                max_hold_days = bars * multiplier
            max_hold_label = f"Max Hold ({bars}{tf})"
            break

    # Position review (maintain) — two independent review timings:
    #   1) Price review at N trading days (above_entry, min_gain)
    #   2) Indicator review(s) at M trading days (pre-computed from TF bar count)
    #      Multiple indicators use AND logic — ALL must pass, ANY fail → exit.
    maintain_price_day = 0
    maintain_conds_simple = []
    maintain_ind_results = []     # list of {passed: bool, review_day: int}
    if maintain:
        maintain_price_day = maintain.get("after_days", 0) or 0
        for cond in maintain.get("conditions", []):
            if cond.get("type") in ("above_entry", "min_gain"):
                maintain_conds_simple.append(cond)
        maintain_ind_results = maintain.get("_indicator_results", [])
        # Backward compat: single indicator fields
        if not maintain_ind_results:
            _single_passed = maintain.get("_indicator_passed")
            _single_day = maintain.get("_indicator_review_day", 0) or 0
            if _single_passed is not None:
                if _single_day == 0:
                    _single_day = maintain_price_day
                maintain_ind_results = [{"passed": _single_passed, "review_day": _single_day}]

    # ── Day-by-day forward walk ───────────────────────────────────
    for j in range(1, len(prices_after_entry)):
        d, p = prices_after_entry[j]

        # Update trailing extreme (trough for shorts, peak for longs)
        if is_short:
            if p < local_extreme:
                local_extreme = p
        else:
            if p > local_extreme:
                local_extreme = p

        # 1. Hard stop loss (absolute priority)
        # Use stop price as exit (simulates stop-loss order fill), not close
        if stop_price is not None and (p >= stop_price if is_short else p <= stop_price):
            exit_date = d
            exit_price = stop_price
            exit_reason = f"Stop -{stop_loss_pct}%"
            break

        # 2a. Price-based position review at day N
        if maintain_price_day > 0 and j == maintain_price_day and maintain_conds_simple:
            any_passed = False
            for cond in maintain_conds_simple:
                ctype = cond.get("type", "")
                if ctype == "above_entry":
                    # For shorts: price must be below entry (in profit)
                    if (p < entry_price if is_short else p > entry_price):
                        any_passed = True
                        break
                elif ctype == "min_gain":
                    gain_pct = ((entry_price - p) / entry_price) * 100 if is_short else ((p - entry_price) / entry_price) * 100
                    if gain_pct >= cond.get("pct", 10):
                        any_passed = True
                        break
            if not any_passed:
                exit_date = d
                exit_price = p
                exit_reason = f"Review ({maintain_price_day}d)"
                break

        # 2b. Indicator-based position review(s) — AND logic
        #     Each indicator has its own review day. If ANY fails → exit.
        for _ir in maintain_ind_results:
            _ir_day = _ir.get("review_day", 0)
            if _ir_day > 0 and j == _ir_day and _ir.get("passed") is not None:
                if not _ir["passed"]:
                    exit_date = d
                    exit_price = p
                    exit_reason = f"Review (ind {_ir_day}d)"
                    break
        if exit_date:
            break

        # 3. Trailing stop
        if trail_pct is not None:
            if is_short:
                drawdown = (p - local_extreme) / local_extreme if local_extreme > 0 else 0
            else:
                drawdown = (local_extreme - p) / local_extreme if local_extreme > 0 else 0
            if drawdown >= trail_pct:
                exit_date = d
                exit_price = p
                exit_reason = f"Trail -{trail_rule['pct']}%"
                break

        # 4. Take profit
        if tp_price is not None and (p <= tp_price if is_short else p >= tp_price):
            exit_date = d
            exit_price = tp_price  # simulates limit order fill at target
            exit_reason = f"TP +{tp_rule['pct']}%"
            break

        # 5. Max hold
        if max_hold_days is not None and j >= max_hold_days:
            exit_date = d
            exit_price = p
            exit_reason = max_hold_label
            break

    # If no exit triggered, hold to present
    if not exit_date:
        exit_price = prices_after_entry[-1][1]
        exit_reason = "Open"

    # Signal-based exits are handled later by _handle_signal_exits()

    # Calculate metrics
    if exit_date:
        entry_dt = datetime.strptime(actual_entry_date, "%Y-%m-%d")
        exit_dt = datetime.strptime(exit_date, "%Y-%m-%d")
        holding_days = (exit_dt - entry_dt).days
    else:
        entry_dt = datetime.strptime(actual_entry_date, "%Y-%m-%d")
        last_dt = datetime.strptime(prices_after_entry[-1][0], "%Y-%m-%d")
        holding_days = (last_dt - entry_dt).days
        exit_price = prices_after_entry[-1][1]

    if is_short:
        return_pct = ((entry_price - exit_price) / entry_price) * 100 if entry_price else 0
    else:
        return_pct = ((exit_price - entry_price) / entry_price) * 100 if entry_price else 0

    # Max drawdown from entry to exit (worst adverse move against position)
    worst_dd = 0.0
    end_idx = len(prices_after_entry)
    if exit_date:
        for j, (d, p) in enumerate(prices_after_entry):
            if d == exit_date:
                end_idx = j + 1
                break

    if is_short:
        # For shorts: track lowest price (best point), dd = rise from trough
        trough = entry_price
        for j in range(end_idx):
            d, p = prices_after_entry[j]
            if p < trough:
                trough = p
            dd = ((p - trough) / trough) * 100 if trough else 0
            if dd > worst_dd:
                worst_dd = dd
    else:
        # For longs: track highest price (best point), dd = fall from peak
        peak = entry_price
        for j in range(end_idx):
            d, p = prices_after_entry[j]
            if p > peak:
                peak = p
            dd = ((peak - p) / peak) * 100 if peak else 0
            if dd > worst_dd:
                worst_dd = dd

    # Build price series for chart (context: 40 bars before, trade, 20 after)
    chart_start = max(0, entry_idx - 40)
    chart_end_idx = end_idx + entry_idx if exit_date else len(daily_prices)
    chart_end = min(len(daily_prices), chart_end_idx + 20)
    price_series = [daily_prices[k][1] for k in range(chart_start, chart_end)]
    date_series = [daily_prices[k][0] for k in range(chart_start, chart_end)]
    chart_entry_idx = entry_idx - chart_start
    chart_exit_idx = (end_idx - 1) + (entry_idx - chart_start) if exit_date else len(price_series) - 1

    return {
        "entry_date": actual_entry_date,
        "entry_price": round(entry_price, 2),
        "exit_date": exit_date,
        "exit_price": round(exit_price, 2),
        "return_pct": round(return_pct, 1),
        "holding_days": holding_days,
        "max_drawdown_pct": round(-worst_dd, 1),
        "exit_reason": exit_reason,
        "price_series": price_series,
        "date_series": date_series,
        "_entryIdx": chart_entry_idx,
        "_exitIdx": chart_exit_idx,
    }


def _handle_signal_exits(conn, trades, exit_rules, div_adj, stop_loss_pct=None, maintain=None, direction="long", price_cache=None, entry_mode="next_open"):
    """
    For signal-based exits, scan the backtest_indicators table
    from entry date forward to find the first matching exit signal.
    Also enforces hard stop loss & position review (maintain) which take
    priority over signal exits — whichever triggers first chronologically wins.
    Modifies trades in-place.
    """
    signal_rules = [r for r in exit_rules if r.get("type") == "signal"]
    if not signal_rules:
        return

    is_short = direction == "short"
    c = conn.cursor()

    # Extract max_bars (hold) rule — deferred from Phase 1 so it competes with signals
    max_hold_days = None
    max_hold_label = None
    for rule in exit_rules:
        if rule.get("type") == "max_bars":
            bars = int(rule.get("bars", 20))
            tf = rule.get("timeframe", "1D")
            multiplier = {"1W": 5, "2W": 10, "1M": 21, "3M": 63, "6M": 126, "12M": 252}.get(tf, 1)
            max_hold_days = bars * multiplier
            max_hold_label = f"Hold {bars}{tf}"
            break

    # Pre-compute stop price helper (above entry for shorts, below for longs)
    if stop_loss_pct and stop_loss_pct > 0:
        if is_short:
            stop_price_fn = lambda ep: ep * (1 + stop_loss_pct / 100.0)
        else:
            stop_price_fn = lambda ep: ep * (1 - stop_loss_pct / 100.0)
    else:
        stop_price_fn = None

    # Pre-compute take profit helper (below entry for shorts, above for longs)
    tp_rule = next((r for r in exit_rules if r.get("type") == "take_profit"), None)
    tp_pct = float(tp_rule["pct"]) if tp_rule else None
    if tp_pct:
        if is_short:
            tp_price_fn = lambda ep: ep * (1 - tp_pct / 100.0)
        else:
            tp_price_fn = lambda ep: ep * (1 + tp_pct / 100.0)
    else:
        tp_price_fn = None

    # Pre-compute maintain settings
    maintain_price_day = 0
    maintain_conds_simple = []
    maintain_ind_conds = []  # list of indicator condition dicts (AND logic)
    if maintain:
        maintain_price_day = maintain.get("after_days", 0) or 0
        for cond in maintain.get("conditions", []):
            if cond.get("type") in ("above_entry", "min_gain"):
                maintain_conds_simple.append(cond)
            elif cond.get("type") == "indicator":
                maintain_ind_conds.append(cond)

    for trade in trades:
        if trade.get("exit_reason") != "Open":
            continue  # Already has an exit from a non-signal rule

        ticker = trade["ticker"]
        entry_date = trade["entry_date"]
        entry_price = trade["entry_price"]

        # First find the signal exit date from DB
        signal_exit_date = None
        signal_exit_tf = "1D"
        signal_exit_indicator = ""
        signal_exit_label = ""
        for rule in signal_rules:
            indicator = rule.get("indicator", "")
            tf = rule.get("timeframe", "1D")
            value = rule.get("value", "")

            if not indicator or not value:
                continue

            # Meta indicators use 1D rows
            meta_indicators = {"above_wma30", "wma30_cross", "hvc_triggered"}
            if indicator in meta_indicators:
                tf = "1D"

            # Build query for exit signal after entry date
            params = [ticker, div_adj, tf, entry_date]
            if indicator == "above_wma30":
                col_val = 1 if value in ("true", "1", True) else 0
                where = "ticker=? AND div_adj=? AND timeframe=? AND date>? AND above_wma30=?"
                params.append(col_val)
            elif indicator == "hvc_triggered":
                if value in ("bull", "bear"):
                    where = "ticker=? AND div_adj=? AND timeframe=? AND date>? AND hvc_triggered=1 AND hvc_candle_dir=?"
                    params.append(value)
                elif value == "gap_up":
                    where = "ticker=? AND div_adj=? AND timeframe=? AND date>? AND hvc_triggered=1 AND hvc_gap_type='up'"
                elif value == "gap_up_defended":
                    where = "ticker=? AND div_adj=? AND timeframe=? AND date>? AND hvc_triggered=1 AND hvc_gap_type='up' AND hvc_gap_closed=0"
                elif value == "gap_down":
                    where = "ticker=? AND div_adj=? AND timeframe=? AND date>? AND hvc_triggered=1 AND hvc_gap_type='down'"
                elif value == "gap_down_closed":
                    where = "ticker=? AND div_adj=? AND timeframe=? AND date>? AND hvc_triggered=1 AND hvc_gap_type='down' AND hvc_gap_closed=1"
                else:
                    where = "ticker=? AND div_adj=? AND timeframe=? AND date>? AND hvc_triggered=1"
            elif indicator == "band_flip":
                where = "ticker=? AND div_adj=? AND timeframe=? AND date>? AND band_flip=1"
            else:
                multi = _expand_multi_value(indicator, value)
                if multi:
                    placeholders_v = ",".join(["?"] * len(multi))
                    where = f"ticker=? AND div_adj=? AND timeframe=? AND date>? AND {indicator} IN ({placeholders_v})"
                    params.extend(multi)
                else:
                    where = f"ticker=? AND div_adj=? AND timeframe=? AND date>? AND {indicator}=?"
                    params.append(value)

            c.execute(
                f"SELECT date, close_price FROM backtest_indicators WHERE {where} ORDER BY date ASC LIMIT 1",
                params
            )
            row = c.fetchone()
            if row:
                signal_exit_date = row[0]
                signal_exit_tf = tf
                signal_exit_label = f"{indicator} {value} ({tf})"
                break  # First matching exit rule wins

        # For higher-timeframe signal exits, the DB stores the bar's start date
        # (e.g. Monday for weekly).  Resolve to the last daily bar of that period
        # so the day-by-day walk can shift to next-day open (realistic execution).
        signal_end_date = None  # last daily date when signal is confirmed at close
        if signal_exit_date:
            signal_end_date = _resolve_bar_end_date(
                c, ticker, div_adj, signal_exit_tf, signal_exit_date
            )

        # Now walk daily prices day-by-day checking stop loss, maintain, and signal
        # Whichever triggers first wins.
        if price_cache and ticker in price_cache:
            daily_prices = price_cache[ticker]
        else:
            daily_prices = _load_daily_prices(ticker)
            if price_cache is not None:
                price_cache[ticker] = daily_prices
        if not daily_prices:
            continue

        # trade["entry_date"] is already the actual entry date, set by
        # _compute_forward_returns (next-day open or signal close).  Do NOT re-shift it.
        entry_idx = None
        for i in range(len(daily_prices)):
            if daily_prices[i][0] == entry_date:
                entry_idx = i
                break
        if entry_idx is None:
            continue

        actual_entry_date = daily_prices[entry_idx][0]
        if entry_mode == "signal_close":
            entry_price = daily_prices[entry_idx][1]  # close price on signal day
        else:
            entry_price = daily_prices[entry_idx][2]  # open price on entry day
        trade["entry_date"] = actual_entry_date
        trade["entry_price"] = round(entry_price, 2)

        # Pre-check maintain indicator conditions (AND logic — all must pass)
        ind_results = []  # [{passed: bool, review_day: int}, ...]
        if maintain_ind_conds and maintain:
            for ic in maintain_ind_conds:
                ind_tf = ic.get("timeframe", "1D")
                ind_after_bars = ic.get("after_bars", 0) or 0
                this_passed = None
                this_review_day = 0

                if ind_after_bars > 0 and ind_tf == "1D":
                    # 1D: after_bars is already in daily bars
                    this_review_day = ind_after_bars
                    if entry_idx + this_review_day < len(daily_prices):
                        review_date = daily_prices[entry_idx + this_review_day][0]
                        this_passed = _check_maintain_indicator(
                            conn, ticker, review_date,
                            ic.get("indicator", ""), ind_tf,
                            ic.get("value", ""), div_adj
                        )
                elif ind_after_bars > 0:
                    # Higher TF: convert TF bar count to daily review day
                    rows = c.execute(
                        "SELECT date FROM backtest_indicators "
                        "WHERE ticker=? AND div_adj=? AND timeframe=? AND date>=? "
                        "ORDER BY date ASC LIMIT ?",
                        (ticker, div_adj, ind_tf, actual_entry_date,
                         ind_after_bars)
                    ).fetchall()
                    if len(rows) >= ind_after_bars:
                        bar_date = rows[ind_after_bars - 1][0]
                        end_date = _resolve_bar_end_date(
                            c, ticker, div_adj, ind_tf, bar_date)
                        if end_date:
                            this_passed = _check_maintain_indicator(
                                conn, ticker, end_date,
                                ic.get("indicator", ""), ind_tf,
                                ic.get("value", ""), div_adj
                            )
                            for k in range(entry_idx, len(daily_prices)):
                                if daily_prices[k][0] > end_date:
                                    this_review_day = k - entry_idx
                                    break
                elif maintain_price_day > 0:
                    # Fallback: no after_bars set, use price review day
                    this_review_day = maintain_price_day
                    review_date = daily_prices[entry_idx + this_review_day][0] if entry_idx + this_review_day < len(daily_prices) else None
                    if review_date:
                        this_passed = _check_maintain_indicator(
                            conn, ticker, review_date,
                            ic.get("indicator", ""), ind_tf,
                            ic.get("value", ""), div_adj
                        )

                if this_passed is not None:
                    ind_results.append({"passed": this_passed, "review_day": this_review_day})

        stop_price = stop_price_fn(entry_price) if stop_price_fn else None
        tp_price = tp_price_fn(entry_price) if tp_price_fn else None
        exit_date = None
        exit_price = None
        exit_reason = None
        local_extreme = entry_price  # peak for longs, trough for shorts
        dd_extreme = entry_price     # for drawdown tracking
        worst_dd = 0.0

        for i in range(entry_idx + 1, len(daily_prices)):
            d, p, o = daily_prices[i]
            bar_num = i - entry_idx  # trading days since entry

            # Track drawdown (adverse move against position)
            if is_short:
                if p < dd_extreme:
                    dd_extreme = p
                dd = ((p - dd_extreme) / dd_extreme) * 100 if dd_extreme else 0
            else:
                if p > dd_extreme:
                    dd_extreme = p
                dd = ((dd_extreme - p) / dd_extreme) * 100 if dd_extreme else 0
            if dd > worst_dd:
                worst_dd = dd

            # Track trailing extreme (trough for shorts, peak for longs)
            if is_short:
                if p < local_extreme:
                    local_extreme = p
            else:
                if p > local_extreme:
                    local_extreme = p

            # 1. Hard stop loss (highest priority)
            # Use stop price as exit (simulates stop-loss order fill), not close
            if stop_price is not None and (p >= stop_price if is_short else p <= stop_price):
                exit_date = d
                exit_price = stop_price
                exit_reason = f"Stop -{stop_loss_pct}%"
                break

            # 2a. Price-based position review at day N
            if maintain_price_day > 0 and bar_num == maintain_price_day and maintain_conds_simple:
                any_passed = False
                for mc in maintain_conds_simple:
                    ctype = mc.get("type", "")
                    if ctype == "above_entry":
                        if (p < entry_price if is_short else p > entry_price):
                            any_passed = True
                            break
                    elif ctype == "min_gain":
                        gain_pct = ((entry_price - p) / entry_price) * 100 if is_short else ((p - entry_price) / entry_price) * 100
                        if gain_pct >= mc.get("pct", 10):
                            any_passed = True
                            break
                if not any_passed:
                    exit_date = d
                    exit_price = p
                    exit_reason = f"Review ({maintain_price_day}d)"
                    break

            # 2b. Indicator-based position review(s) — AND logic
            for _ir in ind_results:
                _ir_day = _ir.get("review_day", 0)
                if _ir_day > 0 and bar_num == _ir_day and _ir.get("passed") is not None:
                    if not _ir["passed"]:
                        exit_date = d
                        exit_price = p
                        exit_reason = f"Review (ind {_ir_day}d)"
                        break
            if exit_date:
                break

            # 3. Take profit
            if tp_price is not None and (p <= tp_price if is_short else p >= tp_price):
                exit_date = d
                exit_price = tp_price  # simulates limit order fill at target
                exit_reason = f"TP +{tp_pct}%"
                break

            # 4. Signal exit (from DB lookup)
            # signal_end_date = last daily bar of the signal's TF period.
            # Signal confirmed at close of that date → exit next day at open.
            if signal_end_date and d > signal_end_date:
                exit_date = d
                exit_price = o  # open price of the first actionable day
                exit_reason = signal_exit_label
                break

            # 5. Max hold (deferred from Phase 1 to compete with signal exits)
            if max_hold_days and bar_num >= max_hold_days:
                exit_date = d
                exit_price = p
                exit_reason = max_hold_label
                break

        # Apply result
        if exit_date and exit_price:
            entry_dt = datetime.strptime(actual_entry_date, "%Y-%m-%d")
            exit_dt = datetime.strptime(exit_date, "%Y-%m-%d")

            trade["exit_date"] = exit_date
            trade["exit_price"] = round(exit_price, 2)
            trade["holding_days"] = (exit_dt - entry_dt).days
            if is_short:
                trade["return_pct"] = round(((entry_price - exit_price) / entry_price) * 100, 1)
            else:
                trade["return_pct"] = round(((exit_price - entry_price) / entry_price) * 100, 1)
            trade["exit_reason"] = exit_reason
            trade["max_drawdown_pct"] = round(-worst_dd, 1)

            # Rebuild price series for chart
            exit_idx_chart = None
            for k in range(len(daily_prices)):
                if daily_prices[k][0] == exit_date:
                    exit_idx_chart = k
                    break
            chart_start = max(0, entry_idx - 40)
            chart_end = min(len(daily_prices), (exit_idx_chart or entry_idx) + 20)
            trade["price_series"] = [daily_prices[k][1] for k in range(chart_start, chart_end)]
            trade["date_series"] = [daily_prices[k][0] for k in range(chart_start, chart_end)]
            trade["_entryIdx"] = entry_idx - chart_start
            trade["_exitIdx"] = (exit_idx_chart or entry_idx) - chart_start
        elif signal_exit_date is None and exit_date is None:
            # No signal exit found and no stop/maintain triggered — stays Open
            # but still recalculate drawdown up to present
            trade["max_drawdown_pct"] = round(-worst_dd, 1)


def run_backtest(payload):
    """
    Main entry point. Takes the frontend payload and returns results.

    payload: {
        entry: [{indicator, timeframe, value, within_bars}, ...],
        exit: [{type, ...}, ...],
        universe: str (watchlist name or "all"),
        sector: str (unused for now),
        div_adj: bool,
        lookback_years: int
    }

    Returns: {
        ok: True,
        trades: [...],
        summary: {...},
        distribution: [...]
    }
    """
    t0 = time.time()
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    init_db(conn)
    c = conn.cursor()

    # ── Timing instrumentation ─────────────────────────────────────
    timings = {}

    # ── Cache check ─────────────────────────────────────────────────
    payload_str = json.dumps(payload, sort_keys=True, default=str)
    payload_hash = hashlib.sha256(payload_str.encode()).hexdigest()[:16]

    # Get current precompute timestamp (used as cache invalidation key)
    c.execute("SELECT value FROM backtest_meta WHERE key='last_precompute'")
    precompute_row = c.fetchone()
    precompute_ts = precompute_row[0] if precompute_row else ""

    # Invalidate in-memory caches early (before universe query uses them)
    _check_cache_validity(precompute_ts)

    c.execute("SELECT result_json, precompute_ts FROM backtest_cache WHERE payload_hash=?",
              (payload_hash,))
    cache_row = c.fetchone()
    if cache_row and cache_row[1] == precompute_ts:
        cached_result = json.loads(cache_row[0])
        cached_result["cached"] = True
        cached_result["elapsed_sec"] = round(time.time() - t0, 3)
        conn.close()
        return cached_result

    timings["setup"] = round(time.time() - t0, 3)

    # ── No cache hit — run backtest ─────────────────────────────────
    # Check if pre-computed data exists (LIMIT 1 avoids full table scan)
    c.execute("SELECT 1 FROM backtest_indicators LIMIT 1")
    if not c.fetchone():
        conn.close()
        return {"ok": False, "error": "No pre-computed data. Run pre-computation first."}

    entry_conditions = payload.get("entry", [])
    exit_rules = payload.get("exit", [])

    # Lazy backfill: if any entry condition uses sweep bias confirmation,
    # ensure forward returns are populated (fast no-op if already done)
    has_sweep_bias = any(
        ec.get("indicator") in ("clusterbomb", "monster") and
        any(tag in (ec.get("value") or "") for tag in ("buy", "sell", "1w_", "1m_"))
        for ec in entry_conditions
    )
    if has_sweep_bias:
        try:
            c.execute("SELECT COUNT(*) FROM clusterbomb_events WHERE return_1w IS NULL")
            null_count = c.fetchone()[0]
            if null_count > 0:
                t_bf = time.time()
                conn.close()
                backfill_clusterbomb_returns()
                conn = sqlite3.connect(DB_PATH, timeout=30)
                c = conn.cursor()
                timings["backfill"] = round(time.time() - t_bf, 3)
                timings["backfill_rows"] = null_count
        except Exception:
            pass

    t_mid = time.time()
    universe = payload.get("universe", "all")
    div_adj = 1 if payload.get("div_adj") else 0
    lookback_years = payload.get("lookback_years", 0)
    stop_loss_pct = payload.get("stop_loss_pct")  # Hard stop from entry (e.g. 15 = -15%)
    dedup_setting = payload.get("dedup", 30)       # 'first', int cooldown days, or 0
    maintain = payload.get("maintain")             # {after_days, conditions: [{type,...}]}
    direction = payload.get("direction", "long")   # "long" or "short"
    entry_mode = payload.get("entry_mode", "next_open")  # "next_open" or "signal_close"
    confirmed_only = payload.get("confirmed_only", True)  # cross-TF look-ahead bias prevention
    date_from = payload.get("date_from")             # "YYYY-MM-DD" or None
    date_to   = payload.get("date_to")               # "YYYY-MM-DD" or None

    # Compute lookback date and end_date
    # Explicit date_from/date_to override lookback_years when provided
    lookback_date = None
    end_date = None
    if date_from or date_to:
        lookback_date = date_from   # explicit start (or None = from beginning)
        end_date = date_to          # explicit end (or None = to present)
    elif lookback_years > 0:
        lookback_date = (datetime.now() - timedelta(days=lookback_years * 365)).strftime("%Y-%m-%d")

    # Get universe tickers
    universe_tickers = _get_tickers_for_universe(universe, conn, precompute_ts)
    timings["universe"] = round(time.time() - t_mid, 3)

    t_phase = time.time()

    # ── Signal matching (with in-memory cache) ─────────────────────
    signal_key = _build_signal_cache_key(
        entry_conditions, universe_tickers, div_adj,
        lookback_date, end_date, dedup_setting, confirmed_only
    )
    cached_signals = _signal_cache.get(signal_key)
    signals_from_cache = False

    if cached_signals and cached_signals["precompute_ts"] == precompute_ts:
        matches = cached_signals["matches"]
        signals_from_cache = True
    else:
        # Full signal matching + dedup + higher-TF resolution
        matches = _match_entry_conditions(conn, entry_conditions, universe_tickers, div_adj, lookback_date, end_date, confirmed_only=confirmed_only)

        if not matches:
            conn.close()
            return {"ok": True, "trades": [], "summary": _empty_summary(), "distribution": []}

        # De-duplicate signals based on user preference
        if dedup_setting == "first":
            matches = _deduplicate_signals(matches, cooldown_days=99999)
        elif isinstance(dedup_setting, (int, float)) and dedup_setting > 0:
            matches = _deduplicate_signals(matches, cooldown_days=int(dedup_setting))

        # Resolve higher-TF entry dates to actionable daily dates
        # Use `or "1D"` to handle empty string from noTF indicators (clusterbomb, rs_rating, etc.)
        primary_tf = (entry_conditions[0].get("timeframe") or "1D") if entry_conditions else "1D"
        meta_indicators = {"above_wma30", "wma30_cross", "hvc_triggered"}
        if entry_conditions and entry_conditions[0].get("indicator") in meta_indicators:
            primary_tf = "1D"

        if primary_tf != "1D":
            resolved = []
            for ticker, bar_date, price in matches:
                resolved_end = _resolve_bar_end_date(c, ticker, div_adj, primary_tf, bar_date)
                if resolved_end:
                    resolved.append((ticker, resolved_end, price))
            matches = resolved

        # Store in signal cache
        _signal_cache[signal_key] = {
            "precompute_ts": precompute_ts,
            "matches": matches,
        }

    timings["signals"] = round(time.time() - t_phase, 3)
    timings["signal_count"] = len(matches)
    timings["signals_cached"] = signals_from_cache
    t_phase = time.time()

    if not matches:
        conn.close()
        return {"ok": True, "trades": [], "summary": _empty_summary(), "distribution": [],
                "timings": timings}

    # Compute forward returns for each match
    # Cache daily prices per ticker
    price_cache = {}
    trades = []

    # Separate signal exits from non-signal exits for processing
    non_signal_exits = [r for r in exit_rules if r.get("type") != "signal"]
    has_signal_exits = any(r.get("type") == "signal" for r in exit_rules)

    for ticker, entry_date, entry_price in matches:
        if entry_price is None or entry_price <= 0:
            continue

        if ticker not in price_cache:
            price_cache[ticker] = _load_daily_prices(ticker)

        daily_prices = price_cache[ticker]
        if not daily_prices:
            continue

        # Pre-check maintain indicator conditions (AND logic — all must pass)
        trade_maintain = maintain
        if maintain:
            ind_conds = [cc for cc in maintain.get("conditions", []) if cc.get("type") == "indicator"]
            if ind_conds:
                # Find entry index in daily prices
                entry_idx = None
                for j in range(len(daily_prices)):
                    if daily_prices[j][0] == entry_date:
                        entry_idx = j
                        break

                if entry_idx is not None:
                    actual_entry_date = daily_prices[entry_idx][0]
                    m_price_day = maintain.get("after_days", 0) or 0
                    ind_results = []  # [{passed: bool, review_day: int}, ...]

                    for ic in ind_conds:
                        ind_tf = ic.get("timeframe", "1D")
                        ind_after_bars = ic.get("after_bars", 0) or 0
                        this_passed = None
                        this_review_day = 0

                        if ind_after_bars > 0 and ind_tf == "1D":
                            # 1D: after_bars is already in daily bars
                            this_review_day = ind_after_bars
                            if entry_idx + this_review_day < len(daily_prices):
                                review_date = daily_prices[entry_idx + this_review_day][0]
                                this_passed = _check_maintain_indicator(
                                    conn, ticker, review_date,
                                    ic.get("indicator", ""), ind_tf,
                                    ic.get("value", ""), div_adj
                                )
                        elif ind_after_bars > 0:
                            # Higher TF: convert TF bar count to daily review day
                            rows = c.execute(
                                "SELECT date FROM backtest_indicators "
                                "WHERE ticker=? AND div_adj=? AND timeframe=? AND date>=? "
                                "ORDER BY date ASC LIMIT ?",
                                (ticker, div_adj, ind_tf, actual_entry_date,
                                 ind_after_bars)
                            ).fetchall()
                            if len(rows) >= ind_after_bars:
                                bar_date = rows[ind_after_bars - 1][0]
                                end_date = _resolve_bar_end_date(
                                    c, ticker, div_adj, ind_tf, bar_date)
                                if end_date:
                                    this_passed = _check_maintain_indicator(
                                        conn, ticker, end_date,
                                        ic.get("indicator", ""), ind_tf,
                                        ic.get("value", ""), div_adj
                                    )
                                    for k in range(entry_idx, len(daily_prices)):
                                        if daily_prices[k][0] > end_date:
                                            this_review_day = k - entry_idx
                                            break
                        elif m_price_day > 0:
                            # Fallback: no after_bars set, use price review day
                            this_review_day = m_price_day
                            review_date = daily_prices[entry_idx + this_review_day][0] if entry_idx + this_review_day < len(daily_prices) else None
                            if review_date:
                                this_passed = _check_maintain_indicator(
                                    conn, ticker, review_date,
                                    ic.get("indicator", ""), ind_tf,
                                    ic.get("value", ""), div_adj
                                )

                        if this_passed is not None:
                            ind_results.append({"passed": this_passed, "review_day": this_review_day})

                    # Inject results into a per-trade copy of maintain
                    if ind_results:
                        trade_maintain = dict(maintain)
                        trade_maintain["_indicator_results"] = ind_results

        # Apply non-signal exit rules first.
        # When signal exits exist, defer max_bars to Phase 2 so hold and signal
        # compete in the same day-by-day walk (otherwise hold always wins).
        if has_signal_exits:
            rules_to_apply = [r for r in non_signal_exits if r.get("type") != "max_bars"] or [{"type": "none"}]
        else:
            rules_to_apply = non_signal_exits if non_signal_exits else [{"type": "none"}]
        trade = _compute_forward_returns(
            entry_date, entry_price, daily_prices, rules_to_apply,
            stop_loss_pct=stop_loss_pct, maintain=trade_maintain,
            direction=direction, entry_mode=entry_mode
        )

        if trade is None:
            continue

        trade["ticker"] = ticker
        trades.append(trade)

    timings["returns"] = round(time.time() - t_phase, 3)
    t_phase = time.time()

    # Handle signal-based exits (requires DB lookups)
    # Pass stop_loss and maintain so they are enforced even with signal exits
    if has_signal_exits:
        _handle_signal_exits(conn, trades, exit_rules, div_adj,
                             stop_loss_pct=stop_loss_pct, maintain=maintain,
                             direction=direction, price_cache=price_cache,
                             entry_mode=entry_mode)

    timings["exits"] = round(time.time() - t_phase, 3)

    conn.close()

    # Build summary and distribution (from ALL trades before any truncation)
    t_post = time.time()
    summary = _build_summary(trades)
    distribution = _build_distribution(trades)

    elapsed = time.time() - t0
    timings["post"] = round(time.time() - t_post, 3)

    # Hard limit on returned trades to prevent browser crashes
    MAX_TRADES_RETURNED = 5000
    total_signals = len(trades)
    result_limited = False
    if len(trades) > MAX_TRADES_RETURNED:
        result_limited = True
        # Sort by absolute return to keep most interesting trades
        trades.sort(key=lambda t: abs(t.get("return_pct", 0)), reverse=True)
        trades = trades[:MAX_TRADES_RETURNED]

    timings["total"] = round(elapsed, 3)
    timings["trades"] = len(trades)

    result = {
        "ok": True,
        "trades": trades,
        "summary": summary,
        "distribution": distribution,
        "elapsed_sec": round(elapsed, 2),
        "total_signals": total_signals,
        "result_limited": result_limited,
        "max_trades_returned": MAX_TRADES_RETURNED if result_limited else None,
        "signals_cached": signals_from_cache,
        "timings": timings,
    }

    # ── Write to cache ──────────────────────────────────────────────
    try:
        cache_conn = sqlite3.connect(DB_PATH, timeout=10)
        cache_conn.execute("PRAGMA journal_mode=WAL")
        cache_c = cache_conn.cursor()
        cache_c.execute(CREATE_BT_CACHE)  # ensure table exists
        cache_c.execute(
            "INSERT OR REPLACE INTO backtest_cache (payload_hash, precompute_ts, result_json, created_at) "
            "VALUES (?, ?, ?, ?)",
            (payload_hash, precompute_ts, json.dumps(result), datetime.now().isoformat())
        )
        cache_conn.commit()
        cache_conn.close()
    except Exception:
        pass  # cache write failure is non-fatal

    return result


def _empty_summary():
    return {
        "total_signals": 0, "win_rate": 0, "avg_return_pct": 0,
        "median_return_pct": 0, "avg_holding_days": 0, "profit_factor": 0,
        "expectancy": 0, "payoff_ratio": 0,
        "best": {"ticker": "--", "return_pct": 0},
        "worst": {"ticker": "--", "return_pct": 0},
    }


def _build_summary(trades):
    if not trades:
        return _empty_summary()

    returns = [t["return_pct"] for t in trades]
    wins = [r for r in returns if r > 0]
    losses = [r for r in returns if r <= 0]

    win_rate = len(wins) / len(returns) if returns else 0
    avg_ret = sum(returns) / len(returns) if returns else 0
    median_ret = sorted(returns)[len(returns) // 2] if returns else 0
    avg_hold = sum(t["holding_days"] for t in trades) / len(trades) if trades else 0

    gross_profit = sum(wins) if wins else 0
    gross_loss = abs(sum(losses)) if losses else 0
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else (999.99 if gross_profit > 0 else 0)

    # Expectancy: (win_rate × avg_win) - (loss_rate × avg_loss)
    avg_win = (sum(wins) / len(wins)) if wins else 0
    avg_loss = (abs(sum(losses)) / len(losses)) if losses else 0
    expectancy = (win_rate * avg_win) - ((1 - win_rate) * avg_loss)

    # Payoff ratio: avg_win / avg_loss
    payoff_ratio = avg_win / avg_loss if avg_loss > 0 else (999.99 if avg_win > 0 else 0)

    best = max(trades, key=lambda t: t["return_pct"])
    worst = min(trades, key=lambda t: t["return_pct"])

    return {
        "total_signals": len(trades),
        "win_rate": round(win_rate, 3),
        "avg_return_pct": round(avg_ret, 1),
        "median_return_pct": round(median_ret, 1),
        "avg_holding_days": round(avg_hold),
        "profit_factor": round(profit_factor, 2),
        "expectancy": round(expectancy, 1),
        "payoff_ratio": round(payoff_ratio, 2),
        "best": {"ticker": best["ticker"], "return_pct": round(best["return_pct"], 1)},
        "worst": {"ticker": worst["ticker"], "return_pct": round(worst["return_pct"], 1)},
    }


def _build_distribution(trades):
    """Build return distribution buckets for the histogram."""
    if not trades:
        return []

    buckets = [
        ("-50%+", -9999, -50),
        ("-50%", -50, -25),
        ("-25%", -25, -10),
        ("-10%", -10, -5),
        ("-5%", -5, 0),
        ("0%", 0, 5),
        ("+5%", 5, 10),
        ("+10%", 10, 25),
        ("+25%", 25, 50),
        ("+50%", 50, 100),
        ("+100%+", 100, 9999),
    ]

    dist = []
    for label, lo, hi in buckets:
        count = sum(1 for t in trades if lo <= t["return_pct"] < hi)
        dist.append({"bucket": label, "count": count})

    return dist


# ══════════════════════════════════════════════════════════════════
# CLI ENTRY POINT
# ══════════════════════════════════════════════════════════════════

def _progress(done, total, errors, elapsed):
    pct = done / total * 100 if total else 0
    rate = done / elapsed if elapsed > 0 else 0
    eta = (total - done) / rate if rate > 0 else 0
    print(f"  [{done}/{total}] {pct:.0f}% | {errors} errors | {rate:.0f} tickers/s | ETA {eta:.0f}s", flush=True)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Backtest pre-computation engine")
    parser.add_argument("--force", action="store_true", help="Force recompute all tickers")
    parser.add_argument("--watchlist", type=str, help="Only compute for this watchlist")
    parser.add_argument("--div-adj", type=int, default=0, choices=[0, 1], help="Dividend adjustment (0 or 1)")
    parser.add_argument("--status", action="store_true", help="Show current pre-compute status")
    args = parser.parse_args()

    if args.status:
        conn = sqlite3.connect(DB_PATH)
        init_db(conn)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM backtest_indicators")
        total = c.fetchone()[0]
        c.execute("SELECT COUNT(DISTINCT ticker) FROM backtest_indicators")
        tickers = c.fetchone()[0]
        c.execute("SELECT MIN(date), MAX(date) FROM backtest_indicators")
        dates = c.fetchone()
        c.execute("SELECT value FROM backtest_meta WHERE key='precompute_stats'")
        meta = c.fetchone()
        conn.close()
        print(f"Backtest indicators: {total:,} rows, {tickers} tickers")
        print(f"Date range: {dates[0]} to {dates[1]}")
        if meta:
            print(f"Last run: {json.loads(meta[0])}")
        sys.exit(0)

    print(f"Pre-computing backtest indicators (div_adj={args.div_adj}, force={args.force})")
    if args.watchlist:
        print(f"Watchlist: {args.watchlist}")

    stats = precompute_all(
        div_adj=args.div_adj,
        force=args.force,
        watchlist=args.watchlist,
        progress_callback=_progress
    )

    print(f"\nDone! {stats['done']} tickers in {stats['elapsed_sec']}s")
    print(f"  Inserted: {stats['inserted']:,} rows")
    print(f"  Skipped: {stats['skipped']} (already computed)")
    print(f"  Errors: {stats['errors']}")
