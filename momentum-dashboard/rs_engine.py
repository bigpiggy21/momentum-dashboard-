"""
Couloir Technologies — Relative Strength Rating Engine

Computes IBD-style RS ratings (1-99 percentile) across a universe of stocks.
Reads daily close data from CSV cache, computes weighted quarterly performance
vs SPY benchmark, ranks all tickers, and stores results to SQLite.

Usage:
    python rs_engine.py --universe Russell3000 --workers 8 --once
    python rs_engine.py --universe Debug --workers 1 --once
    python rs_engine.py --universe Russell3000 --backfill --days 60

The RS score formula (IBD-style, from TradingView):
    perfQ1 = close / close[63]    # Last quarter  (40% weight)
    perfQ2 = close / close[126]   # 2 quarters    (20%)
    perfQ3 = close / close[189]   # 3 quarters    (20%)
    perfQ4 = close / close[252]   # 4 quarters    (20%)

    rs_stock = 0.4*perfQ1 + 0.2*perfQ2 + 0.2*perfQ3 + 0.2*perfQ4
    rs_ref   = same formula for SPY

    raw_score = rs_stock / rs_ref * 100

    Percentile rank across the universe → RS Rating 1-99.
"""

import argparse
import json
import os
import sys
import time
import sqlite3
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone
from concurrent.futures import ProcessPoolExecutor, as_completed

# Fix Unicode output on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from config import _load_watchlists, DB_PATH
from data_fetcher import _load_cache, _cache_path
from change_detector import init_db, _get_db, _db_write

# ---------- Constants ----------

BENCHMARK_TICKER = "SPY"
MIN_BARS_FOR_RS = 253       # Need 252 + 1 bars to compute full year quarterly lookbacks
FULL_LOOKBACK = 252          # Full year of trading days
RS_30WMA_PERIOD = 30         # Weeks for RS line moving average
RS_NEW_HIGH_LOOKBACK = 252   # 52-week lookback for new highs


# ============================================================
# DATA LOADING
# ============================================================

def load_daily_data(api_ticker):
    """Load daily OHLC data for a ticker from CSV cache.

    Returns DataFrame with columns [timestamp, close, high, low] or None.
    Only loads the last 300 bars for efficiency.
    """
    df = _load_cache(api_ticker, "day")
    if df is None or df.empty:
        return None
    # Keep only what we need
    cols = [c for c in ["timestamp", "close", "high", "low"] if c in df.columns]
    if "close" not in df.columns or "timestamp" not in df.columns:
        return None
    df = df[cols].copy()
    # Trim to last 550 bars (252 lookback + ~300 buffer for RS 30WMA)
    if len(df) > 550:
        df = df.iloc[-550:].reset_index(drop=True)
    return df


def load_benchmark():
    """Load SPY daily close data. Called once, reused for all tickers."""
    df = _load_cache(BENCHMARK_TICKER, "day")
    if df is None or df.empty:
        # Try the API ticker variant
        df = _load_cache("SPY", "day")
    if df is None or df.empty:
        print(f"ERROR: No cached data for benchmark {BENCHMARK_TICKER}")
        print(f"  Expected file: {_cache_path(BENCHMARK_TICKER, 'day')}")
        print(f"  Run a collection first to populate the cache.")
        return None
    return df[["timestamp", "close"]].copy()


def _load_ticker_data(args):
    """Worker function for parallel data loading.

    Args: (display_ticker, api_ticker)
    Returns: (display_ticker, DataFrame or None)
    """
    display_ticker, api_ticker = args
    df = load_daily_data(api_ticker)
    return (display_ticker, df)


# ============================================================
# RS SCORE COMPUTATION
# ============================================================

def compute_rs_score_latest(daily_close, benchmark_close):
    """Compute the raw RS score for the latest bar.

    Uses the weighted quarterly formula:
        rs_stock = 0.4*(close/close[63]) + 0.2*(close/close[126])
                 + 0.2*(close/close[189]) + 0.2*(close/close[252])
        Same for benchmark.
        raw_score = rs_stock / rs_ref * 100

    Args:
        daily_close: Series of close prices (most recent last), indexed by date.
        benchmark_close: Series of SPY close prices, indexed by date.

    Returns:
        float raw RS score, or None if insufficient data.
    """
    # Align by date (inner join)
    stock = daily_close.copy()
    bench = benchmark_close.copy()

    # Normalize index to date only
    if hasattr(stock.index, 'normalize'):
        stock.index = stock.index.normalize()
    if hasattr(bench.index, 'normalize'):
        bench.index = bench.index.normalize()

    aligned = pd.DataFrame({"stock": stock, "bench": bench}).dropna()
    n = len(aligned)

    if n < MIN_BARS_FOR_RS:
        return None

    s = aligned["stock"].values
    b = aligned["bench"].values

    # IBD quarterly lookbacks — exact periods, no clamping
    # Q1=3mo, Q2=6mo, Q3=9mo, Q4=12mo (trading days)
    perf_s_q1 = s[-1] / s[-1 - 63] if s[-1 - 63] != 0 else 1.0
    perf_s_q2 = s[-1] / s[-1 - 126] if s[-1 - 126] != 0 else 1.0
    perf_s_q3 = s[-1] / s[-1 - 189] if s[-1 - 189] != 0 else 1.0
    perf_s_q4 = s[-1] / s[-1 - 252] if s[-1 - 252] != 0 else 1.0

    perf_b_q1 = b[-1] / b[-1 - 63] if b[-1 - 63] != 0 else 1.0
    perf_b_q2 = b[-1] / b[-1 - 126] if b[-1 - 126] != 0 else 1.0
    perf_b_q3 = b[-1] / b[-1 - 189] if b[-1 - 189] != 0 else 1.0
    perf_b_q4 = b[-1] / b[-1 - 252] if b[-1 - 252] != 0 else 1.0

    rs_stock = 0.4 * perf_s_q1 + 0.2 * perf_s_q2 + 0.2 * perf_s_q3 + 0.2 * perf_s_q4
    rs_ref = 0.4 * perf_b_q1 + 0.2 * perf_b_q2 + 0.2 * perf_b_q3 + 0.2 * perf_b_q4

    if rs_ref == 0:
        return None

    return (rs_stock / rs_ref) * 100


def compute_rs_score_series(daily_close, benchmark_close):
    """Compute RS score timeseries (for RS line / 30WMA / new high detection).

    Returns a pandas Series of raw RS scores indexed by date.
    """
    stock = daily_close.copy()
    bench = benchmark_close.copy()

    if hasattr(stock.index, 'normalize'):
        stock.index = stock.index.normalize()
    if hasattr(bench.index, 'normalize'):
        bench.index = bench.index.normalize()

    aligned = pd.DataFrame({"stock": stock, "bench": bench}).dropna()
    n = len(aligned)

    if n < MIN_BARS_FOR_RS:
        return pd.Series(dtype=float)

    s = aligned["stock"]
    b = aligned["bench"]

    # IBD quarterly lookbacks — exact periods
    rs_stock = (0.4 * (s / s.shift(63)) +
                0.2 * (s / s.shift(126)) +
                0.2 * (s / s.shift(189)) +
                0.2 * (s / s.shift(252)))

    rs_bench = (0.4 * (b / b.shift(63)) +
                0.2 * (b / b.shift(126)) +
                0.2 * (b / b.shift(189)) +
                0.2 * (b / b.shift(252)))

    rs_line = (rs_stock / rs_bench) * 100
    return rs_line.dropna()


# ============================================================
# RANKING
# ============================================================

def rank_universe(ticker_scores):
    """Rank all tickers by raw RS score to produce percentile ratings (1-99).

    Args:
        ticker_scores: dict of {ticker: raw_rs_score} (only valid scores)

    Returns:
        dict of {ticker: {'rs_rating': int, 'rs_rank': int, 'rs_score': float}}
    """
    if not ticker_scores:
        return {}

    # Sort by score descending
    sorted_tickers = sorted(ticker_scores.items(), key=lambda x: x[1], reverse=True)
    total = len(sorted_tickers)

    result = {}
    for rank_idx, (ticker, score) in enumerate(sorted_tickers):
        # Percentile: what fraction of the universe this stock beats
        # rank_idx=0 is the best → percentile close to 99
        if total == 1:
            percentile = 50
        else:
            percentile = round(((total - 1 - rank_idx) / (total - 1)) * 98) + 1

        percentile = max(1, min(99, percentile))

        result[ticker] = {
            "rs_rating": percentile,
            "rs_rank": rank_idx + 1,
            "rs_score": round(score, 4),
        }

    return result


# ============================================================
# BREAKOUT & SIGNAL DETECTION
# ============================================================

def detect_rs_new_high(rs_series, lookback=RS_NEW_HIGH_LOOKBACK):
    """Check if the current RS score is at a 52-week high.

    Returns True if the latest RS value equals the max over lookback period.
    """
    if rs_series is None or len(rs_series) < 2:
        return False
    window = rs_series.iloc[-min(lookback, len(rs_series)):]
    latest = rs_series.iloc[-1]
    return latest >= window.max()


def detect_price_new_high(daily_df, lookback=RS_NEW_HIGH_LOOKBACK):
    """Check if the current price is at a 52-week high (from daily highs)."""
    if daily_df is None or "high" not in daily_df.columns or len(daily_df) < 2:
        return False
    window = daily_df["high"].iloc[-min(lookback, len(daily_df)):]
    latest = daily_df["high"].iloc[-1]
    return latest >= window.max()


def compute_rs_30wma(rs_series):
    """Compute 30-week moving average of the RS score line.

    Resamples daily RS to weekly (last value per week), computes SMA(30).

    Returns:
        dict with 'above_rs_30wma' (bool) and 'rs_30wma_value' (float).
    """
    if rs_series is None or len(rs_series) < 30:
        return {"above_rs_30wma": None, "rs_30wma_value": None}

    # Resample to weekly (Friday close)
    weekly = rs_series.resample("W-FRI").last().dropna()
    if len(weekly) < RS_30WMA_PERIOD:
        return {"above_rs_30wma": None, "rs_30wma_value": None}

    wma = weekly.rolling(RS_30WMA_PERIOD).mean()
    latest_rs = weekly.iloc[-1]
    latest_wma = wma.iloc[-1]

    if pd.isna(latest_wma):
        return {"above_rs_30wma": None, "rs_30wma_value": None}

    return {
        "above_rs_30wma": bool(latest_rs > latest_wma),
        "rs_30wma_value": round(float(latest_wma), 4),
    }


# ============================================================
# MONSTER SCORE
# ============================================================

def compute_monster_score(rdata, persistence_days, hvc_data=None):
    """Compute composite Monster Score (0-100).

    Components:
      RS Velocity   (25 pts): 5d change (up to 10) + 20d change (up to 15)
      RS Level      (20 pts): Tiered by current RS rating
      Persistence   (15 pts): Consecutive days RS >= 80 (from pre-loaded data)
      Price Confirm (15 pts): Above WMA (8) + price new high (7)
      RS New High   (10 pts): RS at 52-week high
      HVC + Gaps    (15 pts): Institutional volume confirmation
    """
    score = 0.0

    # 1. RS Velocity (25 pts max)
    chg5 = rdata.get("rs_change_5d") or 0
    chg20 = rdata.get("rs_change_20d") or 0
    score += min(max(chg5, 0), 10) * 1.0    # 0-10 pts
    score += min(max(chg20, 0), 15) * 1.0    # 0-15 pts

    # 2. RS Level (20 pts max)
    rs = rdata.get("rs_rating") or 0
    if rs >= 99:   score += 20
    elif rs >= 95: score += 18
    elif rs >= 90: score += 15
    elif rs >= 80: score += 10
    elif rs >= 70: score += 5
    elif rs >= 60: score += 2

    # 3. Persistence (15 pts max)
    score += min(persistence_days / 60.0, 1.0) * 15

    # 4. Price Confirmation (15 pts max)
    if rdata.get("above_wma30") or rdata.get("above_rs_30wma"):
        score += 8
    if rdata.get("price_new_high"):
        score += 7

    # 5. RS New High (10 pts max)
    if rdata.get("rs_new_high"):
        score += 10

    # 6. HVC + Gaps (15 pts max) — institutional volume confirmation
    if hvc_data:
        hvc_count = hvc_data.get("hvc_count", 0)
        gap_count = hvc_data.get("gap_count", 0)
        gaps_open = hvc_data.get("gaps_open", 0)
        # HVC count: 1 event = 4pts, 2 = 7pts, 3+ = 9pts
        if hvc_count >= 3:
            score += 9
        elif hvc_count >= 2:
            score += 7
        elif hvc_count >= 1:
            score += 4
        # Gap-up bonus: open gaps = unfilled institutional demand
        if gaps_open >= 2:
            score += 6
        elif gaps_open >= 1:
            score += 4
        elif gap_count >= 1:
            score += 2  # gap existed but filled — still somewhat bullish

    return round(min(score, 100), 1)


def preload_persistence(universe_name, threshold=80):
    """Pre-load rs_history and compute persistence (consecutive days >= threshold)
    for ALL tickers in one query. Returns dict: {ticker: int_days}."""
    conn = _get_db()
    c = conn.cursor()
    c.execute("""
        SELECT ticker, rs_rating FROM rs_history
        WHERE universe = ?
        ORDER BY ticker, trade_date DESC
    """, (universe_name,))
    rows = c.fetchall()
    conn.close()

    # Group by ticker, count consecutive days >= threshold from most recent
    result = {}
    current_ticker = None
    streak = 0
    counting = True
    for ticker, rating in rows:
        if ticker != current_ticker:
            if current_ticker is not None:
                result[current_ticker] = streak
            current_ticker = ticker
            streak = 0
            counting = True
        if counting:
            if rating is not None and rating >= threshold:
                streak += 1
            else:
                counting = False
    if current_ticker is not None:
        result[current_ticker] = streak
    return result


# ============================================================
# DATABASE OPERATIONS
# ============================================================

def get_historical_rating(ticker, universe, days_ago):
    """Look up RS rating from rs_history N trading days ago.

    Returns the rating (int) or None if not found.
    """
    conn = _get_db()
    c = conn.cursor()
    # Look back extra calendar days to account for weekends/holidays
    since = (datetime.now(timezone.utc) - timedelta(days=int(days_ago * 1.6))).strftime("%Y-%m-%d")
    c.execute("""
        SELECT rs_rating FROM rs_history
        WHERE ticker = ? AND universe = ? AND trade_date >= ?
        ORDER BY trade_date ASC LIMIT 1
    """, (ticker, universe, since))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None


def get_meta_for_ticker(ticker):
    """Read price, price_change_pct, above_wma30, wma30_value from meta_snapshots.

    Returns dict or empty dict.
    """
    conn = _get_db()
    c = conn.cursor()
    c.execute("""
        SELECT price, price_change_pct, above_wma30, wma30_value
        FROM meta_snapshots
        WHERE ticker = ?
        ORDER BY timestamp DESC LIMIT 1
    """, (ticker,))
    row = c.fetchone()
    conn.close()
    if not row:
        return {}
    return {
        "price": row[0],
        "price_change_pct": row[1],
        "above_wma30": bool(row[2]) if row[2] is not None else None,
        "wma30_value": row[3],
    }


def save_rankings(rankings_data, universe, computed_at):
    """Bulk save current RS rankings to rs_rankings table."""
    def _write():
        conn = _get_db()
        c = conn.cursor()
        c.execute("DELETE FROM rs_rankings WHERE universe = ?", (universe,))
        for ticker, data in rankings_data.items():
            c.execute("""
                INSERT OR REPLACE INTO rs_rankings
                (ticker, universe, rs_score, rs_rating, rs_rank,
                 rs_change_5d, rs_change_20d, rs_new_high, price_new_high,
                 above_rs_30wma, sector, price, price_change_pct,
                 above_wma30, wma30_value, computed_at,
                 price_return_1m, price_return_3m, price_return_6m, monster_score,
                 hvc_count, gap_count, gaps_open)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                ticker, universe,
                data.get("rs_score"), data.get("rs_rating"), data.get("rs_rank"),
                data.get("rs_change_5d"), data.get("rs_change_20d"),
                int(data.get("rs_new_high", False)),
                int(data.get("price_new_high", False)),
                int(data["above_rs_30wma"]) if data.get("above_rs_30wma") is not None else None,
                data.get("sector", ""),
                data.get("price"), data.get("price_change_pct"),
                int(data["above_wma30"]) if data.get("above_wma30") is not None else None,
                data.get("wma30_value"),
                computed_at,
                data.get("price_return_1m"), data.get("price_return_3m"),
                data.get("price_return_6m"), data.get("monster_score"),
                data.get("hvc_count", 0), data.get("gap_count", 0),
                data.get("gaps_open", 0),
            ))
        conn.commit()
        conn.close()

    _db_write(_write)


def save_history(rankings_data, universe, trade_date):
    """Save today's RS ratings to rs_history."""
    def _write():
        conn = _get_db()
        c = conn.cursor()
        for ticker, data in rankings_data.items():
            c.execute("""
                INSERT OR IGNORE INTO rs_history
                (ticker, universe, trade_date, rs_score, rs_rating, price)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                ticker, universe, trade_date,
                data.get("rs_score"), data.get("rs_rating"), data.get("price"),
            ))
        conn.commit()
        conn.close()

    _db_write(_write)


def save_events(events):
    """Save RS events (breakouts, signals) to rs_events table."""
    if not events:
        return

    def _write():
        conn = _get_db()
        c = conn.cursor()
        for ev in events:
            c.execute("""
                INSERT OR IGNORE INTO rs_events
                (ticker, universe, event_type, event_date, rs_rating, price, details, detected_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                ev["ticker"], ev["universe"], ev["event_type"],
                ev["event_date"], ev.get("rs_rating"), ev.get("price"),
                json.dumps(ev.get("details", {})),
                datetime.now(timezone.utc).isoformat(),
            ))
        conn.commit()
        conn.close()

    _db_write(_write)


# ============================================================
# UNIVERSE HELPERS
# ============================================================

def build_universe(universe_name):
    """Build ticker list with sector info from a watchlist.

    Returns:
        list of (display_ticker, api_ticker, sector_name)
    """
    all_wls = _load_watchlists()
    wl_groups = all_wls.get(universe_name, [])

    if not wl_groups:
        print(f"ERROR: Universe '{universe_name}' not found.")
        print(f"  Available: {list(all_wls.keys())}")
        return []

    tickers = []
    seen = set()
    for group_name, group_tickers in wl_groups:
        for display, api, atype in group_tickers:
            if display not in seen:
                seen.add(display)
                tickers.append((display, api, group_name))

    return tickers


# ============================================================
# MAIN COMPUTATION
# ============================================================

def compute_universe(universe_name, workers=8, target_date=None):
    """Compute RS ratings for an entire universe.

    Steps:
        1. Load universe ticker list
        2. Load SPY benchmark data (once)
        3. Load daily close for all tickers (parallel)
        4. Compute raw RS score for each ticker
        5. Rank all tickers → percentile 1-99
        6. Compute rate-of-change (vs rs_history)
        7. Detect breakouts (RS new high before price)
        8. Compute RS 30WMA status
        9. Save rankings, history, and events
    """
    t0 = time.time()
    print(f"\n{'='*60}")
    print(f"RS RATING ENGINE — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Universe: {universe_name}")
    print(f"{'='*60}")

    # 1. Build universe
    universe = build_universe(universe_name)
    if not universe:
        return
    print(f"  Tickers: {len(universe)}")

    # 2. Load benchmark
    bench_df = load_benchmark()
    if bench_df is None or bench_df.empty:
        print("ERROR: Cannot load benchmark data. Aborting.")
        return

    bench_close = bench_df.set_index("timestamp")["close"]
    print(f"  Benchmark: {BENCHMARK_TICKER} ({len(bench_close)} daily bars)")

    # 3. Load daily data for all tickers (parallel)
    t1 = time.time()
    ticker_data = {}  # {display_ticker: DataFrame}
    sector_map = {}   # {display_ticker: sector_name}

    load_args = [(display, api) for display, api, sector in universe]
    for display, api, sector in universe:
        sector_map[display] = sector

    if workers <= 1:
        for display, api in load_args:
            _, df = _load_ticker_data((display, api))
            if df is not None and len(df) >= MIN_BARS_FOR_RS:
                ticker_data[display] = df
    else:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_load_ticker_data, args): args[0]
                       for args in load_args}
            for future in as_completed(futures):
                display, df = future.result()
                if df is not None and len(df) >= MIN_BARS_FOR_RS:
                    ticker_data[display] = df

    load_time = time.time() - t1
    print(f"  Loaded: {len(ticker_data)}/{len(universe)} tickers "
          f"({len(universe) - len(ticker_data)} insufficient data) [{load_time:.1f}s]")

    # 4. Compute raw RS scores
    t2 = time.time()
    ticker_scores = {}
    ticker_rs_series = {}  # for new high / 30WMA detection

    for display, df in ticker_data.items():
        stock_close = df.set_index("timestamp")["close"]
        score = compute_rs_score_latest(stock_close, bench_close)
        if score is not None and not np.isnan(score):
            ticker_scores[display] = score
            # Also compute the RS series for breakout/30WMA detection
            rs_series = compute_rs_score_series(stock_close, bench_close)
            if len(rs_series) > 0:
                ticker_rs_series[display] = rs_series

    score_time = time.time() - t2
    print(f"  Scored: {len(ticker_scores)} tickers [{score_time:.1f}s]")

    # 5. Rank the universe
    ranked = rank_universe(ticker_scores)
    print(f"  Ranked: {len(ranked)} tickers")

    # 6-8. Enrich with rate-of-change, breakouts, 30WMA, meta data
    t3 = time.time()
    events = []
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    computed_at = datetime.now(timezone.utc).isoformat()

    # Pre-load persistence data for monster score (single DB query)
    persistence_map = preload_persistence(universe_name, threshold=80)

    # Pre-load HVC data for monster score (single DB query)
    try:
        from change_detector import preload_hvc_for_rs
        hvc_map = preload_hvc_for_rs(lookback_days=90)
        print(f"  HVC data: {len(hvc_map)} tickers with events")
    except Exception as _hvc_e:
        print(f"  HVC data: unavailable ({_hvc_e})")
        hvc_map = {}

    n_breakouts = 0
    n_rs_new_high = 0

    for ticker, rdata in ranked.items():
        # Rate of change
        hist_5d = get_historical_rating(ticker, universe_name, 5)
        hist_20d = get_historical_rating(ticker, universe_name, 20)
        rdata["rs_change_5d"] = (rdata["rs_rating"] - hist_5d) if hist_5d is not None else None
        rdata["rs_change_20d"] = (rdata["rs_rating"] - hist_20d) if hist_20d is not None else None

        # Sector
        rdata["sector"] = sector_map.get(ticker, "")

        # Meta data (price, wma30) from existing snapshots
        meta = get_meta_for_ticker(ticker)
        rdata["price"] = meta.get("price")
        rdata["price_change_pct"] = meta.get("price_change_pct")
        rdata["above_wma30"] = meta.get("above_wma30")
        rdata["wma30_value"] = meta.get("wma30_value")

        # If no price from meta, use latest close from our data
        if rdata["price"] is None and ticker in ticker_data:
            rdata["price"] = float(ticker_data[ticker]["close"].iloc[-1])

        # Price returns (1m=21d, 3m=63d, 6m=126d)
        td = ticker_data.get(ticker)
        if td is not None and len(td) > 0:
            closes = td["close"].values
            cur = float(closes[-1])
            for _lbl, _lb in [("price_return_1m", 21), ("price_return_3m", 63), ("price_return_6m", 126)]:
                if len(closes) > _lb and closes[-1 - _lb] != 0:
                    rdata[_lbl] = round(((cur / float(closes[-1 - _lb])) - 1) * 100, 2)
                else:
                    rdata[_lbl] = None
        else:
            rdata["price_return_1m"] = None
            rdata["price_return_3m"] = None
            rdata["price_return_6m"] = None

        # RS new high / price new high
        rs_series = ticker_rs_series.get(ticker)
        rdata["rs_new_high"] = detect_rs_new_high(rs_series)
        rdata["price_new_high"] = detect_price_new_high(ticker_data.get(ticker))

        # RS 30WMA
        wma_data = compute_rs_30wma(rs_series)
        rdata["above_rs_30wma"] = wma_data["above_rs_30wma"]

        # Breakout detection
        if rdata["rs_new_high"]:
            n_rs_new_high += 1
            if not rdata["price_new_high"]:
                # RS new high BEFORE price new high — strongest signal
                n_breakouts += 1
                events.append({
                    "ticker": ticker,
                    "universe": universe_name,
                    "event_type": "rs_breakout",
                    "event_date": today_str,
                    "rs_rating": rdata["rs_rating"],
                    "price": rdata["price"],
                    "details": {
                        "rs_score": rdata["rs_score"],
                        "sector": rdata["sector"],
                        "description": "RS at 52-week high, price not at high",
                    },
                })
            else:
                events.append({
                    "ticker": ticker,
                    "universe": universe_name,
                    "event_type": "rs_new_high",
                    "event_date": today_str,
                    "rs_rating": rdata["rs_rating"],
                    "price": rdata["price"],
                    "details": {
                        "rs_score": rdata["rs_score"],
                        "sector": rdata["sector"],
                    },
                })

        # HVC data enrichment
        hvc_info = hvc_map.get(ticker)
        rdata["hvc_count"] = hvc_info["hvc_count"] if hvc_info else 0
        rdata["gap_count"] = hvc_info["gap_count"] if hvc_info else 0
        rdata["gaps_open"] = hvc_info["gaps_open"] if hvc_info else 0

        # Monster score (with HVC data)
        rdata["monster_score"] = compute_monster_score(rdata, persistence_map.get(ticker, 0), hvc_info)

    enrich_time = time.time() - t3

    # 9. Save to database
    t4 = time.time()
    save_rankings(ranked, universe_name, computed_at)
    save_history(ranked, universe_name, today_str)
    save_events(events)
    save_time = time.time() - t4

    # Summary
    total_time = time.time() - t0
    above_90 = sum(1 for d in ranked.values() if d["rs_rating"] >= 90)
    above_80 = sum(1 for d in ranked.values() if d["rs_rating"] >= 80)

    print(f"\n{'─'*60}")
    print(f"RS Engine complete: {len(ranked)} ranked in {total_time:.1f}s")
    print(f"  Load: {load_time:.1f}s | Score: {score_time:.1f}s | "
          f"Enrich: {enrich_time:.1f}s | Save: {save_time:.1f}s")
    print(f"  Monsters (RS>=90): {above_90} | Strong (RS>=80): {above_80}")
    print(f"  RS new highs: {n_rs_new_high} | RS breakouts: {n_breakouts}")
    print(f"  Events logged: {len(events)}")
    print(f"{'─'*60}")

    # Print top 10
    top = sorted(ranked.items(), key=lambda x: x[1]["rs_rating"], reverse=True)[:10]
    print(f"\nTop 10:")
    for ticker, d in top:
        chg = f"  {d['rs_change_20d']:+d}" if d.get("rs_change_20d") is not None else ""
        nh = " *NH" if d.get("rs_new_high") else ""
        print(f"  {d['rs_rank']:>4}. {ticker:<8} RS {d['rs_rating']:>2} "
              f"(score {d['rs_score']:.2f}){chg}{nh}  [{d['sector']}]")


def backfill_history(universe_name, days=60, workers=8):
    """Backfill rs_history for the last N calendar days.

    Loads all ticker data once, then computes RS scores for each
    historical date by slicing the data up to that date.
    """
    print(f"\n{'='*60}")
    print(f"RS BACKFILL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Universe: {universe_name}, Days: {days}")
    print(f"{'='*60}")

    # Build universe
    universe = build_universe(universe_name)
    if not universe:
        return
    sector_map = {d: s for d, a, s in universe}

    # Load benchmark (full)
    bench_df = load_benchmark()
    if bench_df is None:
        return
    bench_full = bench_df.set_index("timestamp")["close"]

    # Load all ticker data
    print(f"  Loading {len(universe)} tickers...")
    t0 = time.time()
    all_data = {}
    load_args = [(display, api) for display, api, sector in universe]

    if workers <= 1:
        for display, api in load_args:
            _, df = _load_ticker_data((display, api))
            if df is not None and len(df) >= MIN_BARS_FOR_RS:
                all_data[display] = df.set_index("timestamp")["close"]
    else:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_load_ticker_data, args): args[0]
                       for args in load_args}
            for future in as_completed(futures):
                display, df = future.result()
                if df is not None and len(df) >= MIN_BARS_FOR_RS:
                    all_data[display] = df.set_index("timestamp")["close"]

    print(f"  Loaded: {len(all_data)} tickers [{time.time()-t0:.1f}s]")

    # Generate trading dates from benchmark data (last N calendar days)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    # Strip timezone to match tz-naive benchmark index from CSV data
    cutoff_ts = pd.Timestamp(cutoff).tz_localize(None) if pd.Timestamp(cutoff).tz is not None else pd.Timestamp(cutoff)
    bench_dates = bench_full.index[bench_full.index >= cutoff_ts]
    trading_days = sorted(bench_dates)

    print(f"  Trading days to backfill: {len(trading_days)}")

    # For each trading day, compute scores and save history
    for i, date in enumerate(trading_days):
        date_str = date.strftime("%Y-%m-%d")

        # Slice all data up to this date
        ticker_scores = {}
        for ticker, close_series in all_data.items():
            sliced = close_series[close_series.index <= date]
            if len(sliced) < MIN_BARS_FOR_RS:
                continue
            bench_sliced = bench_full[bench_full.index <= date]
            score = compute_rs_score_latest(sliced, bench_sliced)
            if score is not None and not np.isnan(score):
                ticker_scores[ticker] = score

        if not ticker_scores:
            continue

        # Rank
        ranked = rank_universe(ticker_scores)

        # Get latest price for each ticker on this date
        for ticker, rdata in ranked.items():
            close_series = all_data.get(ticker)
            if close_series is not None:
                sliced = close_series[close_series.index <= date]
                if len(sliced) > 0:
                    rdata["price"] = float(sliced.iloc[-1])

        # Save history only
        save_history(ranked, universe_name, date_str)

        if (i + 1) % 5 == 0 or i == len(trading_days) - 1:
            print(f"  [{i+1}/{len(trading_days)}] {date_str}: "
                  f"{len(ranked)} ranked")

    print(f"\n{'─'*60}")
    print(f"Backfill complete: {len(trading_days)} days processed")
    print(f"{'─'*60}")


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Couloir RS Rating Engine")
    parser.add_argument("--universe", type=str, default="Russell3000",
                        help="Watchlist to use as ranking universe")
    parser.add_argument("--workers", type=int, default=8,
                        help="Parallel workers for data loading")
    parser.add_argument("--once", action="store_true",
                        help="Single pass, no scheduling")
    parser.add_argument("--backfill", action="store_true",
                        help="Backfill RS history for past N days")
    parser.add_argument("--days", type=int, default=90,
                        help="Days to backfill (default: 90)")

    args = parser.parse_args()

    init_db()

    if args.backfill:
        backfill_history(args.universe, days=args.days, workers=args.workers)
    else:
        compute_universe(args.universe, workers=args.workers)

    if args.once:
        return


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nRS Engine stopped.")
