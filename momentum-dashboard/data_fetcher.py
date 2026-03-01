"""
Data fetcher for Massive.com API.
Pulls hourly and daily OHLC data, aggregates into custom timeframes.

CACHING & INCREMENTAL UPDATES:
- First run: fetches full history, saves to local CSV files in cache/
- Subsequent runs: loads cache, fetches only new bars since last timestamp, appends
- Result: first run takes minutes, refreshes take seconds
"""

import os
import requests
import time
import hashlib
import numpy as np
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from config import MASSIVE_API_KEY, MASSIVE_BASE_URL

CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")
DIV_CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache", "dividends")

def _get_backfill_days():
    """Read backfill_years from scheduler_config.json, convert to days. Default: 10 years."""
    try:
        import json
        cfg_path = os.path.join(os.path.dirname(__file__), "scheduler_config.json")
        with open(cfg_path, "r") as f:
            cfg = json.load(f)
        years = cfg.get("backfill_years", 10)
        return int(years) * 365
    except Exception:
        return 3650  # 10 years default


def _is_backfill_enabled():
    """Check if backward backfill is enabled in scheduler_config.json. Default: True."""
    try:
        import json
        cfg_path = os.path.join(os.path.dirname(__file__), "scheduler_config.json")
        with open(cfg_path, "r") as f:
            cfg = json.load(f)
        return cfg.get("backfill", True)
    except Exception:
        return True


def get_data_fingerprint(api_ticker):
    """Compute a fingerprint of a ticker's cached data files.
    Returns a hash string based on cache file sizes and last bar timestamps.
    This is stable across file touches — only changes when actual data changes.
    If any cache file is missing, returns None (must scan)."""
    parts = []
    for timespan in ("hour", "day"):
        path = _cache_path(api_ticker, timespan)
        if not os.path.exists(path):
            return None
        stat = os.stat(path)
        # Use file size as a proxy for content — if data changes, size changes
        parts.append(f"{timespan}:{stat.st_size}")
        # Also include last line (last bar) as a content check
        try:
            with open(path, 'rb') as f:
                f.seek(max(0, stat.st_size - 200))
                last_chunk = f.read().decode('utf-8', errors='replace').strip()
                last_line = last_chunk.split('\n')[-1]
                parts.append(last_line)
        except Exception:
            parts.append("err")
    
    # Include dividend cache
    safe_ticker = api_ticker.replace(":", "_").replace("/", "_")
    div_path = os.path.join(DIV_CACHE_DIR, f"{safe_ticker}_div.csv")
    if os.path.exists(div_path):
        stat = os.stat(div_path)
        parts.append(f"div:{stat.st_size}")
    else:
        parts.append("no_div")
    
    return hashlib.md5("|".join(parts).encode()).hexdigest()


def _ensure_cache_dir():
    os.makedirs(CACHE_DIR, exist_ok=True)


def _cache_path(ticker, timespan):
    safe_ticker = ticker.replace(":", "_").replace("/", "_")
    return os.path.join(CACHE_DIR, f"{safe_ticker}_{timespan}.csv")


def _load_cache(ticker, timespan):
    path = _cache_path(ticker, timespan)
    if not os.path.exists(path):
        return None
    try:
        df = pd.read_csv(path, parse_dates=["timestamp"])
        df = df.sort_values("timestamp").reset_index(drop=True)
        return df
    except Exception as e:
        print(f"    Cache read error for {ticker}/{timespan}: {e}")
        return None


def _save_cache(df, ticker, timespan):
    if df is None or df.empty:
        return
    _ensure_cache_dir()
    path = _cache_path(ticker, timespan)
    df.to_csv(path, index=False)


def _api_fetch(ticker, multiplier, timespan, from_date, to_date, asset_type="stock", limit=50000):
    """Raw API call to Massive.com with pagination."""
    url = f"{MASSIVE_BASE_URL}/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from_date}/{to_date}"
    params = {"adjusted": "true", "sort": "asc", "limit": limit}
    headers = {"Authorization": f"Bearer {MASSIVE_API_KEY}"}
    
    all_results = []
    max_retries = 3
    
    while True:
        for attempt in range(max_retries):
            try:
                resp = requests.get(url, params=params, headers=headers, timeout=30)
                if resp.status_code == 429:
                    wait = 2 ** (attempt + 1)  # 2, 4, 8 seconds
                    print(f"    Rate limited, waiting {wait}s (attempt {attempt+1}/{max_retries})")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                break
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1 and '429' in str(e):
                    wait = 2 ** (attempt + 1)
                    print(f"    Rate limited, waiting {wait}s (attempt {attempt+1}/{max_retries})")
                    time.sleep(wait)
                    continue
                print(f"    API error for {ticker}: {e}")
                return all_results
        else:
            print(f"    Max retries exceeded for {ticker}")
            return all_results
        
        data = resp.json()
        status = data.get("status", "")
        if status not in ("OK", "DELAYED", "ok"):
            print(f"    API status={status} for {ticker} ({timespan})")
            if not data.get("results"):
                break
        
        results = data.get("results", [])
        if not results:
            break
        all_results.extend(results)
        
        next_url = data.get("next_url")
        if next_url:
            url = next_url
            params = {}
        else:
            break
    
    return all_results


def _results_to_df(results):
    """Convert API results list to a clean DataFrame."""
    if not results:
        return pd.DataFrame()
    
    df = pd.DataFrame(results)
    rename_map = {}
    if "t" in df.columns: rename_map["t"] = "timestamp"
    if "o" in df.columns: rename_map["o"] = "open"
    if "h" in df.columns: rename_map["h"] = "high"
    if "l" in df.columns: rename_map["l"] = "low"
    if "c" in df.columns: rename_map["c"] = "close"
    if "v" in df.columns: rename_map["v"] = "volume"
    if "vw" in df.columns: rename_map["vw"] = "vwap"
    if "n" in df.columns: rename_map["n"] = "n_trades"
    df = df.rename(columns=rename_map)
    
    if "timestamp" in df.columns:
        if df["timestamp"].iloc[0] > 1e12:
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        else:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
    
    df = df.sort_values("timestamp").reset_index(drop=True)
    keep_cols = [c for c in ["timestamp", "open", "high", "low", "close", "volume", "vwap", "n_trades"] if c in df.columns]
    return df[keep_cols]


def _is_market_hours():
    """Check if US market is currently open (roughly 9:30 AM - 4:00 PM ET, weekdays).
    Uses a generous window including extended hours (4 AM - 8 PM ET)."""
    from datetime import timezone
    now_utc = datetime.now(timezone.utc)
    # ET is UTC-5 (EST) or UTC-4 (EDT). Use UTC-5 as conservative estimate.
    et_hour = (now_utc.hour - 5) % 24
    is_weekday = now_utc.weekday() < 5  # Mon-Fri
    is_extended_hours = 4 <= et_hour < 20  # 4 AM to 8 PM ET
    return is_weekday and is_extended_hours


def fetch_with_cache(api_ticker, timespan, asset_type="stock", full_history_days=8000, recent_days=1825, cache_max_age_minutes=5):
    """
    Smart fetch: uses cache + incremental update.
    
    First run:  fetch full history -> save cache
    Later runs: load cache -> fetch only new bars -> merge -> save cache
    
    During market closed hours, skips API if cache was updated within
    cache_max_age_minutes. During market hours, always fetches fresh data.
    """
    today = datetime.now().strftime("%Y-%m-%d")
    
    if timespan == "hour":
        history_start = (datetime.now() - timedelta(days=recent_days)).strftime("%Y-%m-%d")
    else:
        history_start = (datetime.now() - timedelta(days=full_history_days)).strftime("%Y-%m-%d")
    
    cached = _load_cache(api_ticker, timespan)
    
    if cached is not None and not cached.empty:
        cache_file = _cache_path(api_ticker, timespan)

        # BACKWARD BACKFILL: if cache doesn't go back to history_start, fetch older data
        earliest_cached = cached["timestamp"].min()
        history_start_ts = pd.Timestamp(history_start)
        if hasattr(earliest_cached, 'tz') and earliest_cached.tz is not None:
            earliest_cached = earliest_cached.tz_localize(None)
        backfill_gap_days = (earliest_cached - history_start_ts).days
        if backfill_gap_days > 30 and (timespan == "hour" or _is_backfill_enabled()):
            backfill_end = (earliest_cached + timedelta(days=1)).strftime("%Y-%m-%d")
            print(f"    ⏪ Backfilling {api_ticker} {timespan}: {history_start} → {backfill_end} "
                  f"({backfill_gap_days} days of older data)", flush=True)
            old_results = _api_fetch(api_ticker, 1, timespan, history_start, backfill_end, asset_type)
            old_data = _results_to_df(old_results)
            if not old_data.empty:
                # Prepend old data before existing cache
                cached = pd.concat([old_data, cached], ignore_index=True)
                cached = cached.drop_duplicates(subset=["timestamp"], keep="last")
                cached = cached.sort_values("timestamp").reset_index(drop=True)
                print(f"    ✅ Backfilled {len(old_data)} older {timespan} bars for {api_ticker}", flush=True)

        # During market closed hours, skip forward API fetch if cache is fresh
        if not _is_market_hours():
            if os.path.exists(cache_file):
                age_minutes = (time.time() - os.path.getmtime(cache_file)) / 60
                if age_minutes < 1440 and backfill_gap_days <= 30:
                    return cached

        # INCREMENTAL FORWARD: fetch only from last cached timestamp
        last_ts = cached["timestamp"].max()
        fetch_from = (last_ts - timedelta(days=1)).strftime("%Y-%m-%d")

        results = _api_fetch(api_ticker, 1, timespan, fetch_from, today, asset_type)
        new_data = _results_to_df(results)

        if not new_data.empty:
            cached = cached[cached["timestamp"] < new_data["timestamp"].min()]
            merged = pd.concat([cached, new_data], ignore_index=True)
            merged = merged.drop_duplicates(subset=["timestamp"], keep="last")
            merged = merged.sort_values("timestamp").reset_index(drop=True)
            _save_cache(merged, api_ticker, timespan)
            return merged
        else:
            # Save cache (may have backfill data) and touch for freshness
            _save_cache(cached, api_ticker, timespan)
            return cached
    else:
        # FULL FETCH: no cache exists
        results = _api_fetch(api_ticker, 1, timespan, history_start, today, asset_type)
        df = _results_to_df(results)
        _save_cache(df, api_ticker, timespan)
        return df


def fetch_ticker_data(display_ticker, api_ticker, asset_type):
    """
    Fetch all required data for a single ticker using cache.
    Returns dict with 'hourly', 'daily', and 'weekly' DataFrames.
    """
    print(f"  Fetching {display_ticker} ({api_ticker})...")
    
    history_days = _get_backfill_days()
    hourly = fetch_with_cache(api_ticker, "hour", asset_type, recent_days=1825)
    daily = fetch_with_cache(api_ticker, "day", asset_type, full_history_days=history_days)
    weekly = fetch_with_cache(api_ticker, "week", asset_type, full_history_days=history_days)
    
    h_count = len(hourly) if not hourly.empty else 0
    d_count = len(daily) if not daily.empty else 0
    w_count = len(weekly) if not weekly.empty else 0
    
    cached_marker = ""
    hourly_cache = _cache_path(api_ticker, "hour")
    if os.path.exists(hourly_cache):
        mtime = os.path.getmtime(hourly_cache)
        age_seconds = time.time() - mtime
        if age_seconds < 5:
            cached_marker = " [incremental]"
        else:
            cached_marker = " [from cache]"
    
    print(f"    {h_count} hourly, {d_count} daily, {w_count} weekly bars{cached_marker}")
    
    return {"hourly": hourly, "daily": daily, "weekly": weekly}


def aggregate_bars(df, n_bars):
    """Aggregate n consecutive bars into one."""
    if df.empty or len(df) < n_bars:
        return pd.DataFrame()
    
    records = []
    data = df.to_dict("records")
    remainder = len(data) % n_bars
    start_idx = remainder
    
    for i in range(start_idx, len(data), n_bars):
        chunk = data[i:i + n_bars]
        if len(chunk) < n_bars:
            continue
        records.append({
            "timestamp": chunk[0]["timestamp"],
            "open": chunk[0]["open"],
            "high": max(c["high"] for c in chunk),
            "low": min(c["low"] for c in chunk),
            "close": chunk[-1]["close"],
            "volume": sum(c.get("volume", 0) or 0 for c in chunk),
        })
    
    return pd.DataFrame(records)


def _aggregate_5d_epoch(daily_df):
    """Aggregate daily bars into 5-trading-day bars matching TradingView.
    Delegates to the generalized calendar-anchored function."""
    return _aggregate_nd_calendar(daily_df, 5)


# One verified TV bar start date per timeframe (exchange-level, all tickers).
# Used as the anchor to align our calendar-based grouping with TV's.
# Only need ONE date each — the trading calendar handles all holidays.
_TV_ANCHOR = {
    2: '2026-02-19',   # Known 2D bar start from TV export
    3: '2026-02-20',   # Known 3D bar start from TV export
    5: '2026-02-17',   # Known 5D bar start from TV export
    '2w': '2026-02-17', # Known 2W bar start from TV export
    '6w': '2026-02-17', # Known 6W bar start from TV export
}


def _get_td_lookup(start_year, end_year):
    """Get cached trading-day lookup dict {date: index} for a year range.

    Cached at module level to avoid rebuilding the ~5000-entry dict
    on every call to _aggregate_nd_calendar (called 5-8 times per ticker).
    """
    from trading_calendar import trading_days as get_trading_days
    cal = get_trading_days(start_year, end_year)
    return {d: i for i, d in enumerate(cal)}

# Module-level cache for td_lookup — keyed by (start_year, end_year)
_td_lookup_cache = {}

def _get_td_lookup_cached(start_year, end_year):
    """Return cached trading-day lookup, building once per year range."""
    key = (start_year, end_year)
    if key not in _td_lookup_cache:
        _td_lookup_cache[key] = _get_td_lookup(start_year, end_year)
    return _td_lookup_cache[key]


def _aggregate_nd_calendar(input_df, key, td_lookup=None):
    """Aggregate bars into multi-period bars matching TradingView.

    Uses the NYSE trading calendar to assign each bar a trading day number,
    then groups every N trading days from a verified TV anchor date.

    This is fully automatic — no reference files needed. The trading
    calendar computes holidays algorithmically and a single anchor date
    per timeframe ensures exact alignment with TV.

    Args:
        input_df: DataFrame with 'timestamp' column (daily or weekly bars)
        key: int (2,3,5) for daily-based, str ('2w','6w') for weekly-based
        td_lookup: optional pre-built {date: index} dict (avoids rebuild per call)
    """
    if input_df.empty:
        return pd.DataFrame()

    if isinstance(key, str) and key.endswith('w'):
        n = int(key[:-1])  # '2w' -> 2, '6w' -> 6
        is_weekly = True
    else:
        n = int(key)
        is_weekly = False

    df = input_df.copy().sort_values('timestamp').reset_index(drop=True)

    # Get anchor date for this timeframe
    anchor_str = _TV_ANCHOR.get(key)
    if not anchor_str:
        print(f"  ⚠ No TV anchor for {key}, falling back to simple grouping")
        return aggregate_bars(df, n)

    anchor_date = pd.Timestamp(anchor_str).date()

    if is_weekly:
        # For weekly-based timeframes (2W, 6W):
        # Input bars are 1W bars (one per calendar week).
        # We need to find the anchor in the weekly bar sequence and group from there.
        df_dates = df['timestamp'].dt.tz_localize(None).dt.normalize()

        # Find the anchor week (the weekly bar that contains or starts on the anchor date)
        anchor_ts = pd.Timestamp(anchor_date)
        # Find the weekly bar closest to the anchor
        diffs = (df_dates - anchor_ts).abs()
        anchor_idx = diffs.idxmin()

        # Group every N weeks from the anchor
        df['bar_group'] = (df.index - anchor_idx) // n
    else:
        # For daily-based timeframes (2D, 3D, 5D):
        if td_lookup is None:
            # Build lookup if not provided (backwards compatibility)
            first_date = df['timestamp'].iloc[0]
            last_date = df['timestamp'].iloc[-1]

            if hasattr(first_date, 'date'):
                first_d = first_date.date() if not isinstance(first_date, type(anchor_date)) else first_date
                last_d = last_date.date() if not isinstance(last_date, type(anchor_date)) else last_date
            else:
                first_d = pd.Timestamp(first_date).date()
                last_d = pd.Timestamp(last_date).date()

            cal_end_year = max(last_d.year, anchor_date.year)
            td_lookup = _get_td_lookup_cached(first_d.year, cal_end_year)

        # Find the anchor's trading day index
        if anchor_date not in td_lookup:
            print(f"  ⚠ Anchor {anchor_date} not in trading calendar, falling back")
            return aggregate_bars(df, n)

        anchor_td_idx = td_lookup[anchor_date]

        # Assign each daily bar its trading day number relative to anchor
        # Vectorised: convert timestamps to dates and map through lookup
        bar_dates = df['timestamp'].dt.tz_localize(None).dt.date if df['timestamp'].dt.tz is not None else df['timestamp'].dt.date

        bar_groups = bar_dates.map(td_lookup)
        bar_groups = (bar_groups - anchor_td_idx) // n

        df['bar_group'] = bar_groups
        df = df.dropna(subset=['bar_group'])
        df['bar_group'] = df['bar_group'].astype(int)

    if df.empty:
        return pd.DataFrame()

    bars = df.groupby('bar_group').agg(
        timestamp=('timestamp', 'first'),
        open=('open', 'first'),
        high=('high', 'max'),
        low=('low', 'min'),
        close=('close', 'last'),
        volume=('volume', 'sum'),
    ).reset_index(drop=True)

    bars = bars.sort_values('timestamp').reset_index(drop=True)
    return bars


def clean_ohlc(df, max_wick_pct=0.20):
    """Clean OHLC data by clamping anomalous wicks (bad ticks from API).
    
    Uses a rolling median of the body range to detect anomalous wicks.
    If a wick extends more than max_wick_pct of the price beyond the body,
    clamp it. Default 3% catches extreme bad ticks while allowing
    legitimate volatile days (e.g. tariff crash days with ~5-8% body ranges).
    """
    if df.empty:
        return df
    df = df.copy()
    body_low = df[["open", "close"]].min(axis=1)
    body_high = df[["open", "close"]].max(axis=1)
    
    # Use a percentage of the body midpoint as threshold
    body_mid = (body_low + body_high) / 2
    threshold_low = body_low - body_mid * max_wick_pct
    threshold_high = body_high + body_mid * max_wick_pct
    
    bad_low = df["low"] < threshold_low
    bad_high = df["high"] > threshold_high
    if bad_low.any():
        df.loc[bad_low, "low"] = body_low[bad_low]
    if bad_high.any():
        df.loc[bad_high, "high"] = body_high[bad_high]
    return df


def fetch_dividends(api_ticker):
    """Fetch dividend history for a ticker, with caching.
    Returns list of (ex_date, cash_amount) sorted by date descending."""
    os.makedirs(DIV_CACHE_DIR, exist_ok=True)
    safe_ticker = api_ticker.replace(":", "_").replace("/", "_")
    cache_path = os.path.join(DIV_CACHE_DIR, f"{safe_ticker}_div.csv")
    
    # Cache dividends for 24 hours
    if os.path.exists(cache_path):
        age = time.time() - os.path.getmtime(cache_path)
        if age < 86400:
            try:
                df = pd.read_csv(cache_path, parse_dates=["ex_date"])
                return df
            except Exception:
                pass
    
    # Fetch from API
    url = f"{MASSIVE_BASE_URL.replace('/v2', '/v3')}/reference/dividends"
    params = {"ticker": api_ticker, "limit": 100, "sort": "ex_dividend_date", "order": "desc"}
    headers = {"Authorization": f"Bearer {MASSIVE_API_KEY}"}
    
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        
        if not results:
            # Save empty cache to avoid re-fetching
            pd.DataFrame(columns=["ex_date", "cash_amount"]).to_csv(cache_path, index=False)
            return pd.DataFrame(columns=["ex_date", "cash_amount"])
        
        divs = []
        for d in results:
            if d.get("ex_dividend_date") and d.get("cash_amount"):
                divs.append({
                    "ex_date": pd.to_datetime(d["ex_dividend_date"]),
                    "cash_amount": float(d["cash_amount"]),
                })
        
        df = pd.DataFrame(divs)
        if not df.empty:
            df = df.sort_values("ex_date", ascending=False).reset_index(drop=True)
        df.to_csv(cache_path, index=False)
        return df
    except Exception as e:
        return pd.DataFrame(columns=["ex_date", "cash_amount"])


def apply_dividend_adjustment(df, dividends, bar_end_dates=None):
    """Apply backward dividend adjustment to OHLC data.
    
    TradingView's ADJ mode: adjusts historical prices downward by the
    cumulative dividend amount. For each ex-date, all bars before that date
    have their OHLC reduced by a factor of (close - dividend) / close,
    calculated at the close just before the ex-date.
    
    Vectorised: computes a single cumulative factor per bar, then applies once.
    """
    if df.empty or dividends.empty:
        return df
    
    df = df.copy()
    
    # Sort dividends by ex_date ascending for cumulative application
    divs = dividends.sort_values("ex_date").reset_index(drop=True)
    
    # Reference dates for comparison
    if bar_end_dates is not None:
        ref_dates = bar_end_dates.values
    else:
        ref_dates = df["timestamp"].values
    
    # Build cumulative factor array — start at 1.0 for every bar
    cum_factor = np.ones(len(df), dtype=float)
    
    # Work backwards from most recent to oldest dividend
    for i in range(len(divs) - 1, -1, -1):
        ex_date = divs.loc[i, "ex_date"]
        cash = divs.loc[i, "cash_amount"]
        
        # Find the close price just before the ex-date
        mask = ref_dates < np.datetime64(ex_date)
        if not mask.any():
            continue
        
        # Last bar before ex-date
        last_idx = np.where(mask)[0][-1]
        close_before = df["close"].iloc[last_idx]
        if close_before <= 0:
            continue
        
        # Proportional factor for this dividend
        factor = (close_before - cash) / close_before
        
        # Multiply into cumulative factor for all bars before ex-date
        cum_factor[mask] *= factor
    
    # Apply cumulative factor once to all OHLC columns
    for col in ("open", "high", "low", "close"):
        if col in df.columns:
            df[col] = df[col].values * cum_factor
    
    return df


def _aggregate_by_week(daily_df):
    """Aggregate daily bars into weekly bars by calendar week.
    Uses ISO week (Monday start) to match TradingView's weekly bar boundaries."""
    if daily_df.empty:
        return pd.DataFrame()
    
    df = daily_df.copy()
    # Group by ISO year-week so calendar weeks align properly
    df["year_week"] = df["timestamp"].dt.isocalendar().year.astype(str) + "-" + \
                      df["timestamp"].dt.isocalendar().week.astype(str).str.zfill(2)
    
    weekly = df.groupby("year_week").agg(
        timestamp=("timestamp", "first"),
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
    ).reset_index(drop=True)
    
    weekly = weekly.sort_values("timestamp").reset_index(drop=True)
    return weekly


def _compute_bar_end_dates(multi_day_bars, daily_bars):
    """For each multi-day bar, find the last trading day it contains.
    
    Uses daily bar timestamps to determine the actual last trading day
    within each multi-day bar's range. This is needed for dividend adjustment
    of weekly bars — a bar should only be adjusted if its last trading day
    is before the ex-date.
    
    Returns a Series aligned with multi_day_bars index.
    """
    daily_dates = daily_bars["timestamp"].sort_values().values
    bar_starts = multi_day_bars["timestamp"].values
    end_dates = pd.Series(pd.NaT, index=multi_day_bars.index)
    
    for i in range(len(bar_starts)):
        start = bar_starts[i]
        if i + 1 < len(bar_starts):
            next_start = bar_starts[i + 1]
            # Find all daily bars >= this bar's start and < next bar's start
            mask = (daily_dates >= start) & (daily_dates < next_start)
        else:
            # Last bar: all daily bars from this start onwards
            mask = daily_dates >= start
        
        matching = daily_dates[mask]
        if len(matching) > 0:
            end_dates.iloc[i] = pd.Timestamp(matching[-1])
        else:
            end_dates.iloc[i] = pd.Timestamp(start)
    
    return end_dates


def build_adjusted_timeframes(tf_data, dividends):
    """Build dividend-adjusted timeframe data FAST by reusing unadjusted tf_data.
    
    Instead of rebuilding all timeframes from scratch:
    1. Adjust daily and hourly OHLC directly
    2. For sub-daily aggregated (4H, 8H) and multi-day (2D, 3D, 5D):
       adjust OHLC directly (no re-aggregation needed)
    3. For weekly+ timeframes: rebuild from adjusted daily
       (preserves accuracy when ex-date falls mid-week)
    
    This skips the expensive calendar lookups and aggregation for 2D/3D/5D.
    """
    if dividends is None or dividends.empty:
        return tf_data
    
    tf_adj = {}
    
    # Get adjusted daily — this is the foundation for weekly+ rebuilds
    adj_daily = None
    if "1D" in tf_data:
        adj_daily = apply_dividend_adjustment(tf_data["1D"].copy(), dividends)
        tf_adj["1D"] = adj_daily
    
    # Hourly timeframes: adjust each directly
    for tf in ("1H", "4H", "8H"):
        if tf in tf_data:
            tf_adj[tf] = apply_dividend_adjustment(tf_data[tf].copy(), dividends)
    
    # Multi-day timeframes: adjust OHLC directly
    # (tiny inaccuracy possible if ex-date falls mid-bar, but negligible for 2D/3D/5D)
    for tf in ("2D", "3D", "5D"):
        if tf in tf_data:
            tf_adj[tf] = apply_dividend_adjustment(tf_data[tf].copy(), dividends)
    
    # Weekly+ timeframes: rebuild from adjusted daily for accuracy
    # This correctly handles ex-dates that fall mid-week
    if adj_daily is not None and not adj_daily.empty:
        weekly_adj = _aggregate_by_week(adj_daily)
        tf_adj["1W"] = weekly_adj
        tf_adj["2W"] = _aggregate_nd_calendar(weekly_adj, '2w')
        tf_adj["6W"] = _aggregate_nd_calendar(weekly_adj, '6w')
        
        monthly = adj_daily.copy()
        monthly["month"] = monthly["timestamp"].dt.to_period("M")
        monthly_agg = monthly.groupby("month").agg(
            timestamp=("timestamp", "first"),
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
        ).reset_index(drop=True)
        
        tf_adj["1M"] = monthly_agg
        tf_adj["3M"] = aggregate_bars(monthly_agg, 3)
        tf_adj["6M"] = aggregate_bars(monthly_agg, 6)
        tf_adj["12M"] = aggregate_bars(monthly_agg, 12)
    else:
        # Fallback: copy weekly+ from unadjusted
        for tf in ("1W", "2W", "6W", "1M", "3M", "6M", "12M"):
            if tf in tf_data:
                tf_adj[tf] = tf_data[tf]
    
    return tf_adj


def build_timeframe_data(raw_data, dividends=None):
    """Build OHLC DataFrames for each timeframe from raw data.
    If dividends provided, applies backward dividend adjustment before indicators."""
    hourly = clean_ohlc(raw_data.get("hourly", pd.DataFrame()))
    raw_daily = raw_data.get("daily", pd.DataFrame())
    daily = clean_ohlc(raw_daily.copy() if not raw_daily.empty else raw_daily)
    weekly = clean_ohlc(raw_data.get("weekly", pd.DataFrame()))
    
    # Apply dividend adjustment if requested
    if dividends is not None and not dividends.empty:
        daily = apply_dividend_adjustment(daily, dividends)
        hourly = apply_dividend_adjustment(hourly, dividends)
    
    timeframe_data = {}

    # Pre-build trading day lookup once for all daily-based calendar aggregations.
    # This avoids rebuilding the ~5000-entry dict 5 times (2D, 3D, 5D, 2W, 6W).
    td_lookup = None
    if not daily.empty:
        first_d = daily['timestamp'].iloc[0]
        last_d = daily['timestamp'].iloc[-1]
        first_year = pd.Timestamp(first_d).year
        last_year = pd.Timestamp(last_d).year
        # Extend to cover TV anchor dates
        td_lookup = _get_td_lookup_cached(first_year, max(last_year, 2026))

    if not hourly.empty:
        timeframe_data["1H"] = hourly.copy()
        timeframe_data["4H"] = aggregate_bars(hourly, 4)
        timeframe_data["8H"] = aggregate_bars(hourly, 8)

    if not daily.empty:
        timeframe_data["1D"] = daily.copy()
        # 2D, 3D, 5D use trading calendar + single TV anchor for exact alignment.
        timeframe_data["2D"] = _aggregate_nd_calendar(daily, 2, td_lookup=td_lookup)
        timeframe_data["3D"] = _aggregate_nd_calendar(daily, 3, td_lookup=td_lookup)
        timeframe_data["5D"] = _aggregate_nd_calendar(daily, 5, td_lookup=td_lookup)

    # Always build weekly from daily data.
    # Polygon's API weekly bars have aggregation bugs (e.g. missing daily lows).
    # Building from daily ensures correctness and handles dividend adjustment
    # at the daily level where ex-date boundaries are precise.
    if not daily.empty:
        weekly_from_daily = _aggregate_by_week(daily)
        timeframe_data["1W"] = weekly_from_daily
        # 2W and 6W use TV anchor + weekly bar sequence for alignment
        timeframe_data["2W"] = _aggregate_nd_calendar(weekly_from_daily, '2w')
        timeframe_data["6W"] = _aggregate_nd_calendar(weekly_from_daily, '6w')
    elif not weekly.empty:
        # Fallback to API weekly if no daily data available
        timeframe_data["1W"] = weekly.copy()
        timeframe_data["2W"] = _aggregate_nd_calendar(weekly, '2w')
        timeframe_data["6W"] = _aggregate_nd_calendar(weekly, '6w')
    
    if not daily.empty:
        monthly = daily.copy()
        monthly["month"] = monthly["timestamp"].dt.to_period("M")
        monthly_agg = monthly.groupby("month").agg(
            timestamp=("timestamp", "first"),
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
        ).reset_index(drop=True)
        
        timeframe_data["1M"] = monthly_agg
        timeframe_data["3M"] = aggregate_bars(monthly_agg, 3)
        timeframe_data["6M"] = aggregate_bars(monthly_agg, 6)
        timeframe_data["12M"] = aggregate_bars(monthly_agg, 12)
    
    return timeframe_data


def clear_cache(ticker=None):
    """Clear cache files. If ticker specified, clear only that ticker."""
    if not os.path.exists(CACHE_DIR):
        return
    if ticker:
        safe_ticker = ticker.replace(":", "_").replace("/", "_")
        for f in os.listdir(CACHE_DIR):
            if f.startswith(safe_ticker + "_"):
                os.remove(os.path.join(CACHE_DIR, f))
                print(f"  Cleared cache: {f}")
    else:
        for f in os.listdir(CACHE_DIR):
            if f.endswith(".csv"):
                os.remove(os.path.join(CACHE_DIR, f))
        print(f"  Cleared all cache files")
