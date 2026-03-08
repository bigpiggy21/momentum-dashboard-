"""
Sweep Engine — Dark pool sweep tracking and clusterbomb detection.

Fetches tick-level trade data from Massive.com (Polygon) API,
filters for dark pool intermarket sweeps, stores them, and detects
"clusterbomb" events (multiple large sweeps on one ticker in one day).

Database tables:
  - sweep_trades:    individual dark pool sweep trades
  - clusterbomb_events: detected clusterbomb signals (3+ sweeps/day above threshold)
  - sweep_meta:      metadata (last fetch timestamps, etc.)
"""

import os
import json
import time as _time
import sqlite3
import hashlib
import threading
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry as _Retry
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from config import MASSIVE_API_KEY, MASSIVE_BASE_URL, DB_PATH


# ---------------------------------------------------------------------------
# Thread-local HTTP session with connection pooling
# ---------------------------------------------------------------------------
_thread_local = threading.local()

def _get_session():
    """Get a thread-local requests.Session with connection pooling.
    Reuses TCP connections across requests on the same thread, avoiding
    the overhead of a new TLS handshake for every single API call."""
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=20,   # number of host pools
            pool_maxsize=20,       # connections per host
            max_retries=0,         # we handle retries ourselves
        )
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        _thread_local.session = s
    return _thread_local.session

# Price cache directory (same as backtest_engine / data_fetcher)
PRICE_CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Massive/Polygon trade API (v3 endpoint)
TRADES_BASE_URL = MASSIVE_BASE_URL.replace("/v2", "/v3")

# Dark pool identification
FINRA_TRF_EXCHANGE_ID = 4          # exchange==4 => FINRA Trade Reporting Facility
INTERMARKET_SWEEP_CONDITION = 14   # condition code 14 => Intermarket Sweep Order

# Minimum notional to store a sweep trade (filters out noise)
MIN_SWEEP_NOTIONAL = 500_000         # $500K — sweeps below this are ignored

# Default clusterbomb thresholds
DEFAULT_CB_MIN_SWEEPS = 3          # minimum sweeps in a day to qualify
DEFAULT_CB_MIN_NOTIONAL = 1_000_000  # $1M minimum per individual sweep
DEFAULT_CB_MIN_TOTAL = 38_000_000    # $38M total notional in the day
DEFAULT_CB_RARITY_DAYS = 60         # no qualifying sweeps for N trading days before = "rare" CB
DEFAULT_CB_RARE_MIN_NOTIONAL = 1_000_000  # min notional to "break" dormancy (independent of CB min_notional)
DEFAULT_MONSTER_MIN_NOTIONAL = 100_000_000  # $100M — single sweep monster threshold

# Sector classifications for filtering
SECTOR_GROUPS = {
    "Technology": ["AAPL", "MSFT", "GOOG", "GOOGL", "META", "AMZN", "CRM", "ORCL", "ADBE", "CSCO", "NOW", "SHOP", "SQ", "SNOW", "NET", "DDOG", "PLTR", "INTU", "WDAY", "TEAM"],
    "Semiconductors": ["NVDA", "TSM", "AVGO", "AMD", "INTC", "QCOM", "TXN", "MU", "AMAT", "LRCX", "KLAC", "SNPS", "CDNS", "MRVL", "NXPI", "ON", "MCHP", "ADI", "MPWR", "SWKS"],
    "AI / Cloud": ["NVDA", "MSFT", "GOOG", "GOOGL", "AMZN", "META", "CRM", "PLTR", "NOW", "SNOW", "DDOG", "NET", "MDB", "AI", "SMCI", "DELL", "HPE"],
    "Cybersecurity": ["CRWD", "PANW", "ZS", "FTNT", "S", "NET", "OKTA", "CYBR", "TENB"],
    "Healthcare": ["JNJ", "UNH", "PFE", "ABBV", "MRK", "LLY", "TMO", "ABT", "DHR", "BMY", "AMGN", "GILD", "ISRG", "MDT", "SYK", "BSX", "REGN", "VRTX", "ZTS", "HCA", "CI", "ELV", "DXCM"],
    "Financials": ["JPM", "BAC", "WFC", "GS", "MS", "BLK", "SCHW", "C", "AXP", "BX", "KKR", "APO", "ICE", "CME", "SPGI", "MCO", "V", "MA"],
    "Consumer Disc.": ["TSLA", "HD", "MCD", "NKE", "SBUX", "TJX", "LOW", "BKNG", "CMG", "ABNB", "LULU", "RCL", "MAR", "YUM", "DG", "DLTR", "ROST"],
    "Energy": ["XOM", "CVX", "COP", "SLB", "EOG", "MPC", "PSX", "VLO", "OXY", "HES", "DVN", "FANG", "HAL", "BKR"],
    "Industrials": ["CAT", "UNP", "HON", "RTX", "DE", "GE", "BA", "LMT", "NOC", "GD", "WM", "ETN", "ITW", "EMR"],
    "Communication": ["GOOG", "GOOGL", "META", "NFLX", "DIS", "CMCSA", "T", "VZ", "TMUS", "EA", "TTWO"],
    "Cons. Staples": ["PG", "KO", "PEP", "COST", "WMT", "PM", "MO", "CL", "MDLZ", "GIS", "STZ"],
    "Materials": ["LIN", "APD", "SHW", "ECL", "NEM", "FCX", "CTVA", "DD", "DOW", "VMC", "MLM"],
    "Real Estate": ["AMT", "PLD", "CCI", "EQIX", "PSA", "O", "SPG", "WELL", "DLR", "AVB"],
    "Utilities": ["NEE", "DUK", "SO", "D", "AEP", "SRE", "EXC", "XEL", "WEC", "ED"],
}


def get_sectors():
    """Return sector groups for the sector filter dropdown."""
    return [{"name": k, "tickers": v, "count": len(v)} for k, v in SECTOR_GROUPS.items()]


# ---------------------------------------------------------------------------
# ETF categories — curated grouping for the ETF sweeps page
# ---------------------------------------------------------------------------

ETF_CATEGORIES_PATH = os.path.join(os.path.dirname(__file__), "etf_categories.json")

_DEFAULT_ETF_CATEGORIES = {
    "Sector": ["XLK", "XLF", "XLE", "XLV", "XLI", "XLY", "XLP", "XLC", "XLB", "XLRE", "XLU"],
    "Broad Market": ["SPY", "QQQ", "IWM", "DIA", "VOO", "VTI", "IVV"],
    "Leveraged": ["TQQQ", "SQQQ", "SPXL", "SPXS", "SOXL", "SOXS", "UPRO", "SPXU", "LABU", "LABD"],
    "Commodity": ["GLD", "SLV", "USO", "UNG", "IAU"],
    "Bond": ["TLT", "TBT", "HYG", "JNK", "LQD", "AGG"],
    "Crypto": ["IBIT", "GBTC", "BITO", "ETHE"],
    "Volatility": ["VXX", "UVXY", "SVXY"],
}


def get_etf_categories():
    """Load ETF category mapping. Falls back to defaults if no config file."""
    try:
        with open(ETF_CATEGORIES_PATH, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return dict(_DEFAULT_ETF_CATEGORIES)


# ---------------------------------------------------------------------------
# ETF ticker cache — fetched from Polygon reference API, cached to JSON
# ---------------------------------------------------------------------------

_ETF_CACHE_PATH = os.path.join(PRICE_CACHE_DIR, "etf_tickers.json")
_etf_set = None  # Lazy-loaded in-memory set
_ETF_CACHE_MAX_AGE_DAYS = 7

# Manual ETF overrides — tickers Polygon classifies as trusts/funds but we treat as ETFs
_ETF_MANUAL_OVERRIDES = {"IAU"}
_ETF_CATEGORIES_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "etf_categories.json")

def _load_etf_category_tickers():
    """Load all tickers from etf_categories.json — these are always treated as ETFs."""
    if os.path.isfile(_ETF_CATEGORIES_PATH):
        try:
            with open(_ETF_CATEGORIES_PATH, "r") as f:
                cats = json.load(f)
            tickers = set()
            for cat_tickers in cats.values():
                tickers.update(cat_tickers)
            return tickers
        except Exception:
            pass
    return set()

def load_etf_set():
    """Load the cached ETF ticker set. Returns empty set if no cache.
    Includes: Polygon ETF cache + manual overrides + etf_categories.json tickers."""
    global _etf_set
    if _etf_set is not None:
        return _etf_set
    if os.path.isfile(_ETF_CACHE_PATH):
        try:
            with open(_ETF_CACHE_PATH, "r") as f:
                data = json.load(f)
            _etf_set = set(data.get("tickers", []))
            _etf_set |= _ETF_MANUAL_OVERRIDES
            _etf_set |= _load_etf_category_tickers()
            return _etf_set
        except Exception as e:
            print(f"[ETF] Failed to load cache: {e}")
    _etf_set = set(_ETF_MANUAL_OVERRIDES) | _load_etf_category_tickers()
    return _etf_set

def refresh_etf_cache(force=False):
    """Fetch all active ETF tickers from Polygon reference API and cache to JSON.
    Skips if cache is fresh (< 7 days old) unless force=True.
    Returns the ETF set."""
    global _etf_set

    # Check if cache is fresh enough
    if not force and os.path.isfile(_ETF_CACHE_PATH):
        try:
            mtime = os.path.getmtime(_ETF_CACHE_PATH)
            age_days = (_time.time() - mtime) / 86400
            if age_days < _ETF_CACHE_MAX_AGE_DAYS:
                return load_etf_set()
        except Exception:
            pass

    print("[ETF] Refreshing ETF ticker cache from Polygon...")
    api_key = MASSIVE_API_KEY
    base = MASSIVE_BASE_URL.replace("/v2", "/v3")
    all_tickers = []
    url = f"{base}/reference/tickers?type=ETF&market=stocks&active=true&limit=1000&apiKey={api_key}"

    session = _get_session()
    pages = 0
    while url:
        try:
            resp = session.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            results = data.get("results", [])
            all_tickers.extend(r["ticker"] for r in results if "ticker" in r)
            pages += 1
            # Follow pagination cursor
            next_url = data.get("next_url")
            if next_url:
                url = f"{next_url}&apiKey={api_key}" if "apiKey" not in next_url else next_url
            else:
                url = None
        except Exception as e:
            print(f"[ETF] API error on page {pages + 1}: {e}")
            break

    if all_tickers:
        os.makedirs(PRICE_CACHE_DIR, exist_ok=True)
        cache_data = {
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "count": len(all_tickers),
            "tickers": sorted(set(all_tickers)),
        }
        with open(_ETF_CACHE_PATH, "w") as f:
            json.dump(cache_data, f)
        _etf_set = set(all_tickers)
        print(f"[ETF] Cached {len(_etf_set)} ETF tickers ({pages} API pages)")
    else:
        print("[ETF] Warning: got 0 ETF tickers from API, keeping old cache")
        return load_etf_set()

    return _etf_set

def is_etf(ticker):
    """Check if a ticker is an ETF."""
    return ticker in load_etf_set()


# ---------------------------------------------------------------------------
# Ticker name cache — human-readable names from Polygon reference API
# ---------------------------------------------------------------------------

_TICKER_NAMES_PATH = os.path.join(PRICE_CACHE_DIR, "ticker_names.json")
_ticker_names = None  # Lazy-loaded dict

def load_ticker_names():
    """Load cached ticker names. Returns dict {ticker: name}."""
    global _ticker_names
    if _ticker_names is not None:
        return _ticker_names
    if os.path.isfile(_TICKER_NAMES_PATH):
        try:
            with open(_TICKER_NAMES_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            _ticker_names = data.get("names", {})
            return _ticker_names
        except Exception as e:
            print(f"[TickerNames] Failed to load cache: {e}")
    _ticker_names = {}
    return _ticker_names


def refresh_ticker_names(force=False):
    """Fetch ticker names from Polygon reference API for all tickers we track.

    Queries both ETF and stock types. Caches to JSON.
    Skips if cache is fresh (< 7 days) unless force=True.
    """
    global _ticker_names

    if not force and os.path.isfile(_TICKER_NAMES_PATH):
        try:
            mtime = os.path.getmtime(_TICKER_NAMES_PATH)
            age_days = (_time.time() - mtime) / 86400
            if age_days < 7:
                return load_ticker_names()
        except Exception:
            pass

    print("[TickerNames] Fetching ticker names from Polygon...", flush=True)
    api_key = MASSIVE_API_KEY
    base = MASSIVE_BASE_URL.replace("/v2", "/v3")
    names = {}
    session = _get_session()

    for ticker_type in ["ETF", "CS"]:  # ETFs + Common Stocks
        url = f"{base}/reference/tickers?type={ticker_type}&market=stocks&active=true&limit=1000&apiKey={api_key}"
        pages = 0
        while url:
            try:
                resp = session.get(url, timeout=15)
                resp.raise_for_status()
                data = resp.json()
                for r in data.get("results", []):
                    if "ticker" in r and "name" in r:
                        names[r["ticker"]] = r["name"]
                pages += 1
                next_url = data.get("next_url")
                if next_url:
                    url = f"{next_url}&apiKey={api_key}" if "apiKey" not in next_url else next_url
                else:
                    url = None
            except Exception as e:
                print(f"[TickerNames] API error ({ticker_type} page {pages + 1}): {e}")
                break
        print(f"[TickerNames] {ticker_type}: {pages} pages, {len(names)} total so far", flush=True)

    if names:
        os.makedirs(PRICE_CACHE_DIR, exist_ok=True)
        cache_data = {
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "count": len(names),
            "names": names,
        }
        with open(_TICKER_NAMES_PATH, "w", encoding="utf-8") as f:
            json.dump(cache_data, f)
        _ticker_names = names
        print(f"[TickerNames] Cached {len(names)} ticker names", flush=True)
    else:
        print("[TickerNames] Warning: got 0 names from API, keeping old cache")
        return load_ticker_names()

    return _ticker_names


def purge_etf_events():
    """Remove ETF-based events from clusterbomb_events table.
    Returns count of deleted events."""
    etf_tickers = load_etf_set()
    if not etf_tickers:
        print("[ETF] No ETF cache loaded, skipping purge")
        return 0
    conn = _get_db()
    placeholders = ",".join("?" * len(etf_tickers))
    tickers_list = sorted(etf_tickers)
    c = conn.cursor()
    # Find which ETFs have events
    rows = c.execute(f"""
        SELECT DISTINCT ticker FROM clusterbomb_events
        WHERE ticker IN ({placeholders})
    """, tickers_list).fetchall()
    etf_event_tickers = [r["ticker"] for r in rows]
    if not etf_event_tickers:
        conn.close()
        print("[ETF] No ETF events found in clusterbomb_events")
        return 0
    # Delete them
    ph2 = ",".join("?" * len(etf_event_tickers))
    c.execute(f"DELETE FROM clusterbomb_events WHERE ticker IN ({ph2})", etf_event_tickers)
    deleted = c.rowcount
    conn.commit()
    conn.close()
    print(f"[ETF] Purged {deleted} events for {len(etf_event_tickers)} ETF tickers: {etf_event_tickers}")
    return deleted


# ---------------------------------------------------------------------------
# Sector resolution — canonical GICS-aligned sector per ticker
# ---------------------------------------------------------------------------

_SECTOR_NORM = {
    "AI / Cloud": "Technology",
    "Cybersecurity": "Technology",
    "Semiconductors": "Technology",
    "Consumer Disc.": "Consumer Discretionary",
    "Healthcare": "Health Care",
    "Cons. Staples": "Consumer Staples",
    "Communication": "Communication Services",
}


def get_ticker_sector_map():
    """Build {ticker: sector} dict.  Primary: rs_rankings.  Fallback: SECTOR_GROUPS."""
    conn = _get_db()
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute("""
            SELECT ticker, sector FROM rs_rankings
            WHERE sector IS NOT NULL AND length(sector) > 0
              AND sector NOT IN ('Benchmarks', 'Large Cap', 'Sectors', '')
            GROUP BY ticker
        """).fetchall()
    except Exception:
        rows = []
    conn.close()
    sector_map = {r["ticker"]: r["sector"] for r in rows}

    # Fallback: SECTOR_GROUPS with normalised names
    for raw_sector, tickers in SECTOR_GROUPS.items():
        norm = _SECTOR_NORM.get(raw_sector, raw_sector)
        for t in tickers:
            if t not in sector_map:
                sector_map[t] = norm
    return sector_map


# ---------------------------------------------------------------------------
# Analysis page — river chart + calendar heatmap data
# ---------------------------------------------------------------------------

# ETFs / index funds to exclude from analysis charts (not individual stocks)
_ANALYSIS_EXCLUDE = {
    "SPY", "QQQ", "IWM", "DIA", "VOO", "VTI", "IVV",
    "XLF", "XLE", "XLK", "XLV", "XLI", "XLP", "XLU", "XLB", "XLRE", "XLC", "XLY",
    "ARKK", "ARKW", "ARKF", "ARKG",
    "TQQQ", "SQQQ", "SPXL", "SPXS", "SOXL", "SOXS", "UPRO", "SPXU",
    "TLT", "HYG", "GLD", "SLV", "USO", "VXX", "UVXY",
    "IAU",
}


def get_river_data(granularity="week", date_from=None, date_to=None,
                   metric="notional", min_notional=0, ticker_filter=None):
    """Aggregate sweep data by time bucket + sector for river/stream chart.

    Args:
        ticker_filter: optional set of tickers to include (None = all tickers).

    Returns {data: [[date, value, sector], ...], sectors: [...], granularity: str}.
    """
    from datetime import datetime as _dt, timedelta as _td
    sector_map = get_ticker_sector_map()
    conn = _get_db()
    conn.row_factory = sqlite3.Row

    where = []
    params = []
    if date_from:
        where.append("trade_date >= ?")
        params.append(date_from)
    if date_to:
        where.append("trade_date <= ?")
        params.append(date_to)
    if min_notional and float(min_notional) > 0:
        where.append("total_notional >= ?")
        params.append(float(min_notional))

    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    rows = conn.execute(f"""
        SELECT ticker, trade_date, total_notional, sweep_count
        FROM sweep_daily_summary {where_sql}
        ORDER BY trade_date
    """, params).fetchall()
    conn.close()

    # Group by (bucket, sector)
    from collections import defaultdict
    buckets = defaultdict(lambda: defaultdict(float))

    for r in rows:
        if r["ticker"] in _ANALYSIS_EXCLUDE:
            continue
        if ticker_filter is not None and r["ticker"] not in ticker_filter:
            continue
        sector = sector_map.get(r["ticker"], "Other")
        dt = _dt.strptime(r["trade_date"], "%Y-%m-%d")
        if granularity == "month":
            bucket = r["trade_date"][:7] + "-01"  # first of month
        else:
            monday = dt - _td(days=dt.weekday())
            bucket = monday.strftime("%Y-%m-%d")

        value = r["total_notional"] if metric == "notional" else r["sweep_count"]
        buckets[bucket][sector] += value

    # Build ECharts themeRiver format
    all_sectors = sorted(set(s for b in buckets.values() for s in b))
    data = []
    for bucket in sorted(buckets):
        for sector in all_sectors:
            val = buckets[bucket].get(sector, 0)
            if metric == "notional":
                val = round(val, 0)
            data.append([bucket, val, sector])

    return {"data": data, "sectors": all_sectors, "granularity": granularity}


def get_heatmap_data(year=2025, metric="notional", event_type="all"):
    """Get daily aggregated data for calendar heatmap.

    Returns {data: [[date, value], ...], year: int, metric: str}.
    """
    conn = _get_db()
    conn.row_factory = sqlite3.Row
    date_from = f"{year}-01-01"
    date_to = f"{year}-12-31"

    # Build ETF exclusion clause
    _excl_placeholders = ",".join("?" for _ in _ANALYSIS_EXCLUDE)
    _excl_list = list(_ANALYSIS_EXCLUDE)

    if metric == "events":
        where = ["event_date >= ?", "event_date <= ?",
                 f"ticker NOT IN ({_excl_placeholders})"]
        params = [date_from, date_to] + _excl_list
        if event_type != "all":
            where.append("COALESCE(event_type, 'clusterbomb') = ?")
            params.append(event_type)
        rows = conn.execute(f"""
            SELECT event_date as date, COUNT(*) as value,
                   SUM(total_notional) as notional
            FROM clusterbomb_events
            WHERE {' AND '.join(where)}
            GROUP BY event_date
        """, params).fetchall()
        data = [[r["date"], r["value"] or 0] for r in rows]
    else:
        rows = conn.execute(f"""
            SELECT trade_date as date,
                   SUM(total_notional) as notional,
                   SUM(sweep_count) as count
            FROM sweep_daily_summary
            WHERE trade_date >= ? AND trade_date <= ?
              AND ticker NOT IN ({_excl_placeholders})
            GROUP BY trade_date
        """, [date_from, date_to] + _excl_list).fetchall()
        if metric == "count":
            data = [[r["date"], r["count"] or 0] for r in rows]
        else:
            data = [[r["date"], round(r["notional"] or 0, 0)] for r in rows]

    conn.close()
    return {"data": data, "year": year, "metric": metric}


# Detection config file — allows UI to persist custom detection params
DETECTION_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "sweep_detection_config.json")


def get_detection_config():
    """Return detection config {stock: {...}, etf: {...}}.

    Dual-profile config for stock and ETF detection thresholds.
    Maintains backward compat with old flat config files on disk.
    """
    stock_defaults = {
        "min_sweeps": DEFAULT_CB_MIN_SWEEPS,       # 3
        "min_notional": DEFAULT_CB_MIN_NOTIONAL,    # $1M
        "min_total": DEFAULT_CB_MIN_TOTAL,          # $10M
        "rarity_days": DEFAULT_CB_RARITY_DAYS,      # 60
        "rare_min_notional": DEFAULT_CB_RARE_MIN_NOTIONAL,  # $1M — independent dormancy-break threshold
        "monster_min_notional": DEFAULT_MONSTER_MIN_NOTIONAL,  # $100M — single monster sweep
    }
    etf_defaults = {
        "min_sweeps": 1,
        "min_notional": 5_000_000,        # $5M — ETFs have higher volume
        "min_total": 75_000_000,          # $75M
        "rarity_days": 20,
        "rare_min_notional": 1_000_000,   # $1M
        "monster_min_notional": 250_000_000,  # $250M
    }
    config = {"stock": dict(stock_defaults), "etf": dict(etf_defaults)}
    try:
        with open(DETECTION_CONFIG_PATH, "r", encoding="utf-8") as f:
            saved = json.load(f)
        if "stock" in saved:
            config["stock"].update(saved["stock"])
        elif "min_sweeps" in saved:
            # Old flat format — treat as stock profile
            config["stock"].update(saved)
        if "etf" in saved:
            config["etf"].update(saved["etf"])
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    return config


def save_detection_config(params):
    """Save detection config to disk.

    Accepts either:
      {"stock": {...}, "etf": {...}}  — nested format
      {"stock": {...}}                — stock-only update
      {"etf": {...}}                  — etf-only update
      {"min_sweeps": 3, ...}          — legacy flat (treated as stock)
    """
    config = get_detection_config()
    _all_keys = ("min_sweeps", "min_notional", "min_total", "rarity_days",
                 "rare_min_notional", "monster_min_notional")
    if "stock" in params and isinstance(params["stock"], dict):
        for key in _all_keys:
            if key in params["stock"]:
                config["stock"][key] = params["stock"][key]
    elif "etf" not in params:
        # Legacy flat format — treat as stock
        for key in _all_keys:
            if key in params:
                config["stock"][key] = params[key]
    if "etf" in params and isinstance(params["etf"], dict):
        for key in _all_keys:
            if key in params["etf"]:
                config["etf"][key] = params["etf"][key]
    with open(DETECTION_CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2)
    return config


# Rate limiting
MAX_RETRIES = 3
RATE_LIMIT_WAIT = 2  # seconds between retries on 429

# ---------------------------------------------------------------------------
# Database setup
# ---------------------------------------------------------------------------

def _get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


def init_sweep_db():
    """Create sweep-related tables if they don't exist."""
    conn = _get_db()
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS sweep_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            trade_date TEXT NOT NULL,         -- YYYY-MM-DD
            trade_time TEXT NOT NULL,         -- HH:MM:SS.fff
            sip_timestamp INTEGER,            -- nanosecond unix ts from SIP
            price REAL NOT NULL,
            size INTEGER NOT NULL,
            notional REAL NOT NULL,           -- price * size
            exchange INTEGER,                 -- exchange ID (4 = FINRA TRF)
            trf_id INTEGER,                   -- TRF facility ID
            conditions TEXT,                  -- JSON array of condition codes
            is_sweep INTEGER DEFAULT 0,       -- 1 if intermarket sweep (condition 14)
            is_darkpool INTEGER DEFAULT 0,    -- 1 if exchange==4 + trf_id present
            fetched_at TEXT NOT NULL,         -- when we fetched this
            UNIQUE(ticker, sip_timestamp, price, size)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS clusterbomb_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            event_date TEXT NOT NULL,          -- YYYY-MM-DD
            sweep_count INTEGER NOT NULL,      -- number of qualifying sweeps
            total_notional REAL NOT NULL,      -- sum of notional across sweeps
            avg_price REAL,                    -- volume-weighted avg price
            min_price REAL,
            max_price REAL,
            direction TEXT,                    -- 'buy' / 'sell' / 'mixed' (inferred)
            is_rare INTEGER DEFAULT 0,         -- 1 if no sweeps N days before/after
            threshold_notional REAL,           -- min notional used for detection
            threshold_sweeps INTEGER,          -- min sweeps used for detection
            threshold_total REAL,              -- min total used for detection
            sweep_ids TEXT,                    -- JSON array of sweep_trade IDs
            detected_at TEXT NOT NULL,
            UNIQUE(ticker, event_date)
        )
    """)

    # --- Migration: fix old UNIQUE(ticker, event_date, threshold_notional, threshold_sweeps) ---
    # Check if old constraint still exists by looking for duplicates on (ticker, event_date)
    dupes = c.execute("""
        SELECT ticker, event_date, COUNT(*) as cnt
        FROM clusterbomb_events
        GROUP BY ticker, event_date
        HAVING cnt > 1
    """).fetchall()
    if dupes:
        print(f"Migrating clusterbomb_events: deduplicating {len(dupes)} ticker/date pairs...")
        # Keep the row with the highest total_notional for each (ticker, event_date)
        c.execute("""
            DELETE FROM clusterbomb_events
            WHERE id NOT IN (
                SELECT id FROM (
                    SELECT id, ROW_NUMBER() OVER (
                        PARTITION BY ticker, event_date
                        ORDER BY total_notional DESC
                    ) as rn
                    FROM clusterbomb_events
                ) WHERE rn = 1
            )
        """)
        conn.commit()
        deleted = sum(r[2] - 1 for r in dupes)
        print(f"  Removed {deleted} duplicate rows.")
        # Recreate table with correct UNIQUE constraint
        c.execute("""
            CREATE TABLE IF NOT EXISTS clusterbomb_events_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                event_date TEXT NOT NULL,
                sweep_count INTEGER NOT NULL,
                total_notional REAL NOT NULL,
                avg_price REAL,
                min_price REAL,
                max_price REAL,
                direction TEXT,
                is_rare INTEGER DEFAULT 0,
                threshold_notional REAL,
                threshold_sweeps INTEGER,
                threshold_total REAL,
                sweep_ids TEXT,
                detected_at TEXT NOT NULL,
                UNIQUE(ticker, event_date)
            )
        """)
        c.execute("""
            INSERT INTO clusterbomb_events_new
            (ticker, event_date, sweep_count, total_notional, avg_price,
             min_price, max_price, direction, is_rare, threshold_notional,
             threshold_sweeps, threshold_total, sweep_ids, detected_at)
            SELECT ticker, event_date, sweep_count, total_notional, avg_price,
                   min_price, max_price, direction, is_rare, threshold_notional,
                   threshold_sweeps, threshold_total, sweep_ids, detected_at
            FROM clusterbomb_events
        """)
        c.execute("DROP TABLE clusterbomb_events")
        c.execute("ALTER TABLE clusterbomb_events_new RENAME TO clusterbomb_events")
        conn.commit()
        print("  Table migrated to UNIQUE(ticker, event_date).")

    # --- Migration: add event_type column ---
    try:
        c.execute("ALTER TABLE clusterbomb_events ADD COLUMN event_type TEXT DEFAULT 'clusterbomb'")
        conn.commit()
        print("Added event_type column to clusterbomb_events.")
    except Exception:
        pass  # Column already exists

    # --- Migration: add forward return columns for backtest bias filtering ---
    existing_cols = {row[1] for row in c.execute("PRAGMA table_info(clusterbomb_events)").fetchall()}
    if "return_1w" not in existing_cols:
        c.execute("ALTER TABLE clusterbomb_events ADD COLUMN return_1w REAL")
        conn.commit()
        print("Added return_1w column to clusterbomb_events.")
    if "return_1m" not in existing_cols:
        c.execute("ALTER TABLE clusterbomb_events ADD COLUMN return_1m REAL")
        conn.commit()
        print("Added return_1m column to clusterbomb_events.")

    if "is_monster" not in existing_cols:
        c.execute("ALTER TABLE clusterbomb_events ADD COLUMN is_monster INTEGER DEFAULT 0")
        conn.commit()
        print("Added is_monster column to clusterbomb_events.")

    if "dormancy_days" not in existing_cols:
        c.execute("ALTER TABLE clusterbomb_events ADD COLUMN dormancy_days INTEGER DEFAULT NULL")
        conn.commit()
        print("Added dormancy_days column to clusterbomb_events.")

    if "max_notional" not in existing_cols:
        c.execute("ALTER TABLE clusterbomb_events ADD COLUMN max_notional REAL")
        conn.commit()
        print("Added max_notional column to clusterbomb_events.")

    if "sweep_rank" not in existing_cols:
        c.execute("ALTER TABLE clusterbomb_events ADD COLUMN sweep_rank INTEGER DEFAULT NULL")
        conn.commit()
        print("Added sweep_rank column to clusterbomb_events.")

    if "daily_rank" not in existing_cols:
        c.execute("ALTER TABLE clusterbomb_events ADD COLUMN daily_rank INTEGER DEFAULT NULL")
        conn.commit()
        print("Added daily_rank column to clusterbomb_events.")

    # Backfill max_notional for any events that still have NULL
    # (runs every startup until fully backfilled, handles crash recovery)
    null_count = c.execute(
        "SELECT COUNT(*) FROM clusterbomb_events WHERE max_notional IS NULL"
    ).fetchone()[0]
    if null_count > 0:
        print(f"Backfilling max_notional for {null_count} events...", flush=True)
        # Use a pre-aggregated temp table for speed instead of correlated subquery
        c.execute("""
            CREATE TEMP TABLE _max_notionals AS
            SELECT ticker, trade_date, MAX(notional) as max_n
            FROM sweep_trades
            WHERE is_darkpool = 1 AND is_sweep = 1
            GROUP BY ticker, trade_date
        """)
        c.execute("""
            UPDATE clusterbomb_events SET max_notional = (
                SELECT max_n FROM _max_notionals
                WHERE _max_notionals.ticker = clusterbomb_events.ticker
                AND _max_notionals.trade_date = clusterbomb_events.event_date
            ) WHERE max_notional IS NULL
        """)
        c.execute("DROP TABLE _max_notionals")
        conn.commit()
        filled = c.execute(
            "SELECT COUNT(*) FROM clusterbomb_events WHERE max_notional IS NOT NULL"
        ).fetchone()[0]
        print(f"  Backfilled max_notional for {filled} events.", flush=True)

    # One-time fix: correct max_notional where daily detection set it = total_notional.
    # For events with sweep_rank, max_notional should be the biggest individual trade,
    # not the day total. Only needs to run once — flag prevents re-running.
    _fix_done = c.execute(
        "SELECT value FROM sweep_meta WHERE key = 'max_notional_fix_v1'"
    ).fetchone() if c.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='sweep_meta'"
    ).fetchone() else None
    if not _fix_done:
        _bad = c.execute("""
            SELECT COUNT(*) FROM clusterbomb_events
            WHERE sweep_rank IS NOT NULL AND max_notional = total_notional
            AND total_notional > 0
        """).fetchone()[0]
        if _bad > 0:
            print(f"Fixing {_bad} events where max_notional was incorrectly set to total_notional...", flush=True)
            c.execute("""
                CREATE TEMP TABLE _true_max AS
                SELECT ticker, trade_date, MAX(notional) as max_n
                FROM sweep_trades
                WHERE is_darkpool = 1 AND is_sweep = 1
                GROUP BY ticker, trade_date
            """)
            c.execute("""
                UPDATE clusterbomb_events SET max_notional = (
                    SELECT max_n FROM _true_max
                    WHERE _true_max.ticker = clusterbomb_events.ticker
                    AND _true_max.trade_date = clusterbomb_events.event_date
                )
                WHERE sweep_rank IS NOT NULL
                AND max_notional = total_notional
                AND total_notional > 0
            """)
            _fixed = c.rowcount
            c.execute("DROP TABLE _true_max")
            conn.commit()
            print(f"  Fixed max_notional for {_fixed} events.", flush=True)

    c.execute("""
        CREATE TABLE IF NOT EXISTS sweep_meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    # Write flag so max_notional fix only runs once
    if not _fix_done:
        c.execute("INSERT OR REPLACE INTO sweep_meta (key, value) VALUES ('max_notional_fix_v1', '1')")
        conn.commit()

    # Track which ticker+date combos have been fetched (even if 0 sweeps found)
    c.execute("""
        CREATE TABLE IF NOT EXISTS sweep_fetch_log (
            ticker TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            sweeps_found INTEGER NOT NULL DEFAULT 0,
            total_trades INTEGER NOT NULL DEFAULT 0,
            fetched_at TEXT NOT NULL,
            PRIMARY KEY(ticker, trade_date)
        )
    """)

    # Indexes for fast lookups
    c.execute("CREATE INDEX IF NOT EXISTS idx_sweep_ticker_date ON sweep_trades(ticker, trade_date)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_sweep_date ON sweep_trades(trade_date)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_sweep_darkpool ON sweep_trades(is_darkpool, is_sweep)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_sweep_notional ON sweep_trades(notional)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_cb_ticker_date ON clusterbomb_events(ticker, event_date)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_cb_date ON clusterbomb_events(event_date)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_cb_rare ON clusterbomb_events(is_rare)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_cb_daily_rank ON clusterbomb_events(daily_rank)")
    # Covering index for detection queries (clusterbomb/rare/monster GROUP BY with notional filter)
    c.execute("CREATE INDEX IF NOT EXISTS idx_sweep_dp_notional ON sweep_trades(is_darkpool, is_sweep, notional, ticker, trade_date)")

    # --- Materialised stats cache (all-time totals updated incrementally) ---
    c.execute("""
        CREATE TABLE IF NOT EXISTS sweep_stats_cache (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            total_sweeps INTEGER DEFAULT 0,
            total_notional REAL DEFAULT 0,
            tickers_tracked INTEGER DEFAULT 0,
            date_min TEXT,
            date_max TEXT,
            last_rebuilt TEXT
        )
    """)
    # Ensure the single row exists
    c.execute("""
        INSERT OR IGNORE INTO sweep_stats_cache (id, total_sweeps, total_notional, tickers_tracked)
        VALUES (1, 0, 0, 0)
    """)

    # --- Pre-aggregated daily summary table ---
    c.execute("""
        CREATE TABLE IF NOT EXISTS sweep_daily_summary (
            ticker TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            sweep_count INTEGER NOT NULL DEFAULT 0,
            total_notional REAL NOT NULL DEFAULT 0,
            total_shares INTEGER NOT NULL DEFAULT 0,
            vwap REAL,
            min_price REAL,
            max_price REAL,
            first_sweep TEXT,
            last_sweep TEXT,
            PRIMARY KEY (ticker, trade_date)
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_sds_date ON sweep_daily_summary(trade_date)")

    conn.commit()
    conn.close()


def rebuild_stats_cache():
    """Full rebuild of sweep_stats_cache from sweep_trades. Call after bulk operations."""
    conn = _get_db()
    row = conn.execute(
        "SELECT COUNT(*), COALESCE(SUM(notional),0), COUNT(DISTINCT ticker), "
        "MIN(trade_date), MAX(trade_date) FROM sweep_trades "
        "WHERE is_darkpool=1 AND is_sweep=1"
    ).fetchone()
    conn.execute("""
        UPDATE sweep_stats_cache SET
            total_sweeps=?, total_notional=?, tickers_tracked=?,
            date_min=?, date_max=?, last_rebuilt=?
        WHERE id=1
    """, (row[0] or 0, row[1] or 0, row[2] or 0, row[3], row[4],
          datetime.now(timezone.utc).isoformat()))
    conn.commit()
    conn.close()
    print(f"  Stats cache rebuilt: {row[0] or 0} sweeps, {row[2] or 0} tickers", flush=True)


def rebuild_daily_summary():
    """Full rebuild of sweep_daily_summary from sweep_trades. Call after bulk operations."""
    conn = _get_db()
    conn.execute("DELETE FROM sweep_daily_summary")
    conn.execute("""
        INSERT INTO sweep_daily_summary
            (ticker, trade_date, sweep_count, total_notional, total_shares,
             vwap, min_price, max_price, first_sweep, last_sweep)
        SELECT
            ticker, trade_date,
            COUNT(*), SUM(notional), SUM(size),
            SUM(price * size) / SUM(size),
            MIN(price), MAX(price),
            MIN(trade_time), MAX(trade_time)
        FROM sweep_trades
        WHERE is_darkpool=1 AND is_sweep=1
        GROUP BY ticker, trade_date
    """)
    count = conn.execute("SELECT COUNT(*) FROM sweep_daily_summary").fetchone()[0]
    conn.commit()
    conn.close()
    print(f"  Daily summary rebuilt: {count} ticker-day rows", flush=True)


# ---------------------------------------------------------------------------
# API: Fetch trades from Massive.com (Polygon v3)
# ---------------------------------------------------------------------------

def _fetch_trades_page(ticker, date_str, cursor=None):
    """
    Fetch one page of tick-level trades for a ticker on a given date.
    Uses GET /v3/trades/{ticker}?timestamp=YYYY-MM-DD&limit=50000
    Returns (results_list, next_cursor_url).
    """
    if cursor:
        url = cursor
        params = {}
    else:
        url = f"{TRADES_BASE_URL}/trades/{ticker}"
        params = {
            "timestamp": date_str,
            "limit": 50000,
            "order": "asc",
            "sort": "timestamp",
        }

    headers = {"Authorization": f"Bearer {MASSIVE_API_KEY}"}

    for attempt in range(MAX_RETRIES):
        try:
            resp = _get_session().get(url, params=params, headers=headers, timeout=180)
            if resp.status_code == 403:
                data = resp.json()
                error_msg = data.get("error", data.get("message", "Not authorized"))
                print(f"    ❌ API 403 for {ticker}: {error_msg}")
                print(f"    → Tick-level trades require Massive.com Stocks Developer ($79/mo) or higher")
                print(f"    → Upgrade at https://massive.com/pricing")
                return [], None
            if resp.status_code == 429:
                wait = RATE_LIMIT_WAIT * (2 ** attempt)
                print(f"    Rate limited on trades, waiting {wait}s (attempt {attempt+1}/{MAX_RETRIES})")
                _time.sleep(wait)
                continue
            resp.raise_for_status()
            data = resp.json()
            results = data.get("results", [])
            next_url = data.get("next_url")
            return results, next_url
        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                _time.sleep(RATE_LIMIT_WAIT * (2 ** attempt))
                continue
            print(f"    API error fetching trades for {ticker} on {date_str}: {e}")
            return [], None

    return [], None


def check_api_access():
    """
    Quick check if the current API key has access to tick-level trade data.
    Returns dict with 'has_access' bool and 'message' string.
    """
    url = f"{TRADES_BASE_URL}/trades/SPY"
    params = {"limit": 1}
    headers = {"Authorization": f"Bearer {MASSIVE_API_KEY}"}

    try:
        resp = _get_session().get(url, params=params, headers=headers, timeout=10)
        if resp.status_code == 200:
            return {"has_access": True, "message": "Trades API accessible"}
        elif resp.status_code == 403:
            return {
                "has_access": False,
                "message": "Tick-level trades require Massive.com Stocks Developer ($79/mo) or higher. "
                           "Current plan only supports OHLC aggregates. Upgrade at massive.com/pricing"
            }
        else:
            return {"has_access": False, "message": f"API returned status {resp.status_code}"}
    except Exception as e:
        return {"has_access": False, "message": f"API check failed: {e}"}


MAX_PAGES_PER_FETCH = 200  # Safety cap: 200 pages × 50k = 10M trades max

def fetch_sweep_trades(ticker, date_str, progress_callback=None):
    """
    Fetch all trades for a ticker on a date, filter for dark pool sweeps.
    Returns list of filtered trade dicts ready for DB insertion.

    A qualifying trade must have:
      - exchange == 4 (FINRA TRF / dark pool)
      - condition 14 in conditions (Intermarket Sweep Order)
    """
    all_sweeps = []
    page = 0
    total_trades = 0
    cursor = None
    fetched_at = datetime.now(timezone.utc).isoformat()
    _page_t0 = _time.time()

    while True:
        page_start = _time.time()
        results, next_cursor = _fetch_trades_page(ticker, date_str, cursor)
        page_ms = int((_time.time() - page_start) * 1000)
        if not results:
            break

        total_trades += len(results)
        page += 1
        # Per-page progress — only show for multi-page fetches (3+ pages)
        if page >= 3:
            print(f"    📡 {ticker} {date_str} pg{page}: {total_trades:,} trades ({page_ms}ms)", flush=True)

        for trade in results:
            exchange = trade.get("exchange", 0)
            conditions = trade.get("conditions", [])
            trf_id = trade.get("trf_id")
            price = trade.get("price", 0)
            size = trade.get("size", 0)

            is_darkpool = (exchange == FINRA_TRF_EXCHANGE_ID and trf_id is not None)
            is_sweep = (INTERMARKET_SWEEP_CONDITION in conditions)

            # We want dark pool sweeps with meaningful notional
            notional = price * size
            if is_darkpool and is_sweep and notional >= MIN_SWEEP_NOTIONAL:
                sip_ts = trade.get("sip_timestamp", 0)

                # Convert nanosecond timestamp to human-readable time
                if sip_ts > 1e18:
                    trade_dt = datetime.fromtimestamp(sip_ts / 1e9, tz=timezone.utc)
                elif sip_ts > 1e15:
                    trade_dt = datetime.fromtimestamp(sip_ts / 1e6, tz=timezone.utc)
                elif sip_ts > 1e12:
                    trade_dt = datetime.fromtimestamp(sip_ts / 1e3, tz=timezone.utc)
                else:
                    trade_dt = datetime.fromtimestamp(sip_ts, tz=timezone.utc)

                all_sweeps.append({
                    "ticker": ticker,
                    "trade_date": date_str,
                    "trade_time": trade_dt.strftime("%H:%M:%S.%f")[:-3],
                    "sip_timestamp": sip_ts,
                    "price": price,
                    "size": size,
                    "notional": round(price * size, 2),
                    "exchange": exchange,
                    "trf_id": trf_id,
                    "conditions": json.dumps(conditions),
                    "is_sweep": 1,
                    "is_darkpool": 1,
                    "fetched_at": fetched_at,
                })

        if progress_callback:
            progress_callback(page, total_trades, len(all_sweeps))

        if not next_cursor:
            break
        if page >= MAX_PAGES_PER_FETCH:
            print(f"    ⚠️  {ticker} {date_str}: hit {MAX_PAGES_PER_FETCH}-page cap at {total_trades:,} trades — skipping rest", flush=True)
            break
        cursor = next_cursor

    # Only print per-day summary if sweeps were found or it was a big fetch
    if len(all_sweeps) > 0 or page >= 3:
        print(f"    {ticker} {date_str}: {total_trades:,} trades, "
              f"{len(all_sweeps)} sweeps ({page} pg)", flush=True)
    return all_sweeps, total_trades


def store_sweep_trades(trades):
    """Insert sweep trades into DB, ignoring duplicates."""
    if not trades:
        return 0

    conn = _get_db()
    c = conn.cursor()
    inserted = 0

    for t in trades:
        try:
            c.execute("""
                INSERT OR IGNORE INTO sweep_trades
                (ticker, trade_date, trade_time, sip_timestamp, price, size, notional,
                 exchange, trf_id, conditions, is_sweep, is_darkpool, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                t["ticker"], t["trade_date"], t["trade_time"], t["sip_timestamp"],
                t["price"], t["size"], t["notional"], t["exchange"], t["trf_id"],
                t["conditions"], t["is_sweep"], t["is_darkpool"], t["fetched_at"]
            ))
            if c.rowcount > 0:
                inserted += 1
        except sqlite3.IntegrityError:
            pass

    conn.commit()
    conn.close()
    return inserted


def _log_fetch(ticker, date_str, sweeps_found, total_trades):
    """Record that we've fetched this ticker+date (even if 0 sweeps found)."""
    conn = _get_db()
    conn.execute("""
        INSERT OR REPLACE INTO sweep_fetch_log
        (ticker, trade_date, sweeps_found, total_trades, fetched_at)
        VALUES (?, ?, ?, ?, ?)
    """, (ticker, date_str, sweeps_found, total_trades,
          datetime.now(timezone.utc).isoformat()))
    conn.commit()
    conn.close()


def _is_fetched(ticker, date_str):
    """Check if we've already fetched this ticker+date (via fetch log)."""
    conn = _get_db()
    row = conn.execute(
        "SELECT 1 FROM sweep_fetch_log WHERE ticker=? AND trade_date=?",
        (ticker, date_str)
    ).fetchone()
    conn.close()
    return row is not None


# Max parallel API workers — sweet spot balances concurrency vs API response time.
# Too many causes server-side backpressure → 90-110s per page for big tickers.
# 30 workers = less contention, potentially faster per-page response.
MAX_FETCH_WORKERS = 8
# Stagger: submit work in waves of this size with a brief gap between
_SUBMIT_WAVE_SIZE = 8


def _fetch_one_ticker_date(ticker, date_str):
    """Fetch sweeps for a single ticker/date (API only, no DB write).
    Returns raw data for the main thread to batch-write to DB."""
    sweeps, total_trades = fetch_sweep_trades(ticker, date_str)
    return ticker, date_str, sweeps, total_trades


def _batch_store(conn, trades_batch, log_batch):
    """Write accumulated trades + fetch-log entries in one transaction.
    Also incrementally updates sweep_stats_cache and sweep_daily_summary."""
    c = conn.cursor()
    inserted = 0
    batch_notional = 0.0
    batch_tickers = set()
    batch_dates = set()
    # Track which (ticker, date) pairs got new inserts for daily summary update
    _dirty_pairs = set()

    for t in trades_batch:
        try:
            c.execute("""
                INSERT OR IGNORE INTO sweep_trades
                (ticker, trade_date, trade_time, sip_timestamp, price, size, notional,
                 exchange, trf_id, conditions, is_sweep, is_darkpool, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                t["ticker"], t["trade_date"], t["trade_time"], t["sip_timestamp"],
                t["price"], t["size"], t["notional"], t["exchange"], t["trf_id"],
                t["conditions"], t["is_sweep"], t["is_darkpool"], t["fetched_at"]
            ))
            if c.rowcount > 0:
                inserted += 1
                batch_notional += t["notional"]
                batch_tickers.add(t["ticker"])
                batch_dates.add(t["trade_date"])
                _dirty_pairs.add((t["ticker"], t["trade_date"]))
        except sqlite3.IntegrityError:
            pass

    for ticker, date_str, sweeps_found, total_trades in log_batch:
        c.execute("""
            INSERT OR REPLACE INTO sweep_fetch_log
            (ticker, trade_date, sweeps_found, total_trades, fetched_at)
            VALUES (?, ?, ?, ?, ?)
        """, (ticker, date_str, sweeps_found, total_trades,
              datetime.now(timezone.utc).isoformat()))

    # --- Incremental stats cache update ---
    if inserted > 0:
        # Update counts; tickers_tracked needs a recount since we can't track unique incrementally
        # But we can do a fast count only on the new tickers set
        c.execute("""
            UPDATE sweep_stats_cache SET
                total_sweeps = total_sweeps + ?,
                total_notional = total_notional + ?,
                date_min = CASE WHEN date_min IS NULL OR ? < date_min THEN ? ELSE date_min END,
                date_max = CASE WHEN date_max IS NULL OR ? > date_max THEN ? ELSE date_max END
            WHERE id=1
        """, (inserted, batch_notional,
              min(batch_dates), min(batch_dates),
              max(batch_dates), max(batch_dates)))
        # Recount distinct tickers (fast — just counts distinct in the whole table)
        row = c.execute(
            "SELECT COUNT(DISTINCT ticker) FROM sweep_trades WHERE is_darkpool=1 AND is_sweep=1"
        ).fetchone()
        c.execute("UPDATE sweep_stats_cache SET tickers_tracked=? WHERE id=1", (row[0],))

    # --- Incremental daily summary update for dirty pairs ---
    for tk, dt in _dirty_pairs:
        c.execute("""
            INSERT OR REPLACE INTO sweep_daily_summary
                (ticker, trade_date, sweep_count, total_notional, total_shares,
                 vwap, min_price, max_price, first_sweep, last_sweep)
            SELECT
                ticker, trade_date,
                COUNT(*), SUM(notional), SUM(size),
                SUM(price * size) / SUM(size),
                MIN(price), MAX(price),
                MIN(trade_time), MAX(trade_time)
            FROM sweep_trades
            WHERE ticker=? AND trade_date=? AND is_darkpool=1 AND is_sweep=1
        """, (tk, dt))

    conn.commit()
    return inserted


# How many completed results to accumulate before flushing to DB
# Keep low so server crashes don't lose too much buffered data
_DB_FLUSH_INTERVAL = 5


def fetch_and_store_sweeps(tickers, start_date, end_date=None, progress_callback=None, cancel_event=None):
    """
    Fetch dark pool sweep data for multiple tickers over a date range.
    Stores results in DB. Returns summary stats.

    Architecture: 150 worker threads do HTTP fetches in parallel.
    The main thread collects results and batch-writes to SQLite every
    ~50 completions, eliminating DB write contention between threads.

    Args:
        tickers: list of ticker symbols
        start_date: "YYYY-MM-DD" start
        end_date: "YYYY-MM-DD" end (defaults to today)
        progress_callback: fn(ticker, date, sweeps_found, total_so_far)
    """
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    init_sweep_db()

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    total_sweeps = 0
    total_inserted = 0
    stats = {"tickers": len(tickers), "dates": 0, "sweeps_found": 0, "inserted": 0}

    current = start_dt
    dates = []
    while current <= end_dt:
        # Skip weekends
        if current.weekday() < 5:
            dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    # Build work items, skipping already-fetched combos
    # Bulk-load fetch log into a set (one query instead of N×D individual checks)
    conn_check = _get_db()
    placeholders = ",".join("?" for _ in tickers)
    rows = conn_check.execute(
        f"SELECT ticker, trade_date FROM sweep_fetch_log WHERE ticker IN ({placeholders})",
        tickers
    ).fetchall()
    conn_check.close()
    fetched_set = set((r[0], r[1]) for r in rows)
    print(f"  Loaded {len(fetched_set)} fetch-log entries for skip check", flush=True)

    # Today's date should ALWAYS be re-fetched (market day may still be open,
    # or the previous fetch ran pre-market and found nothing).
    # Historical dates respect the fetch log — no redundant re-fetching.
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    today_refetch = 0

    work = []
    skipped = 0
    for ticker in tickers:
        for date_str in dates:
            if (ticker, date_str) in fetched_set:
                if date_str == today_str:
                    work.append((ticker, date_str))
                    today_refetch += 1
                else:
                    skipped += 1
                continue
            work.append((ticker, date_str))
    if today_refetch:
        print(f"  Re-fetching {today_refetch} ticker(s) for today ({today_str})", flush=True)

    stats["dates"] = len(dates)
    total_jobs = len(work)
    print(f"\nFetching sweeps: {len(tickers)} tickers × {len(dates)} trading days", flush=True)
    print(f"  {skipped} already fetched, {total_jobs} remaining ({MAX_FETCH_WORKERS} workers)", flush=True)

    if total_jobs == 0:
        print("  Nothing to fetch — all ticker+date combos already in fetch log.", flush=True)
        stats["sweeps_found"] = 0
        stats["inserted"] = 0
        stats["skipped"] = skipped
        return stats

    completed = 0
    # Accumulate for batch DB writes (main thread only — no contention)
    _trades_buf = []
    _log_buf = []
    db_conn = _get_db()
    _t0 = _time.time()

    # Bounded submission: keep at most SUBMIT_AHEAD futures queued at a time
    # to avoid OOM / multi-minute stalls when work list is millions of items.
    SUBMIT_AHEAD = max(MAX_FETCH_WORKERS * 4, 64)  # e.g. 32-64 pending futures

    with ThreadPoolExecutor(max_workers=MAX_FETCH_WORKERS) as pool:
        work_iter = iter(work)
        futures = {}
        cancelled = False

        # Seed the initial batch
        for _ in range(min(SUBMIT_AHEAD, total_jobs)):
            t, d = next(work_iter)
            f = pool.submit(_fetch_one_ticker_date, t, d)
            futures[f] = (t, d)
        print(f"  Streaming jobs ({SUBMIT_AHEAD} ahead, {MAX_FETCH_WORKERS} workers)...", flush=True)

        while futures:
            # Check for cancellation
            if cancel_event and cancel_event.is_set():
                if _trades_buf or _log_buf:
                    n_ins = _batch_store(db_conn, _trades_buf, _log_buf)
                    total_inserted += n_ins
                    _trades_buf.clear()
                    _log_buf.clear()
                for f in futures:
                    f.cancel()
                print(f"\n  Fetch cancelled after {completed}/{total_jobs} jobs.", flush=True)
                cancelled = True
                break

            # Wait for any one future to complete (with timeout so we can check cancel)
            done = set()
            for f in list(futures):
                if f.done():
                    done.add(f)
            if not done:
                _time.sleep(0.05)
                continue

            for future in done:
                t_d = futures.pop(future)
                try:
                    ticker, date_str, sweeps, n_total_trades = future.result()
                    n_sweeps = len(sweeps)
                    total_sweeps += n_sweeps
                    completed += 1

                    # Accumulate for batch write
                    _trades_buf.extend(sweeps)
                    _log_buf.append((ticker, date_str, n_sweeps, n_total_trades))

                    # Flush to DB periodically
                    if len(_log_buf) >= _DB_FLUSH_INTERVAL or completed == total_jobs:
                        n_ins = _batch_store(db_conn, _trades_buf, _log_buf)
                        total_inserted += n_ins
                        _trades_buf.clear()
                        _log_buf.clear()

                    # Print progress — every 25th job, or when sweeps found
                    elapsed = _time.time() - _t0
                    rate = completed / elapsed if elapsed > 0 else 0
                    if n_sweeps > 0 or completed % 25 == 0 or completed == total_jobs:
                        print(f"  [{completed}/{total_jobs}] {ticker} {date_str} — "
                              f"{n_sweeps} sweeps  ({rate:.1f}/s, {total_sweeps} total)", flush=True)
                    if progress_callback:
                        progress_callback(ticker, date_str, n_sweeps, total_sweeps,
                                          completed, total_jobs)
                except Exception as e:
                    print(f"    ⚠ Error fetching {t_d[0]} {t_d[1]}: {e}", flush=True)
                    completed += 1

                # Submit next job to keep the pipeline full
                try:
                    nt, nd = next(work_iter)
                    nf = pool.submit(_fetch_one_ticker_date, nt, nd)
                    futures[nf] = (nt, nd)
                except StopIteration:
                    pass  # no more work to submit

    elapsed = _time.time() - _t0
    print(f"\n  Done: {completed} jobs in {elapsed:.1f}s ({completed/elapsed:.1f} jobs/s)", flush=True)

    # Final flush in case anything remains
    if _trades_buf or _log_buf:
        n_ins = _batch_store(db_conn, _trades_buf, _log_buf)
        total_inserted += n_ins
    db_conn.close()

    stats["sweeps_found"] = total_sweeps
    stats["inserted"] = total_inserted
    stats["skipped"] = skipped

    # Update meta
    conn = _get_db()
    conn.execute(
        "INSERT OR REPLACE INTO sweep_meta (key, value) VALUES (?, ?)",
        ("last_fetch", json.dumps({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tickers": tickers,
            "start_date": start_date,
            "end_date": end_date,
            "stats": stats,
        }))
    )
    conn.commit()
    conn.close()

    # Backfill daily OHLC price data for tickers that need it
    _backfill_prices(tickers)

    print(f"\nSweep fetch complete: {total_sweeps} sweeps found, {total_inserted} new rows inserted", flush=True)
    return stats


def _backfill_prices(tickers):
    """
    Ensure daily price CSVs exist for all tickers. Uses data_fetcher's
    fetch_with_cache which does incremental updates (only fetches new bars).
    """
    try:
        from data_fetcher import fetch_with_cache as df_fetch
    except ImportError:
        print("  Warning: data_fetcher not available, skipping price backfill")
        return

    need_backfill = []
    for ticker in tickers:
        safe = ticker.replace("/", "_").replace(":", "_")
        path = os.path.join(PRICE_CACHE_DIR, f"{safe}_day.csv")
        if not os.path.exists(path):
            need_backfill.append(ticker)
        else:
            # Check if stale (older than 24h)
            age_hours = (_time.time() - os.path.getmtime(path)) / 3600
            if age_hours > 24:
                need_backfill.append(ticker)

    if not need_backfill:
        print(f"  Price data: all {len(tickers)} tickers up to date")
        return

    print(f"\n  Backfilling daily prices for {len(need_backfill)} tickers...")
    for i, ticker in enumerate(need_backfill):
        try:
            df_fetch(ticker, "day")
            print(f"    [{i+1}/{len(need_backfill)}] {ticker} ✓")
        except Exception as e:
            print(f"    [{i+1}/{len(need_backfill)}] {ticker} ✗ {e}")
        _time.sleep(0.15)  # gentle rate limiting on aggs endpoint


# ---------------------------------------------------------------------------
# Clusterbomb Detection
# ---------------------------------------------------------------------------

def detect_clusterbombs(
    min_sweeps=DEFAULT_CB_MIN_SWEEPS,
    min_notional=DEFAULT_CB_MIN_NOTIONAL,
    min_total=DEFAULT_CB_MIN_TOTAL,
    rarity_days=DEFAULT_CB_RARITY_DAYS,
    rare_min_notional=None,
    ticker=None,
    tickers=None,
    date_from=None,
    date_to=None,
    exclude_etfs=True,
    etf_only=False,
):
    """
    Scan sweep_trades for clusterbomb events.

    A clusterbomb is triggered when a single ticker has >= min_sweeps dark pool
    sweeps in a single day, each >= min_notional, with combined total >= min_total.

    "Rare" clusterbombs additionally have no sweeps meeting the threshold for
    rarity_days before and after the event date.

    Args:
        tickers: optional list of tickers to include (for dual-profile detection)

    Returns list of detected events and stores them in clusterbomb_events table.
    """
    conn = _get_db()
    c = conn.cursor()

    # Build query to get qualifying sweep trades
    where = ["is_darkpool = 1", "is_sweep = 1", f"notional >= {min_notional}"]
    params = []

    if ticker:
        where.append("ticker = ?")
        params.append(ticker)
    if tickers:
        placeholders = ",".join("?" * len(tickers))
        where.append(f"ticker IN ({placeholders})")
        params.extend(tickers)
    if date_from:
        where.append("trade_date >= ?")
        params.append(date_from)
    if date_to:
        where.append("trade_date <= ?")
        params.append(date_to)

    where_sql = " AND ".join(where)

    # Group by ticker + date, aggregate
    rows = c.execute(f"""
        SELECT
            ticker,
            trade_date,
            COUNT(*) as sweep_count,
            SUM(notional) as total_notional,
            MAX(notional) as max_notional,
            SUM(price * size) / SUM(size) as vwap,
            MIN(price) as min_price,
            MAX(price) as max_price,
            GROUP_CONCAT(id) as sweep_ids
        FROM sweep_trades
        WHERE {where_sql}
        GROUP BY ticker, trade_date
        HAVING COUNT(*) >= ? AND SUM(notional) >= ?
        ORDER BY trade_date DESC, total_notional DESC
    """, params + [min_sweeps, min_total]).fetchall()

    # Filter ETF tickers
    if etf_only:
        etf_tickers = load_etf_set()
        rows = [r for r in rows if r["ticker"] in etf_tickers] if etf_tickers else []
    elif exclude_etfs:
        etf_tickers = load_etf_set()
        if etf_tickers:
            rows = [r for r in rows if r["ticker"] not in etf_tickers]

    events = []
    detected_at = datetime.now(timezone.utc).isoformat()

    for row in rows:
        row_ticker = row["ticker"]
        event_date = row["trade_date"]
        sweep_ids = row["sweep_ids"].split(",")

        # Determine direction from individual sweeps
        # (simplified: compare VWAP to open/close if available, else "unknown")
        direction = _infer_sweep_direction(conn, sweep_ids)

        # Check rarity: no sweeps meeting threshold N days before or after
        # Use rare_min_notional if provided, else fall back to min_notional
        _rare_notional = rare_min_notional if rare_min_notional is not None else min_notional
        is_rare = 0
        if rarity_days > 0:
            is_rare = _check_rarity(conn, row_ticker, event_date,
                                    _rare_notional, rarity_days)

        event = {
            "ticker": row_ticker,
            "event_date": event_date,
            "sweep_count": row["sweep_count"],
            "total_notional": round(row["total_notional"], 2),
            "max_notional": round(row["max_notional"], 2) if row["max_notional"] else None,
            "avg_price": round(row["vwap"], 2) if row["vwap"] else None,
            "min_price": row["min_price"],
            "max_price": row["max_price"],
            "direction": direction,
            "is_rare": is_rare,
            "threshold_notional": min_notional,
            "threshold_sweeps": min_sweeps,
            "threshold_total": min_total,
            "sweep_ids": json.dumps([int(x) for x in sweep_ids]),
            "detected_at": detected_at,
        }
        events.append(event)

    # Store events
    for ev in events:
        try:
            c.execute("""
                INSERT OR REPLACE INTO clusterbomb_events
                (ticker, event_date, sweep_count, total_notional, max_notional,
                 avg_price, min_price, max_price, direction, is_rare,
                 threshold_notional, threshold_sweeps, threshold_total,
                 sweep_ids, detected_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                ev["ticker"], ev["event_date"], ev["sweep_count"],
                ev["total_notional"], ev["max_notional"], ev["avg_price"],
                ev["min_price"], ev["max_price"], ev["direction"], ev["is_rare"],
                ev["threshold_notional"], ev["threshold_sweeps"],
                ev["threshold_total"], ev["sweep_ids"], ev["detected_at"]
            ))
        except sqlite3.IntegrityError:
            pass

    conn.commit()
    conn.close()

    print(f"Clusterbomb detection: {len(events)} events found "
          f"({sum(1 for e in events if e['is_rare'])} rare)")
    return events


def _infer_sweep_direction(conn, sweep_ids):
    """
    Infer whether sweeps are buy-side or sell-side.
    Uses trade conditions and price action as heuristics.
    Returns 'buy', 'sell', or 'mixed'.
    """
    if not sweep_ids:
        return "mixed"

    placeholders = ",".join("?" * len(sweep_ids))
    rows = conn.execute(f"""
        SELECT price, size, conditions, trade_time
        FROM sweep_trades
        WHERE id IN ({placeholders})
        ORDER BY trade_time ASC
    """, [int(x) for x in sweep_ids]).fetchall()

    if not rows:
        return "mixed"

    # Simple heuristic: if price trend during sweeps is upward, likely buy-side
    prices = [r["price"] for r in rows]
    if len(prices) >= 2:
        first_half_avg = sum(prices[:len(prices)//2]) / max(1, len(prices)//2)
        second_half_avg = sum(prices[len(prices)//2:]) / max(1, len(prices) - len(prices)//2)
        pct_change = (second_half_avg - first_half_avg) / first_half_avg * 100

        if pct_change > 0.1:
            return "buy"
        elif pct_change < -0.1:
            return "sell"

    return "mixed"


def _check_rarity(conn, ticker, event_date, min_notional, rarity_days):
    """
    Check if there are no qualifying sweeps for N trading days
    before the event date.  Returns the dormancy gap in calendar days
    if rare, or 0 if not rare.

    IMPORTANT: only marks rare if the fetch_log confirms we actually have
    data covering the full lookback window.  Without this guard, every
    ticker's first clusterbomb would appear "rare" simply because we
    haven't fetched earlier data.
    """
    event_dt = datetime.strptime(event_date, "%Y-%m-%d")
    calendar_lookback = int(rarity_days * 1.5)   # trading→calendar days
    before_dt = event_dt - timedelta(days=calendar_lookback)
    before_str = before_dt.strftime("%Y-%m-%d")

    # Guard: do we have fetch_log entries covering the start of the window?
    # We require at least one fetched date within the first week of the
    # lookback window.  If not, we don't have enough history to judge.
    guard_end = (before_dt + timedelta(days=7)).strftime("%Y-%m-%d")
    has_early_data = conn.execute("""
        SELECT COUNT(*) FROM sweep_fetch_log
        WHERE ticker = ? AND trade_date >= ? AND trade_date <= ?
    """, (ticker, before_str, guard_end)).fetchone()[0]

    if has_early_data == 0:
        return 0   # insufficient history — can't declare rare

    # Find the most recent qualifying sweep date before this event
    prev_row = conn.execute("""
        SELECT MAX(trade_date) FROM sweep_trades
        WHERE ticker = ? AND trade_date < ?
        AND is_darkpool = 1 AND is_sweep = 1 AND notional >= ?
    """, (ticker, event_date, min_notional)).fetchone()
    prev_date = prev_row[0] if prev_row else None

    if prev_date:
        # There was a prior qualifying sweep — check if it's outside the window
        prev_dt = datetime.strptime(prev_date, "%Y-%m-%d")
        gap_days = (event_dt - prev_dt).days
        if gap_days >= int(rarity_days * 1.5):  # calendar-day equivalent
            return gap_days
        else:
            return 0  # not rare — prior sweep within window
    else:
        # No prior qualifying sweep at all — use distance from earliest fetched date
        earliest = conn.execute("""
            SELECT MIN(trade_date) FROM sweep_fetch_log WHERE ticker = ?
        """, (ticker,)).fetchone()
        if earliest and earliest[0]:
            gap_days = (event_dt - datetime.strptime(earliest[0], "%Y-%m-%d")).days
            return gap_days if gap_days >= int(rarity_days * 1.5) else 0
        return 0


def detect_rare_sweep_days(min_notional=1_000_000, rarity_days=60, tickers=None,
                           date_from=None, date_to=None, exclude_etfs=True,
                           etf_only=False):
    """
    Detect days where a ticker had sweep activity after a long quiet period,
    regardless of whether it meets clusterbomb thresholds (min_sweeps / min_total).

    A "rare sweep day" = any day with qualifying sweeps where the same ticker had
    NO qualifying sweep activity for rarity_days calendar days prior.

    Stored as event_type='rare_sweep' in clusterbomb_events.
    Existing clusterbombs on the same (ticker, date) are NOT overwritten
    (INSERT OR IGNORE respects UNIQUE(ticker, event_date)).

    Returns list of detected rare sweep events.
    """
    conn = _get_db()
    c = conn.cursor()

    where = ["is_darkpool = 1", "is_sweep = 1", f"notional >= {min_notional}"]
    params = []
    if tickers:
        placeholders = ",".join("?" * len(tickers))
        where.append(f"ticker IN ({placeholders})")
        params.extend(tickers)
    if date_from:
        where.append("trade_date >= ?")
        params.append(date_from)
    if date_to:
        where.append("trade_date <= ?")
        params.append(date_to)
    where_sql = " AND ".join(where)

    # Get all ticker+date sweep activity (scoped to date range if provided)
    rows = c.execute(f"""
        SELECT ticker, trade_date,
               COUNT(*) as sweep_count,
               SUM(notional) as total_notional,
               MAX(notional) as max_notional,
               SUM(price * size) / SUM(size) as vwap,
               MIN(price) as min_price,
               MAX(price) as max_price
        FROM sweep_trades
        WHERE {where_sql}
        GROUP BY ticker, trade_date
        ORDER BY ticker, trade_date
    """, params).fetchall()

    # Filter ETF tickers
    if etf_only:
        etf_tickers = load_etf_set()
        rows = [r for r in rows if r["ticker"] in etf_tickers] if etf_tickers else []
    elif exclude_etfs:
        etf_tickers = load_etf_set()
        if etf_tickers:
            rows = [r for r in rows if r["ticker"] not in etf_tickers]

    # Group by ticker
    from collections import defaultdict
    ticker_dates = defaultdict(list)
    for row in rows:
        ticker_dates[row["ticker"]].append(row)

    events = []
    detected_at = datetime.now(timezone.utc).isoformat()

    for ticker, date_rows in ticker_dates.items():
        # Sort by date (should already be sorted from SQL)
        date_rows.sort(key=lambda r: r["trade_date"])

        for i, row in enumerate(date_rows):
            event_date = row["trade_date"]

            # Check rarity — returns dormancy gap in days (0 = not rare)
            dormancy = _check_rarity(conn, ticker, event_date, min_notional, rarity_days)
            if not dormancy:
                continue

            events.append({
                "ticker": ticker,
                "event_date": event_date,
                "sweep_count": row["sweep_count"],
                "total_notional": round(row["total_notional"], 2),
                "max_notional": round(row["max_notional"], 2) if row["max_notional"] else None,
                "avg_price": round(row["vwap"], 2) if row["vwap"] else None,
                "min_price": row["min_price"],
                "max_price": row["max_price"],
                "direction": "mixed",
                "is_rare": 1,
                "event_type": "rare_sweep",
                "dormancy_days": dormancy,
            })

    # Store — INSERT OR IGNORE so existing CB events take priority
    for ev in events:
        try:
            c.execute("""
                INSERT OR IGNORE INTO clusterbomb_events
                (ticker, event_date, sweep_count, total_notional, max_notional,
                 avg_price, min_price, max_price, direction, is_rare, event_type,
                 threshold_notional, threshold_sweeps, threshold_total,
                 sweep_ids, detected_at, dormancy_days)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                ev["ticker"], ev["event_date"], ev["sweep_count"],
                ev["total_notional"], ev["max_notional"],
                ev["avg_price"], ev["min_price"],
                ev["max_price"], ev["direction"], ev["is_rare"],
                ev["event_type"], min_notional, 0, 0, "[]", detected_at,
                ev.get("dormancy_days")
            ))
        except sqlite3.IntegrityError:
            pass

    conn.commit()
    conn.close()

    print(f"Rare sweep detection: {len(events)} rare sweep days found")
    return events


def detect_monster_sweeps(monster_min_notional=DEFAULT_MONSTER_MIN_NOTIONAL, tickers=None,
                          date_from=None, date_to=None, exclude_etfs=True,
                          etf_only=False):
    """
    Detect days where a single dark pool sweep exceeds the monster threshold.

    Two-step approach:
    1. UPDATE existing clusterbomb_events to set is_monster=1 where any individual
       sweep on that day exceeds the threshold.
    2. INSERT new events for days not already in the table (standalone monster days
       that didn't meet CB thresholds).

    This means a clusterbomb can also be a monster — both flags coexist.
    """
    if monster_min_notional is None:
        return []

    conn = _get_db()
    c = conn.cursor()

    where = ["is_darkpool = 1", "is_sweep = 1", f"notional >= {monster_min_notional}"]
    params = []
    if tickers:
        placeholders = ",".join("?" * len(tickers))
        where.append(f"ticker IN ({placeholders})")
        params.extend(tickers)
    if date_from:
        where.append("trade_date >= ?")
        params.append(date_from)
    if date_to:
        where.append("trade_date <= ?")
        params.append(date_to)

    where_sql = " AND ".join(where)

    # Find all ticker+date combos with at least one monster-sized sweep
    rows = c.execute(f"""
        SELECT
            ticker,
            trade_date,
            COUNT(*) as sweep_count,
            SUM(notional) as total_notional,
            MAX(notional) as max_notional,
            SUM(price * size) / SUM(size) as vwap,
            MIN(price) as min_price,
            MAX(price) as max_price,
            GROUP_CONCAT(id) as sweep_ids
        FROM sweep_trades
        WHERE {where_sql}
        GROUP BY ticker, trade_date
        ORDER BY trade_date DESC, total_notional DESC
    """, params).fetchall()

    # Filter ETF tickers
    if etf_only:
        etf_tickers = load_etf_set()
        rows = [r for r in rows if r["ticker"] in etf_tickers] if etf_tickers else []
    elif exclude_etfs:
        etf_tickers = load_etf_set()
        if etf_tickers:
            rows = [r for r in rows if r["ticker"] not in etf_tickers]

    updated = 0
    inserted = 0
    detected_at = datetime.now(timezone.utc).isoformat()
    events = []

    for row in rows:
        ticker = row["ticker"]
        event_date = row["trade_date"]

        # Step 1: Try to UPDATE existing event — also set max_notional
        # (use the larger of existing max_notional and the monster sweep's max)
        _monster_max = round(row["max_notional"], 2) if row["max_notional"] else None
        c.execute("""
            UPDATE clusterbomb_events SET is_monster = 1,
                max_notional = CASE
                    WHEN COALESCE(max_notional, 0) < COALESCE(?, 0) THEN ?
                    ELSE max_notional END
            WHERE ticker = ? AND event_date = ?
        """, (_monster_max, _monster_max, ticker, event_date))

        if c.rowcount > 0:
            updated += 1
            events.append({"ticker": ticker, "event_date": event_date, "updated": True})
        else:
            # Step 2: No existing event — INSERT as standalone monster_sweep
            sweep_ids = row["sweep_ids"].split(",")
            direction = _infer_sweep_direction(conn, sweep_ids)
            try:
                c.execute("""
                    INSERT INTO clusterbomb_events
                        (ticker, event_date, sweep_count, total_notional,
                         max_notional, avg_price,
                         min_price, max_price, direction, is_rare, is_monster,
                         event_type, threshold_notional, threshold_sweeps,
                         threshold_total, sweep_ids, detected_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 1, 'monster_sweep', ?, 0, 0, ?, ?)
                """, (
                    ticker, event_date, row["sweep_count"],
                    round(row["total_notional"], 2),
                    round(row["max_notional"], 2) if row["max_notional"] else None,
                    round(row["vwap"], 4) if row["vwap"] else None,
                    round(row["min_price"], 4) if row["min_price"] else None,
                    round(row["max_price"], 4) if row["max_price"] else None,
                    direction, monster_min_notional,
                    json.dumps([int(x) for x in sweep_ids]), detected_at
                ))
                inserted += 1
                events.append({"ticker": ticker, "event_date": event_date, "inserted": True})
            except sqlite3.IntegrityError:
                pass

    conn.commit()
    conn.close()

    print(f"Monster sweep detection: {updated} existing events flagged, "
          f"{inserted} new monster-only events inserted "
          f"(threshold: ${monster_min_notional:,.0f})")
    return events


def detect_ranked_sweeps(rank_limit=100, tickers=None,
                         date_from=None, date_to=None,
                         exclude_etfs=True, etf_only=False):
    """
    For each ticker, rank individual sweep trades by notional (biggest = rank 1).

    Two-step approach (mirrors detect_monster_sweeps):
    1. UPDATE existing clusterbomb_events to set sweep_rank where any trade
       on that day falls within the top rank_limit.
    2. INSERT new events with event_type='ranked_sweep' for days with
       top-ranked trades but no existing event.

    Rankings are per-ticker: each ticker's trades are ranked independently.
    A day's sweep_rank = the best (lowest) rank of any individual trade on that day.
    """
    conn = _get_db()
    c = conn.cursor()

    # Build WHERE clause for sweep_trades
    # IMPORTANT: date_from/date_to must NOT filter the ranking CTE — rankings
    # must always consider ALL historical trades. Date filters only restrict
    # which results get processed, so incremental runs rank today's trades
    # against the full history.
    cte_where = ["is_darkpool = 1", "is_sweep = 1"]
    cte_params = []
    if tickers:
        placeholders = ",".join("?" * len(tickers))
        cte_where.append(f"ticker IN ({placeholders})")
        cte_params.extend(tickers)

    cte_where_sql = " AND ".join(cte_where)

    # Date filter for outer query only (restricts which results we process)
    outer_where = []
    outer_params = []
    if date_from:
        outer_where.append("trade_date >= ?")
        outer_params.append(date_from)
    if date_to:
        outer_where.append("trade_date <= ?")
        outer_params.append(date_to)
    outer_where_sql = (" AND " + " AND ".join(outer_where)) if outer_where else ""

    # Compute per-ticker rankings using window function, then aggregate to day level
    print(f"Ranked sweep detection: ranking trades (limit top {rank_limit})...", flush=True)
    import time as _time
    _t0 = _time.time()
    rows = c.execute(f"""
        WITH ranked_trades AS (
            SELECT ticker, trade_date, notional, id,
                   ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY notional DESC) as trade_rank
            FROM sweep_trades
            WHERE {cte_where_sql}
        )
        SELECT
            ticker,
            trade_date,
            MIN(trade_rank) as best_rank,
            MAX(notional) as max_notional,
            COUNT(*) as sweep_count,
            SUM(notional) as total_notional,
            GROUP_CONCAT(id) as sweep_ids
        FROM ranked_trades
        WHERE trade_rank <= ?{outer_where_sql}
        GROUP BY ticker, trade_date
        ORDER BY best_rank
    """, cte_params + [rank_limit] + outer_params).fetchall()
    _tickers_in_result = len(set(r["ticker"] for r in rows))
    print(f"Ranked sweep detection: query done in {_time.time()-_t0:.1f}s — "
          f"{len(rows)} day-events across {_tickers_in_result} tickers", flush=True)

    # Filter ETF tickers
    if etf_only:
        etf_tickers = load_etf_set()
        rows = [r for r in rows if r["ticker"] in etf_tickers] if etf_tickers else []
    elif exclude_etfs:
        etf_tickers = load_etf_set()
        if etf_tickers:
            rows = [r for r in rows if r["ticker"] not in etf_tickers]

    updated = 0
    inserted = 0
    detected_at = datetime.now(timezone.utc).isoformat()

    _total_rows = len(rows)
    for _i, row in enumerate(rows):
        if _i > 0 and _i % 500 == 0:
            print(f"  Ranked sweep detection: processing {_i}/{_total_rows} "
                  f"({updated} updated, {inserted} inserted)...", flush=True)
        ticker = row["ticker"]
        event_date = row["trade_date"]
        best_rank = row["best_rank"]

        # Step 1: Try to UPDATE existing event's sweep_rank + max_notional
        # max_notional = biggest individual trade (from ranked_trades query).
        # Daily detection sets max_notional = total_notional (wrong for per-trade),
        # so we always overwrite with the accurate per-trade value here.
        _trade_max = round(row["max_notional"], 2) if row["max_notional"] else None
        c.execute("""
            UPDATE clusterbomb_events SET sweep_rank = ?,
                max_notional = COALESCE(?, max_notional)
            WHERE ticker = ? AND event_date = ?
            AND (sweep_rank IS NULL OR sweep_rank > ?)
        """, (best_rank, _trade_max, ticker, event_date, best_rank))

        if c.rowcount > 0:
            updated += 1
        else:
            # Check if event exists but already has a better rank
            existing = c.execute(
                "SELECT sweep_rank FROM clusterbomb_events WHERE ticker = ? AND event_date = ?",
                (ticker, event_date)
            ).fetchone()
            if existing:
                # Event exists with equal or better rank — skip
                continue

            # Step 2: No existing event — INSERT as standalone ranked_sweep
            sweep_ids = row["sweep_ids"].split(",") if row["sweep_ids"] else []
            direction = _infer_sweep_direction(conn, sweep_ids) if sweep_ids else "mixed"

            # Get price info for this day
            price_row = c.execute("""
                SELECT SUM(price * size) / SUM(size) as vwap,
                       MIN(price) as min_price, MAX(price) as max_price
                FROM sweep_trades
                WHERE ticker = ? AND trade_date = ? AND is_darkpool = 1 AND is_sweep = 1
            """, (ticker, event_date)).fetchone()

            try:
                c.execute("""
                    INSERT INTO clusterbomb_events
                        (ticker, event_date, sweep_count, total_notional,
                         max_notional, avg_price,
                         min_price, max_price, direction, is_rare, is_monster,
                         event_type, threshold_notional, threshold_sweeps,
                         threshold_total, sweep_ids, detected_at, sweep_rank)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 'ranked_sweep', 0, 0, 0, ?, ?, ?)
                """, (
                    ticker, event_date, row["sweep_count"],
                    round(row["total_notional"], 2),
                    round(row["max_notional"], 2) if row["max_notional"] else None,
                    round(price_row["vwap"], 4) if price_row and price_row["vwap"] else None,
                    round(price_row["min_price"], 4) if price_row and price_row["min_price"] else None,
                    round(price_row["max_price"], 4) if price_row and price_row["max_price"] else None,
                    direction,
                    json.dumps([int(x) for x in sweep_ids]) if sweep_ids else "[]",
                    detected_at, best_rank
                ))
                inserted += 1
            except sqlite3.IntegrityError:
                pass

    conn.commit()
    conn.close()

    print(f"Ranked sweep detection: {updated} existing events ranked, "
          f"{inserted} new ranked-only events inserted "
          f"(rank limit: top {rank_limit})")
    return {"updated": updated, "inserted": inserted}


def detect_ranked_daily(rank_limit=100, min_sweeps=2, tickers=None,
                        date_from=None, date_to=None,
                        exclude_etfs=True, etf_only=False):
    """
    Rank days per-ticker by total daily notional (from sweep_daily_summary).
    Only counts days with at least min_sweeps sweeps.

    Two-step approach (mirrors detect_ranked_sweeps):
    1. UPDATE existing clusterbomb_events to set daily_rank.
    2. INSERT new events with event_type='ranked_daily' for days not already in table.

    Rankings are per-ticker: each ticker's days are ranked independently.
    """
    conn = _get_db()
    c = conn.cursor()

    # Build WHERE for sweep_daily_summary
    # IMPORTANT: date_from/date_to must NOT filter the ranking CTE — rankings
    # must always consider ALL historical data. Date filters only restrict which
    # results get processed (outer WHERE), so incremental runs (e.g. today only)
    # still rank today's data against the full history.
    cte_where = [f"sweep_count >= {min_sweeps}"]
    cte_params = []
    if tickers:
        placeholders = ",".join("?" * len(tickers))
        cte_where.append(f"ticker IN ({placeholders})")
        cte_params.extend(tickers)

    cte_where_sql = " AND ".join(cte_where)

    # Date filter for outer query only (restricts which results we process)
    outer_where = ["day_rank <= ?"]
    outer_params = [rank_limit]
    if date_from:
        outer_where.append("trade_date >= ?")
        outer_params.append(date_from)
    if date_to:
        outer_where.append("trade_date <= ?")
        outer_params.append(date_to)
    outer_where_sql = " AND ".join(outer_where)

    print(f"Ranked daily detection: ranking days (limit top {rank_limit}, "
          f"min_sweeps={min_sweeps})...", flush=True)
    import time as _time
    _t0 = _time.time()

    rows = c.execute(f"""
        WITH ranked_days AS (
            SELECT ticker, trade_date, total_notional, sweep_count, vwap,
                   min_price, max_price,
                   ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY total_notional DESC) as day_rank
            FROM sweep_daily_summary
            WHERE {cte_where_sql}
        )
        SELECT ticker, trade_date, day_rank, total_notional, sweep_count,
               vwap, min_price, max_price
        FROM ranked_days
        WHERE {outer_where_sql}
        ORDER BY day_rank
    """, cte_params + outer_params).fetchall()

    # Filter ETF tickers
    if etf_only:
        etf_tickers = load_etf_set()
        rows = [r for r in rows if r["ticker"] in etf_tickers] if etf_tickers else []
    elif exclude_etfs:
        etf_tickers = load_etf_set()
        if etf_tickers:
            rows = [r for r in rows if r["ticker"] not in etf_tickers]

    _tickers_in_result = len(set(r["ticker"] for r in rows))
    print(f"Ranked daily detection: query done in {_time.time()-_t0:.1f}s — "
          f"{len(rows)} day-events across {_tickers_in_result} tickers", flush=True)

    updated = 0
    inserted = 0
    detected_at = datetime.now(timezone.utc).isoformat()
    _total_rows = len(rows)

    for _i, row in enumerate(rows):
        if _i > 0 and _i % 500 == 0:
            print(f"  Ranked daily detection: processing {_i}/{_total_rows} "
                  f"({updated} updated, {inserted} inserted)...", flush=True)
        ticker = row["ticker"]
        event_date = row["trade_date"]
        day_rank = row["day_rank"]

        # Step 1: Try to UPDATE existing event's daily_rank
        c.execute("""
            UPDATE clusterbomb_events SET daily_rank = ?
            WHERE ticker = ? AND event_date = ?
            AND (daily_rank IS NULL OR daily_rank > ?)
        """, (day_rank, ticker, event_date, day_rank))

        if c.rowcount > 0:
            updated += 1
        else:
            # Check if event exists but already has a better rank
            existing = c.execute(
                "SELECT daily_rank FROM clusterbomb_events WHERE ticker = ? AND event_date = ?",
                (ticker, event_date)
            ).fetchone()
            if existing:
                continue

            # Step 2: No existing event — INSERT as standalone ranked_daily
            direction = _infer_sweep_direction_from_summary(conn, ticker, event_date)

            try:
                c.execute("""
                    INSERT INTO clusterbomb_events
                        (ticker, event_date, sweep_count, total_notional,
                         max_notional, avg_price,
                         min_price, max_price, direction, is_rare, is_monster,
                         event_type, threshold_notional, threshold_sweeps,
                         threshold_total, sweep_ids, detected_at, daily_rank)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 'ranked_daily', 0, 0, 0, '[]', ?, ?)
                """, (
                    ticker, event_date, row["sweep_count"],
                    round(row["total_notional"], 2),
                    round(row["total_notional"], 2),  # max_notional = total for daily
                    round(row["vwap"], 4) if row["vwap"] else None,
                    round(row["min_price"], 4) if row["min_price"] else None,
                    round(row["max_price"], 4) if row["max_price"] else None,
                    direction,
                    detected_at, day_rank
                ))
                inserted += 1
            except sqlite3.IntegrityError:
                pass

    conn.commit()
    conn.close()

    print(f"Ranked daily detection: {updated} existing events ranked, "
          f"{inserted} new daily-only events inserted "
          f"(rank limit: top {rank_limit}, min_sweeps: {min_sweeps})")
    return {"updated": updated, "inserted": inserted}


def _infer_sweep_direction_from_summary(conn, ticker, trade_date):
    """Infer buy/sell/mixed direction from sweep_trades for a given day."""
    rows = conn.execute("""
        SELECT id FROM sweep_trades
        WHERE ticker = ? AND trade_date = ? AND is_darkpool = 1 AND is_sweep = 1
        LIMIT 20
    """, (ticker, trade_date)).fetchall()
    if not rows:
        return "mixed"
    sweep_ids = [str(r["id"]) for r in rows]
    return _infer_sweep_direction(conn, sweep_ids)


def get_trade_ranks_for_ticker(ticker):
    """Return {trade_date: best_rank} mapping for a single ticker.
    Ranks individual sweep trades by notional DESC across all time.
    Used by chart endpoint for per-bubble rank coloring."""
    conn = _get_db()
    rows = conn.execute("""
        SELECT trade_date, MIN(trade_rank) as best_rank FROM (
            SELECT trade_date,
                   ROW_NUMBER() OVER (ORDER BY notional DESC) as trade_rank
            FROM sweep_trades
            WHERE ticker = ? AND is_darkpool = 1 AND is_sweep = 1
        )
        GROUP BY trade_date
    """, [ticker]).fetchall()
    conn.close()
    return {r["trade_date"]: r["best_rank"] for r in rows}


# ---------------------------------------------------------------------------
# Incremental detection (for live scanner)
# ---------------------------------------------------------------------------

def detect_today():
    """Run all detection passes on today's data only (stocks + ETFs).

    Used by the live sweep daemon to detect events incrementally.
    Returns dict of event counts for both stock and ETF detections.
    Safe to call repeatedly — INSERT OR IGNORE prevents duplicates.
    """
    full_cfg = get_detection_config()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # --- Stock detection (exclude ETFs) ---
    cfg = full_cfg.get("stock", {})
    cbs = detect_clusterbombs(
        min_sweeps=cfg.get("min_sweeps", DEFAULT_CB_MIN_SWEEPS),
        min_notional=cfg.get("min_notional", DEFAULT_CB_MIN_NOTIONAL),
        min_total=cfg.get("min_total", DEFAULT_CB_MIN_TOTAL),
        rarity_days=cfg.get("rarity_days", DEFAULT_CB_RARITY_DAYS),
        rare_min_notional=cfg.get("rare_min_notional", DEFAULT_CB_RARE_MIN_NOTIONAL),
        date_from=today, date_to=today,
        exclude_etfs=True, etf_only=False,
    )
    rares = detect_rare_sweep_days(
        min_notional=cfg.get("rare_min_notional", DEFAULT_CB_RARE_MIN_NOTIONAL),
        rarity_days=cfg.get("rarity_days", DEFAULT_CB_RARITY_DAYS),
        date_from=today, date_to=today,
        exclude_etfs=True, etf_only=False,
    )
    monsters = detect_monster_sweeps(
        monster_min_notional=cfg.get("monster_min_notional", DEFAULT_MONSTER_MIN_NOTIONAL),
        date_from=today, date_to=today,
        exclude_etfs=True, etf_only=False,
    )

    # --- ETF detection (ETFs only — daily ranking + single ranking + rare) ---
    ecfg = full_cfg.get("etf", {})
    etf_daily = detect_ranked_daily(
        rank_limit=100,
        min_sweeps=ecfg.get("min_sweeps_daily", 1),
        date_from=today, date_to=today,
        exclude_etfs=False, etf_only=True,
    )
    etf_rares = detect_rare_sweep_days(
        min_notional=ecfg.get("rare_min_notional", 1_000_000),
        rarity_days=ecfg.get("rarity_days", 20),
        date_from=today, date_to=today,
        exclude_etfs=False, etf_only=True,
    )
    etf_ranked = detect_ranked_sweeps(
        rank_limit=100,
        date_from=today, date_to=today,
        exclude_etfs=False, etf_only=True,
    )

    return {
        "clusterbomb": len(cbs),
        "rare_sweep": len(rares),
        "monster_sweep": len(monsters),
        "etf_daily": etf_daily.get("updated", 0) + etf_daily.get("inserted", 0),
        "etf_rare_sweep": len(etf_rares),
        "etf_ranked": etf_ranked.get("updated", 0) + etf_ranked.get("inserted", 0),
    }


# ---------------------------------------------------------------------------
# Query helpers (for API endpoints)
# ---------------------------------------------------------------------------

def get_sweep_summary(ticker=None, date_from=None, date_to=None, limit=100):
    """
    Get sweep trade summary grouped by ticker + date.
    Uses pre-aggregated sweep_daily_summary table for speed.
    Falls back to raw GROUP BY if summary table is empty.
    """
    conn = _get_db()

    # Check if daily summary table has data
    has_summary = conn.execute("SELECT 1 FROM sweep_daily_summary LIMIT 1").fetchone()

    if has_summary:
        # Fast path: read from pre-aggregated table
        where = []
        params = []
        if ticker:
            where.append("ticker = ?")
            params.append(ticker)
        if date_from:
            where.append("trade_date >= ?")
            params.append(date_from)
        if date_to:
            where.append("trade_date <= ?")
            params.append(date_to)
        where_sql = (" WHERE " + " AND ".join(where)) if where else ""
        rows = conn.execute(f"""
            SELECT ticker, trade_date, sweep_count, total_notional, total_shares,
                   vwap, min_price, max_price, first_sweep, last_sweep
            FROM sweep_daily_summary{where_sql}
            ORDER BY trade_date DESC, total_notional DESC
            LIMIT ?
        """, params + [limit]).fetchall()
    else:
        # Fallback: raw GROUP BY (slow but works before first rebuild)
        where = ["is_darkpool = 1", "is_sweep = 1"]
        params = []
        if ticker:
            where.append("ticker = ?")
            params.append(ticker)
        if date_from:
            where.append("trade_date >= ?")
            params.append(date_from)
        if date_to:
            where.append("trade_date <= ?")
            params.append(date_to)
        where_sql = " AND ".join(where)
        rows = conn.execute(f"""
            SELECT
                ticker, trade_date,
                COUNT(*) as sweep_count,
                SUM(notional) as total_notional,
                SUM(size) as total_shares,
                SUM(price * size) / SUM(size) as vwap,
                MIN(price) as min_price, MAX(price) as max_price,
                MIN(trade_time) as first_sweep, MAX(trade_time) as last_sweep
            FROM sweep_trades WHERE {where_sql}
            GROUP BY ticker, trade_date
            ORDER BY trade_date DESC, total_notional DESC
            LIMIT ?
        """, params + [limit]).fetchall()

    result = []
    for r in rows:
        result.append({
            "ticker": r["ticker"],
            "date": r["trade_date"],
            "sweep_count": r["sweep_count"],
            "total_notional": round(r["total_notional"], 2),
            "total_shares": r["total_shares"],
            "vwap": round(r["vwap"], 2) if r["vwap"] else None,
            "min_price": r["min_price"],
            "max_price": r["max_price"],
            "first_sweep": r["first_sweep"],
            "last_sweep": r["last_sweep"],
        })

    conn.close()
    return result


def get_sweep_detail(ticker, date_str):
    """Get individual sweep trades for a ticker on a specific date."""
    conn = _get_db()
    rows = conn.execute("""
        SELECT * FROM sweep_trades
        WHERE ticker = ? AND trade_date = ? AND is_darkpool = 1 AND is_sweep = 1
        ORDER BY trade_time ASC
    """, (ticker, date_str)).fetchall()

    result = []
    for r in rows:
        result.append({
            "id": r["id"],
            "time": r["trade_time"],
            "price": r["price"],
            "size": r["size"],
            "notional": r["notional"],
            "trf_id": r["trf_id"],
            "conditions": json.loads(r["conditions"]) if r["conditions"] else [],
        })

    conn.close()
    return result


def get_ticker_day_ranks(tickers=None):
    """Rank each day's total sweep notional per-ticker (descending).

    Returns dict: {(ticker, date): {"rank": N, "total_days": M, "day_notional": X}}
    Uses RANK() window function — ties share the same rank.
    Uses pre-aggregated sweep_daily_summary if available.
    """
    conn = _get_db()
    has_summary = conn.execute("SELECT 1 FROM sweep_daily_summary LIMIT 1").fetchone()

    if has_summary:
        # Fast path: pre-aggregated table (no GROUP BY needed)
        where = ""
        params = []
        if tickers:
            placeholders = ",".join("?" * len(tickers))
            where = f"WHERE ticker IN ({placeholders})"
            params = list(tickers)
        rows = conn.execute(f"""
            SELECT ticker, trade_date, total_notional as day_notional,
                   sweep_count,
                   RANK() OVER (PARTITION BY ticker ORDER BY total_notional DESC) as notional_rank,
                   COUNT(*) OVER (PARTITION BY ticker) as total_days
            FROM sweep_daily_summary
            {where}
        """, params).fetchall()
    else:
        # Fallback: raw GROUP BY
        where = "WHERE is_darkpool = 1 AND is_sweep = 1"
        params = []
        if tickers:
            placeholders = ",".join("?" * len(tickers))
            where += f" AND ticker IN ({placeholders})"
            params = list(tickers)
        rows = conn.execute(f"""
            SELECT ticker, trade_date, day_notional, sweep_count, notional_rank, total_days
            FROM (
                SELECT ticker, trade_date,
                    SUM(notional) as day_notional, COUNT(*) as sweep_count,
                    RANK() OVER (PARTITION BY ticker ORDER BY SUM(notional) DESC) as notional_rank,
                    COUNT(*) OVER (PARTITION BY ticker) as total_days
                FROM sweep_trades {where}
                GROUP BY ticker, trade_date
            )
        """, params).fetchall()

    conn.close()

    result = {}
    for r in rows:
        result[(r["ticker"], r["trade_date"])] = {
            "rank": r["notional_rank"],
            "total_days": r["total_days"],
            "day_notional": r["day_notional"],
        }
    return result


def get_clusterbombs(ticker=None, date_from=None, date_to=None,
                     rare_only=False, limit=100, min_total=None,
                     exclude_etfs=True, etf_only=False):
    """Get detected clusterbomb events, optionally filtered by min_total."""
    conn = _get_db()
    where = []
    params = []

    if etf_only and not ticker:
        etf_tickers = sorted(load_etf_set())
        if etf_tickers:
            where.append(f"ticker IN ({','.join('?' * len(etf_tickers))})")
            params.extend(etf_tickers)
        else:
            conn.close()
            return []
    elif exclude_etfs and not ticker:
        etf_tickers = load_etf_set()
        if etf_tickers:
            where.append(f"ticker NOT IN ({','.join('?' * len(etf_tickers))})")
            params.extend(sorted(etf_tickers))

    if ticker:
        where.append("ticker = ?")
        params.append(ticker)
    if date_from:
        where.append("event_date >= ?")
        params.append(date_from)
    if date_to:
        where.append("event_date <= ?")
        params.append(date_to)
    if rare_only:
        where.append("is_rare = 1")
    if min_total is not None:
        if etf_only:
            where.append("(daily_rank IS NOT NULL OR sweep_rank IS NOT NULL OR is_rare = 1)")
        else:
            where.append("(total_notional >= ? OR COALESCE(is_monster, 0) = 1 OR is_rare = 1)")
            params.append(min_total)

    where_sql = (" AND ".join(where)) if where else "1=1"
    rows = conn.execute(f"""
        SELECT * FROM clusterbomb_events
        WHERE {where_sql}
        ORDER BY event_date DESC, total_notional DESC
        LIMIT ?
    """, params + [limit]).fetchall()

    result = []
    # Collect tickers for rank lookup
    event_tickers = list(set(r["ticker"] for r in rows))
    rank_data = get_ticker_day_ranks(tickers=event_tickers) if event_tickers else {}

    _split_cache = {}
    today = datetime.now().strftime("%Y-%m-%d")
    today_dt = datetime.strptime(today, "%Y-%m-%d")
    now = datetime.now()

    for r in rows:
        rk = rank_data.get((r["ticker"], r["event_date"]), {})
        # event_type may not exist in old DBs before migration
        try:
            evt = r["event_type"] or "clusterbomb"
        except (IndexError, KeyError):
            evt = "clusterbomb"

        ticker = r["ticker"]
        avg_price = r["avg_price"] or 0

        # --- Split adjustment (forward AND reverse splits) ---
        if avg_price > 0 and ticker not in _split_cache:
            _split_cache[ticker] = _load_daily_prices(ticker)
        split_factor = 1.0
        if avg_price > 0 and _split_cache.get(ticker) is not None:
            df = _split_cache[ticker]
            mask = df["timestamp"] <= r["event_date"]
            if mask.any():
                adj_close = float(df[mask].iloc[-1]["close"])
                if adj_close > 0:
                    ratio = avg_price / adj_close
                    if ratio > 1.5:
                        # Forward split (e.g. 10:1): raw price > adjusted
                        split_factor = round(ratio)
                    elif ratio < 0.667:
                        # Reverse split (e.g. 1:4, 1:10): raw price < adjusted
                        split_factor = 1.0 / round(1.0 / ratio)
        if split_factor != 1.0:
            avg_price = avg_price / split_factor

        # Current price + % gain + days since
        current_price = _get_latest_price(ticker)
        pct_gain = ((current_price / avg_price) - 1) * 100 if avg_price > 0 and current_price > 0 else 0
        try:
            event_dt = datetime.strptime(r["event_date"], "%Y-%m-%d")
            days_since = (today_dt - event_dt).days
        except Exception:
            days_since = 0

        result.append({
            "id": r["id"],
            "ticker": ticker,
            "date": r["event_date"],
            "sweep_count": r["sweep_count"],
            "total_notional": r["total_notional"],
            "avg_price": avg_price,
            "min_price": r["min_price"] / split_factor if r["min_price"] else 0,
            "max_price": r["max_price"] / split_factor if r["max_price"] else 0,
            "direction": r["direction"],
            "is_rare": bool(r["is_rare"]),
            "is_monster": bool(r["is_monster"]) if "is_monster" in r.keys() else False,
            "event_type": evt,
            "max_notional": r["max_notional"] if "max_notional" in r.keys() else None,
            "sweep_rank": r["sweep_rank"] if "sweep_rank" in r.keys() else None,
            "daily_rank": r["daily_rank"] if "daily_rank" in r.keys() else None,
            "rank": rk.get("rank"),
            "total_days": rk.get("total_days"),
            "current_price": current_price,
            "pct_gain": round(pct_gain, 2),
            "days_since": days_since,
            "thresholds": {
                "notional": r["threshold_notional"],
                "sweeps": r["threshold_sweeps"],
                "total": r["threshold_total"],
            },
        })

    # Compute period returns (1W, 1M, 3M, 6M, 1Y, 2Y)
    _price_cache = {}
    _periods = [("return_1w", 7), ("return_1m", 30), ("return_3m", 90),
                ("return_6m", 180), ("return_1y", 365), ("return_2y", 730)]
    for ev_dict in result:
        ticker = ev_dict["ticker"]
        avg_p = ev_dict["avg_price"]
        if not avg_p or avg_p <= 0:
            for label, _ in _periods:
                ev_dict[label] = None
            continue
        if ticker not in _price_cache:
            _price_cache[ticker] = _load_daily_prices(ticker)
        df = _price_cache[ticker]
        try:
            event_dt = datetime.strptime(ev_dict["date"], "%Y-%m-%d")
        except Exception:
            for label, _ in _periods:
                ev_dict[label] = None
            continue
        for label, days_offset in _periods:
            target_dt = event_dt + timedelta(days=days_offset)
            if target_dt > now:
                ev_dict[label] = None
                continue
            if df is None or df.empty:
                ev_dict[label] = None
                continue
            target_str = target_dt.strftime("%Y-%m-%d")
            mask = df["timestamp"] <= target_str
            if mask.any():
                price = float(df[mask].iloc[-1]["close"])
                ev_dict[label] = round(((price / avg_p) - 1) * 100, 2)
            else:
                ev_dict[label] = None

    conn.close()
    return result


def get_sweep_stats(min_total=None, tickers=None, date_from=None, date_to=None,
                    min_sweeps=None, monster_min=None, rare_min=None, rare_days=None,
                    rank_from=None, rank_to=None,
                    daily_from=None, daily_to=None,
                    exclude_etfs=True, full_db=False, etf_only=False):
    """Get overview stats for the sweeps page header.
    Supports filtering by min_total, tickers list, date range, and per-type
    display filters so the stats bar reflects the currently active page filters.

    Optimised: single query for sweep_trades stats (COUNT + SUM + DISTINCT + MIN/MAX),
    single query for clusterbomb_events counts, server-side 2Y fallback.
    """
    conn = _get_db()
    stats = {}

    # Server-side fallback: if no date range and not requesting full DB, default to 2 years
    if not date_from and not full_db:
        date_from = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")

    # --- ETF filtering ---
    _etf_filter_tickers = []
    _etf_filter_mode = None  # 'include' or 'exclude'
    if etf_only:
        _etf_filter_tickers = sorted(load_etf_set())
        _etf_filter_mode = "include"
    elif exclude_etfs:
        _etf_filter_tickers = sorted(load_etf_set())
        _etf_filter_mode = "exclude"

    # --- Single sweep_trades query for all stats ---
    sw_where = ["is_darkpool=1", "is_sweep=1"]
    sw_params = []
    if _etf_filter_tickers:
        op = "IN" if _etf_filter_mode == "include" else "NOT IN"
        sw_where.append(f"ticker {op} ({','.join('?' * len(_etf_filter_tickers))})")
        sw_params.extend(_etf_filter_tickers)
    if tickers:
        placeholders = ",".join("?" * len(tickers))
        sw_where.append(f"ticker IN ({placeholders})")
        sw_params.extend(tickers)
    if date_from:
        sw_where.append("trade_date >= ?")
        sw_params.append(date_from)
    if date_to:
        sw_where.append("trade_date <= ?")
        sw_params.append(date_to)
    sw_sql = " AND ".join(sw_where)

    row = conn.execute(
        f"SELECT COUNT(*), COALESCE(SUM(notional),0), COUNT(DISTINCT ticker), "
        f"MIN(trade_date), MAX(trade_date) FROM sweep_trades WHERE {sw_sql}", sw_params
    ).fetchone()
    stats["total_sweeps"] = row[0] or 0
    stats["total_notional"] = round(row[1] or 0, 2)
    stats["tickers_tracked"] = row[2] or 0
    stats["date_range"] = {"from": row[3], "to": row[4]}

    # --- Single clusterbomb_events query for CB + rare + monster counts ---
    cb_where = []
    cb_params = []
    if _etf_filter_tickers:
        op = "IN" if _etf_filter_mode == "include" else "NOT IN"
        cb_where.append(f"ticker {op} ({','.join('?' * len(_etf_filter_tickers))})")
        cb_params.extend(_etf_filter_tickers)
    if min_total is not None:
        if etf_only:
            cb_where.append("(daily_rank IS NOT NULL OR sweep_rank IS NOT NULL OR is_rare = 1)")
        else:
            cb_where.append("(total_notional >= ? OR COALESCE(is_monster, 0) = 1 OR is_rare = 1)")
            cb_params.append(min_total)
    # Per-type display filters
    if min_sweeps:
        if etf_only:
            cb_where.append("(daily_rank IS NULL OR sweep_count >= ?)")
        else:
            cb_where.append("(COALESCE(event_type,'clusterbomb') != 'clusterbomb' OR sweep_count >= ?)")
        cb_params.append(min_sweeps)
    if etf_only and (daily_from is not None or daily_to is not None or rank_from is not None or rank_to is not None):
        # OR across layers: include if daily_rank in range OR sweep_rank in range OR is_rare
        _df = daily_from if daily_from is not None else 1
        _dt = daily_to if daily_to is not None else 50
        _rf = rank_from if rank_from is not None else 1
        _rt = rank_to if rank_to is not None else 50
        cb_where.append("((daily_rank >= ? AND daily_rank <= ?) OR (sweep_rank >= ? AND sweep_rank <= ?) OR is_rare = 1)")
        cb_params.extend([_df, _dt, _rf, _rt])
    elif monster_min:
        cb_where.append("(COALESCE(is_monster, 0) = 0 AND COALESCE(event_type,'clusterbomb') != 'monster_sweep' OR COALESCE(max_notional, total_notional) >= ?)")
        cb_params.append(monster_min)
    if rare_min:
        cb_where.append("(COALESCE(event_type,'clusterbomb') != 'rare_sweep' OR total_notional >= ?)")
        cb_params.append(rare_min)
    if rare_days:
        cb_where.append("(COALESCE(event_type,'clusterbomb') != 'rare_sweep' OR COALESCE(dormancy_days, 999) >= ?)")
        cb_params.append(rare_days)
    if tickers:
        placeholders = ",".join("?" * len(tickers))
        cb_where.append(f"ticker IN ({placeholders})")
        cb_params.extend(tickers)
    if date_from:
        cb_where.append("event_date >= ?")
        cb_params.append(date_from)
    if date_to:
        cb_where.append("event_date <= ?")
        cb_params.append(date_to)

    cb_sql = (" WHERE " + " AND ".join(cb_where)) if cb_where else ""

    row = conn.execute(
        f"SELECT COUNT(*), "
        f"SUM(CASE WHEN is_rare=1 THEN 1 ELSE 0 END), "
        f"SUM(CASE WHEN COALESCE(is_monster,0)=1 THEN 1 ELSE 0 END), "
        f"SUM(CASE WHEN sweep_rank IS NOT NULL THEN 1 ELSE 0 END), "
        f"SUM(CASE WHEN daily_rank IS NOT NULL THEN 1 ELSE 0 END) "
        f"FROM clusterbomb_events{cb_sql}", cb_params
    ).fetchone()
    stats["clusterbombs"] = row[0] or 0
    stats["rare_clusterbombs"] = row[1] or 0
    stats["monster_sweeps"] = row[2] or 0
    stats["ranked_sweeps"] = row[3] or 0
    stats["daily_events"] = row[4] or 0

    # Last fetch info
    meta = conn.execute("SELECT value FROM sweep_meta WHERE key='last_fetch'").fetchone()
    stats["last_fetch"] = json.loads(meta[0]) if meta else None

    conn.close()
    return stats


def get_sweep_chart_data(ticker, date_from=None, date_to=None, timeframe="1D",
                         min_total=None, monster_min=None,
                         min_sweeps=None, rare_min=None, rare_days=None,
                         rank_from=None, rank_to=None,
                         daily_from=None, daily_to=None, etf_only=False):
    """
    Get sweep data formatted for chart overlay on candlestick.
    Returns sweeps as markers with price/size/time for chart rendering.
    Also returns clusterbomb dates for highlighting.
    If min_total is set, only clusterbombs with total_notional >= min_total are returned.
    Per-type display filters (monster_min, min_sweeps, rare_min, rare_days)
    filter their respective event types by the given thresholds.
    """
    conn = _get_db()
    params = [ticker]
    date_where = ""

    if date_from:
        date_where += " AND trade_date >= ?"
        params.append(date_from)
    if date_to:
        date_where += " AND trade_date <= ?"
        params.append(date_to)

    # Individual sweeps for markers
    sweeps = conn.execute(f"""
        SELECT trade_date, trade_time, price, size, notional
        FROM sweep_trades
        WHERE ticker = ? AND is_darkpool = 1 AND is_sweep = 1 {date_where}
        ORDER BY trade_date, trade_time
    """, params).fetchall()

    sweep_markers = []
    for s in sweeps:
        sweep_markers.append({
            "date": s["trade_date"],
            "time": s["trade_time"],
            "price": s["price"],
            "size": s["size"],
            "notional": s["notional"],
        })

    # Clusterbomb dates for day-level highlighting (filtered by min_total)
    cb_params = [ticker]
    cb_where = ""
    if date_from:
        cb_where += " AND event_date >= ?"
        cb_params.append(date_from)
    if date_to:
        cb_where += " AND event_date <= ?"
        cb_params.append(date_to)
    if min_total is not None:
        if etf_only:
            cb_where += " AND (daily_rank IS NOT NULL OR sweep_rank IS NOT NULL OR is_rare = 1)"
        else:
            cb_where += " AND (total_notional >= ? OR COALESCE(is_monster, 0) = 1 OR is_rare = 1)"
            cb_params.append(min_total)
    # Per-type display filters
    if min_sweeps:
        if etf_only:
            cb_where += " AND (daily_rank IS NULL OR sweep_count >= ?)"
        else:
            cb_where += " AND (COALESCE(event_type,'clusterbomb') != 'clusterbomb' OR sweep_count >= ?)"
        cb_params.append(min_sweeps)
    if etf_only and (daily_from is not None or daily_to is not None or rank_from is not None or rank_to is not None):
        # OR across layers: include if daily_rank in range OR sweep_rank in range OR is_rare
        _df = daily_from if daily_from is not None else 1
        _dt = daily_to if daily_to is not None else 50
        _rf = rank_from if rank_from is not None else 1
        _rt = rank_to if rank_to is not None else 50
        cb_where += " AND ((daily_rank >= ? AND daily_rank <= ?) OR (sweep_rank >= ? AND sweep_rank <= ?) OR is_rare = 1)"
        cb_params.extend([_df, _dt, _rf, _rt])
    elif monster_min:
        cb_where += " AND (COALESCE(is_monster, 0) = 0 AND COALESCE(event_type,'clusterbomb') != 'monster_sweep' OR COALESCE(max_notional, total_notional) >= ?)"
        cb_params.append(monster_min)
    if rare_min:
        cb_where += " AND (COALESCE(event_type,'clusterbomb') != 'rare_sweep' OR total_notional >= ?)"
        cb_params.append(rare_min)
    if rare_days:
        cb_where += " AND (COALESCE(event_type,'clusterbomb') != 'rare_sweep' OR COALESCE(dormancy_days, 999) >= ?)"
        cb_params.append(rare_days)

    cbs = conn.execute(f"""
        SELECT event_date, sweep_count, total_notional, direction, is_rare,
               COALESCE(event_type, 'clusterbomb') as event_type,
               COALESCE(is_monster, 0) as is_monster, max_notional, sweep_rank, daily_rank
        FROM clusterbomb_events
        WHERE ticker = ? {cb_where}
        ORDER BY event_date
    """, cb_params).fetchall()

    # For ETF mode, compute per-trade ranks on-the-fly for more accurate bubble coloring
    _trade_ranks = {}
    if etf_only:
        _trade_ranks = get_trade_ranks_for_ticker(ticker)

    clusterbomb_dates = []
    for cb in cbs:
        # Use on-the-fly rank if available (more accurate), fall back to stored sweep_rank
        sr = _trade_ranks.get(cb["event_date"]) if _trade_ranks else cb["sweep_rank"]
        clusterbomb_dates.append({
            "date": cb["event_date"],
            "sweep_count": cb["sweep_count"],
            "total_notional": cb["total_notional"],
            "max_notional": cb["max_notional"],
            "direction": cb["direction"],
            "is_rare": bool(cb["is_rare"]),
            "event_type": cb["event_type"],
            "is_monster": bool(cb["is_monster"]),
            "sweep_rank": sr,
            "daily_rank": cb["daily_rank"],
        })

    conn.close()

    # --- Split adjustment for raw sweep trade prices ---
    # Raw Polygon trade prices are NOT split-adjusted; candle data IS.
    # Detect split by comparing first available sweep price to adjusted close.
    _daily = _load_daily_prices(ticker)
    if _daily is not None and sweep_markers:
        # Build a quick date→adj_close lookup
        _adj_lookup = {}
        for _, row in _daily.iterrows():
            _adj_lookup[row["timestamp"].strftime("%Y-%m-%d") if hasattr(row["timestamp"], "strftime")
                        else str(row["timestamp"])[:10]] = float(row["close"])

        for sm in sweep_markers:
            raw_price = sm["price"]
            if raw_price <= 0:
                continue
            adj_close = _adj_lookup.get(sm["date"])
            if adj_close and adj_close > 0:
                ratio = raw_price / adj_close
                if ratio > 1.5:
                    sf = round(ratio)
                    sm["price"] = round(raw_price / sf, 4)
                elif ratio < 0.667:
                    sf = 1.0 / round(1.0 / ratio)
                    sm["price"] = round(raw_price / sf, 4)

    # Merge per-ticker rank data into clusterbomb entries
    if clusterbomb_dates:
        rank_data = get_ticker_day_ranks(tickers=[ticker])
        for cb in clusterbomb_dates:
            rk = rank_data.get((ticker, cb["date"]), {})
            cb["rank"] = rk.get("rank")
            cb["total_days"] = rk.get("total_days")

    # Load OHLC candles from price cache
    candles = _load_price_candles(ticker, date_from, date_to, timeframe=timeframe)

    # Compute cumulative sweep notional by date (for DP volume chart overlay)
    daily_notional = defaultdict(float)
    for sm in sweep_markers:
        daily_notional[sm["date"]] += sm["notional"]
    sorted_dates = sorted(daily_notional.keys())
    cumulative = []
    running = 0.0
    for d in sorted_dates:
        running += daily_notional[d]
        cumulative.append({"date": d, "value": round(running, 2)})

    return {
        "sweeps": sweep_markers,
        "clusterbombs": clusterbomb_dates,
        "candles": candles,
        "cumulative_notional": cumulative,
    }


# Timeframe -> CSV suffix mapping
_TF_SUFFIX = {"1h": "hour", "4h": "hour", "1D": "day", "1W": "week"}


def _merge_live_bars_into_df(df, ticker, timeframe, suffix):
    """Merge live price bars from the running LivePriceDaemon into a DataFrame.

    Appends today's in-memory bars (hourly/daily) from the live WebSocket feed
    so charts display fresh intraday data without waiting for the next CSV flush.
    Returns the modified DataFrame (or original if no live data available).
    """
    try:
        from app import _live_price_daemon
        if not _live_price_daemon:
            return df
        status = _live_price_daemon.get_status()
        if not status.get("connected"):
            return df
    except (ImportError, AttributeError):
        return df

    live = _live_price_daemon.get_latest_bars(ticker)
    if not live:
        return df

    trade_date = live.get("trade_date")
    if not trade_date:
        return df

    today_start = pd.Timestamp(trade_date)
    today_end = today_start + pd.Timedelta(days=1)

    if timeframe in ("1h", "4h"):
        # Merge hourly bars (completed hours + current partial hour)
        hourly_bars = live.get("hourly", [])
        current_hour = live.get("current_hour")
        all_bars = list(hourly_bars)
        if current_hour:
            all_bars.append(current_hour)
        if not all_bars:
            return df

        live_rows = []
        for b in all_bars:
            live_rows.append({
                "timestamp": pd.Timestamp.utcfromtimestamp(b["t"]).tz_localize(None),
                "open": b["o"], "high": b["h"], "low": b["l"],
                "close": b["c"], "volume": b["v"],
            })
        live_df = pd.DataFrame(live_rows)

        # Remove any existing rows for today from CSV data
        if not df.empty:
            df = df[(df["timestamp"] < today_start) | (df["timestamp"] >= today_end)]

        df = pd.concat([df, live_df], ignore_index=True)
        df = df.sort_values("timestamp").reset_index(drop=True)

    elif timeframe == "1D":
        # Merge today's running daily bar
        daily = live.get("daily")
        if not daily:
            return df

        live_row = pd.DataFrame([{
            "timestamp": pd.Timestamp(trade_date),
            "open": daily["o"], "high": daily["h"], "low": daily["l"],
            "close": daily["c"], "volume": daily["v"],
        }])

        # Remove today's row from CSV data if present
        if not df.empty:
            df = df[df["timestamp"] < today_start]

        df = pd.concat([df, live_row], ignore_index=True)
        df = df.sort_values("timestamp").reset_index(drop=True)

    # 1W: skip — weekly bar only makes sense after full week
    return df


def _load_price_candles(ticker, date_from=None, date_to=None, timeframe="1D"):
    """Load OHLC candles from the cache CSV for chart rendering.

    Supported timeframes: '1h', '4h', '1D', '1W'.
    For '4h' we load hourly bars and resample to 4-hour.

    If the LivePriceDaemon is running, today's bars are merged from
    in-memory live data for real-time freshness.
    """
    safe = ticker.replace("/", "_").replace(":", "_")
    suffix = _TF_SUFFIX.get(timeframe, "day")
    path = os.path.join(PRICE_CACHE_DIR, f"{safe}_{suffix}.csv")
    if not os.path.exists(path):
        # Fall back to daily if requested TF not cached
        path = os.path.join(PRICE_CACHE_DIR, f"{safe}_day.csv")
        timeframe = "1D"
        if not os.path.exists(path):
            return []
    try:
        df = pd.read_csv(path, parse_dates=["timestamp"])
        df = df.sort_values("timestamp").reset_index(drop=True)

        # Merge live bars from WebSocket daemon (if running)
        df = _merge_live_bars_into_df(df, ticker, timeframe, suffix)

        if date_from:
            df = df[df["timestamp"] >= pd.Timestamp(date_from)]
        if date_to:
            df = df[df["timestamp"] <= pd.Timestamp(date_to + " 23:59:59")]

        # Resample hourly to 4h if requested
        if timeframe == "4h" and suffix == "hour":
            df = df.set_index("timestamp")
            df = df.resample("4h", offset="9h30min").agg({
                "open": "first", "high": "max", "low": "min",
                "close": "last", "volume": "sum",
            }).dropna(subset=["open"]).reset_index()

        candles = []
        use_time = timeframe in ("1h", "4h")
        for _, row in df.iterrows():
            ts = row["timestamp"]
            if use_time:
                # lightweight-charts needs unix seconds for intraday
                candles.append({
                    "time": int(ts.timestamp()),
                    "open": round(float(row["open"]), 2),
                    "high": round(float(row["high"]), 2),
                    "low": round(float(row["low"]), 2),
                    "close": round(float(row["close"]), 2),
                    "volume": int(row.get("volume", 0)) if pd.notna(row.get("volume")) else 0,
                })
            else:
                candles.append({
                    "time": ts.strftime("%Y-%m-%d"),
                    "open": round(float(row["open"]), 2),
                    "high": round(float(row["high"]), 2),
                    "low": round(float(row["low"]), 2),
                    "close": round(float(row["close"]), 2),
                    "volume": int(row.get("volume", 0)) if pd.notna(row.get("volume")) else 0,
                })
        return candles
    except Exception as e:
        print(f"  Warning: Could not load candles for {ticker} ({timeframe}): {e}")
        return []


# ---------------------------------------------------------------------------
# Clusterbomb Tracker — live tracking panel
# ---------------------------------------------------------------------------

def get_tracker_data(min_total=10_000_000, tickers=None,
                     date_from=None, date_to=None,
                     limit=200, offset=0,
                     min_sweeps=None, monster_min=None,
                     rare_min=None, rare_days=None,
                     rank_from=None, rank_to=None,
                     daily_from=None, daily_to=None,
                     exclude_etfs=True, etf_only=False):
    """
    Get clusterbomb events with current price, % gain, days since, and bias.
    Returns (events_list, total_count) sorted by date descending.

    Args:
        min_total: minimum total notional for a clusterbomb to appear
        tickers: list of ticker symbols to filter by (None = all)
        date_from: earliest event_date to include (YYYY-MM-DD)
        date_to: latest event_date to include (YYYY-MM-DD)
        limit: max events to return per page (default 200, 0 = unlimited)
        offset: pagination offset
        min_sweeps: minimum sweep count for clusterbombs (display filter)
        monster_min: minimum notional for monster sweeps (display filter)
        rare_min: minimum notional for rare sweeps (display filter)
        rare_days: minimum dormancy days for rare sweeps (display filter)
        rank_from: minimum sweep rank to include (ETF mode, replaces monster_min)
        rank_to: maximum sweep rank to include (ETF mode, replaces monster_min)
        daily_from: minimum daily rank to include (ETF mode)
        daily_to: maximum daily rank to include (ETF mode)
    """
    # Build dynamic WHERE clause
    if etf_only:
        # ETF mode: ranking-based — include if ANY layer matches
        where = ["(daily_rank IS NOT NULL OR sweep_rank IS NOT NULL OR is_rare = 1)"]
        params = []
    else:
        # Stock mode: threshold-based — monster/rare bypass min_total
        where = ["(total_notional >= ? OR COALESCE(is_monster, 0) = 1 OR is_rare = 1)"]
        params = [min_total]

    # Per-type display filters (each only applies to its own event_type)
    if min_sweeps:
        # For ETF mode, min_sweeps applies to daily-ranked events; for stock mode, to CBs
        if etf_only:
            where.append("(daily_rank IS NULL OR sweep_count >= ?)")
        else:
            where.append("(COALESCE(event_type,'clusterbomb') != 'clusterbomb' OR sweep_count >= ?)")
        params.append(min_sweeps)
    if etf_only and (daily_from is not None or daily_to is not None or rank_from is not None or rank_to is not None):
        # OR across layers: include if daily_rank in range OR sweep_rank in range OR is_rare
        _df = daily_from if daily_from is not None else 1
        _dt = daily_to if daily_to is not None else 50
        _rf = rank_from if rank_from is not None else 1
        _rt = rank_to if rank_to is not None else 50
        where.append("((daily_rank >= ? AND daily_rank <= ?) OR (sweep_rank >= ? AND sweep_rank <= ?) OR is_rare = 1)")
        params.extend([_df, _dt, _rf, _rt])
    elif monster_min:
        # Stock mode: Filter monster events by their largest individual sweep (max_notional)
        where.append("(COALESCE(is_monster, 0) = 0 AND COALESCE(event_type,'clusterbomb') != 'monster_sweep' OR COALESCE(max_notional, total_notional) >= ?)")
        params.append(monster_min)
    if rare_min:
        where.append("(COALESCE(event_type,'clusterbomb') != 'rare_sweep' OR total_notional >= ?)")
        params.append(rare_min)
    if rare_days:
        where.append("(COALESCE(event_type,'clusterbomb') != 'rare_sweep' OR COALESCE(dormancy_days, 999) >= ?)")
        params.append(rare_days)

    if tickers:
        placeholders = ",".join("?" * len(tickers))
        where.append(f"ticker IN ({placeholders})")
        params.extend(tickers)
    if date_from:
        where.append("event_date >= ?")
        params.append(date_from)
    if date_to:
        where.append("event_date <= ?")
        params.append(date_to)

    # ETF ticker filtering
    if etf_only:
        etf_tickers = sorted(load_etf_set())
        if etf_tickers:
            placeholders_etf = ",".join("?" * len(etf_tickers))
            where.append(f"ticker IN ({placeholders_etf})")
            params.extend(etf_tickers)
        else:
            return [], 0
    elif exclude_etfs:
        etf_tickers = load_etf_set()
        if etf_tickers:
            placeholders_etf = ",".join("?" * len(etf_tickers))
            where.append(f"ticker NOT IN ({placeholders_etf})")
            params.extend(sorted(etf_tickers))

    # Fallback: if no tickers AND no date_from, default to current month
    if not tickers and not date_from:
        month_start = datetime.now().replace(day=1).strftime("%Y-%m-%d")
        where.append("event_date >= ?")
        params.append(month_start)

    where_sql = " AND ".join(where)

    conn = _get_db()

    # Count total matching events (for pagination)
    total_count = conn.execute(
        f"SELECT COUNT(*) FROM clusterbomb_events WHERE {where_sql}", params
    ).fetchone()[0]

    # Fetch paginated events
    limit_clause = f" LIMIT {int(limit)} OFFSET {int(offset)}" if limit else ""
    events = conn.execute(f"""
        SELECT ticker, event_date, sweep_count, total_notional,
               avg_price, min_price, max_price, direction, is_rare,
               COALESCE(is_monster, 0) as is_monster,
               COALESCE(event_type, 'clusterbomb') as event_type,
               dormancy_days, max_notional, sweep_rank, daily_rank
        FROM clusterbomb_events
        WHERE {where_sql}
        ORDER BY event_date DESC{limit_clause}
    """, params).fetchall()
    conn.close()

    today = datetime.now().strftime("%Y-%m-%d")

    # Compute rank data — skip if too many tickers (slow during large fetches)
    event_tickers = list(set(ev["ticker"] for ev in events))
    if len(event_tickers) <= 100:
        rank_data = get_ticker_day_ranks(tickers=event_tickers) if event_tickers else {}
    else:
        rank_data = {}  # skip ranking for very large result sets

    result = []
    # Cache for split adjustment — keyed by ticker
    _split_cache = {}
    for ev in events:
        ticker = ev["ticker"]
        avg_price = ev["avg_price"] or 0

        # --- Split adjustment ---
        # Raw trade prices may be pre-split. Compare to split-adjusted candle
        # data to detect and correct for stock splits (forward AND reverse).
        if avg_price > 0 and ticker not in _split_cache:
            _split_cache[ticker] = _load_daily_prices(ticker)
        split_factor = 1.0
        if avg_price > 0 and _split_cache.get(ticker) is not None:
            df = _split_cache[ticker]
            event_date_str = ev["event_date"]
            # Find the adjusted close on or just before the event date
            mask = df["timestamp"] <= event_date_str
            if mask.any():
                adj_close = float(df[mask].iloc[-1]["close"])
                if adj_close > 0:
                    ratio = avg_price / adj_close
                    if ratio > 1.5:
                        # Forward split (e.g. 10:1, 4:1): raw price > adjusted
                        split_factor = round(ratio)
                    elif ratio < 0.667:
                        # Reverse split (e.g. 1:4, 1:10): raw price < adjusted
                        # split_factor < 1 so dividing by it multiplies up
                        split_factor = 1.0 / round(1.0 / ratio)
        if split_factor != 1.0:
            avg_price = avg_price / split_factor

        # Get current price from most recent candle in cache
        current_price = _get_latest_price(ticker)
        pct_gain = ((current_price / avg_price) - 1) * 100 if avg_price > 0 and current_price > 0 else 0

        # Days since event
        try:
            event_dt = datetime.strptime(ev["event_date"], "%Y-%m-%d")
            today_dt = datetime.strptime(today, "%Y-%m-%d")
            days_since = (today_dt - event_dt).days
        except Exception:
            days_since = 0

        # Bias: bullish if current > avg_price, bearish if below
        bias = "bullish" if current_price > avg_price else "bearish"

        rk = rank_data.get((ticker, ev["event_date"]), {})
        result.append({
            "ticker": ticker,
            "date": ev["event_date"],
            "sweep_count": ev["sweep_count"],
            "total_notional": ev["total_notional"],
            "avg_price": avg_price,
            "min_price": ev["min_price"] / split_factor if ev["min_price"] else 0,
            "max_price": ev["max_price"] / split_factor if ev["max_price"] else 0,
            "direction": ev["direction"],
            "is_rare": bool(ev["is_rare"]),
            "is_monster": bool(ev["is_monster"]),
            "event_type": ev["event_type"],
            "dormancy_days": ev["dormancy_days"],
            "max_notional": ev["max_notional"],
            "sweep_rank": ev["sweep_rank"],
            "daily_rank": ev["daily_rank"],
            "current_price": current_price,
            "pct_gain": round(pct_gain, 2),
            "days_since": days_since,
            "bias": bias,
            "rank": rk.get("rank"),
            "total_days": rk.get("total_days"),
            "return_1w": None, "return_1m": None, "return_3m": None,
            "return_6m": None, "return_1y": None, "return_2y": None,
        })

    # Compute period returns (1W, 1M, 3M, 6M, 1Y, 2Y) — skip for huge result sets
    if len(result) > 500:
        return result, total_count  # too many events — skip period returns for speed
    _price_cache = {}
    now = datetime.now()
    _periods = [("return_1w", 7), ("return_1m", 30), ("return_3m", 90),
                ("return_6m", 180), ("return_1y", 365), ("return_2y", 730)]
    for ev_dict in result:
        ticker = ev_dict["ticker"]
        avg_price = ev_dict["avg_price"]
        if not avg_price or avg_price <= 0:
            continue
        if ticker not in _price_cache:
            _price_cache[ticker] = _load_daily_prices(ticker)
        df = _price_cache[ticker]
        if df is None or df.empty:
            continue
        try:
            event_dt = datetime.strptime(ev_dict["date"], "%Y-%m-%d")
        except Exception:
            continue
        for label, days_offset in _periods:
            target_dt = event_dt + timedelta(days=days_offset)
            if target_dt > now:
                continue  # Future — stays None (N/A)
            target_str = target_dt.strftime("%Y-%m-%d")
            mask = df["timestamp"] <= target_str
            if mask.any():
                price = float(df[mask].iloc[-1]["close"])
                ev_dict[label] = round(((price / avg_price) - 1) * 100, 2)

    return result, total_count


# ---------------------------------------------------------------------------
# In-memory price cache — avoids re-reading CSVs on every page load
# Key: ticker (str) → value: sorted pandas DataFrame (or None if no file)
# Populated lazily on first access; can be refreshed with _invalidate_price_cache()
# ---------------------------------------------------------------------------
_price_mem_cache = {}       # {ticker: DataFrame|None}
_price_cache_lock = threading.Lock()
_price_cache_mtime = {}     # {ticker: float} — file mtime when last loaded


def _load_daily_prices(ticker):
    """Load daily price DataFrame, using in-memory cache.
    Re-reads from disk only if the file has been modified since last load."""
    safe = ticker.replace("/", "_").replace(":", "_")
    path = os.path.join(PRICE_CACHE_DIR, f"{safe}_day.csv")

    if not os.path.exists(path):
        _price_mem_cache[ticker] = None
        return None

    try:
        mtime = os.path.getmtime(path)
    except OSError:
        return _price_mem_cache.get(ticker)

    # Return cached if file hasn't changed
    if ticker in _price_mem_cache and _price_cache_mtime.get(ticker) == mtime:
        return _price_mem_cache[ticker]

    # (Re)load from disk
    try:
        df = pd.read_csv(path, parse_dates=["timestamp"])
        if df.empty:
            _price_mem_cache[ticker] = None
            _price_cache_mtime[ticker] = mtime
            return None
        df = df.sort_values("timestamp")
        _price_mem_cache[ticker] = df
        _price_cache_mtime[ticker] = mtime
        return df
    except Exception:
        _price_mem_cache[ticker] = None
        return None


def _get_latest_price(ticker):
    """Get the most recent closing price (from memory cache)."""
    df = _load_daily_prices(ticker)
    if df is None:
        return 0
    try:
        return float(df.iloc[-1]["close"])
    except Exception:
        return 0


def _invalidate_price_cache(ticker=None):
    """Clear price cache — all tickers or a specific one. Call after price data updates."""
    with _price_cache_lock:
        if ticker:
            _price_mem_cache.pop(ticker, None)
            _price_cache_mtime.pop(ticker, None)
        else:
            _price_mem_cache.clear()
            _price_cache_mtime.clear()
