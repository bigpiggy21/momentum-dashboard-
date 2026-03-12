"""
Momentum Dashboard — Main Application
Orchestrates data fetching, indicator calculations, change detection,
and serves the HTML dashboard.

Usage:
    python app.py                  # Run single scan + serve dashboard
    python app.py --scan-only      # Run scan without starting server
    python app.py --serve-only     # Start server with existing data
    python app.py --ticker NVDA    # Scan single ticker
"""

import argparse
import gzip
import json
import os
import requests
import sys
import threading
import time

# Fix Unicode output on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
from datetime import datetime, timedelta, timezone
from http.server import HTTPServer, SimpleHTTPRequestHandler, ThreadingHTTPServer
import urllib.parse

SAVED_SEARCHES_PATH = os.path.join(os.path.dirname(__file__), "saved_searches.json")
SAVED_LOG_FILTERS_PATH = os.path.join(os.path.dirname(__file__), "saved_log_filters.json")
FETCH_JOB_PATH = os.path.join(os.path.dirname(__file__), "sweep_fetch_job.json")

# In-memory sweep fetch progress (shared between background thread and API)
_sweep_fetch_progress = {
    "running": False,
    "completed": 0,
    "total": 0,
    "sweeps_found": 0,
    "current_ticker": "",
    "current_date": "",
    "rate": 0.0,
    "phase": "",       # "fetching", "detecting", "done", "error"
    "log_lines": [],   # last N log lines for the UI
}
_sweep_fetch_lock = threading.Lock()
_sweep_fetch_cancel = threading.Event()  # signal to cancel running fetch

# In-memory single-ticker backfill progress
_ticker_backfill = {
    "running": False,
    "ticker": "",
    "phase": "",       # "collecting", "computing", "done", "error"
    "message": "",
    "t0": 0,
}
_ticker_backfill_lock = threading.Lock()

# Unified live daemon instance (single WebSocket for price + sweeps)
_live_daemon = None
_live_daemon_lock = threading.Lock()
_daemon_intentionally_stopped = True  # True until user starts or auto-start fires

# Server-side tracker response cache (60s TTL)
_tracker_cache = {}       # {cache_key: {"data": response_dict, "ts": float}}
_TRACKER_CACHE_TTL = 60   # seconds

def _parse_asset_class(query):
    """Parse asset_class param from query string.
    Returns 'stock', 'etf', or 'all'. Backward-compat: etf_only=1 → 'etf'."""
    ac = (query or {}).get("asset_class", [None])[0]
    if ac in ("stock", "etf", "all"):
        return ac
    if (query or {}).get("etf_only", ["0"])[0] == "1":
        return "etf"
    return "stock"

def _asset_class_flags(ac):
    """Convert asset_class string to (exclude_etfs, etf_only) booleans."""
    if ac == "etf":
        return False, True
    elif ac == "all":
        return False, False
    else:  # "stock"
        return True, False

_portfolio_cache = {"data": None, "ts": 0}
_PORTFOLIO_CACHE_TTL = 300        # 5 minutes for positions/cash
_portfolio_prices_cache = {"prices": {}, "open_raw": set(), "ts": 0}
_PORTFOLIO_PRICES_TTL = 120       # 2 minutes for current prices (lightweight)
_portfolio_trades_resp_cache = {"data": None, "ts": 0}
_PORTFOLIO_TRADES_RESP_TTL = 30   # 30 seconds — assembled response cache (DB reads are fast)

# ── Portfolio helpers ──────────────────────────────────────────────────
def _get_t212_headers():
    """Return T212 API auth headers (Basic or Bearer)."""
    import base64
    from config import TRADING212_API_KEY
    t212_secret = getattr(__import__("config"), "TRADING212_API_SECRET", "")
    if t212_secret:
        creds = base64.b64encode(f"{TRADING212_API_KEY}:{t212_secret}".encode()).decode()
        return {"Authorization": f"Basic {creds}"}
    return {"Authorization": TRADING212_API_KEY}

_PORTFOLIO_BASELINE = 67522.0  # Estimated portfolio value on 2026-03-01 (derived from 8.4% gain = £5,691)

def _init_portfolio_db():
    """Create portfolio_snapshots table if not exists. Seeds March 1 baseline."""
    import sqlite3
    conn = sqlite3.connect(DB_PATH, timeout=5)
    # Check if we have the old date-only PK schema and need migration
    cur = conn.execute("PRAGMA table_info(portfolio_snapshots)")
    cols = [r[1] for r in cur.fetchall()]
    if cols and "ts" not in cols:
        # Old schema — migrate data
        old_rows = conn.execute("SELECT date, total_value, total_pct, note FROM portfolio_snapshots").fetchall()
        conn.execute("DROP TABLE portfolio_snapshots")
        conn.execute("""
            CREATE TABLE portfolio_snapshots (
                ts TEXT PRIMARY KEY,
                date TEXT,
                total_value REAL,
                baseline REAL,
                total_pct REAL,
                is_close INTEGER DEFAULT 0,
                note TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ps_date ON portfolio_snapshots(date)")
        for row in old_rows:
            pct = round((row[1] - _PORTFOLIO_BASELINE) / _PORTFOLIO_BASELINE * 100, 2) if row[1] else 0
            conn.execute(
                "INSERT OR IGNORE INTO portfolio_snapshots (ts, date, total_value, baseline, total_pct, is_close, note) "
                "VALUES (?, ?, ?, ?, ?, 0, ?)",
                (row[0] + "T12:00", row[0], row[1], _PORTFOLIO_BASELINE, pct, row[3]))
    else:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS portfolio_snapshots (
                ts TEXT PRIMARY KEY,
                date TEXT,
                total_value REAL,
                baseline REAL,
                total_pct REAL,
                is_close INTEGER DEFAULT 0,
                note TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ps_date ON portfolio_snapshots(date)")
    # Portfolio trades table — persistent trade history from T212
    conn.execute("""
        CREATE TABLE IF NOT EXISTS portfolio_trades (
            date TEXT NOT NULL,
            ticker TEXT NOT NULL,
            raw_ticker TEXT NOT NULL,
            name TEXT,
            side TEXT NOT NULL,
            avg_fill REAL NOT NULL,
            total_qty REAL NOT NULL,
            num_fills INTEGER DEFAULT 1,
            rpnl REAL DEFAULT 0,
            net_val REAL DEFAULT 0,
            synced_at TEXT NOT NULL,
            PRIMARY KEY (date, ticker, side)
        )
    """)

    # Seed March 1 baseline if not present
    has_baseline = conn.execute("SELECT 1 FROM portfolio_snapshots WHERE date = '2026-03-01'").fetchone()
    if not has_baseline:
        conn.execute(
            "INSERT OR IGNORE INTO portfolio_snapshots (ts, date, total_value, baseline, total_pct, is_close, note) "
            "VALUES ('2026-03-01T08:00', '2026-03-01', ?, ?, 0.0, 1, 'Estimated baseline')",
            (_PORTFOLIO_BASELINE, _PORTFOLIO_BASELINE))
    conn.commit()
    conn.close()

def _record_portfolio_snapshot(total_value, is_close=False):
    """Record an hourly portfolio snapshot. % change computed from the fixed March 1 baseline."""
    import sqlite3
    now = datetime.now(timezone.utc)
    ts = now.strftime("%Y-%m-%dT%H:%M")
    date_str = now.strftime("%Y-%m-%d")
    pct = round((total_value - _PORTFOLIO_BASELINE) / _PORTFOLIO_BASELINE * 100, 2) if _PORTFOLIO_BASELINE > 0 else 0.0

    conn = sqlite3.connect(DB_PATH, timeout=5)
    existing = conn.execute("SELECT 1 FROM portfolio_snapshots WHERE ts = ?", (ts,)).fetchone()
    if existing:
        conn.execute("UPDATE portfolio_snapshots SET total_value = ?, baseline = ?, total_pct = ?, is_close = ? WHERE ts = ?",
                      (total_value, _PORTFOLIO_BASELINE, pct, int(is_close), ts))
    else:
        conn.execute("INSERT INTO portfolio_snapshots (ts, date, total_value, baseline, total_pct, is_close, note) VALUES (?, ?, ?, ?, ?, ?, ?)",
                      (ts, date_str, total_value, _PORTFOLIO_BASELINE, pct, int(is_close), None))
    conn.commit()
    conn.close()

def _sync_portfolio_trades(full=False):
    """Sync T212 order history into portfolio_trades table.

    full=True:  paginate all pages (initial backfill)
    full=False: fetch page 1 only, stop if all orders already in DB (incremental)
    Returns (new_count, total_count).
    """
    import sqlite3
    from collections import defaultdict
    from config import TRADING212_API_KEY
    if not TRADING212_API_KEY or TRADING212_API_KEY == "YOUR_T212_API_KEY_HERE":
        return (0, 0)

    _init_portfolio_db()
    base = "https://live.trading212.com/api/v0"
    headers = _get_t212_headers()

    # Fetch order pages
    all_orders = []
    url = f"{base}/equity/history/orders?limit=50"
    max_pages = 20 if full else 1
    for page_num in range(max_pages):
        try:
            if page_num > 0:
                time.sleep(11)  # respect 6 req/min rate limit
            resp = requests.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            items = data.get("items", [])
            all_orders.extend(items)
            next_page = data.get("nextPagePath")
            if not next_page or not items:
                break
            url = f"https://live.trading212.com{next_page}"
        except Exception:
            break

    # Parse fills
    raw_fills = []
    for item in all_orders:
        order = item.get("order", {})
        fill = item.get("fill", {})
        fill_price = float(fill.get("price") or 0)
        if fill_price <= 0:
            continue
        filled_qty = float(fill.get("quantity") or order.get("filledQuantity") or 0)
        date_str = (fill.get("filledAt") or order.get("createdAt") or "")[:10]
        if date_str < "2026-03-01":
            continue
        raw_ticker = order.get("ticker", "")
        instrument = order.get("instrument", {})
        ticker_name = instrument.get("name", "")
        clean_ticker = raw_ticker.split("_")[0] if "_" in raw_ticker else raw_ticker
        clean_ticker = clean_ticker.rstrip("abcdefghijklmnopqrstuvwxyz") or clean_ticker
        side_str = order.get("side", "").upper()
        side = "Buy" if side_str == "BUY" else "Sell"
        rpnl = 0.0
        net_val = 0.0
        if side == "Sell":
            wallet = fill.get("walletImpact", {})
            rpnl = float(wallet.get("realisedProfitLoss") or 0)
            net_val = float(wallet.get("netValue") or 0)
        raw_fills.append({
            "date": date_str, "raw_ticker": raw_ticker, "ticker": clean_ticker,
            "name": ticker_name, "side": side, "fill_price": fill_price,
            "qty": filled_qty, "rpnl": rpnl, "net_val": net_val,
        })

    # Group by (date, ticker, side)
    groups = defaultdict(list)
    for f in raw_fills:
        groups[(f["date"], f["ticker"], f["side"])].append(f)

    # Upsert into DB
    now_str = datetime.now(timezone.utc).isoformat() + "Z"
    conn = sqlite3.connect(DB_PATH, timeout=5)
    new_count = 0
    for (date, ticker, side), fills in groups.items():
        name = fills[0]["name"]
        raw_t = fills[0]["raw_ticker"]
        total_qty = sum(f["qty"] for f in fills)
        num_fills = len(fills)
        if side == "Buy":
            total_cost = sum(f["fill_price"] * f["qty"] for f in fills)
            avg_fill = total_cost / total_qty if total_qty > 0 else 0
            rpnl_sum = 0.0
            net_sum = 0.0
        else:
            avg_fill = sum(f["fill_price"] * f["qty"] for f in fills) / total_qty if total_qty > 0 else 0
            rpnl_sum = sum(f["rpnl"] for f in fills)
            net_sum = sum(f["net_val"] for f in fills)

        existing = conn.execute(
            "SELECT 1 FROM portfolio_trades WHERE date=? AND ticker=? AND side=?",
            (date, ticker, side)).fetchone()
        conn.execute(
            "INSERT OR REPLACE INTO portfolio_trades "
            "(date, ticker, raw_ticker, name, side, avg_fill, total_qty, num_fills, rpnl, net_val, synced_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (date, ticker, raw_t, name, side, avg_fill, total_qty, num_fills, rpnl_sum, net_sum, now_str))
        if not existing:
            new_count += 1

    conn.commit()
    total_count = conn.execute("SELECT COUNT(*) FROM portfolio_trades").fetchone()[0]
    conn.close()
    return (new_count, total_count)


def _refresh_portfolio_prices():
    """Lightweight refresh of current prices from T212 positions API.
    Single call, no pagination. Cached for 2 minutes."""
    global _portfolio_prices_cache
    if time.time() - _portfolio_prices_cache["ts"] < _PORTFOLIO_PRICES_TTL:
        return _portfolio_prices_cache

    from config import TRADING212_API_KEY
    if not TRADING212_API_KEY or TRADING212_API_KEY == "YOUR_T212_API_KEY_HERE":
        return _portfolio_prices_cache

    try:
        base = "https://live.trading212.com/api/v0"
        headers = _get_t212_headers()
        resp = requests.get(f"{base}/equity/portfolio", headers=headers, timeout=15)
        resp.raise_for_status()
        prices = {}
        open_raw = set()
        for p in resp.json():
            raw = p.get("ticker", "")
            open_raw.add(raw)
            prices[raw] = {
                "currentPrice": float(p.get("currentPrice") or 0),
                "averagePrice": float(p.get("averagePrice") or 0),
            }
        _portfolio_prices_cache = {"prices": prices, "open_raw": open_raw, "ts": time.time()}
    except Exception as e:
        print(f"[PORTFOLIO] Price refresh error: {e}", flush=True)

    return _portfolio_prices_cache


def _get_portfolio_ticker_names():
    """Load ticker names from portfolio_trades DB + sweep engine fallback."""
    import sqlite3
    names = {}
    # DB names first (from T212 instrument data)
    try:
        _init_portfolio_db()
        conn = sqlite3.connect(DB_PATH, timeout=5)
        rows = conn.execute("SELECT ticker, name FROM portfolio_trades WHERE name IS NOT NULL AND name != ''").fetchall()
        conn.close()
        for ticker, name in rows:
            names[ticker] = name
    except Exception:
        pass
    # Sweep engine names as fallback
    try:
        from sweep_engine import load_ticker_names as _load_names
        for k, v in (_load_names() or {}).items():
            if k not in names:
                names[k] = v
    except Exception:
        pass
    return names


def _portfolio_snapshot_thread():
    """Background thread: records a portfolio snapshot every 5 min + keeps prices warm."""
    print("[PORTFOLIO] Snapshot thread started (5-min interval)", flush=True)
    while True:
        try:
            time.sleep(300)  # 5 minutes
            from config import TRADING212_API_KEY
            if not TRADING212_API_KEY or TRADING212_API_KEY == "YOUR_T212_API_KEY_HERE":
                continue
            headers = _get_t212_headers()
            base = "https://live.trading212.com/api/v0"
            resp = requests.get(f"{base}/equity/account/cash", headers=headers, timeout=15)
            resp.raise_for_status()
            total = float(resp.json().get("total", 0))
            if total <= 0:
                continue
            # Check if this is around LSE close (16:30 UK time)
            # UK is UTC+0 before ~Mar 29, UTC+1 after (BST)
            now_utc = datetime.now(timezone.utc)
            uk_hour = now_utc.hour  # ~correct for early March (UTC+0)
            is_close = (uk_hour == 16 and now_utc.minute >= 20) or (uk_hour == 17 and now_utc.minute <= 10)
            _record_portfolio_snapshot(total, is_close=is_close)
            if is_close:
                print(f"[PORTFOLIO] Snapshot: £{total:.0f} (close)", flush=True)
            # Keep prices warm so next page load has fresh gains
            try:
                _refresh_portfolio_prices()
            except Exception:
                pass
        except Exception as e:
            print(f"[PORTFOLIO] Snapshot error: {e}", flush=True)


def _portfolio_trade_sync_thread():
    """Background thread: syncs T212 order history to DB every 30 minutes."""
    try:
        from config import TRADING212_API_KEY
        if not TRADING212_API_KEY or TRADING212_API_KEY == "YOUR_T212_API_KEY_HERE":
            return
    except (ImportError, AttributeError):
        return
    # Initial sync on startup
    try:
        _init_portfolio_db()
        import sqlite3
        conn = sqlite3.connect(DB_PATH, timeout=5)
        count = conn.execute("SELECT COUNT(*) FROM portfolio_trades").fetchone()[0]
        conn.close()
        full = (count == 0)
        new, total = _sync_portfolio_trades(full=full)
        print(f"[PORTFOLIO] Trade sync ({'backfill' if full else 'incremental'}): "
              f"+{new} new ({total} total)", flush=True)
    except Exception as e:
        print(f"[PORTFOLIO] Initial trade sync failed: {e}", flush=True)

    # Periodic incremental sync
    while True:
        try:
            time.sleep(1800)  # 30 minutes
            new, total = _sync_portfolio_trades(full=False)
            if new > 0:
                print(f"[PORTFOLIO] Trade sync: +{new} new trades ({total} total)", flush=True)
        except Exception as e:
            print(f"[PORTFOLIO] Trade sync error: {e}", flush=True)

# ── Daemon watchdog ──────────────────────────────────────────────────────
_WATCHDOG_INTERVAL = 30         # seconds between health checks
_WATCHDOG_MAX_RESTARTS = 5      # max restarts per stability window
_WATCHDOG_STABILITY_WINDOW = 600  # seconds (10 min) — resets restart counter


def _daemon_watchdog():
    """Background thread: auto-restarts the live daemon if it dies unexpectedly.

    Respects _daemon_intentionally_stopped — if the user clicked Stop, the
    watchdog stays dormant until the user clicks Start again.
    """
    global _live_daemon, _daemon_intentionally_stopped
    restart_count = 0
    last_restart_time = 0

    while True:
        time.sleep(_WATCHDOG_INTERVAL)

        # Reset counter after a period of stability
        if time.time() - last_restart_time > _WATCHDOG_STABILITY_WINDOW:
            restart_count = 0

        with _live_daemon_lock:
            if _daemon_intentionally_stopped:
                continue  # user stopped it — don't restart

            if _live_daemon is None:
                continue  # never started

            status = _live_daemon.get_status()
            if status.get("running"):
                continue  # healthy

            # Daemon was running but has died
            if restart_count >= _WATCHDOG_MAX_RESTARTS:
                if restart_count == _WATCHDOG_MAX_RESTARTS:  # log once
                    print(f"[WATCHDOG] Daemon has crashed {restart_count} times in "
                          f"{_WATCHDOG_STABILITY_WINDOW}s — giving up. "
                          f"Manual restart required.", flush=True)
                    restart_count += 1  # prevent repeat logging
                continue

            restart_count += 1
            last_restart_time = time.time()
            error_msg = status.get("error", "unknown")
            print(f"[WATCHDOG] Daemon died (error: {error_msg}). "
                  f"Restarting (attempt {restart_count}/{_WATCHDOG_MAX_RESTARTS})...",
                  flush=True)

            try:
                from live_daemon import (UnifiedLiveDaemon, load_live_config,
                                         load_live_price_config)
                scfg = load_live_config()
                pcfg = load_live_price_config()
                _live_daemon = UnifiedLiveDaemon(price_config=pcfg, sweep_config=scfg)
                _live_daemon.start()
                print(f"[WATCHDOG] Daemon restarted successfully.", flush=True)
            except Exception as e:
                print(f"[WATCHDOG] Restart failed: {e}", flush=True)
                import traceback
                traceback.print_exc()


def _save_fetch_job(tickers, start_date, end_date):
    """Persist fetch job params so we can resume after a crash."""
    try:
        with open(FETCH_JOB_PATH, "w", encoding="utf-8") as f:
            json.dump({"tickers": tickers, "start_date": start_date,
                        "end_date": end_date}, f)
    except Exception as e:
        print(f"[SWEEP] Could not save fetch job: {e}", flush=True)


def _clear_fetch_job():
    """Remove the fetch job file (fetch completed successfully)."""
    try:
        if os.path.exists(FETCH_JOB_PATH):
            os.remove(FETCH_JOB_PATH)
    except Exception:
        pass


def _load_fetch_job():
    """Load an incomplete fetch job (returns dict or None)."""
    try:
        if os.path.exists(FETCH_JOB_PATH):
            with open(FETCH_JOB_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return None


def _load_saved_searches():
    """Load saved searches from JSON file. Returns empty dict if file missing."""
    try:
        with open(SAVED_SEARCHES_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def _save_saved_searches(data):
    """Write saved searches dict to JSON file."""
    with open(SAVED_SEARCHES_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def _load_log_filters():
    """Load saved log filters from JSON file. Returns empty dict if file missing."""
    try:
        with open(SAVED_LOG_FILTERS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def _save_log_filters(data):
    """Write saved log filters dict to JSON file."""
    with open(SAVED_LOG_FILTERS_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

from config import (
    TICKER_GROUPS, PROTOTYPE_TICKERS, TIMEFRAMES,
    REFRESH_INTERVAL_SECONDS, DB_PATH, MASSIVE_API_KEY, MASSIVE_BASE_URL,
    OVERVIEW_GROUPS, INDICATOR_VERSION, _load_watchlists,
)
from config import WATCHLISTS as _INITIAL_WATCHLISTS
from data_fetcher import fetch_ticker_data, build_timeframe_data, clear_cache, fetch_dividends, get_data_fingerprint
from indicators import calculate_all_indicators
from change_detector import (
    init_db, detect_changes, save_snapshot,
    get_recent_events, get_latest_full_state, get_state_at_time,
    check_scan_cache, update_scan_cache,
    get_hvc_events_today, get_hvc_events_history, get_hvc_ticker_summary,
)
from scheduler import WatchlistScheduler
from sweep_engine import (
    init_sweep_db, fetch_and_store_sweeps, detect_clusterbombs,
    get_sweep_summary, get_sweep_detail, get_clusterbombs,
    get_sweep_stats, get_sweep_chart_data, check_api_access,
    get_tracker_data, rebuild_stats_cache, rebuild_daily_summary,
    refresh_etf_cache, load_etf_set,
    load_ticker_names, refresh_ticker_names,
)

# Global scheduler instance
scheduler = WatchlistScheduler()

# Mutable global — updated by reload_watchlists()
WATCHLISTS = dict(_INITIAL_WATCHLISTS)

def reload_watchlists():
    """Re-scan watchlists/ folder to pick up new/changed watchlist files without server restart."""
    global WATCHLISTS
    try:
        WATCHLISTS.clear()
        WATCHLISTS.update(_load_watchlists())
    except Exception as e:
        print(f"[SERVER] Watchlist reload failed: {e}", flush=True)


def get_group_for_ticker(ticker):
    """Find which thematic group a ticker belongs to."""
    for group_name, tickers in TICKER_GROUPS:
        for display, api, atype in tickers:
            if display == ticker:
                return group_name
    return "Other"


def get_ticker_info(display_ticker):
    """Get API ticker and asset type for a display ticker."""
    for wl_name, wl_groups in WATCHLISTS.items():
        for group_name, tickers in wl_groups:
            for display, api, atype in tickers:
                if display == display_ticker:
                    return api, atype
    return display_ticker, "stock"


def scan_ticker(display_ticker):
    """Run full scan for a single ticker: fetch, calculate, detect changes, save.
    Skips computation entirely if data hasn't changed since last scan."""
    api_ticker, asset_type = get_ticker_info(display_ticker)
    
    # 1. Quick check — if data and indicators unchanged, skip EVERYTHING
    #    (no CSV loading, no computation, no API calls)
    data_hash = get_data_fingerprint(api_ticker)
    
    if data_hash:
        unadj_cached = check_scan_cache(display_ticker, 0, data_hash, INDICATOR_VERSION)
        adj_cached = check_scan_cache(display_ticker, 1, data_hash, INDICATOR_VERSION)
        
        if unadj_cached and adj_cached:
            return "skipped"
    
    # 2. Fetch data (updates cache files if new data available)
    raw_data = fetch_ticker_data(display_ticker, api_ticker, asset_type)
    
    if raw_data["daily"].empty:
        print(f"  [SCAN] {display_ticker}: No data, skipping", flush=True)
        return None
    
    # Re-check fingerprint after fetch (data may have changed from API)
    data_hash = get_data_fingerprint(api_ticker)
    
    # If data still matches after fetch, we can skip computation
    if data_hash:
        unadj_cached = check_scan_cache(display_ticker, 0, data_hash, INDICATOR_VERSION)
        adj_cached = check_scan_cache(display_ticker, 1, data_hash, INDICATOR_VERSION)
        
        if unadj_cached and adj_cached:
            print(f"  [SCAN] {display_ticker}: Data unchanged, skipping", flush=True)
            return "skipped"
    
    print(f"  [SCAN] {display_ticker}: Computing indicators...", flush=True)
    
    # 3. Build timeframe data (unadjusted)
    tf_data = build_timeframe_data(raw_data)
    
    # 4. Calculate indicators (unadjusted)
    results = calculate_all_indicators(tf_data, raw_data.get("weekly", None))
    price = results.get('_meta', {}).get('price', 'N/A')
    
    # 5. Detect changes against previous snapshot (unadjusted)
    events = detect_changes(display_ticker, results, div_adj=0)
    if events:
        print(f"  [SCAN] {len(events)} changes: ", end="")
        print(", ".join(f"[{e['timeframe']}] {e['description']}" for e in events[:3]), flush=True)
        if len(events) > 3:
            print(f"     ... and {len(events)-3} more")
    
    # 6. Save snapshot (unadjusted)
    save_snapshot(display_ticker, results, div_adj=0)
    if data_hash:
        update_scan_cache(display_ticker, 0, data_hash, INDICATOR_VERSION)
    
    # 7. Dividend-adjusted pass
    if asset_type in ("stock", "etf"):
        try:
            dividends = fetch_dividends(api_ticker)
            if not dividends.empty:
                tf_data_adj = build_timeframe_data(raw_data, dividends=dividends)
                results_adj = calculate_all_indicators(tf_data_adj, raw_data.get("weekly", None))
                detect_changes(display_ticker, results_adj, div_adj=1)
                save_snapshot(display_ticker, results_adj, div_adj=1)
            else:
                save_snapshot(display_ticker, results, div_adj=1)
        except Exception as e:
            save_snapshot(display_ticker, results, div_adj=1)
            print(f"  [SCAN] Dividend adjustment failed: {e}", flush=True)
    else:
        save_snapshot(display_ticker, results, div_adj=1)
    
    if data_hash:
        update_scan_cache(display_ticker, 1, data_hash, INDICATOR_VERSION)
    
    print(f"  [SCAN] {display_ticker}: ${price}", flush=True)
    return results


def scan_all(tickers=None):
    """Run scan across all tickers (or a subset)."""
    if tickers is None:
        # Build flat list from all watchlists, deduplicated
        seen = set()
        tickers = []
        for wl_name, wl_groups in WATCHLISTS.items():
            for group_name, group_tickers in wl_groups:
                for display, api, atype in group_tickers:
                    if display not in seen:
                        seen.add(display)
                        tickers.append(display)
    
    print(f"\n[SCAN] === MOMENTUM DASHBOARD SCAN — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===", flush=True)
    print(f"[SCAN] Scanning {len(tickers)} tickers", flush=True)
    
    all_results = {}
    skipped = 0
    for i, ticker in enumerate(tickers):
        try:
            result = scan_ticker(ticker)
            if result == "skipped":
                skipped += 1
            elif result:
                all_results[ticker] = result
        except Exception as e:
            print(f"  [SCAN] ERROR {ticker}: {e}", flush=True)
            import traceback
            traceback.print_exc()
    
    if skipped:
        print(f"[SCAN] Complete: {len(all_results)} computed, {skipped} skipped, {len(tickers)} total.", flush=True)
    else:
        print(f"[SCAN] Complete: {len(all_results)}/{len(tickers)} tickers processed.", flush=True)

    # Summary of recent events
    events = get_recent_events(limit=50, since=(datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat())
    if events:
        print(f"[SCAN] Recent changes ({len(events)}):", flush=True)
        for e in events:
            print(f"  {e['timestamp'][:19]} | {e['ticker']:6s} | {e['timeframe']:4s} | {e['description']}", flush=True)
    
    return all_results


# ============================================================
# DASHBOARD SERVER
# ============================================================

class DashboardHandler(SimpleHTTPRequestHandler):
    """HTTP handler that serves the dashboard and API endpoints."""
    
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)
        
        if path.startswith("/static/"):
            self.serve_static(path[8:])  # strip "/static/"
        elif path == "/" or path == "/index.html" or path == "/home":
            self.serve_page("home.html")
        elif path == "/pitch":
            self.serve_page("pitch.html")
        elif path == "/bollingers":
            self.serve_dashboard()
        elif path == "/log":
            self.serve_page("log.html")
        elif path == "/watchlists":
            self.serve_page("watchlists.html")
        elif path == "/search":
            self.serve_page("search.html")
        elif path == "/settings":
            self.serve_page("settings.html")
        elif path == "/backtest":
            self.serve_page("backtest.html")
        elif path == "/api/backtest/status":
            self.serve_backtest_status()
        elif path == "/api/backtest/trade-chart":
            self.serve_backtest_trade_chart()
        elif path == "/hvc":
            self.serve_page("hvc.html")
        elif path == "/sweeps":
            self.serve_page("sweeps_unified.html")
        elif path == "/chart" or path == "/0dte":
            self.serve_page("chart.html")
        elif path == "/etf-sweeps":
            self.send_response(302)
            self.send_header("Location", "/sweeps?asset=etf")
            self.end_headers()
        elif path == "/analysis":
            self.serve_page("analysis.html")
        elif path == "/rs":
            self.serve_page("rs.html")
        elif path == "/tv":
            self.serve_page("tv.html")
        elif path == "/api/rs/rankings":
            self.serve_rs_rankings(query)
        elif path == "/api/rs/movers":
            self.serve_rs_movers(query)
        elif path == "/api/rs/breakouts":
            self.serve_rs_breakouts(query)
        elif path == "/api/rs/sectors":
            self.serve_rs_sectors(query)
        elif path == "/api/rs/history":
            self.serve_rs_history(query)
        elif path == "/api/rs/status":
            self.serve_rs_status(query)
        elif path == "/api/rs/spy":
            self.serve_rs_spy(query)
        elif path == "/api/rs/report":
            self.serve_rs_report(query)
        elif path == "/api/rs/report/dates":
            self.serve_rs_report_dates(query)
        elif path == "/api/rs/report/snapshot":
            self.serve_rs_report_snapshot(query)
        elif path == "/api/hvc/today":
            self.serve_hvc_today(query)
        elif path == "/api/hvc/history":
            self.serve_hvc_history(query)
        elif path == "/api/state":
            self.serve_state(query)
        elif path == "/api/events":
            self.serve_events(query)
        elif path == "/api/delta":
            self.serve_delta(query)
        elif path == "/api/groups":
            self.serve_groups(query)
        elif path == "/api/watchlists":
            self.serve_watchlists()
        elif path == "/api/searches":
            self.serve_saved_searches()
        elif path == "/api/log-filters":
            self.serve_log_filters()
        elif path == "/api/sweeps/api-check":
            self.serve_sweep_api_check()
        elif path == "/api/sweeps/stats":
            self.serve_sweep_stats(query)
        elif path == "/api/sweeps/summary":
            self.serve_sweep_summary(query)
        elif path == "/api/sweeps/diag":
            self.serve_sweep_diag(query)
        elif path == "/api/sweeps/detail":
            self.serve_sweep_detail(query)
        elif path == "/api/sweeps/clusterbombs":
            self.serve_clusterbombs(query)
        elif path == "/api/sweeps/chart":
            self.serve_sweep_chart(query)
        elif path == "/api/sweeps/tracker":
            self.serve_sweep_tracker(query)
        elif path == "/api/sweeps/detection-config":
            self.serve_sweep_detection_config()
        elif path == "/api/sweeps/fetch-progress":
            self.serve_sweep_fetch_progress()
        elif path == "/api/sweeps/pending-queue":
            self.serve_pending_queue()
        elif path == "/api/prices/live":
            self.serve_live_prices(query)
        elif path == "/api/sweeps/live/status":
            self.serve_live_sweep_status()
        elif path == "/api/sweeps/live/events":
            self.serve_live_sweep_events()
        elif path == "/api/sweeps/live/config":
            self.serve_live_sweep_get_config()
        elif path == "/api/sweeps/sectors":
            self.serve_sweep_sectors()
        elif path == "/api/sweeps/etf-categories":
            self.serve_etf_categories()
        elif path == "/api/ticker-names":
            self.serve_ticker_names()
        elif path == "/api/chart/candles" or path == "/api/0dte/candles":
            self.serve_chart_candles(query)
        elif path == "/api/chart/live-bar":
            self.serve_chart_live_bar(query)
        elif path == "/api/chart/bb-signals":
            self.serve_bb_signals(query)
        elif path == "/api/chart/sweeps" or path == "/api/0dte/sweeps":
            self.serve_chart_sweeps(query)
        elif path == "/api/analysis/river":
            self.serve_analysis_river(query)
        elif path == "/api/analysis/heatmap":
            self.serve_analysis_heatmap(query)
        elif path == "/api/scheduler/config":
            self.serve_scheduler_config()
        elif path == "/api/scheduler/status":
            self.serve_scheduler_status()
        elif path == "/api/scheduler/ticker-backfill-status":
            self.serve_ticker_backfill_status()
        elif path == "/api/scheduler/all-tickers":
            self.serve_all_tickers()
        elif path == "/api/daemon/status":
            self.serve_daemon_status()
        elif path == "/api/scheduler/live-price/status":
            self.serve_live_price_status()
        elif path == "/api/scheduler/live-price/config":
            self.serve_live_price_get_config()
        elif path == "/api/eod-compute/config":
            self.serve_eod_compute_config()
        elif path == "/api/eod-compute/status":
            self.serve_eod_compute_status()
        elif path == "/api/scheduler/indicator-compute/config":
            self.serve_indicator_compute_config()
        elif path == "/api/scheduler/indicator-compute/status":
            self.serve_indicator_compute_status()
        # ── TradingView UDF Datafeed Endpoints ──────────────────────
        elif path == "/api/tv/config":
            self.serve_tv_config()
        elif path == "/api/tv/time":
            self.send_json(str(int(time.time())))
        elif path == "/api/tv/symbols":
            self.serve_tv_symbols(query)
        elif path == "/api/tv/search":
            self.serve_tv_search(query)
        elif path == "/api/tv/history":
            self.serve_tv_history(query)
        elif path == "/api/tv/marks":
            self.serve_tv_marks(query)
        elif path == "/api/tv/timescale_marks":
            self.serve_tv_timescale_marks(query)
        elif path == "/api/sweeps/pinescript":
            self.serve_sweep_pinescript(query)
        elif path == "/api/sweeps/pinescript-all":
            self.serve_sweep_pinescript_all(query)
        else:
            super().do_GET()
    
    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        
        if path == "/api/watchlists/save":
            self.save_watchlists()
        elif path == "/api/watchlists/refresh":
            self.refresh_watchlists()
        elif path == "/api/search":
            self.serve_search()
        elif path == "/api/searches/save":
            self.save_search()
        elif path == "/api/searches/delete":
            self.delete_search()
        elif path == "/api/log-filters/save":
            self.save_log_filter()
        elif path == "/api/log-filters/delete":
            self.delete_log_filter()
        elif path == "/api/scheduler/config":
            self.save_scheduler_config()
        elif path == "/api/scheduler/trigger":
            self.trigger_scheduler()
        elif path == "/api/scheduler/trigger-ticker":
            self.trigger_ticker_backfill()
        elif path == "/api/rs/trigger":
            self.trigger_rs_engine()
        elif path == "/api/scheduler/toggle":
            self.toggle_scheduler()
        elif path == "/api/backtest/run":
            self.serve_backtest_run()
        elif path == "/api/backtest/precompute":
            self.serve_backtest_precompute()
        elif path == "/api/backtest/nuke-cache":
            self.serve_backtest_nuke_cache()
        elif path == "/api/sweeps/fetch":
            self.serve_sweep_fetch()
        elif path == "/api/sweeps/fetch-cancel":
            self.serve_sweep_fetch_cancel()
        elif path == "/api/sweeps/fetch-queue":
            self.serve_sweep_fetch_queue()
        elif path == "/api/sweeps/redetect":
            self.serve_sweep_redetect()
        elif path == "/api/sweeps/detection-config":
            self.save_sweep_detection_config()
        elif path == "/api/sweeps/rebuild-cache":
            self.serve_rebuild_cache()
        elif path == "/api/sweeps/live/start":
            self.serve_live_sweep_start()
        elif path == "/api/sweeps/live/stop":
            self.serve_live_sweep_stop()
        elif path == "/api/sweeps/live/config":
            self.serve_live_sweep_save_config()
        elif path == "/api/daemon/config":
            self.serve_daemon_save_config()
        elif path == "/api/scheduler/live-price/start":
            self.serve_live_price_start()
        elif path == "/api/scheduler/live-price/stop":
            self.serve_live_price_stop()
        elif path == "/api/scheduler/live-price/config":
            self.serve_live_price_save_config()
        elif path == "/api/eod-compute/config":
            self.save_eod_compute_config_handler()
        elif path == "/api/eod-compute/trigger":
            self.trigger_eod_compute()
        elif path == "/api/scheduler/indicator-compute/config":
            self.serve_indicator_compute_save_config()
        elif path == "/api/scheduler/indicator-compute/trigger":
            self.serve_indicator_compute_trigger()
        else:
            self.send_response(404)
            self.end_headers()
    
    def serve_static(self, filename):
        """Serve a static file from static/ directory (CSS, JS, images)."""
        # Prevent directory traversal — normalise and ensure it stays within static/
        safe = os.path.normpath(filename).replace("\\", "/")
        if safe.startswith("..") or safe.startswith("/"):
            self.send_response(403)
            self.end_headers()
            self.wfile.write(b"Forbidden")
            return
        static_path = os.path.join(os.path.dirname(__file__), "static", safe)
        content_types = {
            ".css": "text/css; charset=utf-8",
            ".js": "application/javascript; charset=utf-8",
            ".svg": "image/svg+xml",
            ".png": "image/png",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".ico": "image/x-icon",
        }
        ext = os.path.splitext(safe)[1].lower()
        ctype = content_types.get(ext, "application/octet-stream")
        try:
            mode = "r" if ext in (".css", ".js", ".svg") else "rb"
            with open(static_path, mode, encoding="utf-8" if mode == "r" else None) as f:
                data = f.read()
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(data.encode() if isinstance(data, str) else data)
        except FileNotFoundError:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Static file not found")

    def serve_page(self, filename):
        """Serve an HTML page from templates/."""
        page_path = os.path.join(os.path.dirname(__file__), "templates", filename)
        try:
            with open(page_path, "r", encoding="utf-8") as f:
                content = f.read()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(content.encode())
        except FileNotFoundError:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Page not found")
    
    def serve_dashboard(self):
        """Serve the main HTML dashboard."""
        dashboard_path = os.path.join(os.path.dirname(__file__), "templates", "dashboard.html")
        try:
            with open(dashboard_path, "r", encoding="utf-8") as f:
                content = f.read()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(content.encode())
        except FileNotFoundError:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b"Dashboard template not found")
    
    def serve_state(self, query=None):
        """Serve current full state as JSON. Optionally filter by ticker."""
        try:
            div_adj = int((query or {}).get("adj", [0])[0])
            ticker = (query or {}).get("ticker", [None])[0]
            tickers = [ticker] if ticker else None
            state = get_latest_full_state(div_adj=div_adj, tickers=tickers)
            self.send_json(state)
        except Exception as e:
            print(f"[SERVER] serve_state error: {e}", flush=True)
            import traceback
            traceback.print_exc()
            self.send_json({})
    
    def serve_events(self, query):
        """Serve recent events as JSON with filtering."""
        limit = int(query.get("limit", [100])[0])
        ticker = query.get("ticker", [None])[0]
        tf_min = query.get("tf_min", [None])[0]
        indicator = query.get("indicator", [None])[0]
        since = query.get("since", [None])[0]
        bar_status = query.get("bar_status", [None])[0]
        div_adj_raw = query.get("div_adj", [None])[0]
        div_adj = int(div_adj_raw) if div_adj_raw is not None else None

        events = get_recent_events(
            limit=limit, ticker=ticker,
            timeframe_min=tf_min, indicator=indicator,
            since=since, bar_status=bar_status,
            div_adj=div_adj,
        )
        self.send_json(events)
    
    def _get_previous_session_close(self, div_adj):
        """Find the end-of-day timestamp from the previous trading session.

        "Last Session" means: the state of indicators at the close of the
        previous trading day (skipping weekends/holidays).
        Falls back to the most recent snapshot day before today if the
        ideal trading day has no data yet.
        """
        import sqlite3 as _sqlite3
        from datetime import timezone
        from trading_calendar import is_trading_day

        now = datetime.now(timezone.utc)
        today_str = now.strftime("%Y-%m-%d")

        # Walk backwards to find the previous trading day
        check_date = (now - timedelta(days=1)).date()
        for _ in range(10):
            if is_trading_day(check_date):
                break
            check_date -= timedelta(days=1)

        prev_close = datetime(check_date.year, check_date.month, check_date.day,
                              23, 59, 59, tzinfo=timezone.utc)

        # Verify snapshots exist for this date; if not, fall back to the
        # most recent snapshot day before today
        conn = _sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM snapshots WHERE timestamp <= ? AND div_adj=?",
                  (prev_close.isoformat(), div_adj))
        count = c.fetchone()[0]
        if count == 0:
            # Fall back: find the latest snapshot day before today
            c.execute("""SELECT MAX(substr(timestamp,1,10)) FROM snapshots
                         WHERE substr(timestamp,1,10) < ? AND div_adj=?""",
                      (today_str, div_adj))
            row = c.fetchone()
            if row and row[0]:
                fallback_date = row[0]  # e.g. "2026-02-22"
                prev_close = datetime(int(fallback_date[:4]), int(fallback_date[5:7]),
                                      int(fallback_date[8:10]), 23, 59, 59, tzinfo=timezone.utc)
        conn.close()
        return prev_close

    def serve_delta(self, query):
        """Serve delta between current state and state at a previous time."""
        period = query.get("period", ["last"])[0]
        div_adj = int(query.get("adj", [0])[0])
        ticker_filter = query.get("ticker", [None])[0]
        tickers = [ticker_filter] if ticker_filter else None

        now = datetime.now(timezone.utc)
        if period == "last":
            compare_time = self._get_previous_session_close(div_adj)
            if not compare_time:
                compare_time = now - timedelta(days=1)
        elif period == "1d":
            compare_time = now - timedelta(days=1)
        elif period == "1w":
            compare_time = now - timedelta(weeks=1)
        elif period == "1m":
            compare_time = now - timedelta(days=30)
        elif period == "3m":
            compare_time = now - timedelta(days=90)
        else:
            compare_time = now - timedelta(days=1)
        
        old_state = get_state_at_time(compare_time.isoformat(), div_adj=div_adj, tickers=tickers)
        current_state = get_latest_full_state(div_adj=div_adj, tickers=tickers)
        
        # Build delta: for each ticker/tf, compare old vs current
        # Only include cells that changed
        delta = {}
        for ticker, current_data in current_state.items():
            ticker_delta = {}
            old_data = old_state.get(ticker, {})
            
            for tf in ["1H","4H","8H","1D","2D","3D","5D","1W","2W","6W","1M","3M","6M","12M"]:
                cur = current_data.get(tf, {})
                old = old_data.get(tf, {})
                
                if not cur or not old:
                    continue
                
                changed = {}
                if cur.get("mom_color") and cur.get("mom_color") != old.get("mom_color"):
                    changed["mom_color"] = cur["mom_color"]
                    changed["mom_rising"] = cur.get("mom_rising")
                if cur.get("sqz_state") and cur.get("sqz_state") != old.get("sqz_state"):
                    changed["sqz_state"] = cur["sqz_state"]
                if cur.get("band_pos") and cur.get("band_pos") != old.get("band_pos"):
                    changed["band_pos"] = cur["band_pos"]
                if cur.get("acc_state") and cur.get("acc_state") != old.get("acc_state"):
                    changed["acc_state"] = cur["acc_state"]
                if cur.get("dev_signal") and cur.get("dev_signal") != old.get("dev_signal", ""):
                    changed["dev_signal"] = cur["dev_signal"]
                
                if changed:
                    ticker_delta[tf] = changed
            
            if ticker_delta:
                delta[ticker] = ticker_delta
        
        self.send_json(delta)
    
    def serve_groups(self, query=None):
        """Serve ticker group structure for a given watchlist."""
        reload_watchlists()
        wl_name = (query or {}).get("watchlist", [None])[0]
        if wl_name and wl_name in WATCHLISTS:
            source_groups = WATCHLISTS[wl_name]
        else:
            source_groups = TICKER_GROUPS
        
        groups = []
        for group_name, tickers in source_groups:
            groups.append({
                "name": group_name,
                "tickers": [{"display": d, "api": a, "type": t} for d, a, t in tickers],
            })
        self.send_json(groups)
    
    def serve_watchlists(self):
        """Serve list of available watchlists with full group/ticker detail."""
        reload_watchlists()
        result = []
        for name, groups in WATCHLISTS.items():
            wl = {
                "name": name,
                "groups": [],
            }
            for group_name, tickers in groups:
                wl["groups"].append({
                    "name": group_name,
                    "tickers": [{"display": d, "api": a, "type": t} for d, a, t in tickers],
                })
            result.append(wl)
        self.send_json(result)

    def refresh_watchlists(self):
        """POST /api/watchlists/refresh — re-scan watchlists/ folder and return updated list."""
        try:
            reload_watchlists()
            # Also force scheduler to re-discover new watchlists
            scheduler.reload_config()
            total_tickers = 0
            wl_names = []
            for name, groups in WATCHLISTS.items():
                wl_names.append(name)
                for _, tickers in groups:
                    total_tickers += len(tickers)
            print(f"[SERVER] Watchlists refreshed: {len(wl_names)} watchlists, {total_tickers} tickers", flush=True)
            self.send_json({"ok": True, "watchlists": len(wl_names), "tickers": total_tickers,
                            "names": wl_names})
        except Exception as e:
            self.send_json({"ok": False, "error": str(e)})

    def serve_saved_searches(self):
        """GET /api/searches — return all saved search configurations."""
        self.send_json(_load_saved_searches())

    def save_search(self):
        """POST /api/searches/save — save a named search configuration."""
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length else {}
            name = body.get("name", "").strip()
            if not name:
                self.send_json({"ok": False, "error": "Name is required"})
                return
            searches = _load_saved_searches()
            searches[name] = {
                "criteria": body.get("criteria", []),
                "adj": int(body.get("adj", 0)),
                "watchlist": body.get("watchlist", ""),
                "created": datetime.now(timezone.utc).isoformat(),
            }
            _save_saved_searches(searches)
            self.send_json({"ok": True, "name": name})
        except Exception as e:
            self.send_json({"ok": False, "error": str(e)})

    def delete_search(self):
        """POST /api/searches/delete — delete a saved search by name."""
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length else {}
            name = body.get("name", "").strip()
            if not name:
                self.send_json({"ok": False, "error": "Name is required"})
                return
            searches = _load_saved_searches()
            if name in searches:
                del searches[name]
                _save_saved_searches(searches)
                self.send_json({"ok": True})
            else:
                self.send_json({"ok": False, "error": "Search not found"})
        except Exception as e:
            self.send_json({"ok": False, "error": str(e)})

    def serve_log_filters(self):
        """GET /api/log-filters — return all saved log filter configurations."""
        self.send_json(_load_log_filters())

    def save_log_filter(self):
        """POST /api/log-filters/save — save a named log filter configuration."""
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length else {}
            name = body.get("name", "").strip()
            if not name:
                self.send_json({"ok": False, "error": "Name is required"})
                return
            filters = _load_log_filters()
            filters[name] = {
                "timeScope": body.get("timeScope", "24h"),
                "barStatus": body.get("barStatus", "all"),
                "activeInds": body.get("activeInds", {}),
                "activeTFs": body.get("activeTFs", []),
                "ticker": body.get("ticker", ""),
                "created": datetime.now(timezone.utc).isoformat(),
            }
            _save_log_filters(filters)
            self.send_json({"ok": True, "name": name})
        except Exception as e:
            self.send_json({"ok": False, "error": str(e)})

    def delete_log_filter(self):
        """POST /api/log-filters/delete — delete a saved log filter by name."""
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length else {}
            name = body.get("name", "").strip()
            if not name:
                self.send_json({"ok": False, "error": "Name is required"})
                return
            filters = _load_log_filters()
            if name in filters:
                del filters[name]
                _save_log_filters(filters)
                self.send_json({"ok": True})
            else:
                self.send_json({"ok": False, "error": "Filter not found"})
        except Exception as e:
            self.send_json({"ok": False, "error": str(e)})

    # ---- Relative Strength API handlers ----

    def serve_rs_rankings(self, query=None):
        """GET /api/rs/rankings — return RS rankings with optional filters.

        Query params:
            universe  — watchlist name (default: Russell3000)
            min_rs    — minimum RS rating to include (default: 1)
            sort      — column to sort by (default: rs_rating)
            order     — asc|desc (default: desc)
            limit     — max results (default: 200)
            sector    — filter by sector name
            above_wma — 1 to filter for above 30WMA only
        """
        try:
            from change_detector import _get_db
            universe = (query or {}).get("universe", ["Russell3000"])[0]
            min_rs = int((query or {}).get("min_rs", [1])[0])
            sort_col = (query or {}).get("sort", ["rs_rating"])[0]
            order = (query or {}).get("order", ["desc"])[0].upper()
            limit = int((query or {}).get("limit", [200])[0])
            sector = (query or {}).get("sector", [None])[0]
            above_wma = (query or {}).get("above_wma", [None])[0]

            allowed_sorts = {"rs_rating", "rs_rank", "rs_score", "rs_change_5d",
                             "rs_change_20d", "price_change_pct",
                             "price_return_1m", "price_return_3m", "price_return_6m",
                             "monster_score", "hvc_count", "gap_count", "gaps_open",
                             "ticker", "sector", "price",
                             "breakout_count", "new_high_count"}
            if sort_col not in allowed_sorts:
                sort_col = "rs_rating"
            if order not in ("ASC", "DESC"):
                order = "DESC"

            conn = _get_db()
            c = conn.cursor()

            # Join meta_snapshots for price 30WMA fallback + rs_events for breakout counts
            from datetime import timezone
            since_30d = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")

            sql = """SELECT r.*,
                     COALESCE(r.above_rs_30wma, m.above_wma30) as above_wma30_effective,
                     COALESCE(eb.breakout_count, 0) as breakout_count,
                     COALESCE(en.new_high_count, 0) as new_high_count
                     FROM rs_rankings r
                     LEFT JOIN (
                         SELECT ticker, above_wma30
                         FROM meta_snapshots
                         WHERE id IN (SELECT MAX(id) FROM meta_snapshots GROUP BY ticker)
                     ) m ON r.ticker = m.ticker
                     LEFT JOIN (
                         SELECT ticker, COUNT(*) as breakout_count
                         FROM rs_events
                         WHERE universe = ? AND event_type = 'rs_breakout' AND event_date >= ?
                         GROUP BY ticker
                     ) eb ON r.ticker = eb.ticker
                     LEFT JOIN (
                         SELECT ticker, COUNT(*) as new_high_count
                         FROM rs_events
                         WHERE universe = ? AND event_type = 'rs_new_high' AND event_date >= ?
                         GROUP BY ticker
                     ) en ON r.ticker = en.ticker
                     WHERE r.universe = ? AND r.rs_rating >= ?"""
            params = [universe, since_30d, universe, since_30d, universe, min_rs]

            if sector:
                sql += " AND r.sector = ?"
                params.append(sector)
            if above_wma == "1":
                sql += " AND COALESCE(r.above_rs_30wma, m.above_wma30) = 1"

            sql += f" ORDER BY r.{sort_col} {order} LIMIT ?"
            params.append(limit)

            c.execute(sql, params)
            cols = [desc[0] for desc in c.description]
            rows = [dict(zip(cols, row)) for row in c.fetchall()]
            # Merge effective WMA back into above_rs_30wma for frontend
            for row in rows:
                if row.get("above_rs_30wma") is None and row.get("above_wma30_effective") is not None:
                    row["above_rs_30wma"] = row["above_wma30_effective"]

            # Get total count for this universe
            c.execute("SELECT COUNT(*) FROM rs_rankings WHERE universe = ?", (universe,))
            total = c.fetchone()[0]

            conn.close()
            self.send_json({"rankings": rows, "total": total, "universe": universe})
        except Exception as e:
            import traceback; traceback.print_exc()
            self.send_json({"rankings": [], "total": 0, "error": str(e)})

    def serve_rs_movers(self, query=None):
        """GET /api/rs/movers — biggest RS rating changes (rate of change).

        Query params:
            universe — watchlist name (default: Russell3000)
            period   — 5 or 20 (days, default: 20)
            limit    — max results (default: 50)
        """
        try:
            from change_detector import _get_db
            universe = (query or {}).get("universe", ["Russell3000"])[0]
            period = int((query or {}).get("period", [20])[0])
            limit = int((query or {}).get("limit", [50])[0])

            col = "rs_change_20d" if period == 20 else "rs_change_5d"

            conn = _get_db()
            c = conn.cursor()
            c.execute(f"""
                SELECT * FROM rs_rankings
                WHERE universe = ? AND {col} IS NOT NULL
                ORDER BY {col} DESC
                LIMIT ?
            """, (universe, limit))
            cols = [desc[0] for desc in c.description]
            rows = [dict(zip(cols, row)) for row in c.fetchall()]
            conn.close()

            self.send_json({"movers": rows, "period": period, "universe": universe})
        except Exception as e:
            import traceback; traceback.print_exc()
            self.send_json({"movers": [], "error": str(e)})

    def serve_rs_breakouts(self, query=None):
        """GET /api/rs/breakouts — recent RS breakout events.

        Query params:
            universe — watchlist name (default: Russell3000)
            days     — lookback days (default: 7)
        """
        try:
            from change_detector import _get_db
            universe = (query or {}).get("universe", ["Russell3000"])[0]
            days = int((query or {}).get("days", [7])[0])

            from datetime import timezone
            since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")

            conn = _get_db()
            c = conn.cursor()
            c.execute("""
                SELECT e.*, r.rs_rating as current_rs, r.rs_rank, r.sector,
                       r.price as current_price, r.above_rs_30wma
                FROM rs_events e
                LEFT JOIN rs_rankings r ON e.ticker = r.ticker AND e.universe = r.universe
                WHERE e.universe = ? AND e.event_date >= ?
                ORDER BY e.event_date DESC, e.rs_rating DESC
            """, (universe, since))
            cols = [desc[0] for desc in c.description]
            rows = [dict(zip(cols, row)) for row in c.fetchall()]
            conn.close()

            # Parse details JSON
            for row in rows:
                if row.get("details"):
                    try:
                        row["details"] = json.loads(row["details"])
                    except (json.JSONDecodeError, TypeError):
                        pass

            self.send_json({"breakouts": rows, "universe": universe, "since": since})
        except Exception as e:
            import traceback; traceback.print_exc()
            self.send_json({"breakouts": [], "error": str(e)})

    def serve_rs_sectors(self, query=None):
        """GET /api/rs/sectors — sector-level RS aggregates.

        Query params:
            universe — watchlist name (default: Russell3000)
        """
        try:
            from change_detector import _get_db
            universe = (query or {}).get("universe", ["Russell3000"])[0]

            conn = _get_db()
            c = conn.cursor()
            c.execute("""
                SELECT sector,
                       COUNT(*) as count,
                       ROUND(AVG(rs_rating), 1) as avg_rs,
                       ROUND(AVG(rs_score), 2) as avg_score,
                       SUM(CASE WHEN rs_rating >= 80 THEN 1 ELSE 0 END) as above_80,
                       SUM(CASE WHEN rs_rating >= 90 THEN 1 ELSE 0 END) as above_90,
                       SUM(CASE WHEN rs_new_high = 1 THEN 1 ELSE 0 END) as rs_new_highs,
                       SUM(CASE WHEN above_rs_30wma = 1 THEN 1 ELSE 0 END) as above_wma
                FROM rs_rankings
                WHERE universe = ? AND sector != ''
                GROUP BY sector
                ORDER BY avg_rs DESC
            """, (universe,))
            cols = [desc[0] for desc in c.description]
            rows = [dict(zip(cols, row)) for row in c.fetchall()]
            conn.close()

            # Compute percentages
            for row in rows:
                cnt = row["count"] or 1
                row["pct_above_80"] = round((row["above_80"] / cnt) * 100, 1)
                row["pct_above_90"] = round((row["above_90"] / cnt) * 100, 1)
                row["pct_above_wma"] = round((row["above_wma"] / cnt) * 100, 1)

            self.send_json({"sectors": rows, "universe": universe})
        except Exception as e:
            import traceback; traceback.print_exc()
            self.send_json({"sectors": [], "error": str(e)})

    def serve_rs_history(self, query=None):
        """GET /api/rs/history — RS rating timeseries for a single ticker.

        Also returns SPY close prices for the same date range so the
        frontend can visualise divergence between RS and the index.

        Query params:
            ticker   — display ticker (required)
            universe — watchlist name (default: Russell3000)
            days     — lookback days (default: 60)
        """
        try:
            from change_detector import _get_db
            ticker = (query or {}).get("ticker", [None])[0]
            universe = (query or {}).get("universe", ["Russell3000"])[0]
            days = int((query or {}).get("days", [60])[0])

            if not ticker:
                self.send_json({"history": [], "error": "ticker required"})
                return

            from datetime import timezone
            since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")

            conn = _get_db()
            c = conn.cursor()
            c.execute("""
                SELECT trade_date, rs_score, rs_rating, price
                FROM rs_history
                WHERE ticker = ? AND universe = ? AND trade_date >= ?
                ORDER BY trade_date ASC
            """, (ticker, universe, since))
            cols = [desc[0] for desc in c.description]
            rows = [dict(zip(cols, row)) for row in c.fetchall()]
            conn.close()

            # Merge SPY daily close into each row for divergence chart
            try:
                from data_fetcher import _load_cache
                spy_df = _load_cache("SPY", "day")
                if spy_df is not None and not spy_df.empty:
                    spy_df = spy_df[["timestamp", "close"]].copy()
                    spy_df["date"] = spy_df["timestamp"].dt.strftime("%Y-%m-%d")
                    spy_map = dict(zip(spy_df["date"], spy_df["close"]))
                    for row in rows:
                        sc = spy_map.get(row["trade_date"])
                        row["spy_close"] = round(sc, 2) if sc is not None else None
            except Exception:
                pass  # SPY merge is optional; chart falls back gracefully

            self.send_json({"history": rows, "ticker": ticker, "universe": universe})
        except Exception as e:
            import traceback; traceback.print_exc()
            self.send_json({"history": [], "error": str(e)})

    def serve_rs_spy(self, query=None):
        """GET /api/rs/spy — SPY daily close prices for chart overlay.

        Query params:
            days — lookback days (default: 60)
        """
        try:
            days = int((query or {}).get("days", [60])[0])
            since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

            import pandas as _pd
            from data_fetcher import _load_cache
            spy_df = _load_cache("SPY", "day")
            if spy_df is None or spy_df.empty:
                self.send_json({"spy": []})
                return
            spy_df = spy_df[["timestamp", "close"]].copy()
            spy_df["date"] = spy_df["timestamp"].dt.strftime("%Y-%m-%d")
            spy_df = spy_df[spy_df["date"] >= since]
            result = [{"date": r["date"], "close": round(r["close"], 2)}
                      for _, r in spy_df.iterrows()]
            self.send_json({"spy": result})
        except Exception as e:
            import traceback; traceback.print_exc()
            self.send_json({"spy": [], "error": str(e)})

    def serve_rs_report(self, query=None):
        """GET /api/rs/report — daily intelligence report.

        Sections: sector_rotation, new_monsters, accelerating, emerging, divergences, hvc_activity.
        Query param: save=1 to force-save snapshot (auto-saves once daily).
        """
        try:
            from change_detector import _get_db
            universe = (query or {}).get("universe", ["Russell3000"])[0]
            force_save = (query or {}).get("save", ["0"])[0] == "1"

            conn = _get_db()
            c = conn.cursor()

            # --- Sector Rotation: current vs ~20 trading days ago ---
            c.execute("""
                SELECT sector, ROUND(AVG(rs_rating), 1) as avg_rs, COUNT(*) as cnt
                FROM rs_rankings WHERE universe = ? AND sector != ''
                GROUP BY sector
            """, (universe,))
            current_sectors = {row[0]: {"avg_rs": row[1], "count": row[2]} for row in c.fetchall()}

            c.execute("""
                SELECT DISTINCT trade_date FROM rs_history
                WHERE universe = ? ORDER BY trade_date DESC LIMIT 20
            """, (universe,))
            dates = [row[0] for row in c.fetchall()]

            sector_rotation = []
            if len(dates) >= 10:
                old_date = dates[-1]
                c.execute("SELECT ticker, sector FROM rs_rankings WHERE universe = ? AND sector != ''",
                          (universe,))
                ticker_sector = {row[0]: row[1] for row in c.fetchall()}

                c.execute("SELECT ticker, rs_rating FROM rs_history WHERE universe = ? AND trade_date = ?",
                          (universe, old_date))
                old_ratings = {row[0]: row[1] for row in c.fetchall()}

                old_sectors = {}
                for t, s in ticker_sector.items():
                    if t in old_ratings:
                        old_sectors.setdefault(s, []).append(old_ratings[t])
                old_avg = {s: round(sum(v) / len(v), 1) for s, v in old_sectors.items() if v}

                for sector, data in current_sectors.items():
                    if sector in old_avg:
                        chg = round(data["avg_rs"] - old_avg[sector], 1)
                        sector_rotation.append({
                            "sector": sector, "avg_rs": data["avg_rs"],
                            "change": chg, "count": data["count"]
                        })
                sector_rotation.sort(key=lambda x: x["change"], reverse=True)

            # --- New Monsters: crossed RS >= 90 in last 20 days ---
            c.execute("""
                SELECT ticker, rs_rating, rs_change_20d, sector, price, rs_change_5d,
                       monster_score, price_return_3m, hvc_count, gap_count, gaps_open
                FROM rs_rankings
                WHERE universe = ? AND rs_rating >= 90
                  AND rs_change_20d IS NOT NULL AND rs_change_20d > 0
                  AND (rs_rating - rs_change_20d) < 90
                ORDER BY rs_change_20d DESC LIMIT 15
            """, (universe,))
            cols_b = [d[0] for d in c.description]
            new_monsters = [dict(zip(cols_b, row)) for row in c.fetchall()]

            # --- Accelerating: RS >= 80, highest 5d change ---
            c.execute("""
                SELECT ticker, rs_rating, rs_change_5d, rs_change_20d, sector, price,
                       monster_score, hvc_count, gap_count, gaps_open
                FROM rs_rankings
                WHERE universe = ? AND rs_rating >= 80
                  AND rs_change_5d IS NOT NULL AND rs_change_5d > 0
                ORDER BY rs_change_5d DESC LIMIT 10
            """, (universe,))
            cols_c = [d[0] for d in c.description]
            accelerating = [dict(zip(cols_c, row)) for row in c.fetchall()]

            # --- Emerging: RS 60-80, high 20d change ---
            c.execute("""
                SELECT ticker, rs_rating, rs_change_20d, rs_change_5d, sector, price,
                       monster_score, hvc_count, gap_count, gaps_open
                FROM rs_rankings
                WHERE universe = ? AND rs_rating BETWEEN 60 AND 79
                  AND rs_change_20d IS NOT NULL AND rs_change_20d > 0
                ORDER BY rs_change_20d DESC LIMIT 10
            """, (universe,))
            cols_d = [d[0] for d in c.description]
            emerging = [dict(zip(cols_d, row)) for row in c.fetchall()]

            # --- Divergences: large 3m outperformers ---
            c.execute("""
                SELECT ticker, rs_rating, price_return_3m, price_return_6m, sector, price,
                       monster_score, hvc_count, gap_count, gaps_open
                FROM rs_rankings
                WHERE universe = ? AND price_return_3m IS NOT NULL AND price_return_3m > 30
                ORDER BY price_return_3m DESC LIMIT 10
            """, (universe,))
            cols_e = [d[0] for d in c.description]
            divergences = [dict(zip(cols_e, row)) for row in c.fetchall()]

            # --- HVC Activity: recent institutional volume ---
            c.execute("""
                SELECT ticker, rs_rating, monster_score, sector, price,
                       hvc_count, gap_count, gaps_open
                FROM rs_rankings
                WHERE universe = ? AND hvc_count IS NOT NULL AND hvc_count > 0
                  AND rs_rating >= 70
                ORDER BY hvc_count DESC, monster_score DESC LIMIT 10
            """, (universe,))
            cols_f = [d[0] for d in c.description]
            hvc_activity = [dict(zip(cols_f, row)) for row in c.fetchall()]

            conn.close()

            report_data = {
                "sector_rotation": {
                    "gaining": sector_rotation[:5],
                    "losing": list(reversed(sector_rotation[-5:])) if len(sector_rotation) > 5 else [],
                },
                "new_monsters": new_monsters,
                "accelerating": accelerating,
                "emerging": emerging,
                "divergences": divergences,
                "hvc_activity": hvc_activity,
                "universe": universe,
            }

            # Auto-save snapshot (once per day per universe)
            try:
                from change_detector import save_rs_report_snapshot, get_rs_report_dates
                today_str = datetime.now().strftime("%Y-%m-%d")
                existing = get_rs_report_dates(universe, limit=1)
                if force_save or not existing or existing[0] != today_str:
                    save_rs_report_snapshot(universe, today_str, report_data)
                    report_data["snapshot_saved"] = today_str
            except Exception:
                pass  # non-fatal

            self.send_json(report_data)
        except Exception as e:
            import traceback; traceback.print_exc()
            self.send_json({"error": str(e)})

    def serve_rs_report_dates(self, query=None):
        """GET /api/rs/report/dates — list available report snapshot dates."""
        try:
            from change_detector import get_rs_report_dates
            universe = (query or {}).get("universe", ["Russell3000"])[0]
            dates = get_rs_report_dates(universe, limit=90)
            self.send_json({"dates": dates, "universe": universe})
        except Exception as e:
            self.send_json({"dates": [], "error": str(e)})

    def serve_rs_report_snapshot(self, query=None):
        """GET /api/rs/report/snapshot?date=YYYY-MM-DD — get historical report."""
        try:
            from change_detector import get_rs_report_snapshot
            universe = (query or {}).get("universe", ["Russell3000"])[0]
            date = (query or {}).get("date", [None])[0]
            if not date:
                self.send_json({"error": "date parameter required"})
                return
            snap = get_rs_report_snapshot(universe, date)
            if snap:
                self.send_json(snap)
            else:
                self.send_json({"error": f"No snapshot for {date}"})
        except Exception as e:
            self.send_json({"error": str(e)})

    def serve_rs_status(self, query=None):
        """GET /api/rs/status — last computation time + summary stats."""
        try:
            from change_detector import _get_db
            universe = (query or {}).get("universe", ["Russell3000"])[0]

            conn = _get_db()
            c = conn.cursor()

            c.execute("SELECT computed_at, COUNT(*) FROM rs_rankings WHERE universe = ? LIMIT 1",
                      (universe,))
            row = c.fetchone()
            computed_at = row[0] if row else None
            total = row[1] if row else 0

            c.execute("SELECT COUNT(*) FROM rs_rankings WHERE universe = ? AND rs_rating >= 90",
                      (universe,))
            above_90 = c.fetchone()[0]

            c.execute("SELECT COUNT(*) FROM rs_rankings WHERE universe = ? AND rs_rating >= 80",
                      (universe,))
            above_80 = c.fetchone()[0]

            c.execute("SELECT COUNT(*) FROM rs_rankings WHERE universe = ? AND rs_new_high = 1",
                      (universe,))
            rs_new_highs = c.fetchone()[0]

            conn.close()
            self.send_json({
                "universe": universe,
                "computed_at": computed_at,
                "total": total,
                "above_90": above_90,
                "above_80": above_80,
                "rs_new_highs": rs_new_highs,
            })
        except Exception as e:
            import traceback; traceback.print_exc()
            self.send_json({"error": str(e)})

    def trigger_rs_engine(self):
        """POST /api/rs/trigger — manually trigger RS computation for a universe."""
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length else {}
            universe = body.get("universe", "Russell3000").strip()
            workers = int(body.get("workers", 8))
            backfill = body.get("backfill", False)
            days = int(body.get("days", 90))

            import subprocess
            engine_path = os.path.join(os.path.dirname(__file__), "rs_engine.py")
            cmd = [sys.executable, engine_path, "--universe", universe,
                   "--workers", str(workers), "--once"]
            if backfill:
                cmd.extend(["--backfill", "--days", str(days)])

            def _run_rs():
                try:
                    result = subprocess.run(
                        cmd, capture_output=True, timeout=600,
                        cwd=os.path.dirname(engine_path),
                        encoding="utf-8", errors="replace"
                    )
                    stdout = result.stdout or ""
                    if stdout:
                        print(stdout[-2000:])
                    if result.returncode != 0:
                        stderr = result.stderr or ""
                        print(f"RS Engine error (exit {result.returncode}):")
                        print(stderr[-1000:])
                except Exception as e:
                    print(f"RS Engine trigger failed: {e}")

            t = threading.Thread(target=_run_rs, daemon=True)
            t.start()

            self.send_json({"ok": True, "universe": universe,
                           "backfill": backfill, "message": "RS engine started"})
        except Exception as e:
            self.send_json({"ok": False, "error": str(e)})

    # ---- Scheduler API handlers ----

    def serve_scheduler_config(self):
        """GET /api/scheduler/config — return current scheduler configuration."""
        self.send_json(scheduler.get_config())

    def save_scheduler_config(self):
        """POST /api/scheduler/config — save scheduler configuration."""
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length else {}
            scheduler.save_config(body)
            self.send_json({"ok": True})
        except Exception as e:
            self.send_json({"ok": False, "error": str(e)})

    def serve_scheduler_status(self):
        """GET /api/scheduler/status — return live scheduler status."""
        self.send_json(scheduler.get_status())

    def trigger_scheduler(self):
        """POST /api/scheduler/trigger — manually trigger a watchlist scan."""
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length else {}
            name = body.get("watchlist", "").strip()
            if not name:
                self.send_json({"ok": False, "error": "watchlist name required"})
                return
            result = scheduler.trigger_watchlist(name)
            self.send_json(result)
        except Exception as e:
            self.send_json({"ok": False, "error": str(e)})

    def toggle_scheduler(self):
        """POST /api/scheduler/toggle — enable/disable the global scheduler."""
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length else {}
            enabled = body.get("enabled", True)
            result = scheduler.toggle_enabled(enabled)
            self.send_json(result)
        except Exception as e:
            self.send_json({"ok": False, "error": str(e)})

    # ── Single-Ticker Backfill ────────────────────────────────────────

    def trigger_ticker_backfill(self):
        """POST /api/scheduler/trigger-ticker — backfill one or more tickers (collect + compute).
        Accepts 'ticker' as a single ticker string or comma-separated list."""
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length else {}
            raw = body.get("ticker", "").strip().upper()
            if not raw:
                self.send_json({"ok": False, "error": "ticker required"})
                return

            # Parse comma-separated tickers
            tickers = [t.strip() for t in raw.split(",") if t.strip()]
            if not tickers:
                self.send_json({"ok": False, "error": "ticker required"})
                return

            with _ticker_backfill_lock:
                if _ticker_backfill["running"]:
                    self.send_json({"ok": False, "error": f"Already backfilling {_ticker_backfill['ticker']}"})
                    return
                _ticker_backfill.update({
                    "running": True, "ticker": tickers[0],
                    "tickers": tickers, "completed": 0, "total": len(tickers),
                    "phase": "collecting", "message": f"Collecting {tickers[0]}... (1/{len(tickers)})",
                    "t0": time.time(),
                })

            def _run():
                import subprocess as _sp
                from collector import collect_ticker
                engine_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "engine.py")
                done = 0
                total = len(tickers)
                errors = []

                for i, ticker in enumerate(tickers):
                    try:
                        # Phase 1: Collect OHLC data
                        with _ticker_backfill_lock:
                            _ticker_backfill.update({
                                "ticker": ticker, "completed": done, "total": total,
                                "phase": "collecting",
                                "message": f"Collecting {ticker}... ({i+1}/{total})",
                            })

                        result = collect_ticker(ticker)
                        if result.get("status") == "error":
                            err_msg = result.get("error", "unknown")
                            errors.append(f"{ticker}: {err_msg}")
                            print(f"[BACKFILL] {ticker} collection failed: {err_msg}", flush=True)
                            done += 1
                            continue

                        # Phase 2: Compute indicators via engine subprocess
                        with _ticker_backfill_lock:
                            _ticker_backfill.update({
                                "phase": "computing",
                                "message": f"Computing {ticker}... ({i+1}/{total})",
                            })

                        cmd = [sys.executable, engine_path,
                               "--ticker", ticker,
                               "--workers", "1",
                               "--once"]
                        eng = _sp.run(cmd, capture_output=True, timeout=300,
                                      cwd=os.path.dirname(engine_path),
                                      encoding='utf-8', errors='replace')
                        if eng.returncode != 0:
                            err = eng.stderr[-300:] if eng.stderr else "unknown error"
                            errors.append(f"{ticker}: compute failed")
                            print(f"[BACKFILL] {ticker} compute failed: {err}", flush=True)
                            done += 1
                            continue

                        done += 1
                        print(f"[BACKFILL] {ticker} done ({done}/{total})", flush=True)

                    except Exception as e:
                        errors.append(f"{ticker}: {e}")
                        done += 1
                        print(f"[BACKFILL] {ticker} error: {e}", flush=True)

                elapsed = round(time.time() - _ticker_backfill["t0"], 1)
                err_suffix = f" ({len(errors)} errors)" if errors else ""
                with _ticker_backfill_lock:
                    _ticker_backfill.update({
                        "running": False, "completed": done, "total": total,
                        "phase": "done" if not errors else "done_with_errors",
                        "message": f"{done}/{total} tickers backfilled in {elapsed}s{err_suffix}",
                        "errors": errors,
                    })
                print(f"[BACKFILL] Batch complete: {done}/{total} in {elapsed}s{err_suffix}", flush=True)

            threading.Thread(target=_run, daemon=True).start()
            self.send_json({"ok": True, "tickers": tickers, "count": len(tickers)})

        except Exception as e:
            self.send_json({"ok": False, "error": str(e)})

    def serve_ticker_backfill_status(self):
        """GET /api/scheduler/ticker-backfill-status — progress of ticker backfill (single or batch)."""
        with _ticker_backfill_lock:
            self.send_json({
                "running": _ticker_backfill["running"],
                "ticker": _ticker_backfill.get("ticker", ""),
                "tickers": _ticker_backfill.get("tickers", []),
                "completed": _ticker_backfill.get("completed", 0),
                "total": _ticker_backfill.get("total", 0),
                "phase": _ticker_backfill["phase"],
                "message": _ticker_backfill["message"],
                "errors": _ticker_backfill.get("errors", []),
            })

    def serve_all_tickers(self):
        """GET /api/scheduler/all-tickers — unique tickers across all sources.

        Unions: watchlist tickers + indicator DB + sweep DB + CSV cache files.
        This captures tickers from old backfills, one-off fetches, and sweep data.
        """
        seen = set()

        # 1. Watchlist tickers
        reload_watchlists()
        for _name, groups in WATCHLISTS.items():
            for _group_name, group_tickers in groups:
                for display, _api, _atype in group_tickers:
                    seen.add(display)

        # 2. Indicator DB (snapshots table)
        try:
            db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "momentum_dashboard.db")
            import sqlite3
            conn = sqlite3.connect(db_path)
            for row in conn.execute("SELECT DISTINCT ticker FROM snapshots"):
                seen.add(row[0])
            conn.close()
        except Exception:
            pass

        # 3. Sweep DB (sweep_daily_summary — faster than sweep_trades)
        try:
            db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "momentum_dashboard.db")
            conn = sqlite3.connect(db_path)
            for row in conn.execute("SELECT DISTINCT ticker FROM sweep_daily_summary"):
                seen.add(row[0])
            conn.close()
        except Exception:
            pass

        # 4. CSV cache files ({TICKER}_day.csv)
        try:
            cache_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache")
            if os.path.isdir(cache_dir):
                for fname in os.listdir(cache_dir):
                    if fname.endswith("_day.csv"):
                        seen.add(fname.replace("_day.csv", ""))
        except Exception:
            pass

        tickers = sorted(seen)
        self.send_json({"tickers": tickers, "count": len(tickers)})

    # ── TradingView UDF Datafeed ──────────────────────────────────────

    def serve_tv_config(self):
        """GET /api/tv/config — UDF datafeed configuration."""
        self.send_json({
            "supports_search": True,
            "supports_group_request": False,
            "supports_marks": True,
            "supports_timescale_marks": True,
            "supports_time": True,
            "exchanges": [{"value": "", "name": "All Exchanges", "desc": ""}],
            "symbols_types": [{"name": "All Types", "value": ""}],
            "supported_resolutions": ["60", "240", "D", "W"],
        })

    def serve_tv_symbols(self, query):
        """GET /api/tv/symbols?symbol=NVDA — UDF symbol resolution."""
        symbol = (query or {}).get("symbol", [""])[0].upper()
        self.send_json({
            "name": symbol,
            "ticker": symbol,
            "description": symbol,
            "type": "stock",
            "session": "0930-1600",
            "timezone": "America/New_York",
            "exchange": "US",
            "listed_exchange": "US",
            "minmov": 1,
            "pricescale": 100,
            "has_intraday": True,
            "has_daily": True,
            "has_weekly_and_monthly": True,
            "supported_resolutions": ["60", "240", "D", "W"],
            "volume_precision": 0,
            "data_status": "endofday",
        })

    def serve_tv_search(self, query):
        """GET /api/tv/search?query=NV&limit=30 — UDF symbol search."""
        q = (query or {}).get("query", [""])[0].upper()
        limit = int((query or {}).get("limit", ["30"])[0])
        results = []
        try:
            import sqlite3
            conn = sqlite3.connect(DB_PATH, timeout=5)
            rows = conn.execute(
                "SELECT DISTINCT ticker FROM backtest_indicators "
                "WHERE timeframe='1D' AND ticker LIKE ? LIMIT ?",
                (q + "%", limit),
            ).fetchall()
            conn.close()
            for r in rows:
                results.append({
                    "symbol": r[0], "full_name": r[0], "description": r[0],
                    "exchange": "US", "ticker": r[0], "type": "stock",
                })
        except Exception as e:
            print(f"TV search error: {e}")
        self.send_json(results)

    def serve_tv_history(self, query):
        """GET /api/tv/history?symbol=NVDA&from=...&to=...&resolution=D — UDF OHLCV bars."""
        from sweep_engine import _load_price_candles
        symbol = (query or {}).get("symbol", [""])[0].upper()
        resolution = (query or {}).get("resolution", ["D"])[0]
        from_ts = int((query or {}).get("from", ["0"])[0])
        to_ts = int((query or {}).get("to", ["9999999999"])[0])

        # Map TradingView resolution → our timeframe
        tf_map = {"60": "1h", "240": "4h", "D": "1D", "1D": "1D", "W": "1W", "1W": "1W"}
        tf = tf_map.get(resolution, "1D")

        # Convert timestamps to date strings for the loader
        from_dt = datetime.utcfromtimestamp(from_ts)
        to_dt = datetime.utcfromtimestamp(to_ts)
        date_from = from_dt.strftime("%Y-%m-%d")
        date_to = to_dt.strftime("%Y-%m-%d")

        candles = _load_price_candles(symbol, date_from, date_to, tf)
        if not candles:
            self.send_json({"s": "no_data", "nextTime": None})
            return

        t_arr, o_arr, h_arr, l_arr, c_arr, v_arr = [], [], [], [], [], []
        for c in candles:
            ct = c.get("time")
            if isinstance(ct, str):
                try:
                    ts = int(datetime.strptime(ct, "%Y-%m-%d").timestamp())
                except Exception:
                    continue
            else:
                ts = int(ct)
            if ts < from_ts or ts > to_ts:
                continue
            t_arr.append(ts)
            o_arr.append(c.get("open", 0))
            h_arr.append(c.get("high", 0))
            l_arr.append(c.get("low", 0))
            c_arr.append(c.get("close", 0))
            v_arr.append(c.get("volume", 0))

        if not t_arr:
            self.send_json({"s": "no_data", "nextTime": None})
            return

        self.send_json({
            "s": "ok",
            "t": t_arr, "o": o_arr, "h": h_arr,
            "l": l_arr, "c": c_arr, "v": v_arr,
        })

    def serve_tv_marks(self, query):
        """GET /api/tv/marks?symbol=NVDA&from=...&to=... — sweep event marks on chart."""
        symbol = (query or {}).get("symbol", [""])[0].upper()
        from_ts = int((query or {}).get("from", ["0"])[0])
        to_ts = int((query or {}).get("to", ["9999999999"])[0])
        marks = []
        try:
            import sqlite3
            conn = sqlite3.connect(DB_PATH, timeout=5)
            rows = conn.execute(
                "SELECT event_date, sweep_count, total_notional, direction, is_rare, "
                "COALESCE(event_type, 'clusterbomb') as event_type, "
                "COALESCE(is_monster, 0) as is_monster "
                "FROM clusterbomb_events WHERE ticker = ? ORDER BY event_date",
                (symbol,),
            ).fetchall()
            conn.close()

            for i, r in enumerate(rows):
                event_date, sweep_count, total_notional, direction, is_rare, event_type, is_monster = r
                try:
                    ts = int(datetime.strptime(event_date, "%Y-%m-%d").timestamp())
                except Exception:
                    continue
                if ts < from_ts or ts > to_ts:
                    continue

                if is_monster:
                    color = {"border": "#f97316", "background": "#f97316"}
                    label, label_color, type_str = "M", "#fff", "MONSTER SWEEP"
                elif is_rare:
                    color = {"border": "#a78bfa", "background": "#a78bfa"}
                    label, label_color, type_str = "R", "#fff", "RARE SWEEP"
                else:
                    color = {"border": "#f59e0b", "background": "#f59e0b"}
                    label, label_color, type_str = "CB", "#000", "CLUSTERBOMB"

                notional_str = (
                    f"${total_notional/1e6:.1f}M"
                    if total_notional and total_notional >= 1e6
                    else f"${(total_notional or 0)/1e3:.0f}K"
                )
                direction_str = (direction or "N/A").upper()

                marks.append({
                    "id": i + 1,
                    "time": ts,
                    "color": color,
                    "text": f"{type_str}\nTotal: {notional_str}\nSweeps: {sweep_count}\nDirection: {direction_str}",
                    "label": label,
                    "labelFontColor": label_color,
                    "minSize": 18 if is_monster else 14,
                })
        except Exception as e:
            print(f"TV marks error: {e}")
        self.send_json(marks)

    def serve_tv_timescale_marks(self, query):
        """GET /api/tv/timescale_marks?symbol=NVDA&from=...&to=... — timeline sweep markers."""
        symbol = (query or {}).get("symbol", [""])[0].upper()
        from_ts = int((query or {}).get("from", ["0"])[0])
        to_ts = int((query or {}).get("to", ["9999999999"])[0])
        marks = []
        try:
            import sqlite3
            conn = sqlite3.connect(DB_PATH, timeout=5)
            rows = conn.execute(
                "SELECT event_date, sweep_count, total_notional, direction, is_rare, "
                "COALESCE(event_type, 'clusterbomb') as event_type, "
                "COALESCE(is_monster, 0) as is_monster "
                "FROM clusterbomb_events WHERE ticker = ? ORDER BY event_date",
                (symbol,),
            ).fetchall()
            conn.close()

            for i, r in enumerate(rows):
                event_date, sweep_count, total_notional, direction, is_rare, event_type, is_monster = r
                try:
                    ts = int(datetime.strptime(event_date, "%Y-%m-%d").timestamp())
                except Exception:
                    continue
                if ts < from_ts or ts > to_ts:
                    continue

                if is_monster:
                    color = "orange"
                    label, type_str = "M", "Monster Sweep"
                elif is_rare:
                    color = "purple"
                    label, type_str = "R", "Rare Sweep"
                else:
                    color = "yellow"
                    label, type_str = "CB", "Clusterbomb"

                notional_str = (
                    f"${total_notional/1e6:.1f}M"
                    if total_notional and total_notional >= 1e6
                    else f"${(total_notional or 0)/1e3:.0f}K"
                )

                marks.append({
                    "id": i + 1,
                    "time": ts,
                    "color": color,
                    "label": label,
                    "tooltip": [
                        f"{type_str} — {symbol}",
                        f"{notional_str} total \u2022 {sweep_count} sweeps",
                        f"Direction: {(direction or 'N/A').upper()}",
                    ],
                })
        except Exception as e:
            print(f"TV timescale marks error: {e}")
        self.send_json(marks)

    # ── Pine Script Generator ──────────────────────────────────────

    @staticmethod
    def _load_daily_closes(tickers):
        """Load {ticker: {date_str: close}} from cache/{TICKER}_day.csv.

        Used to compute split-safe price ratios for Pine Script.
        CSV closes are split-adjusted (Polygon adjusted=true), so the
        ratio avg_price/close must account for splits.
        """
        import csv as _csv
        cache_dir = os.path.join(os.path.dirname(__file__), "cache")
        result = {}
        for t in tickers:
            path = os.path.join(cache_dir, f"{t}_day.csv")
            closes = {}
            if os.path.exists(path):
                try:
                    with open(path, "r") as f:
                        for row in _csv.DictReader(f):
                            closes[row["timestamp"][:10]] = float(row["close"])
                except Exception:
                    pass
            result[t] = closes
        return result

    def serve_sweep_pinescript(self, query):
        """GET /api/sweeps/pinescript?ticker=NVDA — generate Pine Script indicator."""
        ticker = (query or {}).get("ticker", [""])[0].upper()
        if not ticker:
            self.send_json({"ok": False, "error": "ticker required"})
            return

        try:
            import sqlite3
            from sweep_engine import get_ticker_day_ranks

            conn = sqlite3.connect(DB_PATH, timeout=5)
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT event_date, sweep_count, total_notional, avg_price, "
                "direction, is_rare, COALESCE(is_monster, 0) as is_monster "
                "FROM clusterbomb_events WHERE ticker = ? ORDER BY event_date",
                (ticker,),
            ).fetchall()
            conn.close()

            # Fetch rank data for this ticker
            rank_data = get_ticker_day_ranks(tickers=[ticker])

            # Load daily closes for split-safe price ratios
            daily_closes = self._load_daily_closes([ticker])
            closes = daily_closes.get(ticker, {})

            cb_dates, cb_tips, cb_ratios = [], [], []
            monster_dates, monster_tips, monster_ratios = [], [], []
            rare_dates, rare_tips, rare_ratios = [], [], []

            for r in rows:
                parts = r["event_date"].split("-")
                y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
                date_key = y * 10000 + m * 100 + d
                notional = r["total_notional"] or 0
                n_str = f"${notional/1e6:.1f}M" if notional >= 1e6 else f"${notional/1e3:.0f}K"
                price = r["avg_price"] or 0
                day_close = closes.get(r["event_date"], 0)
                # Split adjustment: avg_price is raw but day_close is
                # split-adjusted from Polygon CSV cache.
                if day_close > 0 and price > 0:
                    raw_ratio = price / day_close
                    if raw_ratio > 1.5:
                        price = price / round(raw_ratio)
                    elif raw_ratio < 0.667:
                        price = price * round(1.0 / raw_ratio)
                ratio = price / day_close if day_close > 0 and price > 0 else 1.0
                ratio_str = f"{ratio:.4f}"

                # Rank lookup
                rk = rank_data.get((ticker, r["event_date"]), {})
                rank_num = rk.get("rank")
                total_days = rk.get("total_days")
                rank_str = f"#{rank_num}/{total_days}" if rank_num else ""

                direction_str = (r["direction"] or "N/A").upper()
                tip = f"{n_str} | {rank_str} | {r['sweep_count']}sw | {direction_str}" if rank_str else f"{n_str} | {r['sweep_count']}sw | {direction_str}"

                if r["is_monster"]:
                    monster_dates.append(str(date_key))
                    monster_tips.append(f'"{tip}"')
                    monster_ratios.append(ratio_str)
                elif r["is_rare"]:
                    rare_dates.append(str(date_key))
                    rare_tips.append(f'"{tip}"')
                    rare_ratios.append(ratio_str)
                else:
                    cb_dates.append(str(date_key))
                    cb_tips.append(f'"{tip}"')
                    cb_ratios.append(ratio_str)

            # Build the Pine Script
            lines = []
            lines.append("// @version=5")
            date_tag = datetime.now().strftime("%y%m%d")
            lines.append(f'indicator("DPS-{date_tag}-{ticker}", overlay=true, max_labels_count=500)')
            lines.append("")

            # ── Inputs ──
            lines.append("// \u2500\u2500 Sweep Type Toggles \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500")
            lines.append('showCB      = input.bool(true, "Clusterbombs",  group="Sweep Types")')
            lines.append('showMonster = input.bool(true, "Monsters",      group="Sweep Types")')
            lines.append('showRare    = input.bool(true, "Rare Sweeps",   group="Sweep Types")')
            lines.append("")
            lines.append("// \u2500\u2500 Style \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500")
            lines.append('opacity    = input.int(0, "Opacity",')
            lines.append('  minval=0, maxval=100, step=5, tooltip="0 = solid, 100 = invisible", group="Style")')
            lines.append('showShortText = input.bool(true,  "Short Labels (CB / M / R)",  group="Text")')
            lines.append('showDetails   = input.bool(false, "Detail Labels (notional, direction)", group="Text")')
            lines.append("")

            # ── Derived values ──
            lines.append("// \u2500\u2500 Derived \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500")
            lines.append("cbColor      = color.new(#f59e0b, opacity)  // yellow")
            lines.append("monsterColor = color.new(#f97316, opacity)  // orange")
            lines.append("rareColor    = color.new(#a78bfa, opacity)  // purple")
            lines.append("")
            lines.append("dateKey = year * 10000 + month * 100 + dayofmonth")
            lines.append("newDay  = ta.change(time('D')) != 0  // only plot once per day on intraday charts")
            lines.append("")

            # ── Data arrays ──
            def _emit_array(name, items, max_per_line=8):
                if not items:
                    lines.append(f"var {name} = array.new_int(0)")
                    return
                lines.append(f"var {name} = array.from(")
                for i in range(0, len(items), max_per_line):
                    chunk = ", ".join(items[i:i+max_per_line])
                    comma = "," if i + max_per_line < len(items) else ""
                    lines.append(f"  {chunk}{comma}")
                lines.append("  )")

            def _emit_str_array(name, items, max_per_line=4):
                if not items:
                    lines.append(f'var {name} = array.new_string(0)')
                    return
                lines.append(f"var {name} = array.from(")
                for i in range(0, len(items), max_per_line):
                    chunk = ", ".join(items[i:i+max_per_line])
                    comma = "," if i + max_per_line < len(items) else ""
                    lines.append(f"  {chunk}{comma}")
                lines.append("  )")


            def _emit_float_array(name, items, max_per_line=8):
                if not items:
                    lines.append(f"var {name} = array.new_float(0)")
                    return
                lines.append(f"var {name} = array.from(")
                for i in range(0, len(items), max_per_line):
                    chunk = ", ".join(items[i:i+max_per_line])
                    comma = "," if i + max_per_line < len(items) else ""
                    lines.append(f"  {chunk}{comma}")
                lines.append("  )")

            lines.append(f"// Clusterbombs: {len(cb_dates)} events")
            _emit_array("cbDates", cb_dates)
            _emit_float_array("cbRatios", cb_ratios)
            _emit_str_array("cbTips", cb_tips)
            lines.append("")
            lines.append(f"// Monsters: {len(monster_dates)} events")
            _emit_array("monsterDates", monster_dates)
            _emit_float_array("monsterRatios", monster_ratios)
            _emit_str_array("monsterTips", monster_tips)
            lines.append("")
            lines.append(f"// Rare Sweeps: {len(rare_dates)} events")
            _emit_array("rareDates", rare_dates)
            _emit_float_array("rareRatios", rare_ratios)
            _emit_str_array("rareTips", rare_tips)
            lines.append("")

            # ── Detection + price lookup ──
            lines.append("// \u2500\u2500 Detect Events & Price Lookup \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500")
            lines.append("cbIdx      = array.indexof(cbDates, dateKey)")
            lines.append("monsterIdx = array.indexof(monsterDates, dateKey)")
            lines.append("rareIdx    = array.indexof(rareDates, dateKey)")
            lines.append("isCB      = cbIdx >= 0")
            lines.append("isMonster = monsterIdx >= 0")
            lines.append("isRare    = rareIdx >= 0")
            lines.append("// ratio * close = split-adjusted true sweep price")
            lines.append("cbPrice      = isCB      ? array.get(cbRatios, cbIdx) * close           : na")
            lines.append("monsterPrice = isMonster  ? array.get(monsterRatios, monsterIdx) * close : na")
            lines.append("rarePrice    = isRare     ? array.get(rareRatios, rareIdx) * close       : na")
            lines.append("")

            # ── Plot circles — true sweep price, split-safe ──
            lines.append("// \u2500\u2500 Price-anchored circles (plot = true chart data, no drift) \u2500\u2500")
            for kind, flag, color_var, lw, price_var in [
                ("Clusterbomb", "showCB and isCB", "cbColor", 12, "cbPrice"),
                ("Monster", "showMonster and isMonster", "monsterColor", 14, "monsterPrice"),
                ("Rare Sweep", "showRare and isRare", "rareColor", 11, "rarePrice"),
            ]:
                lines.append(f'plot({flag} and newDay ? {price_var} : na, "{kind}", color={color_var},')
                lines.append(f'  style=plot.style_circles, linewidth={lw})')
            lines.append("")

            # Optional text labels above bar
            lines.append("// \u2500\u2500 Text labels (above bar, once per day) \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500")
            for kind, flag, color_var, label_short in [
                ("Clusterbomb label", "showCB and isCB", "cbColor", "CB"),
                ("Monster label", "showMonster and isMonster", "monsterColor", "M"),
                ("Rare label", "showRare and isRare", "rareColor", "R"),
            ]:
                lines.append(f'plotshape({flag} and showShortText and newDay, "{kind}", shape.diamond,')
                lines.append(f'  location.abovebar, color=color.new(color.black, 100), size=size.tiny,')
                lines.append(f'  text="{label_short}", textcolor={color_var})')
            lines.append("")

            # ── Detail labels (floating bubbles with notional/direction) ──
            lines.append("// \u2500\u2500 Detail Labels (optional floating bubbles) \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500")
            lines.append("if showDetails and newDay")
            for kind, flag, price_var, tips_var, idx_var, hex_color, lbl_sz in [
                ("CB", "showCB and isCB", "cbPrice", "cbTips", "cbIdx", "#f59e0b", "size.small"),
                ("M", "showMonster and isMonster", "monsterPrice", "monsterTips", "monsterIdx", "#f97316", "size.normal"),
                ("R", "showRare and isRare", "rarePrice", "rareTips", "rareIdx", "#a78bfa", "size.small"),
            ]:
                lines.append(f"    if {flag}")
                lines.append(f"        tip = {idx_var} >= 0 ? array.get({tips_var}, {idx_var}) : \"\"")
                lines.append(f'        label.new(bar_index, {price_var}, "{kind}\\n" + tip,')
                lines.append(f"          color=color.new({hex_color}, math.max(opacity - 20, 0)),")
                lines.append(f"          textcolor=color.white, style=label.style_label_down, size={lbl_sz})")
            lines.append("")

            lines.append(f'// Generated {datetime.now().strftime("%Y-%m-%d %H:%M")}')
            lines.append(f"// {len(cb_dates)} clusterbombs, {len(monster_dates)} monsters, {len(rare_dates)} rare sweeps")

            script = "\n".join(lines)
            self.send_json({"ok": True, "ticker": ticker, "script": script,
                            "stats": {"clusterbombs": len(cb_dates),
                                      "monsters": len(monster_dates),
                                      "rare": len(rare_dates)}})
        except Exception as e:
            print(f"Pine Script generation error: {e}")
            self.send_json({"ok": False, "error": str(e)})

    def serve_sweep_pinescript_all(self, query=None):
        """GET /api/sweeps/pinescript-all — universal multi-ticker Pine Script indicator.
        Optional params:
          ?details=1   — include tip arrays for detail labels (~90KB extra)
          ?months=6    — only events from last N months (default: 6, max: 36)
          ?types=cb,monster,rare,ranked — comma-separated types to include
          ?top_n=10    — for ranked: top N days per ticker (10 or 20)
        """
        q = query or {}
        include_tips = q.get("details", ["0"])[0] == "1"
        months = min(int(q.get("months", ["6"])[0] or 6), 120)
        types_param = q.get("types", ["cb,monster,rare"])[0]
        include_types = set(t.strip().lower() for t in types_param.split(","))
        top_n = min(int(q.get("top_n", ["10"])[0] or 10), 20)
        tickers_param = q.get("tickers", [None])[0]  # comma-separated ticker list
        watchlist_param = q.get("watchlist", [None])[0]  # watchlist name
        asset_class = _parse_asset_class(q)
        exclude_etfs, etf_only = _asset_class_flags(asset_class)
        try:
            import sqlite3
            from sweep_engine import get_ticker_day_ranks
            from datetime import datetime, timedelta

            cutoff = (datetime.now() - timedelta(days=months * 30)).strftime("%Y-%m-%d")

            # Build ticker filter set if watchlist, tickers, or sector specified
            sector_param = q.get("sector", [None])[0]
            ticker_filter = None
            if tickers_param:
                ticker_filter = set(t.strip().upper() for t in tickers_param.split(",") if t.strip())
            elif watchlist_param and watchlist_param in WATCHLISTS:
                ticker_filter = set()
                for _, tickers in WATCHLISTS[watchlist_param]:
                    for display, api, atype in tickers:
                        ticker_filter.add(display)

            # Sector filter — intersect with or replace ticker_filter
            if sector_param:
                from sweep_engine import SECTOR_GROUPS
                sector_tickers = set(SECTOR_GROUPS.get(sector_param, []))
                if ticker_filter is not None:
                    ticker_filter = ticker_filter & sector_tickers
                else:
                    ticker_filter = sector_tickers

            # Load detection config thresholds for filtering
            from sweep_engine import get_detection_config
            det_cfg = get_detection_config().get("stock", {})
            cb_min_total = det_cfg.get("min_total", 38000000)
            cb_min_sweeps = det_cfg.get("min_sweeps", 3)
            rare_min_notional = det_cfg.get("rare_min_notional", 1000000)
            monster_min_notional = det_cfg.get("monster_min_notional", 100000000)

            # Filter: CBs must meet notional+sweep thresholds, rare/monster have their own
            threshold_filter = (
                "AND ("
                "  (event_type='clusterbomb' AND total_notional >= ? AND sweep_count >= ?)"
                "  OR (event_type='rare_sweep' AND total_notional >= ?)"
                "  OR (event_type='monster_sweep' AND total_notional >= ?)"
                ")"
            )
            threshold_params = (cb_min_total, cb_min_sweeps, rare_min_notional, monster_min_notional)

            conn = sqlite3.connect(DB_PATH, timeout=10)
            conn.row_factory = sqlite3.Row
            if ticker_filter:
                placeholders = ",".join("?" for _ in ticker_filter)
                rows = conn.execute(
                    "SELECT ticker, event_date, sweep_count, total_notional, avg_price, "
                    "direction, is_rare, COALESCE(is_monster, 0) as is_monster "
                    f"FROM clusterbomb_events WHERE event_date >= ? AND ticker IN ({placeholders}) "
                    "AND event_type IN ('clusterbomb', 'rare_sweep', 'monster_sweep') "
                    f"{threshold_filter} "
                    "ORDER BY ticker, event_date",
                    (cutoff, *sorted(ticker_filter), *threshold_params)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT ticker, event_date, sweep_count, total_notional, avg_price, "
                    "direction, is_rare, COALESCE(is_monster, 0) as is_monster "
                    "FROM clusterbomb_events WHERE event_date >= ? "
                    "AND event_type IN ('clusterbomb', 'rare_sweep', 'monster_sweep') "
                    f"{threshold_filter} "
                    "ORDER BY ticker, event_date",
                    (cutoff, *threshold_params)
                ).fetchall()
            conn.close()

            # Get all unique tickers, sorted, and build index map
            all_tickers = sorted(set(r["ticker"] for r in rows))
            ticker_idx = {t: i for i, t in enumerate(all_tickers)}

            # Fetch rank data for all tickers in one batch
            rank_data = get_ticker_day_ranks(tickers=all_tickers) if all_tickers else {}

            # Load daily closes for split-safe price ratios
            daily_closes = self._load_daily_closes(all_tickers)

            # Build combined-key arrays:  key = tickerIndex * 100_000_000 + YYYYMMDD
            cb_keys, cb_tips, cb_ratios = [], [], []
            monster_keys, monster_tips, monster_ratios = [], [], []
            rare_keys, rare_tips, rare_ratios = [], [], []
            stats = {"clusterbombs": 0, "monsters": 0, "rare": 0, "tickers": len(all_tickers)}

            for r in rows:
                parts = r["event_date"].split("-")
                y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
                date_int = y * 10000 + m * 100 + d
                tidx = ticker_idx[r["ticker"]]
                combined_key = tidx * 100_000_000 + date_int

                notional = r["total_notional"] or 0
                n_str = f"${notional/1e6:.1f}M" if notional >= 1e6 else f"${notional/1e3:.0f}K"

                rk = rank_data.get((r["ticker"], r["event_date"]), {})
                rank_num = rk.get("rank")
                total_days = rk.get("total_days")
                rank_str = f"#{rank_num}/{total_days}" if rank_num else ""

                direction_str = (r["direction"] or "N/A").upper()
                tip = (f"{n_str} | {rank_str} | {r['sweep_count']}sw | {direction_str}"
                       if rank_str else
                       f"{n_str} | {r['sweep_count']}sw | {direction_str}")

                price = r["avg_price"] or 0
                day_close = daily_closes.get(r["ticker"], {}).get(r["event_date"], 0)
                # Split adjustment: avg_price is raw (unadjusted) but
                # day_close is split-adjusted from Polygon.  Detect and
                # correct so the ratio stays near 1.0.
                if day_close > 0 and price > 0:
                    raw_ratio = price / day_close
                    if raw_ratio > 1.5:
                        # Forward split (e.g. 10:1) — raw price >> adjusted
                        price = price / round(raw_ratio)
                    elif raw_ratio < 0.667:
                        # Reverse split (e.g. 1:4) — raw price << adjusted
                        price = price * round(1.0 / raw_ratio)
                ratio = price / day_close if day_close > 0 and price > 0 else 1.0
                ratio_str = f"{ratio:.4f}"

                if r["is_monster"]:
                    if "monster" in include_types:
                        monster_keys.append(str(combined_key))
                        monster_tips.append(f'"{tip}"')
                        monster_ratios.append(ratio_str)
                    stats["monsters"] += 1
                elif r["is_rare"]:
                    if "rare" in include_types:
                        rare_keys.append(str(combined_key))
                        rare_tips.append(f'"{tip}"')
                        rare_ratios.append(ratio_str)
                    stats["rare"] += 1
                else:
                    if "cb" in include_types:
                        cb_keys.append(str(combined_key))
                        cb_tips.append(f'"{tip}"')
                        cb_ratios.append(ratio_str)
                    stats["clusterbombs"] += 1

            # ── Ranked daily events (top N per ticker, all-time) ──
            ranked_keys, ranked_labels, ranked_ratios = [], [], []
            stats["ranked"] = 0
            if "ranked" in include_types:
                # Query all events with daily_rank, no date cutoff (all-time)
                ranked_sql = (
                    "SELECT ticker, event_date, daily_rank, total_notional, avg_price "
                    "FROM clusterbomb_events "
                    "WHERE daily_rank IS NOT NULL AND daily_rank <= ? "
                )
                ranked_params = [top_n]

                if ticker_filter:
                    placeholders = ",".join("?" for _ in ticker_filter)
                    ranked_sql += f"AND ticker IN ({placeholders}) "
                    ranked_params.extend(sorted(ticker_filter))

                # ETF filtering
                if exclude_etfs:
                    from sweep_engine import _load_etf_set
                    etf_set = _load_etf_set()
                    if etf_set:
                        etf_ph = ",".join("?" for _ in etf_set)
                        ranked_sql += f"AND ticker NOT IN ({etf_ph}) "
                        ranked_params.extend(sorted(etf_set))
                elif etf_only:
                    from sweep_engine import _load_etf_set
                    etf_set = _load_etf_set()
                    if etf_set:
                        etf_ph = ",".join("?" for _ in etf_set)
                        ranked_sql += f"AND ticker IN ({etf_ph}) "
                        ranked_params.extend(sorted(etf_set))

                ranked_sql += "ORDER BY ticker, daily_rank"

                conn2 = sqlite3.connect(DB_PATH, timeout=10)
                conn2.row_factory = sqlite3.Row
                ranked_rows = conn2.execute(ranked_sql, ranked_params).fetchall()
                conn2.close()

                # Add ranked tickers to the ticker index if not already present
                for rr in ranked_rows:
                    if rr["ticker"] not in ticker_idx:
                        idx = len(all_tickers)
                        all_tickers.append(rr["ticker"])
                        ticker_idx[rr["ticker"]] = idx

                # Load daily closes for any new tickers
                new_tickers = [t for t in all_tickers if t not in daily_closes]
                if new_tickers:
                    daily_closes.update(self._load_daily_closes(new_tickers))

                # Cap at 500 labels total
                max_labels = 500
                for rr in ranked_rows:
                    if len(ranked_keys) >= max_labels:
                        break
                    parts = rr["event_date"].split("-")
                    y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
                    date_int = y * 10000 + m * 100 + d
                    tidx = ticker_idx[rr["ticker"]]
                    combined_key = tidx * 100_000_000 + date_int

                    notional = rr["total_notional"] or 0
                    n_str = f"${notional/1e6:.0f}M" if notional >= 1e6 else f"${notional/1e3:.0f}K"
                    label_text = f"#{rr['daily_rank']} {n_str}"

                    price = rr["avg_price"] or 0
                    day_close = daily_closes.get(rr["ticker"], {}).get(rr["event_date"], 0)
                    if day_close > 0 and price > 0:
                        raw_ratio = price / day_close
                        if raw_ratio > 1.5:
                            price = price / round(raw_ratio)
                        elif raw_ratio < 0.667:
                            price = price * round(1.0 / raw_ratio)
                    ratio = price / day_close if day_close > 0 and price > 0 else 1.0

                    ranked_keys.append(str(combined_key))
                    ranked_labels.append(f'"{label_text}"')
                    ranked_ratios.append(f"{ratio:.4f}")
                    stats["ranked"] += 1

                # Update ticker count (may have grown)
                stats["tickers"] = len(all_tickers)

            # ── Build Pine Script ──
            L = []  # shorthand
            L.append("// @version=5")
            date_tag = datetime.now().strftime("%y%m%d")
            wl_tag = watchlist_param or "All"
            L.append(f'indicator("DPS-{date_tag}-{wl_tag}", overlay=true, max_labels_count=500)')
            L.append("")

            L.append("// \u2500\u2500 Sweep Type Toggles \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500")
            L.append('showCB      = input.bool(true, "Clusterbombs",  group="Sweep Types")')
            L.append('showMonster = input.bool(true, "Monsters",      group="Sweep Types")')
            L.append('showRare    = input.bool(true, "Rare Sweeps",   group="Sweep Types")')
            if "ranked" in include_types:
                L.append('showRanked  = input.bool(true, "Daily Ranked",  group="Sweep Types")')
            L.append("")
            L.append("// \u2500\u2500 Style \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500")
            L.append('opacity    = input.int(0, "Opacity",')
            L.append('  minval=0, maxval=100, step=5, tooltip="0 = solid, 100 = invisible", group="Style")')
            L.append('showShortText = input.bool(true,  "Short Labels (CB / M / R)",  group="Text")')
            if include_tips:
                L.append('showDetails   = input.bool(false, "Detail Labels (notional, rank)", group="Text")')
            L.append("")

            L.append("// \u2500\u2500 Derived \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500")
            L.append("cbColor      = color.new(#f59e0b, opacity)")
            L.append("monsterColor = color.new(#f97316, opacity)")
            L.append("rareColor    = color.new(#a78bfa, opacity)")
            if "ranked" in include_types:
                L.append("rankedColor  = color.new(#22d3ee, opacity)")
            L.append("")

            # Emit helper — auto-chunks at 3900 to stay under Pine's 4000-arg limit
            PINE_ARRAY_LIMIT = 3900

            def _emit(name, items, per_line, empty_type="int"):
                if not items:
                    L.append(f"var {name} = array.new_{empty_type}(0)")
                    return
                if len(items) <= PINE_ARRAY_LIMIT:
                    # Small array — wrap in function to keep main body short
                    L.append(f"_init_{name}() =>")
                    L.append(f"    array.from(")
                    for i in range(0, len(items), per_line):
                        chunk = ", ".join(items[i:i+per_line])
                        comma = "," if i + per_line < len(items) else ""
                        L.append(f"      {chunk}{comma}")
                    L.append("      )")
                    L.append(f"var {name} = _init_{name}()")
                else:
                    # Large array — split into chunk functions, concat in init
                    chunk_names = []
                    for ci in range(0, len(items), PINE_ARRAY_LIMIT):
                        chunk_items = items[ci:ci+PINE_ARRAY_LIMIT]
                        cn = f"_init_{name}_{ci // PINE_ARRAY_LIMIT}"
                        chunk_names.append(cn)
                        L.append(f"{cn}() =>")
                        L.append(f"    array.from(")
                        for i in range(0, len(chunk_items), per_line):
                            row = ", ".join(chunk_items[i:i+per_line])
                            comma = "," if i + per_line < len(chunk_items) else ""
                            L.append(f"      {row}{comma}")
                        L.append("      )")
                    # Init function that concats all chunks
                    L.append(f"_init_{name}() =>")
                    L.append(f"    a = {chunk_names[0]}()")
                    for cn in chunk_names[1:]:
                        L.append(f"    array.concat(a, {cn}())")
                    L.append(f"    a")
                    L.append(f"var {name} = _init_{name}()")

            # Ticker index lookup
            L.append(f"// \u2500\u2500 Ticker Lookup ({len(all_tickers)} tickers) \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500")
            ticker_strings = [f'"{t}"' for t in all_tickers]
            _emit("tickerList", ticker_strings, 10, "string")
            L.append("tidx = array.indexof(tickerList, syminfo.ticker)")
            L.append("dateKey = year * 10000 + month * 100 + dayofmonth")
            L.append("newDay  = ta.change(time('D')) != 0  // only plot once per day on intraday charts")
            L.append("myKey = tidx * 100000000 + dateKey")
            L.append("")

            L.append(f"// \u2500\u2500 Event Data ({stats['clusterbombs']} CB, {stats['monsters']} monster, {stats['rare']} rare) \u2500\u2500")
            _emit("cbKeys", cb_keys, 8)
            _emit("cbRatios", cb_ratios, 8, "float")
            if include_tips:
                _emit("cbTips", cb_tips, 3, "string")
            L.append("")
            _emit("monsterKeys", monster_keys, 8)
            _emit("monsterRatios", monster_ratios, 8, "float")
            if include_tips:
                _emit("monsterTips", monster_tips, 3, "string")
            L.append("")
            _emit("rareKeys", rare_keys, 8)
            _emit("rareRatios", rare_ratios, 8, "float")
            if include_tips:
                _emit("rareTips", rare_tips, 3, "string")
            L.append("")
            if "ranked" in include_types:
                _emit("rankedKeys", ranked_keys, 8)
                _emit("rankedRatios", ranked_ratios, 8, "float")
                _emit("rankedLabels", ranked_labels, 5, "string")
                L.append("")

            # Detection — match current bar against event arrays
            L.append("// ── Detect ──────────────────────────────────────────────")
            L.append("cbIdx      = tidx >= 0 ? array.indexof(cbKeys, myKey)      : -1")
            L.append("monsterIdx = tidx >= 0 ? array.indexof(monsterKeys, myKey)  : -1")
            L.append("rareIdx    = tidx >= 0 ? array.indexof(rareKeys, myKey)     : -1")
            L.append("isCB      = cbIdx >= 0")
            L.append("isMonster = monsterIdx >= 0")
            L.append("isRare    = rareIdx >= 0")
            if "ranked" in include_types:
                L.append("rankedIdx  = tidx >= 0 ? array.indexof(rankedKeys, myKey)  : -1")
                L.append("isRanked  = rankedIdx >= 0")
            L.append("// ratio * close = split-adjusted true sweep price")
            L.append("cbPrice      = isCB      ? array.get(cbRatios, cbIdx) * close           : na")
            L.append("monsterPrice = isMonster  ? array.get(monsterRatios, monsterIdx) * close : na")
            L.append("rarePrice    = isRare     ? array.get(rareRatios, rareIdx) * close       : na")
            if "ranked" in include_types:
                L.append("rankedPrice  = isRanked  ? array.get(rankedRatios, rankedIdx) * close   : na")
            L.append("")

            # Plot circles — true sweep price, split-safe
            L.append("// ── Price-anchored circles (ratio * close = true sweep price) ──")
            plot_items = [
                ("Clusterbomb", "showCB and isCB", "cbColor", 12, "cbPrice"),
                ("Monster", "showMonster and isMonster", "monsterColor", 14, "monsterPrice"),
                ("Rare Sweep", "showRare and isRare", "rareColor", 11, "rarePrice"),
            ]
            if "ranked" in include_types:
                plot_items.append(("Ranked", "showRanked and isRanked", "rankedColor", 10, "rankedPrice"))
            for kind, flag, color_var, lw, price_var in plot_items:
                L.append(f'plot({flag} and newDay ? {price_var} : na, "{kind}", color={color_var},')
                L.append(f'  style=plot.style_circles, linewidth={lw})')
            L.append("")

            # Text labels above bar
            L.append("// ── Text labels (above bar, once per day) ──────────────")
            for kind, flag, color_var, short_label in [
                ("Clusterbomb label", "showCB and isCB", "cbColor", "CB"),
                ("Monster label", "showMonster and isMonster", "monsterColor", "M"),
                ("Rare label", "showRare and isRare", "rareColor", "R"),
            ]:
                L.append(f'plotshape({flag} and showShortText and newDay, "{kind}", shape.diamond,')
                L.append(f'  location.abovebar, color=color.new(color.black, 100), size=size.tiny,')
                L.append(f'  text="{short_label}", textcolor={color_var})')
            L.append("")

            # Ranked labels — always use label.new with rank + notional
            if "ranked" in include_types:
                L.append("// ── Ranked Labels (rank + notional) ────────────────────")
                L.append("if showRanked and isRanked and newDay")
                L.append("    rIdx = array.indexof(rankedKeys, myKey)")
                L.append('    rLbl = rIdx >= 0 ? array.get(rankedLabels, rIdx) : ""')
                L.append('    label.new(bar_index, rankedPrice, rLbl,')
                L.append("      color=color.new(#22d3ee, math.max(opacity - 20, 0)),")
                L.append("      textcolor=color.white, style=label.style_label_down, size=size.small)")
                L.append("")

            # Detail labels (only if tips included)
            if include_tips:
                L.append("// ── Detail Labels ────────────────────────────────────")
                L.append("if showDetails and newDay")
                for short_label, flag, keys_var, tips_var, price_var, hex_color, lbl_sz in [
                    ("CB", "showCB and isCB", "cbKeys", "cbTips", "cbPrice", "#f59e0b", "size.small"),
                    ("M", "showMonster and isMonster", "monsterKeys", "monsterTips", "monsterPrice", "#f97316", "size.normal"),
                    ("R", "showRare and isRare", "rareKeys", "rareTips", "rarePrice", "#a78bfa", "size.small"),
                ]:
                    L.append(f"    if {flag}")
                    L.append(f"        idx = array.indexof({keys_var}, myKey)")
                    L.append(f"        tip = idx >= 0 ? array.get({tips_var}, idx) : \"\"")
                    L.append(f'        label.new(bar_index, {price_var}, "{short_label}\\n" + tip,')
                    L.append(f"          color=color.new({hex_color}, math.max(opacity - 20, 0)),")
                    L.append(f"          textcolor=color.white, style=label.style_label_down, size={lbl_sz})")
            L.append("")

            included = len(cb_keys) + len(monster_keys) + len(rare_keys) + len(ranked_keys)
            L.append(f'// Generated {datetime.now().strftime("%Y-%m-%d %H:%M")} | {months}mo window')
            ranked_str = f", {len(ranked_keys)} ranked" if ranked_keys else ""
            L.append(f"// {stats['tickers']} tickers, {included} events ({len(cb_keys)} CB, {len(monster_keys)} monsters, {len(rare_keys)} rare{ranked_str})")

            script = "\n".join(L)
            stats["included"] = included
            stats["months"] = months
            stats["types"] = list(include_types)
            self.send_json({"ok": True, "script": script, "stats": stats,
                            "tickers": all_tickers, "script_size": len(script)})
        except Exception as e:
            import traceback
            traceback.print_exc()
            self.send_json({"ok": False, "error": str(e)})

    # ── Backtest endpoints ──────────────────────────────────────────

    def serve_backtest_status(self):
        """GET /api/backtest/status — pre-compute status and row counts."""
        try:
            import sqlite3
            from backtest_engine import init_db as bt_init_db
            conn = sqlite3.connect(DB_PATH, timeout=10)
            bt_init_db(conn)
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM backtest_indicators")
            total_rows = c.fetchone()[0]
            c.execute("SELECT COUNT(DISTINCT ticker) FROM backtest_indicators")
            total_tickers = c.fetchone()[0]
            c.execute("SELECT MIN(date), MAX(date) FROM backtest_indicators")
            date_range = c.fetchone()
            c.execute("SELECT value FROM backtest_meta WHERE key='last_precompute'")
            last = c.fetchone()
            c.execute("SELECT value FROM backtest_meta WHERE key='precompute_stats'")
            stats = c.fetchone()
            conn.close()
            self.send_json({
                "ok": True,
                "total_rows": total_rows,
                "total_tickers": total_tickers,
                "date_from": date_range[0] if date_range else None,
                "date_to": date_range[1] if date_range else None,
                "last_precompute": last[0] if last else None,
                "stats": json.loads(stats[0]) if stats else None,
                "ready": total_rows > 0,
            })
        except Exception as e:
            self.send_json({"ok": False, "error": str(e)})

    def serve_backtest_run(self):
        """POST /api/backtest/run — execute a backtest query against pre-computed data."""
        try:
            length = int(self.headers.get("Content-Length", 0))
            payload = json.loads(self.rfile.read(length)) if length else {}

            from backtest_engine import run_backtest
            result = run_backtest(payload)
            self.send_json(result)
        except Exception as e:
            self.send_json({"ok": False, "error": str(e)})

    def serve_backtest_precompute(self):
        """POST /api/backtest/precompute — trigger indicator pre-computation."""
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length else {}

            from backtest_engine import precompute_all
            watchlist = body.get("watchlist")
            force = body.get("force", False)
            div_adj = 1 if body.get("div_adj") else 0

            # Run in a thread to avoid blocking the server
            import threading
            def _run():
                try:
                    precompute_all(div_adj=div_adj, force=force, watchlist=watchlist)
                    # Also backfill clusterbomb forward returns
                    from backtest_engine import backfill_clusterbomb_returns
                    n = backfill_clusterbomb_returns()
                    if n > 0:
                        print(f"Backfilled {n} clusterbomb forward returns.")
                except Exception as e:
                    print(f"Precompute error: {e}")

            t = threading.Thread(target=_run, daemon=True)
            t.start()

            self.send_json({"ok": True, "message": "Pre-computation started in background."})
        except Exception as e:
            self.send_json({"ok": False, "error": str(e)})

    def serve_backtest_nuke_cache(self):
        """POST /api/backtest/nuke-cache — clear all backtest caches (SQLite + in-memory)."""
        try:
            import sqlite3
            from backtest_engine import init_db as bt_init_db
            conn = sqlite3.connect(DB_PATH, timeout=10)
            bt_init_db(conn)
            c = conn.cursor()

            # Count before clearing
            c.execute("SELECT COUNT(*) FROM backtest_cache")
            n_cached = c.fetchone()[0]

            # Clear SQLite cache
            c.execute("DELETE FROM backtest_cache")

            # Bump precompute timestamp so in-memory caches auto-invalidate
            from datetime import datetime
            new_ts = datetime.now(timezone.utc).isoformat()
            c.execute("INSERT OR REPLACE INTO backtest_meta (key, value) VALUES ('last_precompute', ?)", (new_ts,))
            conn.commit()
            conn.close()

            # Clear in-memory caches directly
            import backtest_engine as be
            be._signal_cache.clear()
            be._price_cache.clear()
            be._cache_precompute_ts = None

            self.send_json({
                "ok": True,
                "cleared": n_cached,
                "new_precompute_ts": new_ts,
                "message": f"Nuked {n_cached} cached results + in-memory caches."
            })
        except Exception as e:
            self.send_json({"ok": False, "error": str(e)})

    def serve_backtest_trade_chart(self):
        """GET /api/backtest/trade-chart — extended price context for a trade."""
        try:
            from backtest_engine import _load_daily_prices
            params = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            ticker = params.get("ticker", [""])[0]
            entry_date = params.get("entry", [""])[0]
            exit_date = params.get("exit", [""])[0]
            context = int(params.get("context", ["100"])[0])

            if not ticker or not entry_date:
                self.send_json({"ok": False, "error": "Missing ticker or entry"})
                return

            daily = _load_daily_prices(ticker)
            if not daily:
                self.send_json({"ok": False, "error": "No price data"})
                return

            # Find entry/exit indices
            entry_idx = None
            exit_idx = None
            for k, (d, c, o) in enumerate(daily):
                if d == entry_date:
                    entry_idx = k
                if exit_date and d == exit_date:
                    exit_idx = k

            if entry_idx is None:
                # Find closest date
                for k, (d, c, o) in enumerate(daily):
                    if d >= entry_date:
                        entry_idx = k
                        break
            if entry_idx is None:
                self.send_json({"ok": False, "error": "Entry date not found"})
                return
            if exit_idx is None and exit_date:
                for k, (d, c, o) in enumerate(daily):
                    if d >= exit_date:
                        exit_idx = k
                        break
            if exit_idx is None:
                exit_idx = len(daily) - 1

            # Build extended window
            start = max(0, entry_idx - context)
            end = min(len(daily), exit_idx + context + 1)
            prices = [daily[k][1] for k in range(start, end)]
            dates = [daily[k][0] for k in range(start, end)]

            self.send_json({
                "ok": True,
                "prices": prices,
                "dates": dates,
                "entry_idx": entry_idx - start,
                "exit_idx": exit_idx - start,
            })
        except Exception as e:
            self.send_json({"ok": False, "error": str(e)})

    # ------------------------------------------------------------------
    # SWEEPS / CLUSTERBOMB ENDPOINTS
    # ------------------------------------------------------------------

    def serve_sweep_api_check(self):
        """GET /api/sweeps/api-check — check if API key has trades access."""
        try:
            result = check_api_access()
            self.send_json(result)
        except Exception as e:
            self.send_json({"has_access": False, "message": str(e)})

    def serve_sweep_stats(self, query=None):
        """GET /api/sweeps/stats — overview stats for sweeps page header.
        Supports: min_total, tickers, date_from, date_to, and per-type display filters.
        """
        try:
            min_total_str = (query or {}).get("min_total", [None])[0]
            min_total = float(min_total_str) if min_total_str else None

            tickers_raw = (query or {}).get("tickers", [None])[0]
            tickers = [t.strip().upper() for t in tickers_raw.split(",") if t.strip()] if tickers_raw else None
            date_from = (query or {}).get("date_from", [None])[0]
            date_to = (query or {}).get("date_to", [None])[0]

            # Per-type display filters
            min_sweeps = int((query or {}).get("min_sweeps", ["0"])[0]) or None
            monster_min = float((query or {}).get("monster_min", ["0"])[0]) or None
            rare_min = float((query or {}).get("rare_min", ["0"])[0]) or None
            rare_days = int((query or {}).get("rare_days", ["0"])[0]) or None
            rank_from_str = (query or {}).get("rank_from", [None])[0]
            rank_to_str = (query or {}).get("rank_to", [None])[0]
            rank_from = int(rank_from_str) if rank_from_str else None
            rank_to = int(rank_to_str) if rank_to_str else None
            daily_from_str = (query or {}).get("daily_from", [None])[0]
            daily_to_str = (query or {}).get("daily_to", [None])[0]
            daily_from = int(daily_from_str) if daily_from_str else None
            daily_to = int(daily_to_str) if daily_to_str else None
            full_db = (query or {}).get("full_db", ["0"])[0] == "1"
            asset_class = _parse_asset_class(query)
            exclude_etfs, etf_only = _asset_class_flags(asset_class)

            stats = get_sweep_stats(min_total=min_total, tickers=tickers,
                                    date_from=date_from, date_to=date_to,
                                    min_sweeps=min_sweeps, monster_min=monster_min,
                                    rare_min=rare_min, rare_days=rare_days,
                                    rank_from=rank_from, rank_to=rank_to,
                                    daily_from=daily_from, daily_to=daily_to,
                                    full_db=full_db,
                                    exclude_etfs=exclude_etfs, etf_only=etf_only)
            self.send_json(stats)
        except Exception as e:
            self.send_json({"error": str(e)})

    def serve_sweep_diag(self, query=None):
        """GET /api/sweeps/diag — diagnostic: compare summary vs trades.
        Add &fix=1 to rebuild summary + delete corrupt event for that ticker+date."""
        try:
            ticker = (query or {}).get("ticker", [None])[0]
            date = (query or {}).get("date", [None])[0]
            fix = (query or {}).get("fix", ["0"])[0] == "1"
            if not ticker or not date:
                self.send_json({"error": "ticker and date required"})
                return
            from sweep_engine import _get_db
            conn = _get_db()
            conn.row_factory = __import__('sqlite3').Row

            if fix:
                # Rebuild summary for this ticker+date from actual trades
                conn.execute("""
                    INSERT OR REPLACE INTO sweep_daily_summary
                        (ticker, trade_date, sweep_count, total_notional, total_shares,
                         vwap, min_price, max_price, first_sweep, last_sweep)
                    SELECT ticker, trade_date,
                        COUNT(*), SUM(notional), SUM(size),
                        SUM(price * size) / SUM(size),
                        MIN(price), MAX(price),
                        MIN(trade_time), MAX(trade_time)
                    FROM sweep_trades
                    WHERE ticker=? AND trade_date=? AND is_darkpool=1 AND is_sweep=1
                """, (ticker, date))
                # Delete the corrupt event so redetect recreates it
                conn.execute(
                    "DELETE FROM clusterbomb_events WHERE ticker=? AND event_date=? AND event_type='ranked_daily'",
                    (ticker, date))
                conn.commit()

            # Diagnostic output
            r1 = conn.execute(
                "SELECT is_darkpool, is_sweep, COUNT(*) as cnt, SUM(notional) as total, SUM(size) as shares "
                "FROM sweep_trades WHERE ticker=? AND trade_date=? GROUP BY is_darkpool, is_sweep",
                (ticker, date)).fetchall()
            flag_breakdown = [dict(r) for r in r1]
            r2 = conn.execute(
                "SELECT * FROM sweep_daily_summary WHERE ticker=? AND trade_date=?",
                (ticker, date)).fetchone()
            summary = dict(r2) if r2 else None
            r3 = conn.execute(
                "SELECT event_date, event_type, sweep_count, total_notional, daily_rank, sweep_rank "
                "FROM clusterbomb_events WHERE ticker=? AND event_date=?",
                (ticker, date)).fetchall()
            events = [dict(r) for r in r3]
            conn.close()
            self.send_json({"fixed": fix, "flag_breakdown": flag_breakdown, "summary": summary, "events": events})
        except Exception as e:
            self.send_json({"error": str(e)})

    def serve_sweep_summary(self, query=None):
        """GET /api/sweeps/summary — sweep trades grouped by ticker+date."""
        try:
            ticker = (query or {}).get("ticker", [None])[0]
            date_from = (query or {}).get("date_from", [None])[0]
            date_to = (query or {}).get("date_to", [None])[0]
            limit = int((query or {}).get("limit", [200])[0])
            data = get_sweep_summary(ticker=ticker, date_from=date_from,
                                     date_to=date_to, limit=limit)
            self.send_json({"rows": data, "count": len(data)})
        except Exception as e:
            self.send_json({"rows": [], "count": 0, "error": str(e)})

    def serve_sweep_detail(self, query=None):
        """GET /api/sweeps/detail — individual sweeps for a ticker on a date."""
        try:
            ticker = (query or {}).get("ticker", [None])[0]
            date_str = (query or {}).get("date", [None])[0]
            if not ticker or not date_str:
                self.send_json({"error": "ticker and date required"})
                return
            data = get_sweep_detail(ticker, date_str)
            self.send_json({"trades": data, "count": len(data)})
        except Exception as e:
            self.send_json({"trades": [], "count": 0, "error": str(e)})

    def serve_clusterbombs(self, query=None):
        """GET /api/sweeps/clusterbombs — detected clusterbomb events.
        Optional: min_total to filter by total_notional threshold.
        """
        try:
            ticker = (query or {}).get("ticker", [None])[0]
            date_from = (query or {}).get("date_from", [None])[0]
            date_to = (query or {}).get("date_to", [None])[0]
            rare_only = (query or {}).get("rare_only", ["0"])[0] == "1"
            limit = int((query or {}).get("limit", [200])[0])
            min_total_str = (query or {}).get("min_total", [None])[0]
            min_total = float(min_total_str) if min_total_str else None
            asset_class = _parse_asset_class(query)
            exclude_etfs, etf_only = _asset_class_flags(asset_class)
            data = get_clusterbombs(ticker=ticker, date_from=date_from,
                                    date_to=date_to, rare_only=rare_only,
                                    limit=limit, min_total=min_total,
                                    exclude_etfs=exclude_etfs, etf_only=etf_only)
            # Override current_price + pct_gain with live prices if available
            if _live_daemon is not None and data:
                tickers_needed = list(set(ev["ticker"] for ev in data))
                live = _live_daemon.get_live_prices(tickers_needed)
                for ev in data:
                    lp = live.get(ev["ticker"])
                    if lp and lp.get("price"):
                        ev["current_price"] = round(lp["price"], 4)
                        avg = ev.get("avg_price", 0)
                        ev["pct_gain"] = round((lp["price"] / avg - 1) * 100, 2) if avg > 0 else 0

            # Enrich events with peak trade time (time of largest sweep)
            if data:
                try:
                    from sweep_engine import _get_db
                    conn = _get_db()
                    # Build batch lookup: for each (ticker, date), find time of max notional trade
                    pairs = list(set((ev["ticker"], ev["date"]) for ev in data))
                    time_map = {}  # (ticker, date) → "HH:MM:SS"
                    # Batch in groups of 50 to avoid huge queries
                    for i in range(0, len(pairs), 50):
                        batch = pairs[i:i+50]
                        conditions = " OR ".join(
                            f"(ticker = ? AND trade_date = ?)" for _ in batch
                        )
                        params = []
                        for t, d in batch:
                            params.extend([t, d])
                        rows = conn.execute(f"""
                            SELECT ticker, trade_date, trade_time, notional
                            FROM sweep_trades
                            WHERE ({conditions})
                            AND is_sweep = 1 AND is_darkpool = 1
                            ORDER BY notional DESC
                        """, params).fetchall()
                        for r in rows:
                            key = (r["ticker"], r["trade_date"])
                            if key not in time_map:
                                # First row per (ticker, date) = highest notional
                                raw = r["trade_time"] or ""
                                time_map[key] = raw[:8] if len(raw) >= 8 else raw
                    conn.close()
                    for ev in data:
                        ev["event_time"] = time_map.get((ev["ticker"], ev["date"]), "")
                except Exception as e:
                    for ev in data:
                        ev["event_time"] = ""

            self.send_json({"events": data, "count": len(data)})
        except Exception as e:
            self.send_json({"events": [], "count": 0, "error": str(e)})

    def serve_sweep_chart(self, query=None):
        """GET /api/sweeps/chart — sweep markers + clusterbomb highlights for chart overlay.
        Optional: min_total to filter which clusterbombs appear on chart.
        """
        try:
            ticker = (query or {}).get("ticker", [None])[0]
            date_from = (query or {}).get("date_from", [None])[0]
            date_to = (query or {}).get("date_to", [None])[0]
            if not ticker:
                self.send_json({"error": "ticker required"})
                return
            timeframe = (query or {}).get("timeframe", ["1D"])[0]
            min_total_str = (query or {}).get("min_total", [None])[0]
            min_total = float(min_total_str) if min_total_str else None
            # Per-type display filters (same as tracker endpoint)
            min_sweeps = int((query or {}).get("min_sweeps", ["0"])[0]) or None
            monster_min = float((query or {}).get("monster_min", ["0"])[0]) or None
            rare_min = float((query or {}).get("rare_min", ["0"])[0]) or None
            rare_days = int((query or {}).get("rare_days", ["0"])[0]) or None
            rank_from_str = (query or {}).get("rank_from", [None])[0]
            rank_to_str = (query or {}).get("rank_to", [None])[0]
            rank_from = int(rank_from_str) if rank_from_str else None
            rank_to = int(rank_to_str) if rank_to_str else None
            daily_from_str = (query or {}).get("daily_from", [None])[0]
            daily_to_str = (query or {}).get("daily_to", [None])[0]
            daily_from = int(daily_from_str) if daily_from_str else None
            daily_to = int(daily_to_str) if daily_to_str else None
            asset_class = _parse_asset_class(query)
            exclude_etfs, etf_only = _asset_class_flags(asset_class)
            data = get_sweep_chart_data(ticker, date_from=date_from, date_to=date_to,
                                        timeframe=timeframe, min_total=min_total,
                                        monster_min=monster_min, min_sweeps=min_sweeps,
                                        rare_min=rare_min, rare_days=rare_days,
                                        rank_from=rank_from, rank_to=rank_to,
                                        daily_from=daily_from, daily_to=daily_to,
                                        etf_only=etf_only)
            self.send_json(data)
        except Exception as e:
            self.send_json({"sweeps": [], "clusterbombs": [], "error": str(e)})

    def serve_sweep_tracker(self, query=None):
        """GET /api/sweeps/tracker — clusterbomb tracker with current price + % gain.
        Supports: min_total, tickers, date_from, date_to, limit, offset,
        min_sweeps, monster_min, rare_min, rare_days (per-type display filters).
        """
        try:
            min_total = float((query or {}).get("min_total", ["10000000"])[0])

            tickers_raw = (query or {}).get("tickers", [None])[0]
            tickers = [t.strip().upper() for t in tickers_raw.split(",") if t.strip()] if tickers_raw else None
            date_from = (query or {}).get("date_from", [None])[0]
            date_to = (query or {}).get("date_to", [None])[0]
            limit = int((query or {}).get("limit", ["200"])[0])
            offset = int((query or {}).get("offset", ["0"])[0])

            # Per-type display filters
            min_sweeps = int((query or {}).get("min_sweeps", ["0"])[0]) or None
            monster_min = float((query or {}).get("monster_min", ["0"])[0]) or None
            rare_min = float((query or {}).get("rare_min", ["0"])[0]) or None
            rare_days = int((query or {}).get("rare_days", ["0"])[0]) or None
            rank_from_str = (query or {}).get("rank_from", [None])[0]
            rank_to_str = (query or {}).get("rank_to", [None])[0]
            rank_from = int(rank_from_str) if rank_from_str else None
            rank_to = int(rank_to_str) if rank_to_str else None
            daily_from_str = (query or {}).get("daily_from", [None])[0]
            daily_to_str = (query or {}).get("daily_to", [None])[0]
            daily_from = int(daily_from_str) if daily_from_str else None
            daily_to = int(daily_to_str) if daily_to_str else None
            asset_class = _parse_asset_class(query)
            exclude_etfs, etf_only = _asset_class_flags(asset_class)

            # Sub-filters (stock-only: CB/Monster)
            cb_only = (query or {}).get("cb_only", ["0"])[0] == "1"
            monster_only = (query or {}).get("monster_only", ["0"])[0] == "1"

            # Check server-side cache
            cache_key = (min_total, tuple(tickers) if tickers else None,
                         date_from, date_to, limit, offset,
                         min_sweeps, monster_min, rare_min, rare_days,
                         rank_from, rank_to,
                         daily_from, daily_to,
                         asset_class, cb_only, monster_only)
            now = time.time()
            cached = _tracker_cache.get(cache_key)
            if cached and (now - cached["ts"]) < _TRACKER_CACHE_TTL:
                self.send_json(cached["data"])
                return

            events, total = get_tracker_data(min_total=min_total,
                                             tickers=tickers, date_from=date_from,
                                             date_to=date_to, limit=limit,
                                             offset=offset,
                                             min_sweeps=min_sweeps,
                                             monster_min=monster_min,
                                             rare_min=rare_min,
                                             rare_days=rare_days,
                                             rank_from=rank_from,
                                             rank_to=rank_to,
                                             daily_from=daily_from,
                                             daily_to=daily_to,
                                             exclude_etfs=exclude_etfs,
                                             etf_only=etf_only,
                                             cb_only=cb_only,
                                             monster_only=monster_only)
            # Override current_price + pct_gain with live prices if available
            if _live_daemon is not None and events:
                tickers_needed = list(set(ev["ticker"] for ev in events))
                live = _live_daemon.get_live_prices(tickers_needed)
                for ev in events:
                    lp = live.get(ev["ticker"])
                    if lp and lp.get("price"):
                        ev["current_price"] = round(lp["price"], 4)
                        avg = ev.get("avg_price", 0)
                        ev["pct_gain"] = round((lp["price"] / avg - 1) * 100, 2) if avg > 0 else 0

            response = {"events": events, "total": total, "offset": offset}

            # Cache the response
            _tracker_cache[cache_key] = {"data": response, "ts": now}

            self.send_json(response)
        except Exception as e:
            self.send_json({"events": [], "total": 0, "error": str(e)})

    def serve_sweep_fetch(self):
        """POST /api/sweeps/fetch — trigger sweep data fetching for tickers."""
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length else {}

            tickers = body.get("tickers", [])
            start_date = body.get("start_date")
            end_date = body.get("end_date")

            if not tickers or not start_date:
                self.send_json({"ok": False, "error": "tickers and start_date required"})
                return

            # Guard: reject if a fetch is already running
            with _sweep_fetch_lock:
                if _sweep_fetch_progress["running"]:
                    self.send_json({"ok": False, "error": "A fetch is already running. Cancel it first."})
                    return

            # Run in background thread with progress tracking
            def _progress_cb(ticker, date_str, n_sweeps, total_sweeps, completed, total_jobs):
                with _sweep_fetch_lock:
                    _sweep_fetch_progress["completed"] = completed
                    _sweep_fetch_progress["total"] = total_jobs
                    _sweep_fetch_progress["sweeps_found"] = total_sweeps
                    _sweep_fetch_progress["current_ticker"] = ticker
                    _sweep_fetch_progress["current_date"] = date_str
                    rate = completed / max((time.time() - _sweep_fetch_progress.get("_t0", time.time())), 0.1)
                    _sweep_fetch_progress["rate"] = round(rate, 1)
                    line = f"[{completed}/{total_jobs}] {ticker} {date_str} — {n_sweeps} sweeps ({rate:.1f}/s)"
                    lines = _sweep_fetch_progress["log_lines"]
                    lines.append(line)
                    if len(lines) > 50:
                        _sweep_fetch_progress["log_lines"] = lines[-50:]
                    # Print every 50th update + first one to CMD window
                    if completed == 1 or completed % 50 == 0:
                        pct = int(completed / max(total_jobs, 1) * 100)
                        print(f"  [{completed}/{total_jobs}] {pct}% {ticker} {date_str} — {total_sweeps} sweeps ({rate:.1f}/s)", flush=True)

            def _run():
                _sweep_fetch_cancel.clear()
                with _sweep_fetch_lock:
                    _sweep_fetch_progress.update({
                        "running": True, "completed": 0, "total": 0,
                        "sweeps_found": 0, "current_ticker": "", "current_date": "",
                        "rate": 0.0, "phase": "fetching", "log_lines": [],
                        "_t0": time.time(),
                    })
                try:
                    from sweep_engine import get_detection_config as _get_cfg, detect_rare_sweep_days, detect_monster_sweeps, detect_ranked_sweeps, detect_ranked_daily
                    stats = fetch_and_store_sweeps(tickers, start_date, end_date,
                                                   progress_callback=_progress_cb,
                                                   cancel_event=_sweep_fetch_cancel)
                    if _sweep_fetch_cancel.is_set():
                        _clear_fetch_job()
                        with _sweep_fetch_lock:
                            _sweep_fetch_progress["phase"] = "cancelled"
                            _sweep_fetch_progress["running"] = False
                            _sweep_fetch_progress["log_lines"].append("Fetch cancelled by user.")
                        print("Sweep fetch cancelled by user.", flush=True)
                        return
                    # Auto-detect with stock profile for all fetched tickers
                    with _sweep_fetch_lock:
                        _sweep_fetch_progress["phase"] = "detecting"
                        _sweep_fetch_progress["log_lines"].append("Detecting clusterbombs & rare sweeps...")
                    cfg = _get_cfg()
                    _cb_keys = ("min_sweeps", "min_notional", "min_total", "rarity_days", "rare_min_notional")
                    _sp = {k: cfg["stock"][k] for k in _cb_keys if k in cfg["stock"]}
                    detect_clusterbombs(tickers=tickers, **_sp)
                    detect_rare_sweep_days(
                        min_notional=cfg["stock"].get("rare_min_notional", cfg["stock"]["min_notional"]),
                        rarity_days=cfg["stock"]["rarity_days"],
                        tickers=tickers,
                    )
                    _sm = cfg["stock"].get("monster_min_notional")
                    if _sm:
                        detect_monster_sweeps(monster_min_notional=float(_sm), tickers=tickers)
                    # Stock ranking (daily + single)
                    detect_ranked_daily(rank_limit=100,
                                        min_sweeps=int(cfg["stock"].get("min_sweeps_daily", 1)),
                                        tickers=tickers,
                                        exclude_etfs=True, etf_only=False)
                    detect_ranked_sweeps(rank_limit=100, tickers=tickers,
                                         exclude_etfs=True, etf_only=False)
                    # ETF detection pass — daily ranked + rare + single ranked
                    ecfg = cfg.get("etf", {})
                    detect_ranked_daily(rank_limit=100, min_sweeps=1,
                                        tickers=tickers,
                                        exclude_etfs=False, etf_only=True)
                    detect_rare_sweep_days(
                        min_notional=float(ecfg.get("rare_min_notional", 1_000_000)),
                        rarity_days=int(ecfg.get("rarity_days", 20)),
                        tickers=tickers, exclude_etfs=False, etf_only=True,
                    )
                    detect_ranked_sweeps(rank_limit=100, tickers=tickers,
                                         exclude_etfs=False, etf_only=True)
                    # Option A: backfill price cache for event tickers with no data
                    from sweep_engine import backfill_event_ticker_prices
                    backfill_event_ticker_prices(tickers=tickers)
                    _clear_fetch_job()  # fetch complete — remove job file
                    with _sweep_fetch_lock:
                        _sweep_fetch_progress["phase"] = "done"
                        _sweep_fetch_progress["running"] = False
                        elapsed = time.time() - _sweep_fetch_progress.get("_t0", time.time())
                        _sweep_fetch_progress["log_lines"].append(
                            f"Complete: {stats.get('sweeps_found',0)} sweeps, "
                            f"{stats.get('inserted',0)} inserted in {elapsed:.0f}s")
                    _tracker_cache.clear()  # invalidate tracker cache after new data
                    print(f"Sweep fetch + detect complete: {stats}", flush=True)
                except Exception as e:
                    with _sweep_fetch_lock:
                        _sweep_fetch_progress["phase"] = "error"
                        _sweep_fetch_progress["running"] = False
                        _sweep_fetch_progress["log_lines"].append(f"Error: {e}")
                    print(f"Sweep fetch error: {e}", flush=True)
                    import traceback; traceback.print_exc()

            _save_fetch_job(tickers, start_date, end_date)
            t = threading.Thread(target=_run, daemon=True)
            t.start()
            self.send_json({"ok": True, "message": f"Fetching sweeps for {len(tickers)} tickers from {start_date}. Running in background."})
        except Exception as e:
            self.send_json({"ok": False, "error": str(e)})

    def serve_sweep_fetch_cancel(self):
        """POST /api/sweeps/fetch-cancel — signal the running fetch to stop."""
        _sweep_fetch_cancel.set()
        with _sweep_fetch_lock:
            if _sweep_fetch_progress["running"]:
                _sweep_fetch_progress["log_lines"].append("Cancelling...")
        self.send_json({"ok": True, "message": "Cancel signal sent"})

    # serve_sweep_detect removed — Fetch auto-detects clusterbombs

    def serve_sweep_fetch_progress(self):
        """GET /api/sweeps/fetch-progress — real-time progress of background sweep fetch."""
        with _sweep_fetch_lock:
            data = {
                "running": _sweep_fetch_progress["running"],
                "completed": _sweep_fetch_progress["completed"],
                "total": _sweep_fetch_progress["total"],
                "sweeps_found": _sweep_fetch_progress["sweeps_found"],
                "current_ticker": _sweep_fetch_progress["current_ticker"],
                "current_date": _sweep_fetch_progress["current_date"],
                "rate": _sweep_fetch_progress["rate"],
                "phase": _sweep_fetch_progress["phase"],
                "log_lines": _sweep_fetch_progress["log_lines"][-20:],
            }
        self.send_json(data)

    def serve_pending_queue(self):
        """GET /api/sweeps/pending-queue — return the pending fetch queue."""
        queue_path = os.path.join(os.path.dirname(__file__), "pending_fetch_queue.json")
        try:
            with open(queue_path, "r", encoding="utf-8") as f:
                tickers = json.load(f)
            if not isinstance(tickers, list):
                tickers = []
        except (FileNotFoundError, json.JSONDecodeError):
            tickers = []
        self.send_json({"ok": True, "tickers": tickers, "count": len(tickers)})

    def serve_live_prices(self, query):
        """GET /api/prices/live?tickers=SPY,AAPL — return live prices from daemon + prev close from OHLC cache."""
        global _live_daemon
        tickers_param = query.get("tickers", [""])[0]
        tickers = [t.strip().upper() for t in tickers_param.split(",") if t.strip()] if tickers_param else None

        result = {}

        # 1. Get live prices from daemon's in-memory minute bars
        if _live_daemon is not None:
            live = _live_daemon.get_live_prices(tickers)
            for ticker, data in live.items():
                result[ticker] = data

        # 2. Enrich with prev_close from OHLC cache (for % change)
        from sweep_engine import _load_daily_prices
        target_tickers = tickers if tickers else list(result.keys())
        for ticker in target_tickers:
            df = _load_daily_prices(ticker)
            if df is not None and len(df) >= 1:
                try:
                    prev_close = float(df.iloc[-1]["close"])
                    # If we also have a second-to-last row, that's a better prev_close
                    # (last row might be today's partial data from CSV flush)
                    if len(df) >= 2:
                        last_date = str(df.iloc[-1]["timestamp"])[:10]
                        from datetime import datetime as _dt
                        today = _dt.now().strftime("%Y-%m-%d")
                        if last_date == today:
                            prev_close = float(df.iloc[-2]["close"])
                        else:
                            prev_close = float(df.iloc[-1]["close"])
                    if ticker in result:
                        result[ticker]["prev_close"] = round(prev_close, 4)
                        price = result[ticker]["price"]
                        result[ticker]["change_pct"] = round((price / prev_close - 1) * 100, 2) if prev_close > 0 else 0
                    else:
                        # No live data — fall back to OHLC cache price
                        cache_price = float(df.iloc[-1]["close"])
                        result[ticker] = {
                            "price": round(cache_price, 4),
                            "prev_close": round(prev_close, 4),
                            "change_pct": round((cache_price / prev_close - 1) * 100, 2) if prev_close > 0 else 0,
                            "volume": 0,
                            "updated_at": 0,
                            "source": "cache",
                        }
                except Exception:
                    pass

        self.send_json({"ok": True, "prices": result, "count": len(result),
                        "source": "live" if _live_daemon else "cache"})

    def serve_sweep_fetch_queue(self):
        """POST /api/sweeps/fetch-queue — fetch all tickers from pending queue with 10y lookback."""
        queue_path = os.path.join(os.path.dirname(__file__), "pending_fetch_queue.json")
        try:
            with open(queue_path, "r", encoding="utf-8") as f:
                tickers = json.load(f)
            if not isinstance(tickers, list) or not tickers:
                self.send_json({"ok": False, "error": "Queue is empty"})
                return
        except (FileNotFoundError, json.JSONDecodeError):
            self.send_json({"ok": False, "error": "No pending queue file found"})
            return

        # Optional: accept body params to override defaults
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length else {}
        except Exception:
            body = {}

        lookback_years = body.get("lookback_years", 10)
        start_date = (datetime.now() - timedelta(days=lookback_years * 365)).strftime("%Y-%m-%d")
        end_date = datetime.now().strftime("%Y-%m-%d")

        # Guard: reject if a fetch is already running
        with _sweep_fetch_lock:
            if _sweep_fetch_progress["running"]:
                self.send_json({"ok": False, "error": "A fetch is already running. Cancel it first."})
                return

        # Clear the queue file now that we're starting
        try:
            with open(queue_path, "w", encoding="utf-8") as f:
                json.dump([], f)
        except Exception:
            pass

        # Reuse the same fetch pipeline
        def _progress_cb(ticker, date_str, n_sweeps, total_sweeps, completed, total_jobs):
            with _sweep_fetch_lock:
                _sweep_fetch_progress["completed"] = completed
                _sweep_fetch_progress["total"] = total_jobs
                _sweep_fetch_progress["sweeps_found"] = total_sweeps
                _sweep_fetch_progress["current_ticker"] = ticker
                _sweep_fetch_progress["current_date"] = date_str
                rate = completed / max((time.time() - _sweep_fetch_progress.get("_t0", time.time())), 0.1)
                _sweep_fetch_progress["rate"] = round(rate, 1)
                line = f"[{completed}/{total_jobs}] {ticker} {date_str} — {n_sweeps} sweeps ({rate:.1f}/s)"
                lines = _sweep_fetch_progress["log_lines"]
                lines.append(line)
                if len(lines) > 50:
                    _sweep_fetch_progress["log_lines"] = lines[-50:]
                if completed == 1 or completed % 50 == 0:
                    pct = int(completed / max(total_jobs, 1) * 100)
                    print(f"  [QUEUE] [{completed}/{total_jobs}] {pct}% {ticker} {date_str} — {total_sweeps} sweeps ({rate:.1f}/s)", flush=True)

        def _run():
            _sweep_fetch_cancel.clear()
            with _sweep_fetch_lock:
                _sweep_fetch_progress.update({
                    "running": True, "completed": 0, "total": 0,
                    "sweeps_found": 0, "current_ticker": "", "current_date": "",
                    "rate": 0.0, "phase": "fetching", "log_lines": [
                        f"Queue fetch: {len(tickers)} tickers ({start_date} → {end_date})"
                    ],
                    "_t0": time.time(),
                })
            try:
                from sweep_engine import get_detection_config as _get_cfg, detect_rare_sweep_days, detect_monster_sweeps, detect_ranked_sweeps, detect_ranked_daily
                stats = fetch_and_store_sweeps(tickers, start_date, end_date,
                                               progress_callback=_progress_cb,
                                               cancel_event=_sweep_fetch_cancel)
                if _sweep_fetch_cancel.is_set():
                    # Re-queue cancelled tickers
                    remaining = [t for t in tickers if t not in stats.get("completed_tickers", set())]
                    if remaining:
                        try:
                            with open(queue_path, "r", encoding="utf-8") as f:
                                existing = json.load(f)
                        except Exception:
                            existing = []
                        merged = sorted(set(existing + remaining))
                        with open(queue_path, "w", encoding="utf-8") as f:
                            json.dump(merged, f)
                    _clear_fetch_job()
                    with _sweep_fetch_lock:
                        _sweep_fetch_progress["phase"] = "cancelled"
                        _sweep_fetch_progress["running"] = False
                        _sweep_fetch_progress["log_lines"].append(
                            f"Queue fetch cancelled. {len(remaining)} tickers re-queued.")
                    print(f"[QUEUE] Fetch cancelled, {len(remaining)} tickers re-queued", flush=True)
                    return

                # Detection pass (same as regular fetch)
                with _sweep_fetch_lock:
                    _sweep_fetch_progress["phase"] = "detecting"
                    _sweep_fetch_progress["log_lines"].append("Detecting clusterbombs & rare sweeps...")
                cfg = _get_cfg()
                _cb_keys = ("min_sweeps", "min_notional", "min_total", "rarity_days", "rare_min_notional")
                _sp = {k: cfg["stock"][k] for k in _cb_keys if k in cfg["stock"]}
                detect_clusterbombs(tickers=tickers, **_sp)
                detect_rare_sweep_days(
                    min_notional=cfg["stock"].get("rare_min_notional", cfg["stock"]["min_notional"]),
                    rarity_days=cfg["stock"]["rarity_days"],
                    tickers=tickers,
                )
                _sm = cfg["stock"].get("monster_min_notional")
                if _sm:
                    detect_monster_sweeps(monster_min_notional=float(_sm), tickers=tickers)
                detect_ranked_daily(rank_limit=100,
                                    min_sweeps=int(cfg["stock"].get("min_sweeps_daily", 1)),
                                    tickers=tickers, exclude_etfs=True, etf_only=False)
                detect_ranked_sweeps(rank_limit=100, tickers=tickers,
                                     exclude_etfs=True, etf_only=False)
                # ETF detection pass
                ecfg = cfg.get("etf", {})
                detect_ranked_daily(rank_limit=100, min_sweeps=1,
                                    tickers=tickers, exclude_etfs=False, etf_only=True)
                detect_rare_sweep_days(
                    min_notional=float(ecfg.get("rare_min_notional", 1_000_000)),
                    rarity_days=int(ecfg.get("rarity_days", 20)),
                    tickers=tickers, exclude_etfs=False, etf_only=True,
                )
                detect_ranked_sweeps(rank_limit=100, tickers=tickers,
                                     exclude_etfs=False, etf_only=True)
                from sweep_engine import backfill_event_ticker_prices
                backfill_event_ticker_prices(tickers=tickers)
                _clear_fetch_job()
                with _sweep_fetch_lock:
                    _sweep_fetch_progress["phase"] = "done"
                    _sweep_fetch_progress["running"] = False
                    elapsed = time.time() - _sweep_fetch_progress.get("_t0", time.time())
                    _sweep_fetch_progress["log_lines"].append(
                        f"Queue complete: {stats.get('sweeps_found',0)} sweeps, "
                        f"{stats.get('inserted',0)} inserted in {elapsed:.0f}s")
                _tracker_cache.clear()
                print(f"[QUEUE] Fetch + detect complete: {stats}", flush=True)
            except Exception as e:
                with _sweep_fetch_lock:
                    _sweep_fetch_progress["phase"] = "error"
                    _sweep_fetch_progress["running"] = False
                    _sweep_fetch_progress["log_lines"].append(f"Error: {e}")
                print(f"[QUEUE] Fetch error: {e}", flush=True)
                import traceback; traceback.print_exc()

        _save_fetch_job(tickers, start_date, end_date)
        t = threading.Thread(target=_run, daemon=True)
        t.start()
        self.send_json({"ok": True, "message": f"Queue fetch started: {len(tickers)} tickers from {start_date}",
                        "tickers": len(tickers)})

    # ------------------------------------------------------------------
    # Live sweep scanner endpoints
    # ------------------------------------------------------------------

    def serve_live_sweep_status(self):
        """GET /api/sweeps/live/status — return sweep-focused status from unified daemon."""
        global _live_daemon, _daemon_intentionally_stopped
        if _live_daemon is None:
            self.send_json({
                "running": False, "connected": False, "authenticated": False,
                "subscribed": False, "started_at": None,
                "trades_received": 0, "trades_per_sec": 0.0,
                "sweeps_today": 0, "sweeps_buffered": 0, "sweeps_written": 0,
                "last_sweep": None, "tickers_active": 0,
                "events_detected": {"clusterbomb": 0, "rare_sweep": 0, "monster_sweep": 0},
                "last_detection_at": None, "last_flush_at": None,
                "reconnect_count": 0, "error": None,
                "watchdog_active": not _daemon_intentionally_stopped,
            })
            return
        status = _live_daemon.get_sweep_status()
        status["watchdog_active"] = not _daemon_intentionally_stopped
        self.send_json(status)

    def serve_live_sweep_events(self):
        """GET /api/sweeps/live/events — events detected today by live scanner."""
        from sweep_engine import _get_db
        try:
            today = datetime.now().strftime("%Y-%m-%d")
            conn = _get_db()
            rows = conn.execute("""
                SELECT ticker, event_date, event_type, is_monster, total_notional, sweep_count
                FROM clusterbomb_events
                WHERE event_date = ?
                ORDER BY total_notional DESC
            """, (today,)).fetchall()
            conn.close()
            events = [dict(r) for r in rows]
            self.send_json({"date": today, "events": events})
        except Exception as e:
            self.send_json({"error": str(e)})

    def serve_live_sweep_start(self):
        """POST /api/sweeps/live/start — start the unified live daemon."""
        self._start_unified_daemon()

    def _start_unified_daemon(self):
        """Start the unified daemon (shared by sweep and price start endpoints)."""
        global _live_daemon, _daemon_intentionally_stopped
        from live_daemon import (UnifiedLiveDaemon, load_live_config,
                                 load_live_price_config)

        with _live_daemon_lock:
            if _live_daemon is not None and _live_daemon.get_status().get("running"):
                self.send_json({"ok": True, "message": "Already running",
                                "status": _live_daemon.get_status()})
                return

            try:
                scfg = load_live_config()
                pcfg = load_live_price_config()
                _live_daemon = UnifiedLiveDaemon(price_config=pcfg, sweep_config=scfg)
                _live_daemon.start()
                _daemon_intentionally_stopped = False  # watchdog should protect it
                self.send_json({"ok": True, "message": "Live daemon started",
                                "status": _live_daemon.get_status()})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)})

    def serve_live_sweep_stop(self):
        """POST /api/sweeps/live/stop — stop the unified live daemon."""
        self._stop_unified_daemon()

    def _stop_unified_daemon(self):
        """Stop the unified daemon (shared by sweep and price stop endpoints)."""
        global _live_daemon, _daemon_intentionally_stopped
        with _live_daemon_lock:
            _daemon_intentionally_stopped = True  # don't let watchdog restart it
            if _live_daemon is None or not _live_daemon.get_status().get("running"):
                self.send_json({"ok": True, "message": "Not running"})
                return

            try:
                _live_daemon.stop()
                self.send_json({"ok": True, "message": "Live daemon stopped"})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)})

    def serve_live_sweep_get_config(self):
        """GET /api/sweeps/live/config — return current live scanner configuration."""
        from live_daemon import load_live_config
        try:
            self.send_json(load_live_config())
        except Exception as e:
            self.send_json({"error": str(e)})

    def serve_live_sweep_save_config(self):
        """POST /api/sweeps/live/config — save live scanner configuration."""
        from live_daemon import save_live_config
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length else {}
            save_live_config(body)
            self.send_json({"ok": True})
        except Exception as e:
            self.send_json({"ok": False, "error": str(e)})

    # ------------------------------------------------------------------
    # Unified Daemon endpoints (consolidated price + sweep)
    # ------------------------------------------------------------------

    def serve_daemon_status(self):
        """GET /api/daemon/status — combined price + sweep status from unified daemon."""
        global _live_daemon, _daemon_intentionally_stopped
        if _live_daemon is None:
            from live_daemon import load_live_price_config as _llpc
            _pcfg = _llpc()
            self.send_json({
                "running": False, "connected": False,
                "flush_enabled": _pcfg.get("flush_enabled", True),
                "flush_watchlists": _pcfg.get("flush_watchlists", ["Priority"]),
                "price": {"messages_per_sec": 0, "tickers_active": 0,
                           "messages_received": 0, "last_flush_at": None},
                "sweep": {"trades_per_sec": 0, "sweeps_today": 0,
                           "tickers_active": 0, "sweeps_written": 0,
                           "events_detected": {"clusterbomb": 0, "rare_sweep": 0, "monster_sweep": 0},
                           "last_detection_at": None, "recent_sweeps": []},
                "watchdog_active": not _daemon_intentionally_stopped,
            })
            return
        full = _live_daemon.get_status()
        self.send_json({
            "running": full["running"],
            "connected": full["connected"],
            "authenticated": full["authenticated"],
            "subscribed": full["subscribed"],
            "started_at": full["started_at"],
            "uptime_seconds": full.get("uptime_seconds", 0),
            "reconnect_count": full["reconnect_count"],
            "error": full["error"],
            "watchdog_active": not _daemon_intentionally_stopped,
            "flush_enabled": full.get("flush_enabled", True),
            "flush_watchlists": full.get("flush_watchlists", ["Priority"]),
            "price": {
                "messages_per_sec": full["price_messages_per_sec"],
                "tickers_active": full["price_tickers_active"],
                "messages_received": full["price_messages_received"],
                "last_flush_at": full["price_last_flush_at"],
                "last_message_at": full["price_last_message_at"],
            },
            "sweep": {
                "trades_per_sec": full["trades_per_sec"],
                "sweeps_today": full["sweeps_today"],
                "tickers_active": full["sweep_tickers_active"],
                "sweeps_written": full["sweeps_written"],
                "sweeps_buffered": full.get("sweeps_buffered", 0),
                "events_detected": full["events_detected"],
                "last_detection_at": full["last_detection_at"],
                "last_sweep_flush_at": full["last_sweep_flush_at"],
                "recent_sweeps": full.get("recent_sweeps", []),
            },
        })

    def serve_daemon_save_config(self):
        """POST /api/daemon/config — save both price + sweep config, hot-reload on running daemon."""
        from live_daemon import (save_live_price_config, save_live_config,
                                 load_live_price_config, load_live_config)
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length else {}

            price_cfg = body.get("price", {})
            sweep_cfg = body.get("sweep", {})
            auto_start = body.get("auto_start", False)

            # Inject auto_start into both config sections
            price_cfg["auto_start"] = auto_start
            sweep_cfg["auto_start"] = auto_start

            # Save to respective config files
            # Merge with existing rather than overwrite
            existing_price = load_live_price_config()
            existing_price.update(price_cfg)
            save_live_price_config(existing_price)

            existing_sweep = load_live_config()
            existing_sweep.update(sweep_cfg)
            save_live_config(existing_sweep)

            # Hot-reload on running daemon
            changed = []
            if _live_daemon is not None and _live_daemon.get_status().get("running"):
                changed = _live_daemon.update_config(
                    price_cfg=existing_price, sweep_cfg=existing_sweep)

            self.send_json({"ok": True, "hot_reloaded": changed})
        except Exception as e:
            self.send_json({"ok": False, "error": str(e)})

    # ------------------------------------------------------------------
    # Live Price Feed endpoints (kept for backward compat)
    # ------------------------------------------------------------------

    def serve_live_price_status(self):
        """GET /api/scheduler/live-price/status — return price-focused status from unified daemon."""
        global _live_daemon
        if _live_daemon:
            self.send_json(_live_daemon.get_price_status())
        else:
            self.send_json({"running": False, "connected": False, "error": None})

    def serve_live_price_start(self):
        """POST /api/scheduler/live-price/start — start the unified live daemon."""
        self._start_unified_daemon()

    def serve_live_price_stop(self):
        """POST /api/scheduler/live-price/stop — stop the unified live daemon."""
        self._stop_unified_daemon()

    def serve_live_price_get_config(self):
        """GET /api/scheduler/live-price/config — return current live price feed config."""
        from live_daemon import load_live_price_config
        try:
            self.send_json(load_live_price_config())
        except Exception as e:
            self.send_json({"error": str(e)})

    def serve_live_price_save_config(self):
        """POST /api/scheduler/live-price/config — save live price feed config."""
        from live_daemon import save_live_price_config
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length else {}
            save_live_price_config(body)
            self.send_json({"ok": True})
        except Exception as e:
            self.send_json({"ok": False, "error": str(e)})

    # ------------------------------------------------------------------
    # EOD Compute endpoints
    # ------------------------------------------------------------------

    def serve_eod_compute_config(self):
        """GET /api/eod-compute/config — return current EOD compute config."""
        from live_daemon import load_eod_compute_config
        try:
            self.send_json(load_eod_compute_config())
        except Exception as e:
            self.send_json({"error": str(e)})

    def save_eod_compute_config_handler(self):
        """POST /api/eod-compute/config — save EOD compute config."""
        from live_daemon import save_eod_compute_config
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length else {}
            save_eod_compute_config(body)
            if _live_daemon is not None:
                try:
                    _live_daemon._eod_config = body
                except Exception:
                    pass
            self.send_json({"ok": True})
        except Exception as e:
            self.send_json({"ok": False, "error": str(e)})

    def serve_eod_compute_status(self):
        """GET /api/eod-compute/status — return last run status for RS and HVC."""
        global _live_daemon
        if _live_daemon:
            self.send_json(_live_daemon.get_eod_status())
        else:
            self.send_json({
                "rs": {"last_at": None, "last_result": None, "last_duration_s": 0,
                       "last_error": None, "last_date": None},
                "hvc": {"last_at": None, "last_result": None, "last_duration_s": 0,
                        "last_error": None, "last_date": None},
            })

    def trigger_eod_compute(self):
        """POST /api/eod-compute/trigger — manually trigger EOD compute now.
        Works with or without the live daemon running."""
        global _live_daemon
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length else {}
            compute_type = body.get("type", "both")

            if _live_daemon is not None:
                # Use daemon's methods (tracks status in daemon state)
                cfg = _live_daemon._eod_config or {}

                def _run_via_daemon():
                    if compute_type in ("rs", "both"):
                        _live_daemon._run_eod_rs(cfg.get("rs", {}))
                    if compute_type in ("hvc", "both"):
                        _live_daemon._run_eod_hvc(cfg.get("hvc", {}))

                t = threading.Thread(target=_run_via_daemon, daemon=True)
                t.start()
            else:
                # Daemon not running — run directly
                from live_daemon import load_eod_compute_config
                cfg = load_eod_compute_config()

                def _run_direct():
                    self._run_eod_direct(compute_type, cfg)

                t = threading.Thread(target=_run_direct, daemon=True)
                t.start()

            self.send_json({"ok": True, "message": f"EOD compute ({compute_type}) triggered"})
        except Exception as e:
            self.send_json({"ok": False, "error": str(e)})

    def _run_eod_direct(self, compute_type, cfg):
        """Run EOD compute directly without the live daemon."""
        import subprocess as sp

        if compute_type in ("rs", "both"):
            rs_cfg = cfg.get("rs", {})
            universe = rs_cfg.get("universe", "Russell3000")
            workers = rs_cfg.get("workers", 8)
            print(f"[EOD] Direct: Starting RS computation for {universe}...", flush=True)
            try:
                engine_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rs_engine.py")
                result = sp.run(
                    [sys.executable, engine_path, "--universe", universe,
                     "--workers", str(workers), "--once"],
                    capture_output=True, timeout=600,
                    cwd=os.path.dirname(engine_path),
                    encoding="utf-8", errors="replace"
                )
                if result.returncode == 0:
                    print(f"[EOD] Direct: RS computation complete", flush=True)
                else:
                    print(f"[EOD] Direct: RS FAILED: {(result.stderr or '')[-200:]}", flush=True)
            except Exception as e:
                print(f"[EOD] Direct: RS error: {e}", flush=True)

        if compute_type in ("hvc", "both"):
            hvc_cfg = cfg.get("hvc", {})
            watchlists = hvc_cfg.get("watchlists", ["Priority"])
            workers = hvc_cfg.get("workers", 8)
            print(f"[EOD] Direct: Starting HVC computation for {watchlists}...", flush=True)
            try:
                from config import WATCHLISTS
                from engine import compute_all
                from change_detector import init_db
                init_db()
                tickers = []
                seen = set()
                for wl_name in watchlists:
                    wl_groups = WATCHLISTS.get(wl_name)
                    if not wl_groups:
                        continue
                    for _gn, gt in wl_groups:
                        for display, _api, _at in gt:
                            if display not in seen:
                                seen.add(display)
                                tickers.append(display)
                if tickers:
                    results = compute_all(tickers, workers=workers)
                    ok = sum(1 for r in results if r.get("status") == "ok")
                    print(f"[EOD] Direct: HVC complete ({ok}/{len(tickers)} computed)", flush=True)
                else:
                    print(f"[EOD] Direct: No tickers found in watchlists", flush=True)
            except Exception as e:
                print(f"[EOD] Direct: HVC error: {e}", flush=True)

    # ------------------------------------------------------------------
    # Indicator Compute endpoints (subprocess-based)
    # ------------------------------------------------------------------

    def serve_indicator_compute_config(self):
        """GET /api/scheduler/indicator-compute/config"""
        from live_daemon import load_indicator_compute_config
        try:
            self.send_json(load_indicator_compute_config())
        except Exception as e:
            self.send_json({"error": str(e)})

    def serve_indicator_compute_save_config(self):
        """POST /api/scheduler/indicator-compute/config"""
        from live_daemon import save_indicator_compute_config
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length else {}
            save_indicator_compute_config(body)
            # Hot-reload on running daemon
            if _live_daemon is not None:
                try:
                    _live_daemon._indicator_compute_config = body
                except Exception:
                    pass
            self.send_json({"ok": True})
        except Exception as e:
            self.send_json({"ok": False, "error": str(e)})

    def serve_indicator_compute_status(self):
        """GET /api/scheduler/indicator-compute/status"""
        global _live_daemon
        if _live_daemon:
            with _live_daemon._lock:
                self.send_json({
                    "last_at": _live_daemon._status.get("compute_last_at"),
                    "last_result": _live_daemon._status.get("compute_last_result"),
                    "last_duration_s": _live_daemon._status.get("compute_last_duration_s", 0),
                    "last_error": _live_daemon._status.get("compute_last_error"),
                })
        else:
            self.send_json({
                "last_at": None, "last_result": None,
                "last_duration_s": 0, "last_error": None,
            })

    def serve_indicator_compute_trigger(self):
        """POST /api/scheduler/indicator-compute/trigger — run as subprocess."""
        global _live_daemon
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length else {}
            watchlists = body.get("watchlists", ["Priority"])
            workers = body.get("workers", 8)

            def _run():
                if _live_daemon is not None:
                    # Use daemon's method (tracks status)
                    _live_daemon._run_indicator_compute_subprocess()
                else:
                    # Run directly as subprocess
                    engine_path = os.path.join(
                        os.path.dirname(os.path.abspath(__file__)), "engine.py")
                    args = [sys.executable, engine_path, "--once",
                            "--workers", str(workers)]
                    for wl in watchlists:
                        args.extend(["--watchlist", wl])
                    print(f"[COMPUTE] Direct trigger: {watchlists} w={workers}",
                          flush=True)
                    try:
                        result = subprocess.run(
                            args, capture_output=True, timeout=600,
                            cwd=os.path.dirname(engine_path),
                            encoding="utf-8", errors="replace")
                        if result.returncode == 0:
                            print("[COMPUTE] Direct trigger complete", flush=True)
                        else:
                            print(f"[COMPUTE] Direct trigger FAILED: "
                                  f"{(result.stderr or '')[-200:]}", flush=True)
                    except Exception as e:
                        print(f"[COMPUTE] Direct trigger error: {e}", flush=True)

            t = threading.Thread(target=_run, daemon=True)
            t.start()
            self.send_json({"ok": True, "message": f"Indicator compute triggered for {watchlists}"})
        except Exception as e:
            self.send_json({"ok": False, "error": str(e)})

    # ------------------------------------------------------------------
    # Sweep Detection Config endpoints
    # ------------------------------------------------------------------

    def serve_sweep_detection_config(self):
        """GET /api/sweeps/detection-config — return current detection parameters."""
        from sweep_engine import get_detection_config
        try:
            config = get_detection_config()
            self.send_json(config)
        except Exception as e:
            self.send_json({"error": str(e)})

    def save_sweep_detection_config(self):
        """POST /api/sweeps/detection-config — save detection parameter overrides."""
        from sweep_engine import save_detection_config
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length else {}
            save_detection_config(body)
            self.send_json({"ok": True})
        except Exception as e:
            self.send_json({"ok": False, "error": str(e)})

    def serve_sweep_sectors(self):
        """GET /api/sweeps/sectors — return sector groups for filtering."""
        from sweep_engine import get_sectors
        try:
            self.send_json(get_sectors())
        except Exception as e:
            self.send_json({"error": str(e)})

    def serve_etf_categories(self):
        """GET /api/sweeps/etf-categories — return ETF category grouping."""
        from sweep_engine import get_etf_categories
        try:
            self.send_json(get_etf_categories())
        except Exception as e:
            self.send_json({"error": str(e)})

    def serve_ticker_names(self):
        """GET /api/ticker-names — return cached ticker name map {ticker: name}."""
        try:
            names = load_ticker_names()
            self.send_json(names)
        except Exception as e:
            self.send_json({"error": str(e)})

    def serve_portfolio_data(self):
        """GET /api/portfolio/data — fetch T212 portfolio, return sanitised % data only."""
        global _portfolio_cache
        try:
            # Return cache if fresh
            if _portfolio_cache["data"] and (time.time() - _portfolio_cache["ts"]) < _PORTFOLIO_CACHE_TTL:
                self.send_json(_portfolio_cache["data"])
                return

            from config import TRADING212_API_KEY
            if not TRADING212_API_KEY or TRADING212_API_KEY == "YOUR_T212_API_KEY_HERE":
                self.send_json({"error": "Trading 212 API key not configured"})
                return

            base = "https://live.trading212.com/api/v0"
            headers = _get_t212_headers()

            # Fetch account cash info
            cash_resp = requests.get(f"{base}/equity/account/cash", headers=headers, timeout=15)
            cash_resp.raise_for_status()
            cash_data = cash_resp.json()

            # Fetch open positions
            pos_resp = requests.get(f"{base}/equity/portfolio", headers=headers, timeout=15)
            pos_resp.raise_for_status()
            positions = pos_resp.json()

            # Compute total portfolio value
            total_value = float(cash_data.get("total", 0))
            free_cash = float(cash_data.get("free", 0))
            invested = float(cash_data.get("invested", 0))

            if total_value <= 0:
                self.send_json({"error": "Portfolio value is zero"})
                return

            cash_pct = round(free_cash / total_value * 100, 2)
            invested_pct = round(100 - cash_pct, 2)  # ensures cash + invested = 100%

            # Load ticker names from DB (persistent) + sweep engine fallback
            ticker_names = _get_portfolio_ticker_names()

            # Build sanitised position list (% only, no £ amounts)
            # Pass 1: collect positions with ppl (account currency) for weight calc
            raw_positions = []
            for p in positions:
                raw_ticker = p.get("ticker", "")
                # Clean: AAPL_US_EQ → AAPL, SGDXl_EQ → SGDX
                clean_ticker = raw_ticker.split("_")[0] if "_" in raw_ticker else raw_ticker
                # Strip trailing lowercase suffixes (T212 fractional indicators like 'l')
                clean_ticker = clean_ticker.rstrip("abcdefghijklmnopqrstuvwxyz") or clean_ticker

                avg_price = float(p.get("averagePrice") or 0)
                cur_price = float(p.get("currentPrice") or 0)
                ppl = float(p.get("ppl") or 0)         # P&L in account currency
                fx_ppl = float(p.get("fxPpl") or 0)     # FX P&L in account currency

                # Gain % — averagePrice and currentPrice are in same units (currency-agnostic)
                gain_pct = round((cur_price - avg_price) / avg_price * 100, 2) if avg_price > 0 else 0.0

                # Position current value in account currency = invested_portion + ppl
                # We derive invested_portion from: total_ppl contributes to (total - free - invested_leftover)
                # Simpler: position_value = (invested / n) adjusted by ppl share
                # Most reliable: each position's value in acct currency = its share of invested * (1 + gain)
                total_pnl = ppl + fx_ppl
                raw_positions.append({
                    "ticker": clean_ticker,
                    "name": ticker_names.get(clean_ticker, clean_ticker),
                    "gain": gain_pct,
                    "pnl": total_pnl,
                })

            # Pass 2: compute weights using ppl to derive position values
            # Each position's current value in account currency:
            #   value_i = cost_i + pnl_i, and sum(cost_i) = invested, sum(pnl_i) = total_pnl
            #   so sum(value_i) = invested + total_pnl = total - free
            # We can get cost_i from: cost_i = pnl_i / (gain_ratio) when gain != 0
            # But when gain is 0, cost_i is unknown from API alone.
            # Simplest robust approach: value_i = cost_i * (1 + gain_i/100)
            #   where cost_i = pnl_i / (gain_i/100) when gain != 0
            # Fallback for gain=0: distribute remaining invested equally
            total_invested_value = total_value - free_cash  # = invested + total pnl
            pos_list = []
            for rp in raw_positions:
                gain_ratio = rp["gain"] / 100.0
                if abs(gain_ratio) > 0.0001:
                    cost_i = rp["pnl"] / gain_ratio
                    value_i = cost_i + rp["pnl"]
                else:
                    # gain ≈ 0 → value ≈ cost, distribute invested evenly
                    value_i = invested / max(len(raw_positions), 1)

                weight_pct = round(value_i / total_value * 100, 2) if total_value > 0 else 0.0
                pos_list.append({
                    "ticker": rp["ticker"],
                    "name": rp["name"],
                    "weight": weight_pct,
                    "gain": rp["gain"],
                })

            # Sort by weight descending
            pos_list.sort(key=lambda x: x["weight"], reverse=True)

            # 1D performance — current value vs last trading day's close
            # Find most recent snapshot from a DIFFERENT date than today
            # (on Monday, this gets Friday/weekend snapshots which reflect Friday close)
            day_change_pct = None
            prev_close_date = None
            try:
                import sqlite3 as _sql3
                _init_portfolio_db()
                _pconn = _sql3.connect(DB_PATH, timeout=5)
                today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                # Get the latest snapshot from any previous day (prefer is_close, fallback any)
                prev_row = _pconn.execute(
                    "SELECT total_value, date FROM portfolio_snapshots "
                    "WHERE date < ? ORDER BY ts DESC LIMIT 1",
                    (today_str,)).fetchone()
                _pconn.close()
                if prev_row and prev_row[0] and prev_row[0] > 0:
                    # Skip if only the seed baseline exists (would show total gain, not 1D)
                    if prev_row[1] != "2026-03-01":
                        day_change_pct = round((total_value - prev_row[0]) / prev_row[0] * 100, 2)
                        prev_close_date = prev_row[1]
            except Exception:
                pass

            result = {
                "cashPct": cash_pct,
                "investedPct": invested_pct,
                "dayChangePct": day_change_pct,
                "prevCloseDate": prev_close_date,
                "positions": pos_list,
                "updatedAt": datetime.now(timezone.utc).isoformat() + "Z",
            }

            _portfolio_cache = {"data": result, "ts": time.time()}
            self.send_json(result)

            # Record daily snapshot (non-blocking, best-effort)
            try:
                _init_portfolio_db()
                _record_portfolio_snapshot(total_value)
            except Exception:
                pass  # don't break the main response

        except requests.exceptions.RequestException as e:
            self.send_json({"error": f"T212 API error: {e}"})
        except Exception as e:
            self.send_json({"error": str(e)})

    def serve_portfolio_history(self, query=None):
        """GET /api/portfolio/history — return indexed % snapshots for chart.
        ?range=1d|1w|1m|all (default all).
        1d: intraday hourly snapshots indexed from previous close (0%).
        Others: one point per day indexed from March 1 baseline."""
        try:
            _init_portfolio_db()
            import sqlite3
            conn = sqlite3.connect(DB_PATH, timeout=5)
            conn.row_factory = sqlite3.Row

            range_param = (query or {}).get("range", ["all"])[0] if query else "all"

            if range_param == "1d":
                # 1D: show intraday snapshots indexed from previous close
                now = datetime.now(timezone.utc)
                today_str = now.strftime("%Y-%m-%d")
                now_ts = now.strftime("%Y-%m-%dT%H:%M")

                # Find previous close value (latest snapshot from any day before today)
                prev_row = conn.execute(
                    "SELECT total_value, date FROM portfolio_snapshots "
                    "WHERE date < ? ORDER BY ts DESC LIMIT 1",
                    (today_str,)).fetchone()
                prev_val = prev_row["total_value"] if prev_row else None

                # Get today's snapshots up to current time only (no future/stale points)
                rows = conn.execute(
                    "SELECT ts, total_value FROM portfolio_snapshots "
                    "WHERE date = ? AND ts <= ? ORDER BY ts ASC",
                    (today_str, now_ts)).fetchall()
                conn.close()

                if not prev_val or prev_val <= 0:
                    self.send_json({"snapshots": [], "range": "1d"})
                    return

                snapshots = []
                for r in rows:
                    pct = round((r["total_value"] - prev_val) / prev_val * 100, 2)
                    # Show time portion: "2026-03-09T14:00" -> "14:00"
                    ts = r["ts"]
                    label = ts[11:16] if len(ts) >= 16 else ts
                    snapshots.append({"date": label, "pct": pct})

                self.send_json({"snapshots": snapshots, "range": "1d",
                                "prevClose": prev_row["date"] if prev_row else None})
                return

            # Non-1D ranges: one point per day
            date_filter = "2026-03-01"
            if range_param == "1w":
                date_filter = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
            elif range_param == "1m":
                date_filter = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")

            rows = conn.execute(
                "SELECT date, total_pct, ts FROM portfolio_snapshots "
                "WHERE date >= ? ORDER BY ts ASC",
                (date_filter,)).fetchall()
            conn.close()

            # Deduplicate to latest snapshot per day
            day_map = {}
            for r in rows:
                day_map[r["date"]] = r["total_pct"]

            snapshots = [{"date": d, "pct": p} for d, p in sorted(day_map.items())]
            self.send_json({"snapshots": snapshots})
        except Exception as e:
            self.send_json({"error": str(e)})

    def serve_portfolio_trades(self):
        """GET /api/portfolio/trades — read trades from DB, apply cached current prices.
        Separates open vs closed positions. Returns sanitised trades with % gain only."""
        global _portfolio_trades_resp_cache
        try:
            # Return assembled response cache if fresh (30s)
            if _portfolio_trades_resp_cache["data"] and (time.time() - _portfolio_trades_resp_cache["ts"]) < _PORTFOLIO_TRADES_RESP_TTL:
                self.send_json(_portfolio_trades_resp_cache["data"])
                return

            import sqlite3
            _init_portfolio_db()

            # If DB is empty, trigger a full sync (one-time backfill)
            conn = sqlite3.connect(DB_PATH, timeout=5)
            conn.row_factory = sqlite3.Row
            count = conn.execute("SELECT COUNT(*) FROM portfolio_trades").fetchone()[0]
            conn.close()
            if count == 0:
                try:
                    _sync_portfolio_trades(full=True)
                except Exception as e:
                    print(f"[PORTFOLIO] Initial trade sync failed: {e}", flush=True)

            # Get current prices (cached, 2-min TTL, single API call)
            pc = _refresh_portfolio_prices()
            cur_prices = pc["prices"]
            open_raw = pc["open_raw"]

            # Read all trades from DB
            conn = sqlite3.connect(DB_PATH, timeout=5)
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT date, ticker, raw_ticker, name, side, avg_fill, total_qty, "
                "num_fills, rpnl, net_val FROM portfolio_trades ORDER BY date DESC"
            ).fetchall()
            conn.close()

            # Load ticker names for any gaps
            ticker_names = _get_portfolio_ticker_names()

            # Build open/closed trade lists
            open_trades = []
            closed_trades = []
            for r in rows:
                ticker = r["ticker"]
                raw_t = r["raw_ticker"]
                name = r["name"] or ticker_names.get(ticker, ticker)
                side = r["side"]
                avg_fill = r["avg_fill"]
                num_fills = r["num_fills"]

                if side == "Buy":
                    is_open = raw_t in open_raw
                    if is_open and raw_t in cur_prices:
                        cur_p = cur_prices[raw_t]["currentPrice"]
                        gain_pct = round((cur_p - avg_fill) / avg_fill * 100, 2) if avg_fill > 0 else 0.0
                    else:
                        gain_pct = None
                    trade = {"date": r["date"], "ticker": ticker, "name": name,
                             "side": side, "gain": gain_pct, "fills": num_fills}
                    if is_open:
                        open_trades.append(trade)
                    else:
                        closed_trades.append(trade)
                else:
                    total_rpnl = r["rpnl"]
                    total_net = r["net_val"]
                    total_cost = total_net - total_rpnl
                    gain_pct = round(total_rpnl / total_cost * 100, 2) if total_cost > 0 else None
                    trade = {"date": r["date"], "ticker": ticker, "name": name,
                             "side": side, "gain": gain_pct, "fills": num_fills}
                    closed_trades.append(trade)

            result = {"open": open_trades, "closed": closed_trades}
            _portfolio_trades_resp_cache = {"data": result, "ts": time.time()}
            self.send_json(result)

        except Exception as e:
            self.send_json({"error": str(e)})

    def serve_analysis_river(self, query=None):
        """GET /api/analysis/river — aggregated sweep data for river/stream chart."""
        from sweep_engine import get_river_data
        try:
            granularity = (query or {}).get("granularity", ["week"])[0]
            date_from = (query or {}).get("date_from", [None])[0]
            date_to = (query or {}).get("date_to", [None])[0]
            metric = (query or {}).get("metric", ["notional"])[0]
            min_notional = float((query or {}).get("min_notional", ["0"])[0])
            # Optional watchlist filter → resolve to ticker set
            wl_name = (query or {}).get("watchlist", [None])[0]
            ticker_filter = None
            if wl_name:
                reload_watchlists()
                groups = WATCHLISTS.get(wl_name)
                if groups:
                    ticker_filter = set()
                    for _gname, tickers in groups:
                        for _d, api_ticker, _t in tickers:
                            ticker_filter.add(api_ticker)
            data = get_river_data(granularity=granularity, date_from=date_from,
                                  date_to=date_to, metric=metric,
                                  min_notional=min_notional,
                                  ticker_filter=ticker_filter)
            self.send_json(data)
        except Exception as e:
            self.send_json({"data": [], "sectors": [], "error": str(e)})

    def serve_analysis_heatmap(self, query=None):
        """GET /api/analysis/heatmap — daily aggregated data for calendar heatmap."""
        from sweep_engine import get_heatmap_data
        try:
            year = int((query or {}).get("year", ["2026"])[0])
            metric = (query or {}).get("metric", ["notional"])[0]
            event_type = (query or {}).get("event_type", ["all"])[0]
            data = get_heatmap_data(year=year, metric=metric, event_type=event_type)
            self.send_json(data)
        except Exception as e:
            self.send_json({"data": [], "error": str(e)})

    def serve_sweep_redetect(self):
        """POST /api/sweeps/redetect — re-run clusterbomb detection.
        Accepts optional 'profile' param: 'stock' (default), 'etf', or 'both'.
        Accepts optional 'new_only' param: if true, only detect for tickers
        that have trade data but no existing events (skip already-detected).
        Only wipes and rebuilds events for the requested profile."""
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length else {}

            from sweep_engine import _get_db, get_detection_config as _get_cfg, load_etf_set
            from sweep_engine import detect_rare_sweep_days, detect_monster_sweeps, detect_ranked_sweeps, detect_ranked_daily
            from sweep_engine import get_undetected_tickers
            cfg = _get_cfg()
            profile = body.get("profile", "stock")  # 'stock', 'etf', or 'both'
            new_only = body.get("new_only", False)

            if new_only:
                # --- New-only mode: detect only undetected tickers, no delete ---
                mode_label = "new only"

                events = []
                rare_sweep_events = []
                monster_events = []
                stock_daily = {"updated": 0, "inserted": 0}
                stock_ranked = {"updated": 0, "inserted": 0}
                etf_rare = []
                etf_daily = {"updated": 0, "inserted": 0}
                etf_single = {"updated": 0, "inserted": 0}

                # --- Stock detection (new tickers only) ---
                if profile in ("stock", "both"):
                    stock_tickers = get_undetected_tickers(etf_only=False, exclude_etfs=True)
                    if stock_tickers:
                        stock_params = body.get("stock", body) if body else cfg["stock"]
                        events = detect_clusterbombs(
                            tickers=stock_tickers,
                            min_sweeps=int(stock_params.get("min_sweeps", 3)),
                            min_notional=float(stock_params.get("min_notional", 1_000_000)),
                            min_total=float(stock_params.get("min_total", 10_000_000)),
                            rarity_days=int(stock_params.get("rarity_days", 20)),
                            rare_min_notional=float(stock_params.get("rare_min_notional", 1_000_000)),
                        )
                        rare_sweep_events = detect_rare_sweep_days(
                            min_notional=float(stock_params.get("rare_min_notional", 1_000_000)),
                            rarity_days=int(stock_params.get("rarity_days", 20)),
                            tickers=stock_tickers,
                        )
                        _sm = stock_params.get("monster_min_notional")
                        if _sm:
                            monster_events = detect_monster_sweeps(
                                monster_min_notional=float(_sm),
                                tickers=stock_tickers,
                            )
                        # Stock ranking (same model as ETFs)
                        stock_daily = detect_ranked_daily(
                            rank_limit=100,
                            min_sweeps=int(stock_params.get("min_sweeps_daily", 1)),
                            tickers=stock_tickers,
                            exclude_etfs=True, etf_only=False,
                        )
                        stock_ranked = detect_ranked_sweeps(
                            rank_limit=100,
                            tickers=stock_tickers,
                            exclude_etfs=True, etf_only=False,
                        )
                    else:
                        print("[Redetect] No new stock tickers to detect.", flush=True)

                # --- ETF detection (new tickers only) ---
                if profile in ("etf", "both"):
                    etf_tickers = get_undetected_tickers(etf_only=True, exclude_etfs=False)
                    if etf_tickers:
                        etf_params = body.get("etf") if body else None
                        if not etf_params:
                            etf_params = cfg.get("etf", {})
                        etf_daily = detect_ranked_daily(
                            rank_limit=100, min_sweeps=1,
                            tickers=etf_tickers,
                            exclude_etfs=False, etf_only=True,
                        )
                        _rare_not = float((etf_params or {}).get("rare_min_notional", 1_000_000))
                        _rare_days = int((etf_params or {}).get("rarity_days", 20))
                        etf_rare = detect_rare_sweep_days(
                            min_notional=_rare_not, rarity_days=_rare_days,
                            tickers=etf_tickers,
                            exclude_etfs=False, etf_only=True,
                        )
                        etf_single = detect_ranked_sweeps(
                            rank_limit=100,
                            tickers=etf_tickers,
                            exclude_etfs=False, etf_only=True,
                        )
                    else:
                        print("[Redetect] No new ETF tickers to detect.", flush=True)

            else:
                # --- Full redetect mode: wipe + rebuild ---
                mode_label = "full"

                # Get all tickers with sweep data
                conn = _get_db()
                all_tickers = [r[0] for r in conn.execute(
                    "SELECT DISTINCT ticker FROM sweep_trades WHERE is_darkpool=1 AND is_sweep=1"
                ).fetchall()]

                # Profile-aware delete: only wipe events for the relevant tickers
                etf_set = load_etf_set()
                if profile == "both":
                    conn.execute("DELETE FROM clusterbomb_events")
                    print(f"[Redetect] Wiped ALL events. {len(all_tickers)} tickers to process.", flush=True)
                elif profile == "etf":
                    etf_in = ",".join(f"'{t}'" for t in all_tickers if t in etf_set)
                    if etf_in:
                        conn.execute(f"DELETE FROM clusterbomb_events WHERE ticker IN ({etf_in})")
                    etf_count = sum(1 for t in all_tickers if t in etf_set)
                    print(f"[Redetect] Wiped ETF events. {etf_count} ETF tickers to process.", flush=True)
                else:  # stock
                    stock_in = ",".join(f"'{t}'" for t in all_tickers if t not in etf_set)
                    if stock_in:
                        conn.execute(f"DELETE FROM clusterbomb_events WHERE ticker IN ({stock_in})")
                    stock_count = sum(1 for t in all_tickers if t not in etf_set)
                    print(f"[Redetect] Wiped stock events. {stock_count} stock tickers to process.", flush=True)
                conn.commit()
                conn.close()

                events = []
                rare_sweep_events = []
                monster_events = []
                stock_daily = {"updated": 0, "inserted": 0}
                stock_ranked = {"updated": 0, "inserted": 0}
                etf_rare = []
                etf_daily = {"updated": 0, "inserted": 0}
                etf_single = {"updated": 0, "inserted": 0}

                # --- Stock detection ---
                if profile in ("stock", "both"):
                    print("[Redetect] Starting stock detection...", flush=True)
                    stock_params = body.get("stock", body) if body else cfg["stock"]
                    events = detect_clusterbombs(
                        tickers=all_tickers,
                        min_sweeps=int(stock_params.get("min_sweeps", 3)),
                        min_notional=float(stock_params.get("min_notional", 1_000_000)),
                        min_total=float(stock_params.get("min_total", 10_000_000)),
                        rarity_days=int(stock_params.get("rarity_days", 20)),
                        rare_min_notional=float(stock_params.get("rare_min_notional", 1_000_000)),
                    )
                    rare_sweep_events = detect_rare_sweep_days(
                        min_notional=float(stock_params.get("rare_min_notional", 1_000_000)),
                        rarity_days=int(stock_params.get("rarity_days", 20)),
                        tickers=all_tickers,
                    )
                    _sm = stock_params.get("monster_min_notional")
                    if _sm:
                        monster_events = detect_monster_sweeps(
                            monster_min_notional=float(_sm),
                            tickers=all_tickers,
                        )
                    # Stock ranking (same model as ETFs)
                    stock_daily = detect_ranked_daily(
                        rank_limit=100,
                        min_sweeps=int(stock_params.get("min_sweeps_daily", 1)),
                        tickers=all_tickers,
                        exclude_etfs=True, etf_only=False,
                    )
                    stock_ranked = detect_ranked_sweeps(
                        rank_limit=100,
                        tickers=all_tickers,
                        exclude_etfs=True, etf_only=False,
                    )

                # --- ETF detection ---
                if profile in ("etf", "both"):
                    etf_params = body.get("etf") if body else None
                    if not etf_params:
                        etf_params = cfg.get("etf", {})
                    print(f"[Redetect] ETF daily ranked (rank_limit=100, min_sweeps=1)...", flush=True)
                    etf_daily = detect_ranked_daily(
                        rank_limit=100, min_sweeps=1,
                        tickers=all_tickers,
                        exclude_etfs=False, etf_only=True,
                    )
                    print(f"[Redetect] ETF daily ranked done: {etf_daily}", flush=True)
                    _rare_not = float((etf_params or {}).get("rare_min_notional", 1_000_000))
                    _rare_days = int((etf_params or {}).get("rarity_days", 20))
                    print(f"[Redetect] ETF rare sweeps (rarity_days={_rare_days}, min_notional=${_rare_not:,.0f})...", flush=True)
                    etf_rare = detect_rare_sweep_days(
                        min_notional=_rare_not, rarity_days=_rare_days,
                        tickers=all_tickers,
                        exclude_etfs=False, etf_only=True,
                    )
                    print(f"[Redetect] ETF rare sweeps done: {len(etf_rare)} events", flush=True)
                    print(f"[Redetect] ETF single ranked (rank_limit=100)...", flush=True)
                    etf_single = detect_ranked_sweeps(
                        rank_limit=100,
                        tickers=all_tickers,
                        exclude_etfs=False, etf_only=True,
                    )
                    print(f"[Redetect] ETF single ranked done: {etf_single}", flush=True)

            _tracker_cache.clear()  # invalidate tracker cache after redetect

            rare_cb_count = sum(1 for e in events if e.get("is_rare"))
            total_rare = len(rare_sweep_events) + len(etf_rare)
            stock_daily_total = (stock_daily.get("updated", 0) + stock_daily.get("inserted", 0))
            stock_ranked_total = (stock_ranked.get("updated", 0) + stock_ranked.get("inserted", 0))
            etf_daily_total = (etf_daily.get("updated", 0) + etf_daily.get("inserted", 0))
            etf_single_total = (etf_single.get("updated", 0) + etf_single.get("inserted", 0))
            self.send_json({
                "ok": True,
                "profile": profile,
                "new_only": new_only,
                "events_detected": len(events),
                "rare_cb_count": rare_cb_count,
                "rare_sweep_days": total_rare,
                "monster_sweeps": len(monster_events),
                "stock_daily": stock_daily_total,
                "stock_ranked": stock_ranked_total,
                "etf_rare": len(etf_rare),
                "etf_daily": etf_daily_total,
                "etf_single": etf_single_total,
                "message": f"Re-detected ({profile}, {mode_label}): {len(events)} stock CBs, "
                           f"{total_rare} rare sweep days, {len(monster_events)} stock monsters, "
                           f"Stock ranked: {stock_daily_total} daily + {stock_ranked_total} single, "
                           f"ETF: {etf_daily_total} daily + {etf_single_total} single ranked",
            })
        except Exception as e:
            self.send_json({"ok": False, "error": str(e)})

    def serve_rebuild_cache(self):
        """POST /api/sweeps/rebuild-cache — rebuild materialised stats + daily summary."""
        try:
            print("[SWEEP] Rebuilding stats cache + daily summary...", flush=True)
            rebuild_stats_cache()
            rebuild_daily_summary()
            self.send_json({"ok": True, "message": "Stats cache and daily summary rebuilt."})
        except Exception as e:
            self.send_json({"ok": False, "error": str(e)})

    def _get_watchlist_tickers(self, wl_name):
        """Build a set of display tickers belonging to the given watchlist.
        Returns None if wl_name is empty (= all tickers, no filter)."""
        if not wl_name:
            return None
        reload_watchlists()
        ticker_set = set()
        for group_name, tickers in WATCHLISTS.get(wl_name, []):
            for display, api, atype in tickers:
                ticker_set.add(display)
        return ticker_set if ticker_set else None

    def serve_hvc_today(self, query=None):
        """GET /api/hvc/today — today's HVC events enriched with current prices."""
        try:
            date_str = (query or {}).get("date", [None])[0]
            wl_name = (query or {}).get("watchlist", [None])[0]
            wl_tickers = self._get_watchlist_tickers(wl_name)

            events = get_hvc_events_today(date_str)

            # Filter by watchlist if specified
            if wl_tickers is not None:
                events = [ev for ev in events if ev.get("ticker") in wl_tickers]

            # Enrich with current price from meta_snapshots
            state = get_latest_full_state(div_adj=0)
            for ev in events:
                ticker = ev.get("ticker", "")
                meta = state.get(ticker, {}).get("_meta", {})
                ev["current_price"] = meta.get("price")
                if ev.get("close_price") and meta.get("price"):
                    try:
                        ev["price_change_pct"] = round(
                            ((meta["price"] - ev["close_price"]) / ev["close_price"]) * 100, 2
                        )
                    except (TypeError, ZeroDivisionError):
                        ev["price_change_pct"] = None
                else:
                    ev["price_change_pct"] = None

            self.send_json({"events": events, "count": len(events)})
        except Exception as e:
            import traceback; traceback.print_exc()
            self.send_json({"events": [], "count": 0, "error": str(e)})

    def serve_hvc_history(self, query=None):
        """GET /api/hvc/history — HVC summary + raw events for a period."""
        try:
            period = (query or {}).get("period", ["week"])[0]
            wl_name = (query or {}).get("watchlist", [None])[0]
            wl_tickers = self._get_watchlist_tickers(wl_name)

            from datetime import date as _date
            today = _date.today()
            # "last_session" = last trading day (skip weekends)
            # "this_week" = since Monday of current calendar week
            # "last_week" = last 5 trading sessions (~7 calendar days)
            if period == "today":
                since = today.isoformat()
            elif period == "last_session":
                # Go back to previous trading day
                d = today - timedelta(days=1)
                while d.weekday() >= 5:  # skip Sat(5), Sun(6)
                    d -= timedelta(days=1)
                since = d.isoformat()
            elif period == "this_week":
                # Monday of the current calendar week
                d = today - timedelta(days=today.weekday())  # weekday()=0 is Mon
                since = d.isoformat()
            elif period == "last_week":
                since = (today - timedelta(weeks=1)).isoformat()
            elif period == "week":
                since = (today - timedelta(weeks=1)).isoformat()
            elif period == "month":
                since = (today - timedelta(days=30)).isoformat()
            elif period == "quarter":
                since = (today - timedelta(days=90)).isoformat()
            else:
                since = (today - timedelta(weeks=1)).isoformat()

            summary_dict = get_hvc_ticker_summary(since)
            raw_events = get_hvc_events_history(since)

            # Filter by watchlist if specified
            if wl_tickers is not None:
                summary_dict = {tk: v for tk, v in summary_dict.items() if tk in wl_tickers}
                raw_events = [ev for ev in raw_events if ev.get("ticker") in wl_tickers]

            # Convert summary dict to list and enrich with current price
            state = get_latest_full_state(div_adj=0)
            summary_list = []
            for ticker, info in summary_dict.items():
                item = {"ticker": ticker, **info}
                meta = state.get(ticker, {}).get("_meta", {})
                item["current_price"] = meta.get("price")
                first_price = item.get("first_price")
                if first_price and meta.get("price"):
                    try:
                        item["price_change_pct"] = round(
                            ((meta["price"] - first_price) / first_price) * 100, 2
                        )
                    except (TypeError, ZeroDivisionError):
                        item["price_change_pct"] = None
                else:
                    item["price_change_pct"] = None
                summary_list.append(item)

            # Sort by count descending
            summary_list.sort(key=lambda x: x.get("count", 0), reverse=True)

            self.send_json({
                "summary": summary_list,
                "events": raw_events,
                "period": period,
                "since": since,
            })
        except Exception as e:
            import traceback; traceback.print_exc()
            self.send_json({"summary": [], "events": [], "error": str(e)})

    def save_watchlists(self):
        """Save watchlist data to individual watchlists/*.py files (hot-reload, no restart)."""
        try:
            import re as _re
            length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(length).decode('utf-8')
            wl_data = json.loads(body)

            wl_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "watchlists")
            os.makedirs(wl_dir, exist_ok=True)

            saved_filenames = set()

            for wl in wl_data:
                # Derive filename: "Priority" -> "priority_watchlist.py"
                raw_name = _re.sub(r'[^a-z0-9_]', '', wl['name'].lower().replace(' ', '_').replace('-', '_'))
                fname = f"{raw_name}_watchlist.py"
                saved_filenames.add(fname)

                # Derive variable name: "priority" -> "PRIORITY_GROUPS"
                var_name = raw_name.upper() + "_GROUPS"

                lines = []
                lines.append('"""')
                lines.append(f'{wl["name"]} watchlist.')
                lines.append('Edited via TBD Technologies watchlist manager.')
                lines.append('"""')
                lines.append('')
                lines.append(f'DISPLAY_NAME = "{wl["name"]}"')
                lines.append('')
                lines.append(f'{var_name} = [')
                for grp in wl.get('groups', []):
                    lines.append(f'    ("{grp["name"]}", [')
                    for tk in grp.get('tickers', []):
                        d = tk.get('display', '')
                        a = tk.get('api', d)
                        t = tk.get('type', 'stock')
                        lines.append(f'        ("{d}", "{a}", "{t}"),')
                    lines.append('    ]),')
                lines.append(']')

                fpath = os.path.join(wl_dir, fname)
                with open(fpath, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(lines) + '\n')

            # Remove watchlist files that were deleted in the UI
            for existing in os.listdir(wl_dir):
                if existing.endswith('_watchlist.py') and existing not in saved_filenames:
                    os.remove(os.path.join(wl_dir, existing))

            # Save display ordering
            order = [wl['name'] for wl in wl_data]
            order_path = os.path.join(wl_dir, "order.json")
            with open(order_path, 'w', encoding='utf-8') as f:
                json.dump(order, f)

            # Hot-reload so changes apply immediately (dashboard, scheduler, etc.)
            reload_watchlists()

            self.send_json({"ok": True})
        except Exception as e:
            import traceback; traceback.print_exc()
            self.send_json({"ok": False, "error": str(e)})
    
    def serve_search(self):
        """Handle POST /api/search — multi-criteria screener.

        Payload format:
            {
                criteria: [{indicator, timeframes: [], value, join: 'if'|'and'|'or'}],
                adj: 0|1,
                watchlist: ''|'SP500'|etc
            }

        Logic:
            - First row always has join='if' (acts as starting condition)
            - 'and' rows: ticker must also match this row (intersection)
            - 'or' rows: ticker can match this row instead (union)
            - Within a single row: multiple timeframes = OR (match any)

        Evaluated as groups: consecutive 'and' rows form a group.
        Groups are joined by 'or'. E.g.:
            IF sqz=orange 1W   → group 1 start
            AND mom=aqua 1D    → group 1 continued
            OR sqz=red 1D      → group 2 start (new OR branch)

        Ticker passes if it matches ANY group (all rows within that group).
        """
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length else {}
            criteria = body.get("criteria", [])
            div_adj = int(body.get("adj", 0))
            wl_name = body.get("watchlist", "")

            # Determine ticker universe
            reload_watchlists()
            if wl_name:
                ticker_set = set()
                for group_name, tickers in WATCHLISTS.get(wl_name, []):
                    for display, api, atype in tickers:
                        ticker_set.add(display)
            else:
                ticker_set = None  # all cached

            state = get_latest_full_state(div_adj=div_adj, tickers=list(ticker_set) if ticker_set else None)
            total = len(state)

            # Parse criteria into AND-groups joined by OR
            # Each group is a list of criterion dicts that must ALL match (AND).
            # Groups are combined with OR (ticker matches if ANY group matches).
            and_groups = []
            current_group = []
            for c in criteria:
                val = c.get("value", "")
                if not val:
                    continue
                join = c.get("join", "and")
                if join == "or" and current_group:
                    # Start a new OR branch — save current group, begin new one
                    and_groups.append(current_group)
                    current_group = []
                current_group.append({
                    "indicator": c.get("indicator", ""),
                    "value": val,
                    "timeframes": c.get("timeframes", []),
                })
            if current_group:
                and_groups.append(current_group)

            # Pre-compute HVC data if any criteria use hvc_triggered
            hvc_sets = {}
            hvc_summaries = {}  # {period: {ticker: summary_data}} for compound filtering
            needs_hvc = any(c.get("indicator") == "hvc_triggered" for c in criteria if c.get("value"))
            if needs_hvc:
                from datetime import date as _date
                _today = _date.today()
                period_map = {
                    "today": _today.isoformat(),
                    "week": (_today - timedelta(weeks=1)).isoformat(),
                    "month": (_today - timedelta(days=30)).isoformat(),
                    "quarter": (_today - timedelta(days=90)).isoformat(),
                }
                for period_val, since_date in period_map.items():
                    summary = get_hvc_ticker_summary(since_date)
                    hvc_sets[period_val] = set(summary.keys())
                    hvc_summaries[period_val] = summary

            # Pre-load RS data if any criteria use rs_rating/rs_new_high/monster_score
            rs_data = {}
            needs_rs = any(c.get("indicator") in ("rs_rating", "rs_new_high", "monster_score")
                           for c in criteria if c.get("value"))
            if needs_rs:
                from change_detector import _get_db
                _conn_rs = _get_db()
                _cur_rs = _conn_rs.cursor()
                _cur_rs.execute("SELECT ticker, rs_rating, rs_new_high, monster_score FROM rs_rankings")
                for _row in _cur_rs.fetchall():
                    rs_data[_row[0]] = {"rs_rating": _row[1], "rs_new_high": int(_row[2] or 0),
                                        "monster_score": _row[3]}
                _conn_rs.close()

            # Pre-load sweep event data if any criteria use has_sweep
            sweep_sets = {}  # {period: {ticker: {event_type: count}}}
            needs_sweep = any(c.get("indicator") == "has_sweep" for c in criteria if c.get("value"))
            if needs_sweep:
                from change_detector import _get_db
                from datetime import date as _date2
                _today2 = _date2.today()
                _sweep_periods = {
                    "week": (_today2 - timedelta(weeks=1)).isoformat(),
                    "month": (_today2 - timedelta(days=30)).isoformat(),
                    "quarter": (_today2 - timedelta(days=90)).isoformat(),
                    "half_year": (_today2 - timedelta(days=182)).isoformat(),
                    "year": (_today2 - timedelta(days=365)).isoformat(),
                }
                _conn_sw = _get_db()
                _cur_sw = _conn_sw.cursor()
                for _sp_val, _sp_since in _sweep_periods.items():
                    _cur_sw.execute("""
                        SELECT ticker, COALESCE(event_type, 'clusterbomb'), COUNT(*)
                        FROM clusterbomb_events WHERE event_date >= ?
                        GROUP BY ticker, COALESCE(event_type, 'clusterbomb')
                    """, (_sp_since,))
                    sweep_sets[_sp_val] = {}
                    for _row in _cur_sw.fetchall():
                        _tk, _et, _cnt = _row
                        if _tk not in sweep_sets[_sp_val]:
                            sweep_sets[_sp_val][_tk] = {}
                        sweep_sets[_sp_val][_tk][_et] = _cnt
                _conn_sw.close()

            # Filter tickers: match ANY and_group (OR between groups)
            matches = {}
            for ticker, tdata in state.items():
                if self._ticker_matches_or_groups(ticker, tdata, and_groups, hvc_sets, hvc_summaries,
                                                   state, rs_data, sweep_sets):
                    matches[ticker] = tdata

            # Enrich matched tickers with HVC, sweep, RS data
            enrichment = self._get_screener_enrichment(list(matches.keys()))
            for tk in matches:
                matches[tk]['_enrichment'] = enrichment.get(tk, {})

            self.send_json({"matches": matches, "total": total})
        except Exception as e:
            import traceback
            traceback.print_exc()
            self.send_json({"matches": {}, "total": 0, "error": str(e)})

    def _ticker_matches_or_groups(self, ticker, tdata, and_groups, hvc_sets=None, hvc_summaries=None,
                                   state=None, rs_data=None, sweep_sets=None):
        """Check if ticker matches ANY of the AND-groups (OR between groups).
        Within each group, ALL criteria must match (AND between rows).
        Within each criterion, ANY timeframe can match (OR across TFs)."""
        if not and_groups:
            return False
        for group in and_groups:
            if self._ticker_matches_and_group(ticker, tdata, group, hvc_sets, hvc_summaries,
                                               state, rs_data, sweep_sets):
                return True
        return False

    def _ticker_matches_and_group(self, ticker, tdata, group, hvc_sets=None, hvc_summaries=None,
                                    state=None, rs_data=None, sweep_sets=None):
        """Check if ticker matches ALL criteria in this AND-group."""
        meta = tdata.get("_meta", {})
        for criterion in group:
            ind = criterion["indicator"]
            val = criterion["value"]
            timeframes = criterion["timeframes"]

            # RS Rating filter (e.g. val="gte_90" or "lte_20")
            if ind == "rs_rating":
                if not rs_data or ticker not in rs_data:
                    return False
                rs_r = rs_data[ticker].get("rs_rating", 0) or 0
                parts = val.split("_")
                if len(parts) == 2:
                    if parts[0] == "gte" and rs_r < int(parts[1]):
                        return False
                    elif parts[0] == "lte" and rs_r > int(parts[1]):
                        return False
                continue

            # RS New High filter
            if ind == "rs_new_high":
                if not rs_data or ticker not in rs_data:
                    return False
                if val == "true" and not rs_data[ticker].get("rs_new_high"):
                    return False
                continue

            # Monster Score filter (e.g. val="gte_60")
            if ind == "monster_score":
                if not rs_data or ticker not in rs_data:
                    return False
                ms = rs_data[ticker].get("monster_score", 0) or 0
                parts = val.split("_")
                if len(parts) == 2 and parts[0] == "gte" and ms < int(parts[1]):
                    return False
                continue

            # Sweep activity filter (compound: "period|type|count")
            if ind == "has_sweep":
                parts = val.split("|")
                period_val = parts[0] if parts else "month"
                if not sweep_sets or period_val not in sweep_sets:
                    return False
                tk_sweeps = sweep_sets[period_val].get(ticker, {})
                if not tk_sweeps:
                    return False
                # Type sub-filter
                if len(parts) > 1 and parts[1]:
                    if parts[1] not in tk_sweeps:
                        return False
                # Count sub-filter
                if len(parts) > 2 and parts[2]:
                    total_events = sum(tk_sweeps.values())
                    if parts[2] == "multi" and total_events < 2:
                        return False
                    elif parts[2] == "many" and total_events < 5:
                        return False
                continue

            # Meta-level indicators (no timeframe)
            if ind == "above_wma30":
                actual = meta.get("above_wma30")
                if val == "true" and actual is not True:
                    return False
                if val == "false" and actual is not False:
                    return False
                continue
            if ind == "wma30_cross":
                if meta.get("wma30_cross") != val:
                    return False
                continue
            if ind == "hvc_triggered":
                # Compound HVC filter: value can be "period" or "period|dir|gap"
                # e.g. "month", "month|positive", "quarter|positive|has_gap"
                parts = val.split("|")
                period_val = parts[0] if parts else "month"
                # Check basic period membership
                if not hvc_sets or period_val not in hvc_sets:
                    return False
                if ticker not in hvc_sets[period_val]:
                    return False
                # Compound sub-filters
                if len(parts) > 1 and hvc_summaries and period_val in hvc_summaries:
                    tk_summary = hvc_summaries[period_val].get(ticker)
                    if not tk_summary:
                        return False
                    # Enrich with current price for direction check
                    current_price = (state or {}).get(ticker, {}).get("_meta", {}).get("price")
                    first_price = tk_summary.get("first_price")
                    for sub in parts[1:]:
                        if sub == "positive":
                            if not (current_price and first_price and current_price > first_price):
                                return False
                        elif sub == "negative":
                            if not (current_price and first_price and current_price < first_price):
                                return False
                        elif sub == "has_gap":
                            if tk_summary.get("gap_ups", 0) < 1:
                                return False
                        elif sub == "has_open_gap":
                            if tk_summary.get("gaps_open", 0) < 1:
                                return False
                        elif sub == "multi_hvc":
                            if tk_summary.get("count", 0) < 2:
                                return False
                        elif sub == "bull":
                            # At least one bull HVC — check via events
                            pass  # would need event-level data, skip for now
                continue

            # Timeframe-level indicators — OR across timeframes in this row
            if not timeframes:
                return False  # no TFs selected for a TF-dependent indicator
            any_tf_match = False
            for tf in timeframes:
                tf_data = tdata.get(tf)
                if not tf_data:
                    continue

                if ind == "band_flip":
                    if val == "true" and tf_data.get("band_flip"):
                        any_tf_match = True
                        break
                    continue

                actual = tf_data.get(ind, "")
                if str(actual) == str(val):
                    any_tf_match = True
                    break

            if not any_tf_match:
                return False
        return True

    def _get_screener_enrichment(self, tickers):
        """Batch-fetch HVC, sweep, and RS enrichment for screener results.

        Returns dict: {ticker: {rs: {...}, hvc: [1w,1m,3m,6m,1y], sw: {cb:[...], rare:[...], mon:[...]}}}
        Arrays are ordered [1w, 1m, 3m, 6m, 1y] for period counts.
        """
        if not tickers:
            return {}
        from change_detector import _get_db
        from datetime import date as _date
        today = _date.today()
        d1w = (today - timedelta(days=7)).isoformat()
        d1m = (today - timedelta(days=30)).isoformat()
        d3m = (today - timedelta(days=90)).isoformat()
        d6m = (today - timedelta(days=182)).isoformat()
        d1y = (today - timedelta(days=365)).isoformat()
        ph = ','.join(['?'] * len(tickers))
        tl = list(tickers)
        conn = _get_db()
        c = conn.cursor()
        out = {}

        # 1. RS data — single query from rs_rankings
        try:
            c.execute(f"""SELECT ticker, rs_rating, rs_change_5d, rs_change_20d,
                         rs_new_high, price_new_high, above_rs_30wma, monster_score,
                         sector, price_return_1m, price_return_3m, price_return_6m
                         FROM rs_rankings WHERE ticker IN ({ph})""", tl)
            for row in c.fetchall():
                tk = row[0]
                out[tk] = {'rs': {
                    'r': row[1], 'd5': row[2], 'd20': row[3],
                    'nh': int(row[4] or 0), 'pnh': int(row[5] or 0),
                    'wma': int(row[6] or 0), 'ms': row[7],
                    'sec': row[8] or '',
                    'r1m': row[9], 'r3m': row[10], 'r6m': row[11]
                }}
        except Exception:
            pass

        # 2. HVC counts by period — CASE/WHEN bucketing
        try:
            c.execute(f"""SELECT ticker,
                         SUM(CASE WHEN event_date >= ? THEN 1 ELSE 0 END),
                         SUM(CASE WHEN event_date >= ? THEN 1 ELSE 0 END),
                         SUM(CASE WHEN event_date >= ? THEN 1 ELSE 0 END),
                         SUM(CASE WHEN event_date >= ? THEN 1 ELSE 0 END),
                         COUNT(*)
                         FROM hvc_events WHERE event_date >= ? AND ticker IN ({ph})
                         GROUP BY ticker""",
                      [d1w, d1m, d3m, d6m, d1y] + tl)
            for row in c.fetchall():
                tk = row[0]
                if tk not in out:
                    out[tk] = {}
                out[tk]['hvc'] = [row[1] or 0, row[2] or 0, row[3] or 0,
                                  row[4] or 0, row[5] or 0]
        except Exception:
            pass

        # 3. Sweep events by type and period
        try:
            c.execute(f"""SELECT ticker, COALESCE(event_type, 'clusterbomb'),
                         SUM(CASE WHEN event_date >= ? THEN 1 ELSE 0 END),
                         SUM(CASE WHEN event_date >= ? THEN 1 ELSE 0 END),
                         SUM(CASE WHEN event_date >= ? THEN 1 ELSE 0 END),
                         SUM(CASE WHEN event_date >= ? THEN 1 ELSE 0 END),
                         COUNT(*)
                         FROM clusterbomb_events WHERE event_date >= ? AND ticker IN ({ph})
                         GROUP BY ticker, COALESCE(event_type, 'clusterbomb')""",
                      [d1w, d1m, d3m, d6m, d1y] + tl)
            for row in c.fetchall():
                tk, etype = row[0], row[1]
                if tk not in out:
                    out[tk] = {}
                if 'sw' not in out[tk]:
                    out[tk]['sw'] = {}
                key = 'cb' if etype == 'clusterbomb' else ('rare' if etype == 'rare_sweep' else 'mon')
                out[tk]['sw'][key] = [row[2] or 0, row[3] or 0, row[4] or 0,
                                      row[5] or 0, row[6] or 0]
        except Exception:
            pass

        conn.close()
        return out

    # ====================================================================
    # 0DTE DARKPOOL FLOW
    # ====================================================================

    def serve_chart_candles(self, query=None):
        """GET /api/chart/candles — fetch candles from Polygon for a ticker."""
        query = query or {}
        ticker = query.get("ticker", ["SPY"])[0]
        date_str = query.get("date", [None])[0]
        days = int(query.get("days", ["1"])[0])
        interval = int(query.get("interval", ["1"])[0])
        VALID_INTERVALS = {
            1: ("minute", 1),      # 1m
            5: ("minute", 5),      # 5m
            60: ("hour", 1),       # 1h
            240: ("hour", 4),      # 4h
            1440: ("day", 1),      # 1D
            2880: ("day", 2),      # 2D
            10080: ("week", 1),    # W
            43200: ("month", 1),   # M
            129600: ("month", 3),  # 3M
        }
        if interval not in VALID_INTERVALS:
            interval = 1

        if not date_str:
            from datetime import date as dt_date
            date_str = dt_date.today().isoformat()

        # Compute date range for multi-day
        from datetime import datetime, timedelta
        end_date = datetime.strptime(date_str, "%Y-%m-%d")
        # Weekend/holiday padding scales with range
        weekend_pad = 2 if days <= 10 else int(days * 0.45)
        start_date = end_date - timedelta(days=max(0, days - 1) + weekend_pad)
        from_str = start_date.strftime("%Y-%m-%d")

        # Map interval to Polygon aggregation unit
        agg_unit, agg_mult = VALID_INTERVALS.get(interval, ("minute", 1))

        url = f"{MASSIVE_BASE_URL}/aggs/ticker/{ticker}/range/{agg_mult}/{agg_unit}/{from_str}/{date_str}"
        headers = {"Authorization": f"Bearer {MASSIVE_API_KEY}"}
        params = {"adjusted": "true", "sort": "asc", "limit": 50000}

        all_bars = []
        try:
            while url:
                resp = requests.get(url, params=params, headers=headers, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                results = data.get("results", [])
                for r in results:
                    ts = r.get("t", 0) // 1000  # ms → seconds
                    all_bars.append({
                        "time": ts,
                        "open": r.get("o", 0),
                        "high": r.get("h", 0),
                        "low": r.get("l", 0),
                        "close": r.get("c", 0),
                        "volume": r.get("v", 0),
                    })
                url = data.get("next_url")
                params = {}  # next_url includes params
                if url and MASSIVE_API_KEY:
                    url += f"&apiKey={MASSIVE_API_KEY}"
        except Exception as e:
            self.send_json({"error": str(e), "candles": []})
            return

        # Append today's live bars from daemon (if available)
        live_appended = 0
        if _live_daemon is not None:
            last_hist_ts = all_bars[-1]["time"] if all_bars else 0
            live_bars = _live_daemon.get_live_bars(ticker, interval)
            for lb in live_bars:
                if lb["t"] > last_hist_ts:
                    all_bars.append({
                        "time": lb["t"],
                        "open": lb["o"], "high": lb["h"],
                        "low": lb["l"], "close": lb["c"],
                        "volume": lb.get("v", 0),
                    })
                    live_appended += 1
                elif lb["t"] == last_hist_ts and all_bars:
                    # Update the last bar in-place (merge live into historical)
                    last = all_bars[-1]
                    last["high"] = max(last["high"], lb["h"])
                    last["low"] = min(last["low"], lb["l"])
                    last["close"] = lb["c"]
                    last["volume"] = max(last["volume"], lb.get("v", 0))

        self.send_json({"candles": all_bars, "ticker": ticker,
                        "live_bars": live_appended})

    def serve_chart_live_bar(self, query=None):
        """GET /api/chart/live-bar?ticker=SPY&interval=1 — lightweight live bar from daemon.

        Returns just the latest forming bar(s) from daemon memory.
        Designed for fast polling (1-2s) to give TradingView-style live candle updates.
        """
        query = query or {}
        ticker = (query.get("ticker") or ["SPY"])[0].upper().strip()
        interval = int((query.get("interval") or ["1"])[0])

        if _live_daemon is None:
            self.send_json({"bars": [], "ticker": ticker})
            return

        # Map interval string to minutes
        interval_map = {"1": 1, "5": 5, "15": 15, "30": 30, "60": 60, "1440": 1440}
        mins = interval_map.get(str(interval), interval)

        live_bars = _live_daemon.get_live_bars(ticker, mins)
        if not live_bars:
            self.send_json({"bars": [], "ticker": ticker})
            return

        # Return only the last 2 bars (current forming + previous completed)
        tail = live_bars[-2:] if len(live_bars) > 1 else live_bars
        out = []
        for lb in tail:
            out.append({
                "time": lb["t"],
                "open": lb["o"], "high": lb["h"],
                "low": lb["l"], "close": lb["c"],
                "volume": lb.get("v", 0),
            })

        self.send_json({"bars": out, "ticker": ticker})

    def serve_bb_signals(self, query=None):
        """GET /api/chart/bb-signals?ticker=SPY — BB deviation signals from CSV cache.
        Returns buy/sell for last bar across multiple timeframes, computed locally.
        """
        import numpy as np
        query = query or {}
        ticker = (query.get("ticker") or ["SPY"])[0].upper().strip()

        # Load CSV cache
        safe = ticker.replace(":", "_").replace("/", "_")
        daily_path = os.path.join("cache", f"{safe}_day.csv")
        hourly_path = os.path.join("cache", f"{safe}_hour.csv")

        results = []
        period = 20
        mult = 2.0

        def bb_dev_last(closes, opens):
            """Compute BB deviation for the last bar given close + open arrays."""
            if len(closes) < period:
                return {"buy": False, "sell": False}
            c = closes[-period:]
            sma = float(np.mean(c))
            std = float(np.std(c, ddof=0))
            upper = sma + mult * std
            lower = sma - mult * std
            last_open = opens[-1]
            last_close = closes[-1]
            buy = float(last_open) < lower and float(last_close) > lower
            sell = float(last_open) > upper and float(last_close) < upper
            return {"buy": buy, "sell": sell}

        def aggregate_simple(df, n):
            """Aggregate bars by grouping every n rows."""
            if df is None or len(df) < n:
                return None
            # Trim to exact multiple
            trim = len(df) - (len(df) % n)
            df2 = df.iloc[-trim:].copy()
            groups = np.arange(len(df2)) // n
            agg = df2.groupby(groups).agg(
                open=("open", "first"), high=("high", "max"),
                low=("low", "min"), close=("close", "last")
            )
            return agg

        def aggregate_weekly(df):
            """Aggregate daily bars into weekly (Mon–Fri)."""
            if df is None or len(df) == 0:
                return None
            df2 = df.copy()
            df2["week"] = df2["timestamp"].dt.isocalendar().year.astype(str) + "-" + df2["timestamp"].dt.isocalendar().week.astype(str).str.zfill(2)
            agg = df2.groupby("week").agg(
                open=("open", "first"), high=("high", "max"),
                low=("low", "min"), close=("close", "last")
            )
            return agg

        def aggregate_monthly(df, n_months=1):
            """Aggregate daily bars into N-month bars."""
            if df is None or len(df) == 0:
                return None
            df2 = df.copy()
            df2["month"] = df2["timestamp"].dt.to_period("M")
            monthly = df2.groupby("month").agg(
                open=("open", "first"), high=("high", "max"),
                low=("low", "min"), close=("close", "last")
            )
            if n_months > 1:
                return aggregate_simple(monthly.reset_index(drop=True), n_months)
            return monthly

        # Load daily
        daily_df = None
        if os.path.exists(daily_path):
            try:
                daily_df = pd.read_csv(daily_path, parse_dates=["timestamp"])
                daily_df = daily_df.sort_values("timestamp").reset_index(drop=True)
            except Exception:
                daily_df = None

        # Load hourly
        hourly_df = None
        if os.path.exists(hourly_path):
            try:
                hourly_df = pd.read_csv(hourly_path, parse_dates=["timestamp"])
                hourly_df = hourly_df.sort_values("timestamp").reset_index(drop=True)
            except Exception:
                hourly_df = None

        # Timeframe definitions: (label, source, aggregation)
        tf_defs = []

        # Hourly-based TFs
        if hourly_df is not None and len(hourly_df) >= period:
            # 1H
            tf_defs.append(("1H", hourly_df["close"].values, hourly_df["open"].values))
            # 4H
            agg4 = aggregate_simple(hourly_df[["open","high","low","close"]], 4)
            if agg4 is not None and len(agg4) >= period:
                tf_defs.append(("4H", agg4["close"].values, agg4["open"].values))

        # Daily-based TFs
        if daily_df is not None and len(daily_df) >= period:
            tf_defs.append(("D", daily_df["close"].values, daily_df["open"].values))
            # 2D
            agg2d = aggregate_simple(daily_df[["open","high","low","close"]], 2)
            if agg2d is not None and len(agg2d) >= period:
                tf_defs.append(("2D", agg2d["close"].values, agg2d["open"].values))
            # Weekly
            aggw = aggregate_weekly(daily_df)
            if aggw is not None and len(aggw) >= period:
                tf_defs.append(("W", aggw["close"].values, aggw["open"].values))
            # Monthly
            aggm = aggregate_monthly(daily_df, 1)
            if aggm is not None and len(aggm) >= period:
                tf_defs.append(("M", aggm["close"].values, aggm["open"].values))
            # 3M
            agg3m = aggregate_monthly(daily_df, 3)
            if agg3m is not None and len(agg3m) >= period:
                tf_defs.append(("3M", agg3m["close"].values, agg3m["open"].values))

        for label, closes, opens in tf_defs:
            sig = bb_dev_last(closes, opens)
            results.append({"tf": label, "buy": sig["buy"], "sell": sig["sell"]})

        self.send_json({"ticker": ticker, "signals": results})

    def serve_chart_sweeps(self, query=None):
        """GET /api/chart/sweeps — darkpool sweeps for multiple tickers with percentile sizing."""
        query = query or {}
        tickers_str = query.get("tickers", ["SPY,VOO"])[0]
        tickers = [t.strip() for t in tickers_str.split(",") if t.strip()]
        date_str = query.get("date", [None])[0]
        days = int(query.get("days", ["1"])[0])
        min_notional = float(query.get("min_notional", ["500000"])[0])

        if not date_str:
            from datetime import date as dt_date
            date_str = dt_date.today().isoformat()

        from datetime import datetime, timedelta
        end_date = datetime.strptime(date_str, "%Y-%m-%d")
        # For multi-day, go back extra for weekends
        start_date = end_date - timedelta(days=max(0, days - 1) + 2)
        from_str = start_date.strftime("%Y-%m-%d")

        import sqlite3
        conn = sqlite3.connect(DB_PATH, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.row_factory = sqlite3.Row

        # 1) Get today's sweeps for requested tickers
        placeholders = ",".join("?" for _ in tickers)
        sweeps_sql = f"""
            SELECT ticker, trade_date, trade_time, sip_timestamp, price, size, notional
            FROM sweep_trades
            WHERE ticker IN ({placeholders})
              AND trade_date >= ? AND trade_date <= ?
              AND is_darkpool = 1 AND is_sweep = 1
              AND notional >= ?
            ORDER BY trade_date, trade_time
        """
        rows = conn.execute(sweeps_sql, tickers + [from_str, date_str, min_notional]).fetchall()

        # 2) Build percentile lookup per ticker (3 years of history)
        #    Always use ALL sweeps (no min_notional filter) so percentile stays
        #    stable regardless of the display filter.
        three_years_ago = (end_date - timedelta(days=3*365)).strftime("%Y-%m-%d")
        percentile_cache = {}
        for tk in tickers:
            hist_sql = """
                SELECT notional FROM sweep_trades
                WHERE ticker = ? AND is_darkpool = 1 AND is_sweep = 1
                  AND trade_date >= ?
                ORDER BY notional
            """
            hist = [r[0] for r in conn.execute(hist_sql, (tk, three_years_ago)).fetchall()]
            percentile_cache[tk] = hist  # sorted ascending

        # Price normalisation now done on frontend: non-ref sweeps placed at
        # SPY's candle close for the same timestamp (no ratio needed server-side)

        # 3) Load event metadata (ranks, types) from clusterbomb_events for these tickers+dates
        event_meta = {}  # (ticker, date) → {sweep_rank, daily_rank, is_rare, is_cb, is_monster, event_type}
        try:
            meta_sql = f"""
                SELECT ticker, event_date, event_type, is_rare, is_monster,
                       sweep_rank, daily_rank, total_notional, sweep_count
                FROM clusterbomb_events
                WHERE ticker IN ({placeholders})
                  AND event_date >= ? AND event_date <= ?
            """
            meta_rows = conn.execute(meta_sql, tickers + [from_str, date_str]).fetchall()
            for mr in meta_rows:
                key = (mr["ticker"], mr["event_date"])
                prev = event_meta.get(key)
                # Multiple events can exist per (ticker, date) — merge them
                if prev is None:
                    event_meta[key] = {
                        "sweep_rank": mr["sweep_rank"],
                        "daily_rank": mr["daily_rank"],
                        "is_rare": bool(mr["is_rare"]),
                        "is_cb": mr["event_type"] == "clusterbomb",
                        "is_monster": bool(mr["is_monster"]) or mr["event_type"] == "monster_sweep",
                        "total_notional": mr["total_notional"] or 0,
                        "sweep_count": mr["sweep_count"] or 0,
                    }
                else:
                    # Merge: keep best ranks, OR the flags
                    if mr["sweep_rank"] and (prev["sweep_rank"] is None or mr["sweep_rank"] < prev["sweep_rank"]):
                        prev["sweep_rank"] = mr["sweep_rank"]
                    if mr["daily_rank"] and (prev["daily_rank"] is None or mr["daily_rank"] < prev["daily_rank"]):
                        prev["daily_rank"] = mr["daily_rank"]
                    prev["is_rare"] = prev["is_rare"] or bool(mr["is_rare"])
                    prev["is_cb"] = prev["is_cb"] or (mr["event_type"] == "clusterbomb")
                    prev["is_monster"] = prev["is_monster"] or bool(mr["is_monster"]) or mr["event_type"] == "monster_sweep"
                    prev["total_notional"] = max(prev["total_notional"], mr["total_notional"] or 0)
                    prev["sweep_count"] = max(prev["sweep_count"], mr["sweep_count"] or 0)
        except Exception:
            pass

        # 3b) Build daily aggregate for histogram (all DP sweeps per date, not just filtered)
        daily_agg = {}  # date → {notional, count, tickers}
        try:
            daily_sql = f"""
                SELECT trade_date, SUM(notional) as total_notional, COUNT(*) as cnt
                FROM sweep_trades
                WHERE ticker IN ({placeholders})
                  AND trade_date >= ? AND trade_date <= ?
                  AND is_darkpool = 1 AND is_sweep = 1
                GROUP BY trade_date
                ORDER BY trade_date
            """
            daily_rows = conn.execute(daily_sql, tickers + [from_str, date_str]).fetchall()
            for dr in daily_rows:
                daily_agg[dr["trade_date"]] = {
                    "notional": dr["total_notional"],
                    "count": dr["cnt"],
                }
        except Exception:
            pass

        conn.close()

        # 4) Build response with percentiles
        def _percentile(ticker, notional):
            hist = percentile_cache.get(ticker, [])
            if not hist:
                return 50
            # Binary search for position
            lo, hi = 0, len(hist)
            while lo < hi:
                mid = (lo + hi) // 2
                if hist[mid] < notional:
                    lo = mid + 1
                else:
                    hi = mid
            return min(99, int(lo / len(hist) * 100))

        sweeps = []
        for r in rows:
            # Convert trade_time to UTC unix timestamp to match Polygon candle timestamps.
            # sweep_trades stores sip_timestamp (nanoseconds UTC) — use that if available.
            # Otherwise parse trade_date + trade_time as ET and convert to UTC.
            sip = r["sip_timestamp"] if r["sip_timestamp"] else None
            if sip and sip > 1e15:
                # sip_timestamp is in nanoseconds UTC
                time_unix = int(sip // 1e9)
            elif sip and sip > 1e12:
                # milliseconds
                time_unix = int(sip // 1e3)
            elif sip and sip > 1e9:
                # already seconds
                time_unix = int(sip)
            else:
                # Fallback: parse as ET (UTC-4 during EDT)
                dt_str = f"{r['trade_date']} {r['trade_time'][:8]}"
                try:
                    from datetime import datetime as _dt, timezone as _tz
                    trade_dt = _dt.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
                    # Assume EDT (UTC-4) for US market hours
                    trade_dt = trade_dt.replace(tzinfo=_tz(timedelta(hours=-4)))
                    time_unix = int(trade_dt.timestamp())
                except Exception:
                    continue

            # Snap to minute boundary to match 1-min candle timestamps
            # LC timeToCoordinate returns null for times between candles
            time_unix = (time_unix // 60) * 60

            pct = _percentile(r["ticker"], r["notional"])
            meta = event_meta.get((r["ticker"], r["trade_date"]), {})
            sweeps.append({
                "ticker": r["ticker"],
                "time_unix": time_unix,
                "date": r["trade_date"],
                "time": r["trade_time"][:8],
                "price": r["price"],
                "size": r["size"],
                "notional": r["notional"],
                "percentile": pct,
                "sweep_rank": meta.get("sweep_rank"),
                "daily_rank": meta.get("daily_rank"),
                "is_rare": meta.get("is_rare", False),
                "is_cb": meta.get("is_cb", False),
                "is_monster": meta.get("is_monster", False),
            })

        self.send_json({
            "sweeps": sweeps,
            "tickers": tickers,
            "date": date_str,
            "total": len(sweeps),
            "daily_agg": daily_agg,
        })

    def send_json(self, data):
        """Helper to send JSON response, with optional gzip compression."""
        try:
            body = json.dumps(data, default=str).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            # Gzip compress if client accepts it and payload > 1KB
            ae = self.headers.get("Accept-Encoding", "")
            if "gzip" in ae and len(body) > 1024:
                body = gzip.compress(body, compresslevel=6)
                self.send_header("Content-Encoding", "gzip")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (ConnectionAbortedError, ConnectionResetError, BrokenPipeError):
            pass  # Client disconnected before response was sent
    
    def log_message(self, format, *args):
        """Suppress default logging for cleaner output."""
        pass


class PublicPortfolioHandler(DashboardHandler):
    """Minimal handler that ONLY serves the portfolio page + its API endpoints.
    Intended for public access on a separate port (8081)."""

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        query = urllib.parse.parse_qs(parsed.query) if parsed.query else None

        # Only allow portfolio-related routes — serve standalone public template (no nav header)
        if path == "/" or path == "/portfolio":
            self.serve_page("portfolio_public.html")
        elif path == "/api/portfolio/data":
            self.serve_portfolio_data()
        elif path == "/api/portfolio/history":
            self.serve_portfolio_history(query)
        elif path == "/api/portfolio/trades":
            self.serve_portfolio_trades()
        else:
            self.send_response(404)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"Not found")

    def do_POST(self):
        self.send_response(404)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Not found")


def start_server(port=8080, public_port=8081):
    """Start the dashboard HTTP server + optional public portfolio server."""
    # Start public portfolio server on separate port (background thread)
    try:
        pub_server = ThreadingHTTPServer(("0.0.0.0", public_port), PublicPortfolioHandler)
        pub_thread = threading.Thread(target=pub_server.serve_forever, daemon=True,
                                      name="public-portfolio")
        pub_thread.start()
        print(f"[SERVER] Public portfolio on port {public_port}", flush=True)
        print(f"[SERVER]   http://89.167.110.85:{public_port}", flush=True)
    except Exception as e:
        print(f"[SERVER] Public portfolio failed: {e}", flush=True)

    server = ThreadingHTTPServer(("0.0.0.0", port), DashboardHandler)
    print(f"\n[SERVER] Dashboard running on port {port}", flush=True)
    print(f"[SERVER]   Local:     http://localhost:{port}", flush=True)
    print(f"[SERVER]   Tailscale: http://100.97.33.3:{port}", flush=True)
    print(f"[SERVER]   Press Ctrl+C to stop\n", flush=True)
    server.serve_forever()


def scheduled_scan(tickers, interval):
    """Legacy scan loop — kept for --ticker / --prototype single-ticker scanning."""
    while True:
        time.sleep(interval)
        print(f"\n[SCAN] Scheduled scan at {datetime.now().strftime('%H:%M:%S')}", flush=True)
        try:
            scan_all(tickers)
        except Exception as e:
            print(f"[SCAN] Scheduled scan error: {e}", flush=True)


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Momentum Dashboard")
    parser.add_argument("--scan-only", action="store_true", help="Run scan without starting server")
    parser.add_argument("--serve-only", action="store_true", help="Start server with existing data")
    parser.add_argument("--ticker", type=str, help="Scan a single ticker")
    parser.add_argument("--prototype", action="store_true", help="Use prototype ticker subset (Semis)")
    parser.add_argument("--port", type=int, default=8080, help="Dashboard port (default: 8080)")
    parser.add_argument("--public-port", type=int, default=8081, help="Public portfolio port (default: 8081)")
    parser.add_argument("--no-schedule", action="store_true", help="Don't run scheduled scans")
    parser.add_argument("--watchlist", type=str, action="append", help="Scan only specific watchlist(s), can repeat")
    parser.add_argument("--clear-cache", action="store_true", help="Clear cached data and re-fetch everything")
    
    args = parser.parse_args()
    
    # Check API key
    if MASSIVE_API_KEY == "YOUR_API_KEY_HERE" and not args.serve_only:
        print("[SERVER] Please set your Massive.com API key in config.py", flush=True)
        sys.exit(1)
    
    # Initialize database
    init_db()
    try:
        init_sweep_db()
    except Exception as _e:
        print(f"[SERVER] init_sweep_db() failed (DB likely locked by another process): {_e}", flush=True)
        print("[SERVER] Continuing anyway — tables already exist if backfill is running.", flush=True)

    # Refresh ETF ticker cache (skips if < 7 days old)
    # Note: purge_etf_events() removed — ETF events now shown on ETF sweeps page
    try:
        refresh_etf_cache()
    except Exception as _e:
        print(f"[SERVER] ETF cache refresh failed: {_e}", flush=True)

    # Refresh ticker name cache (skips if < 7 days old)
    try:
        refresh_ticker_names()
    except Exception as _e:
        print(f"[SERVER] Ticker names refresh failed: {_e}", flush=True)

    # Clear cache if requested
    if args.clear_cache:
        clear_cache()
    
    if args.serve_only:
        # Start scheduler (handles all collection + computation)
        if not args.no_schedule:
            scheduler.start()

        # Auto-resume disabled — re-enable if needed for overnight fetches
        # pending_job = _load_fetch_job()
        # if pending_job: ...

        # Auto-rebuild materialised caches if daily summary is empty
        # (first start after adding optimisation tables, or after DB wipe)
        try:
            from sweep_engine import _get_db as _sdb
            _c = _sdb()
            _has = _c.execute("SELECT 1 FROM sweep_daily_summary LIMIT 1").fetchone()
            _c.close()
            if not _has:
                print("[SERVER] Building materialised caches (first run)...", flush=True)
                rebuild_stats_cache()
                rebuild_daily_summary()
                print("[SERVER] Done.", flush=True)
        except Exception as _e:
            print(f"[SERVER] Cache rebuild skipped: {_e}", flush=True)

        # Auto-start unified live daemon if either price or sweep auto_start is set
        try:
            from live_daemon import (UnifiedLiveDaemon, load_live_config,
                                     load_live_price_config)
            _sweep_cfg = load_live_config()
            _price_cfg = load_live_price_config()
            if _sweep_cfg.get("auto_start", False) or _price_cfg.get("auto_start", False):
                global _live_daemon, _daemon_intentionally_stopped
                _live_daemon = UnifiedLiveDaemon(
                    price_config=_price_cfg, sweep_config=_sweep_cfg)
                _live_daemon.start()
                _daemon_intentionally_stopped = False  # watchdog should protect it
                print("[SERVER] Live daemon auto-started (price + sweeps)", flush=True)
        except Exception as _e:
            print(f"[SERVER] Live daemon auto-start skipped: {_e}", flush=True)

        # Start daemon watchdog — auto-restarts the daemon if it crashes
        _wd = threading.Thread(target=_daemon_watchdog, daemon=True,
                               name="daemon-watchdog")
        _wd.start()

        # Start portfolio threads (snapshot + trade sync)
        try:
            _init_portfolio_db()
            _ps = threading.Thread(target=_portfolio_snapshot_thread, daemon=True,
                                   name="portfolio-snapshots")
            _ps.start()
            _pts = threading.Thread(target=_portfolio_trade_sync_thread, daemon=True,
                                    name="portfolio-trade-sync")
            _pts.start()
        except Exception as _e:
            print(f"[SERVER] Portfolio threads failed to start: {_e}", flush=True)

        start_server(args.port, args.public_port)
        return
    
    # Determine ticker list
    if args.ticker:
        tickers = [args.ticker]
    elif args.prototype:
        tickers = PROTOTYPE_TICKERS
    else:
        # Scan all tickers across all watchlists, deduplicated
        seen = set()
        tickers = []
        wl_filter = set(args.watchlist) if args.watchlist else None
        for wl_name, wl_groups in WATCHLISTS.items():
            if wl_filter and wl_name not in wl_filter:
                continue
            for group_name, group_tickers in wl_groups:
                for display, api, atype in group_tickers:
                    if display not in seen:
                        seen.add(display)
                        tickers.append(display)
        wl_label = ", ".join(args.watchlist) if args.watchlist else f"{len(WATCHLISTS)} watchlists"
        print(f"[SCAN] Scanning {len(tickers)} unique tickers from {wl_label}", flush=True)
    
    # Run initial scan
    scan_all(tickers)
    
    if args.scan_only:
        return
    
    # Start the watchlist scheduler (replaces old scheduled_scan thread)
    if not args.no_schedule:
        scheduler.start()
    
    # Start dashboard server
    start_server(args.port, args.public_port)


if __name__ == "__main__":
    main()
