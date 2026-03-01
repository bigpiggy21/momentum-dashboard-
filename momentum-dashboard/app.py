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
import sys
import threading
import time

# Fix Unicode output on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
from datetime import datetime, timedelta
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

# Live sweep daemon instance (created on first start)
_live_daemon = None
_live_daemon_lock = threading.Lock()

# Live price daemon instance (created on first start)
_live_price_daemon = None
_live_price_daemon_lock = threading.Lock()

# Server-side tracker response cache (60s TTL)
_tracker_cache = {}       # {cache_key: {"data": response_dict, "ts": float}}
_TRACKER_CACHE_TTL = 60   # seconds


def _save_fetch_job(tickers, start_date, end_date):
    """Persist fetch job params so we can resume after a crash."""
    try:
        with open(FETCH_JOB_PATH, "w", encoding="utf-8") as f:
            json.dump({"tickers": tickers, "start_date": start_date,
                        "end_date": end_date}, f)
    except Exception as e:
        print(f"Warning: could not save fetch job: {e}")


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
    REFRESH_INTERVAL_SECONDS, DB_PATH, MASSIVE_API_KEY,
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
        print(f"⚠ Watchlist reload failed: {e}")


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
        print(f"  ⏭ {display_ticker}: No data, skipping")
        return None
    
    # Re-check fingerprint after fetch (data may have changed from API)
    data_hash = get_data_fingerprint(api_ticker)
    
    # If data still matches after fetch, we can skip computation
    if data_hash:
        unadj_cached = check_scan_cache(display_ticker, 0, data_hash, INDICATOR_VERSION)
        adj_cached = check_scan_cache(display_ticker, 1, data_hash, INDICATOR_VERSION)
        
        if unadj_cached and adj_cached:
            print(f"  ⏭ {display_ticker}: Data unchanged after fetch, skipping")
            return "skipped"
    
    print(f"  🔄 {display_ticker}: Computing indicators...")
    
    # 3. Build timeframe data (unadjusted)
    tf_data = build_timeframe_data(raw_data)
    
    # 4. Calculate indicators (unadjusted)
    results = calculate_all_indicators(tf_data, raw_data.get("weekly", None))
    price = results.get('_meta', {}).get('price', 'N/A')
    
    # 5. Detect changes against previous snapshot (unadjusted)
    events = detect_changes(display_ticker, results, div_adj=0)
    if events:
        print(f"  ⚡ {len(events)} changes: ", end="")
        print(", ".join(f"[{e['timeframe']}] {e['description']}" for e in events[:3]))
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
            print(f"  ⚠ Dividend adjustment failed: {e}")
    else:
        save_snapshot(display_ticker, results, div_adj=1)
    
    if data_hash:
        update_scan_cache(display_ticker, 1, data_hash, INDICATOR_VERSION)
    
    print(f"  ✓ {display_ticker}: ${price}")
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
    
    print(f"\n{'#'*60}")
    print(f"MOMENTUM DASHBOARD SCAN — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Scanning {len(tickers)} tickers")
    print(f"{'#'*60}")
    
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
            print(f"  ❌ ERROR scanning {ticker}: {e}")
            import traceback
            traceback.print_exc()
    
    print(f"\n{'='*60}")
    if skipped:
        print(f"Scan complete. {len(all_results)} computed, {skipped} skipped (unchanged), {len(tickers)} total.")
    else:
        print(f"Scan complete. {len(all_results)}/{len(tickers)} tickers processed.")
    
    # Summary of recent events
    events = get_recent_events(limit=50, since=(datetime.utcnow() - timedelta(minutes=5)).isoformat())
    if events:
        print(f"\n⚡ Recent changes ({len(events)}):")
        for e in events:
            print(f"  {e['timestamp'][:19]} | {e['ticker']:6s} | {e['timeframe']:4s} | {e['description']}")
    
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
            self.serve_page("sweeps.html")
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
        elif path == "/api/sweeps/live/status":
            self.serve_live_sweep_status()
        elif path == "/api/sweeps/live/events":
            self.serve_live_sweep_events()
        elif path == "/api/sweeps/live/config":
            self.serve_live_sweep_get_config()
        elif path == "/api/sweeps/sectors":
            self.serve_sweep_sectors()
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
        elif path == "/api/scheduler/live-price/status":
            self.serve_live_price_status()
        elif path == "/api/scheduler/live-price/config":
            self.serve_live_price_get_config()
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
        elif path == "/api/scheduler/live-price/start":
            self.serve_live_price_start()
        elif path == "/api/scheduler/live-price/stop":
            self.serve_live_price_stop()
        elif path == "/api/scheduler/live-price/config":
            self.serve_live_price_save_config()
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
            print(f"  ❌ serve_state error: {e}")
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

        now = datetime.utcnow()
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
                "created": datetime.utcnow().isoformat(),
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
                "created": datetime.utcnow().isoformat(),
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
        """POST /api/scheduler/trigger-ticker — backfill a single ticker (collect + compute)."""
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length else {}
            ticker = body.get("ticker", "").strip().upper()
            if not ticker:
                self.send_json({"ok": False, "error": "ticker required"})
                return

            with _ticker_backfill_lock:
                if _ticker_backfill["running"]:
                    self.send_json({"ok": False, "error": f"Already backfilling {_ticker_backfill['ticker']}"})
                    return
                _ticker_backfill.update({
                    "running": True, "ticker": ticker,
                    "phase": "collecting", "message": f"Collecting {ticker}...",
                    "t0": time.time(),
                })

            def _run():
                import subprocess as _sp
                try:
                    # Phase 1: Collect OHLC data
                    from collector import collect_ticker
                    result = collect_ticker(ticker)
                    if result.get("status") == "error":
                        with _ticker_backfill_lock:
                            _ticker_backfill.update({
                                "running": False, "phase": "error",
                                "message": f"Collection failed: {result.get('error', 'unknown')}",
                            })
                        return

                    # Phase 2: Compute indicators via engine subprocess
                    with _ticker_backfill_lock:
                        _ticker_backfill.update({
                            "phase": "computing",
                            "message": f"Computing indicators for {ticker}...",
                        })

                    engine_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "engine.py")
                    cmd = [sys.executable, engine_path,
                           "--ticker", ticker,
                           "--workers", "1",
                           "--once"]
                    eng = _sp.run(cmd, capture_output=True, timeout=300,
                                  cwd=os.path.dirname(engine_path),
                                  encoding='utf-8', errors='replace')
                    if eng.returncode != 0:
                        err = eng.stderr[-300:] if eng.stderr else "unknown error"
                        with _ticker_backfill_lock:
                            _ticker_backfill.update({
                                "running": False, "phase": "error",
                                "message": f"Compute failed: {err}",
                            })
                        return

                    elapsed = round(time.time() - _ticker_backfill["t0"], 1)
                    with _ticker_backfill_lock:
                        _ticker_backfill.update({
                            "running": False, "phase": "done",
                            "message": f"{ticker} backfilled in {elapsed}s",
                        })
                    print(f"[BACKFILL] {ticker} done in {elapsed}s", flush=True)

                except Exception as e:
                    with _ticker_backfill_lock:
                        _ticker_backfill.update({
                            "running": False, "phase": "error",
                            "message": f"Error: {e}",
                        })

            threading.Thread(target=_run, daemon=True).start()
            self.send_json({"ok": True, "ticker": ticker})

        except Exception as e:
            self.send_json({"ok": False, "error": str(e)})

    def serve_ticker_backfill_status(self):
        """GET /api/scheduler/ticker-backfill-status — progress of single-ticker backfill."""
        with _ticker_backfill_lock:
            self.send_json({
                "running": _ticker_backfill["running"],
                "ticker": _ticker_backfill["ticker"],
                "phase": _ticker_backfill["phase"],
                "message": _ticker_backfill["message"],
            })

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

            cb_dates, cb_tips, cb_prices = [], [], []
            monster_dates, monster_tips, monster_prices = [], [], []
            rare_dates, rare_tips, rare_prices = [], [], []

            for r in rows:
                parts = r["event_date"].split("-")
                y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
                date_key = y * 10000 + m * 100 + d
                notional = r["total_notional"] or 0
                n_str = f"${notional/1e6:.1f}M" if notional >= 1e6 else f"${notional/1e3:.0f}K"
                price = r["avg_price"] or 0
                price_str = f"{price:.2f}"

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
                    monster_prices.append(price_str)
                elif r["is_rare"]:
                    rare_dates.append(str(date_key))
                    rare_tips.append(f'"{tip}"')
                    rare_prices.append(price_str)
                else:
                    cb_dates.append(str(date_key))
                    cb_tips.append(f'"{tip}"')
                    cb_prices.append(price_str)

            # Build the Pine Script
            lines = []
            lines.append("// @version=5")
            lines.append(f'indicator("Darkpool Sweeps \u2014 {ticker}", overlay=true, max_labels_count=500)')
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
            _emit_float_array("cbPrices", cb_prices)
            _emit_str_array("cbTips", cb_tips)
            lines.append("")
            lines.append(f"// Monsters: {len(monster_dates)} events")
            _emit_array("monsterDates", monster_dates)
            _emit_float_array("monsterPrices", monster_prices)
            _emit_str_array("monsterTips", monster_tips)
            lines.append("")
            lines.append(f"// Rare Sweeps: {len(rare_dates)} events")
            _emit_array("rareDates", rare_dates)
            _emit_float_array("rarePrices", rare_prices)
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
            lines.append("cbPrice      = isCB      ? array.get(cbPrices, cbIdx)           : na")
            lines.append("monsterPrice = isMonster  ? array.get(monsterPrices, monsterIdx) : na")
            lines.append("rarePrice    = isRare     ? array.get(rarePrices, rareIdx)       : na")
            lines.append("")

            # ── Plot circles — true data-point rendering, perfectly price-anchored ──
            lines.append("// \u2500\u2500 Price-anchored circles (plot = true chart data, no drift) \u2500\u2500")
            for kind, flag, color_var, lw, price_var in [
                ("Clusterbomb", "showCB and isCB", "cbColor", 3, "cbPrice"),
                ("Monster", "showMonster and isMonster", "monsterColor", 4, "monsterPrice"),
                ("Rare Sweep", "showRare and isRare", "rareColor", 3, "rarePrice"),
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
        Optional ?details=1 to include tip arrays for detail labels (~90KB extra).
        """
        include_tips = (query or {}).get("details", ["0"])[0] == "1"
        try:
            import sqlite3
            from sweep_engine import get_ticker_day_ranks

            conn = sqlite3.connect(DB_PATH, timeout=10)
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT ticker, event_date, sweep_count, total_notional, avg_price, "
                "direction, is_rare, COALESCE(is_monster, 0) as is_monster "
                "FROM clusterbomb_events ORDER BY ticker, event_date"
            ).fetchall()
            conn.close()

            # Get all unique tickers, sorted, and build index map
            all_tickers = sorted(set(r["ticker"] for r in rows))
            ticker_idx = {t: i for i, t in enumerate(all_tickers)}

            # Fetch rank data for all tickers in one batch
            rank_data = get_ticker_day_ranks(tickers=all_tickers) if all_tickers else {}

            # Build combined-key arrays:  key = tickerIndex * 100_000_000 + YYYYMMDD
            cb_keys, cb_tips, cb_prices = [], [], []
            monster_keys, monster_tips, monster_prices = [], [], []
            rare_keys, rare_tips, rare_prices = [], [], []
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
                price_str = f"{price:.2f}"

                if r["is_monster"]:
                    monster_keys.append(str(combined_key))
                    monster_tips.append(f'"{tip}"')
                    monster_prices.append(price_str)
                    stats["monsters"] += 1
                elif r["is_rare"]:
                    rare_keys.append(str(combined_key))
                    rare_tips.append(f'"{tip}"')
                    rare_prices.append(price_str)
                    stats["rare"] += 1
                else:
                    cb_keys.append(str(combined_key))
                    cb_tips.append(f'"{tip}"')
                    cb_prices.append(price_str)
                    stats["clusterbombs"] += 1

            # ── Build Pine Script ──
            L = []  # shorthand
            L.append("// @version=5")
            L.append('indicator("Darkpool Sweeps", overlay=true, max_labels_count=500)')
            L.append("")

            L.append("// \u2500\u2500 Sweep Type Toggles \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500")
            L.append('showCB      = input.bool(true, "Clusterbombs",  group="Sweep Types")')
            L.append('showMonster = input.bool(true, "Monsters",      group="Sweep Types")')
            L.append('showRare    = input.bool(true, "Rare Sweeps",   group="Sweep Types")')
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
            L.append("")

            # Ticker index lookup
            L.append(f"// \u2500\u2500 Ticker Lookup ({len(all_tickers)} tickers) \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500")
            # Emit ticker array
            L.append("var tickerList = array.from(")
            for i in range(0, len(all_tickers), 10):
                chunk = ", ".join(f'"{t}"' for t in all_tickers[i:i+10])
                comma = "," if i + 10 < len(all_tickers) else ""
                L.append(f"  {chunk}{comma}")
            L.append("  )")
            L.append("tidx = array.indexof(tickerList, syminfo.ticker)")
            L.append("dateKey = year * 10000 + month * 100 + dayofmonth")
            L.append("newDay  = ta.change(time('D')) != 0  // only plot once per day on intraday charts")
            L.append("myKey = tidx * 100000000 + dateKey")
            L.append("")

            # Emit data arrays
            def _emit(name, items, per_line, empty_type="int"):
                if not items:
                    L.append(f"var {name} = array.new_{empty_type}(0)")
                    return
                L.append(f"var {name} = array.from(")
                for i in range(0, len(items), per_line):
                    chunk = ", ".join(items[i:i+per_line])
                    comma = "," if i + per_line < len(items) else ""
                    L.append(f"  {chunk}{comma}")
                L.append("  )")

            L.append(f"// \u2500\u2500 Event Data ({stats['clusterbombs']} CB, {stats['monsters']} monster, {stats['rare']} rare) \u2500\u2500")
            _emit("cbKeys", cb_keys, 8)
            _emit("cbPrices", cb_prices, 8, "float")
            if include_tips:
                _emit("cbTips", cb_tips, 3, "string")
            L.append("")
            _emit("monsterKeys", monster_keys, 8)
            _emit("monsterPrices", monster_prices, 8, "float")
            if include_tips:
                _emit("monsterTips", monster_tips, 3, "string")
            L.append("")
            _emit("rareKeys", rare_keys, 8)
            _emit("rarePrices", rare_prices, 8, "float")
            if include_tips:
                _emit("rareTips", rare_tips, 3, "string")
            L.append("")

            # Detection — match current bar against event arrays
            L.append("// ── Detect ──────────────────────────────────────────────")
            L.append("cbIdx      = tidx >= 0 ? array.indexof(cbKeys, myKey)      : -1")
            L.append("monsterIdx = tidx >= 0 ? array.indexof(monsterKeys, myKey)  : -1")
            L.append("rareIdx    = tidx >= 0 ? array.indexof(rareKeys, myKey)     : -1")
            L.append("isCB      = cbIdx >= 0")
            L.append("isMonster = monsterIdx >= 0")
            L.append("isRare    = rareIdx >= 0")
            L.append("cbPrice      = isCB      ? array.get(cbPrices, cbIdx)           : na")
            L.append("monsterPrice = isMonster  ? array.get(monsterPrices, monsterIdx) : na")
            L.append("rarePrice    = isRare     ? array.get(rarePrices, rareIdx)       : na")
            L.append("")

            # Plot circles — true chart data points, perfectly price-anchored
            L.append("// ── Price-anchored circles (plot = true chart data, no drift) ──")
            for kind, flag, color_var, lw, price_var in [
                ("Clusterbomb", "showCB and isCB", "cbColor", 3, "cbPrice"),
                ("Monster", "showMonster and isMonster", "monsterColor", 4, "monsterPrice"),
                ("Rare Sweep", "showRare and isRare", "rareColor", 3, "rarePrice"),
            ]:
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

            L.append(f'// Generated {datetime.now().strftime("%Y-%m-%d %H:%M")}')
            L.append(f"// {stats['tickers']} tickers, {stats['clusterbombs']} CB, {stats['monsters']} monsters, {stats['rare']} rare")

            script = "\n".join(L)
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
            new_ts = datetime.utcnow().isoformat()
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
            init_sweep_db()
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

            stats = get_sweep_stats(min_total=min_total, tickers=tickers,
                                    date_from=date_from, date_to=date_to,
                                    min_sweeps=min_sweeps, monster_min=monster_min,
                                    rare_min=rare_min, rare_days=rare_days)
            self.send_json(stats)
        except Exception as e:
            self.send_json({"error": str(e)})

    def serve_sweep_summary(self, query=None):
        """GET /api/sweeps/summary — sweep trades grouped by ticker+date."""
        try:
            init_sweep_db()
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
            init_sweep_db()
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
            init_sweep_db()
            ticker = (query or {}).get("ticker", [None])[0]
            date_from = (query or {}).get("date_from", [None])[0]
            date_to = (query or {}).get("date_to", [None])[0]
            rare_only = (query or {}).get("rare_only", ["0"])[0] == "1"
            limit = int((query or {}).get("limit", [200])[0])
            min_total_str = (query or {}).get("min_total", [None])[0]
            min_total = float(min_total_str) if min_total_str else None
            data = get_clusterbombs(ticker=ticker, date_from=date_from,
                                    date_to=date_to, rare_only=rare_only,
                                    limit=limit, min_total=min_total)
            self.send_json({"events": data, "count": len(data)})
        except Exception as e:
            self.send_json({"events": [], "count": 0, "error": str(e)})

    def serve_sweep_chart(self, query=None):
        """GET /api/sweeps/chart — sweep markers + clusterbomb highlights for chart overlay.
        Optional: min_total to filter which clusterbombs appear on chart.
        """
        try:
            init_sweep_db()
            ticker = (query or {}).get("ticker", [None])[0]
            date_from = (query or {}).get("date_from", [None])[0]
            date_to = (query or {}).get("date_to", [None])[0]
            if not ticker:
                self.send_json({"error": "ticker required"})
                return
            timeframe = (query or {}).get("timeframe", ["1D"])[0]
            min_total_str = (query or {}).get("min_total", [None])[0]
            min_total = float(min_total_str) if min_total_str else None
            data = get_sweep_chart_data(ticker, date_from=date_from, date_to=date_to,
                                        timeframe=timeframe, min_total=min_total)
            self.send_json(data)
        except Exception as e:
            self.send_json({"sweeps": [], "clusterbombs": [], "error": str(e)})

    def serve_sweep_tracker(self, query=None):
        """GET /api/sweeps/tracker — clusterbomb tracker with current price + % gain.
        Supports: min_total, tickers, date_from, date_to, limit, offset,
        min_sweeps, monster_min, rare_min, rare_days (per-type display filters).
        """
        try:
            init_sweep_db()
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

            # Check server-side cache
            cache_key = (min_total, tuple(tickers) if tickers else None,
                         date_from, date_to, limit, offset,
                         min_sweeps, monster_min, rare_min, rare_days)
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
                                             rare_days=rare_days)
            response = {"events": events, "total": total, "offset": offset}

            # Cache the response
            _tracker_cache[cache_key] = {"data": response, "ts": now}

            self.send_json(response)
        except Exception as e:
            self.send_json({"events": [], "total": 0, "error": str(e)})

    def serve_sweep_fetch(self):
        """POST /api/sweeps/fetch — trigger sweep data fetching for tickers."""
        try:
            init_sweep_db()
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
                    from sweep_engine import get_detection_config as _get_cfg, detect_rare_sweep_days, detect_monster_sweeps
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

    # ------------------------------------------------------------------
    # Live sweep scanner endpoints
    # ------------------------------------------------------------------

    def serve_live_sweep_status(self):
        """GET /api/sweeps/live/status — return live scanner daemon status."""
        global _live_daemon
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
            })
            return
        self.send_json(_live_daemon.get_status())

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
        """POST /api/sweeps/live/start — start the live sweep daemon."""
        global _live_daemon
        from live_sweep_daemon import LiveSweepDaemon, load_live_config

        with _live_daemon_lock:
            if _live_daemon is not None and _live_daemon.get_status().get("running"):
                self.send_json({"ok": True, "message": "Already running",
                                "status": _live_daemon.get_status()})
                return

            try:
                cfg = load_live_config()
                _live_daemon = LiveSweepDaemon(config=cfg)
                _live_daemon.start()
                self.send_json({"ok": True, "message": "Live scanner started",
                                "status": _live_daemon.get_status()})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)})

    def serve_live_sweep_stop(self):
        """POST /api/sweeps/live/stop — stop the live sweep daemon."""
        global _live_daemon
        with _live_daemon_lock:
            if _live_daemon is None or not _live_daemon.get_status().get("running"):
                self.send_json({"ok": True, "message": "Not running"})
                return

            try:
                _live_daemon.stop()
                self.send_json({"ok": True, "message": "Live scanner stopped"})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)})

    def serve_live_sweep_get_config(self):
        """GET /api/sweeps/live/config — return current live scanner configuration."""
        from live_sweep_daemon import load_live_config
        try:
            self.send_json(load_live_config())
        except Exception as e:
            self.send_json({"error": str(e)})

    def serve_live_sweep_save_config(self):
        """POST /api/sweeps/live/config — save live scanner configuration."""
        from live_sweep_daemon import save_live_config
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length else {}
            save_live_config(body)
            self.send_json({"ok": True})
        except Exception as e:
            self.send_json({"ok": False, "error": str(e)})

    # ------------------------------------------------------------------
    # Live Price Feed endpoints
    # ------------------------------------------------------------------

    def serve_live_price_status(self):
        """GET /api/scheduler/live-price/status — return live price daemon status."""
        global _live_price_daemon
        if _live_price_daemon:
            self.send_json(_live_price_daemon.get_status())
        else:
            self.send_json({"running": False, "connected": False, "error": None})

    def serve_live_price_start(self):
        """POST /api/scheduler/live-price/start — start the live price daemon."""
        global _live_price_daemon
        try:
            from live_price_daemon import LivePriceDaemon, load_live_price_config

            with _live_price_daemon_lock:
                if _live_price_daemon and _live_price_daemon.get_status().get("running"):
                    self.send_json(_live_price_daemon.get_status())
                    return

                cfg = load_live_price_config()
                _live_price_daemon = LivePriceDaemon(config=cfg)
                _live_price_daemon.start()

            # Brief wait for connection
            time.sleep(0.5)
            self.send_json(_live_price_daemon.get_status())
        except Exception as e:
            self.send_json({"ok": False, "error": str(e)})

    def serve_live_price_stop(self):
        """POST /api/scheduler/live-price/stop — stop the live price daemon."""
        global _live_price_daemon
        try:
            with _live_price_daemon_lock:
                if _live_price_daemon:
                    _live_price_daemon.stop()
            self.send_json({"ok": True, "running": False})
        except Exception as e:
            self.send_json({"ok": False, "error": str(e)})

    def serve_live_price_get_config(self):
        """GET /api/scheduler/live-price/config — return current live price feed config."""
        from live_price_daemon import load_live_price_config
        try:
            self.send_json(load_live_price_config())
        except Exception as e:
            self.send_json({"error": str(e)})

    def serve_live_price_save_config(self):
        """POST /api/scheduler/live-price/config — save live price feed config."""
        from live_price_daemon import save_live_price_config
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length else {}
            save_live_price_config(body)
            self.send_json({"ok": True})
        except Exception as e:
            self.send_json({"ok": False, "error": str(e)})

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

    def serve_analysis_river(self, query=None):
        """GET /api/analysis/river — aggregated sweep data for river/stream chart."""
        from sweep_engine import get_river_data, init_sweep_db
        try:
            init_sweep_db()
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
        from sweep_engine import get_heatmap_data, init_sweep_db
        try:
            init_sweep_db()
            year = int((query or {}).get("year", ["2026"])[0])
            metric = (query or {}).get("metric", ["notional"])[0]
            event_type = (query or {}).get("event_type", ["all"])[0]
            data = get_heatmap_data(year=year, metric=metric, event_type=event_type)
            self.send_json(data)
        except Exception as e:
            self.send_json({"data": [], "error": str(e)})

    def serve_sweep_redetect(self):
        """POST /api/sweeps/redetect — re-run clusterbomb detection with dual profiles."""
        try:
            init_sweep_db()
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length else {}

            from sweep_engine import _get_db, get_detection_config as _get_cfg
            cfg = _get_cfg()
            stock_params = body.get("stock", body) if body else cfg["stock"]

            # Get all tickers with sweep data
            conn = _get_db()
            all_tickers = [r[0] for r in conn.execute(
                "SELECT DISTINCT ticker FROM sweep_trades WHERE is_darkpool=1 AND is_sweep=1"
            ).fetchall()]
            # Clear all existing events before full re-detect
            conn.execute("DELETE FROM clusterbomb_events")
            conn.commit()
            conn.close()

            # Single-profile detection for all tickers
            from sweep_engine import detect_rare_sweep_days, detect_monster_sweeps
            events = detect_clusterbombs(
                tickers=all_tickers,
                min_sweeps=int(stock_params.get("min_sweeps", 3)),
                min_notional=float(stock_params.get("min_notional", 1_000_000)),
                min_total=float(stock_params.get("min_total", 10_000_000)),
                rarity_days=int(stock_params.get("rarity_days", 60)),
                rare_min_notional=float(stock_params.get("rare_min_notional", 1_000_000)),
            )

            rare_sweep_events = detect_rare_sweep_days(
                min_notional=float(stock_params.get("rare_min_notional", 1_000_000)),
                rarity_days=int(stock_params.get("rarity_days", 60)),
                tickers=all_tickers,
            )

            monster_events = []
            _sm = stock_params.get("monster_min_notional")
            if _sm:
                monster_events = detect_monster_sweeps(
                    monster_min_notional=float(_sm),
                    tickers=all_tickers,
                )

            _tracker_cache.clear()  # invalidate tracker cache after redetect

            rare_cb_count = sum(1 for e in events if e.get("is_rare"))
            self.send_json({
                "ok": True,
                "events_detected": len(events),
                "rare_cb_count": rare_cb_count,
                "rare_sweep_days": len(rare_sweep_events),
                "monster_sweeps": len(monster_events),
                "message": f"Re-detected {len(events)} clusterbombs, {len(rare_sweep_events)} rare sweep days, {len(monster_events)} monster sweeps",
            })
        except Exception as e:
            self.send_json({"ok": False, "error": str(e)})

    def serve_rebuild_cache(self):
        """POST /api/sweeps/rebuild-cache — rebuild materialised stats + daily summary."""
        try:
            init_sweep_db()
            print("Rebuilding stats cache + daily summary...", flush=True)
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
                # Derive filename: "Leverage" -> "leverage_watchlist.py"
                raw_name = _re.sub(r'[^a-z0-9_]', '', wl['name'].lower().replace(' ', '_').replace('-', '_'))
                fname = f"{raw_name}_watchlist.py"
                saved_filenames.add(fname)

                # Derive variable name: "leverage" -> "LEVERAGE_GROUPS"
                var_name = raw_name.upper() + "_GROUPS"

                lines = []
                lines.append('"""')
                lines.append(f'{wl["name"]} watchlist.')
                lines.append('Edited via TBD Technologies watchlist manager.')
                lines.append('"""')
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


def start_server(port=8080):
    """Start the dashboard HTTP server."""
    server = ThreadingHTTPServer(("0.0.0.0", port), DashboardHandler)
    print(f"\nDashboard running at http://localhost:{port}")
    print(f"   Press Ctrl+C to stop\n")
    server.serve_forever()


def scheduled_scan(tickers, interval):
    """Legacy scan loop — kept for --ticker / --prototype single-ticker scanning."""
    while True:
        time.sleep(interval)
        print(f"\n🔄 Scheduled scan at {datetime.now().strftime('%H:%M:%S')}")
        try:
            scan_all(tickers)
        except Exception as e:
            print(f"❌ Scheduled scan error: {e}")


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
    parser.add_argument("--no-schedule", action="store_true", help="Don't run scheduled scans")
    parser.add_argument("--watchlist", type=str, action="append", help="Scan only specific watchlist(s), can repeat")
    parser.add_argument("--clear-cache", action="store_true", help="Clear cached data and re-fetch everything")
    
    args = parser.parse_args()
    
    # Check API key
    if MASSIVE_API_KEY == "YOUR_API_KEY_HERE" and not args.serve_only:
        print("❌ Please set your Massive.com API key in config.py")
        sys.exit(1)
    
    # Initialize database
    init_db()
    init_sweep_db()

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
                print("📊 Building materialised caches (first run)...")
                rebuild_stats_cache()
                rebuild_daily_summary()
                print("📊 Done.")
        except Exception as _e:
            print(f"⚠️  Cache rebuild skipped: {_e}")

        # Auto-start live sweep daemon if configured
        try:
            from live_sweep_daemon import LiveSweepDaemon, load_live_config
            _live_cfg = load_live_config()
            if _live_cfg.get("auto_start", False):
                global _live_daemon
                _live_daemon = LiveSweepDaemon(config=_live_cfg)
                _live_daemon.start()
                print("⚡ Live sweep scanner auto-started")
        except Exception as _e:
            print(f"⚠️  Live scanner auto-start skipped: {_e}")

        # Auto-start live price daemon if configured
        try:
            from live_price_daemon import LivePriceDaemon, load_live_price_config
            _price_cfg = load_live_price_config()
            if _price_cfg.get("auto_start", False):
                global _live_price_daemon
                _live_price_daemon = LivePriceDaemon(config=_price_cfg)
                _live_price_daemon.start()
                print("📡 Live price feed auto-started")
        except Exception as _e:
            print(f"⚠️  Live price feed auto-start skipped: {_e}")

        start_server(args.port)
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
        print(f"📋 Scanning {len(tickers)} unique tickers from {wl_label}")
    
    # Run initial scan
    scan_all(tickers)
    
    if args.scan_only:
        return
    
    # Start the watchlist scheduler (replaces old scheduled_scan thread)
    if not args.no_schedule:
        scheduler.start()
    
    # Start dashboard server
    start_server(args.port)


if __name__ == "__main__":
    main()
