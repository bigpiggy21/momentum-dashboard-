"""
Unified Live Daemon — single WebSocket for both price ingestion and sweep detection.

Connects to Polygon delayed WebSocket feed, subscribes to BOTH:
  - AM.*  (per-minute aggregate bars → price CSV flush)
  - T.*   (individual trades → dark pool sweep filtering → DB + detection)

Polygon allows only ONE concurrent WebSocket per API key, so both must share
a single connection.

Runs as a background daemon thread inside app.py.

Usage (standalone test):
    python live_daemon.py
"""

import json
import os
import queue
import subprocess
import sqlite3
import sys
import threading
import time
import traceback
from collections import defaultdict, deque
from datetime import datetime, timezone, timedelta

import pandas as pd

PENDING_QUEUE_PATH = os.path.join(os.path.dirname(__file__), "pending_fetch_queue.json")

try:
    import websocket
except ImportError:
    websocket = None
    print("WARNING: websocket-client not installed. Run: pip install websocket-client")

from config import MASSIVE_API_KEY, DB_PATH


def _daemon_log(event, detail=None):
    """Persist a daemon lifecycle event to the daemon_log table."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=5)
        conn.execute(
            "INSERT INTO daemon_log (ts, event, detail) VALUES (?, ?, ?)",
            (datetime.now(timezone.utc).isoformat(), event, detail))
        conn.commit()
        conn.close()
    except Exception:
        pass  # never let logging break the daemon

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")

# Sweep filtering (match sweep_engine.py)
FINRA_TRF_EXCHANGE_ID = 4
INTERMARKET_SWEEP_CONDITION = 14
# Condition 41 (Trade Thru Exempt) and 53 (QCT) are NOT used for filtering.
# Only condition 14 (ISO) reliably identifies intermarket sweeps.
# Condition 41 appears on closing crosses, VWAP fills, late blocks, QCTs, etc.
MIN_SWEEP_NOTIONAL = 500_000  # $500K

DEFAULT_WS_URL = "wss://delayed.polygon.io/stocks"

# Price defaults
DEFAULT_FLUSH_INTERVAL = 2   # minutes

# Sweep defaults
DEFAULT_DETECTION_INTERVAL = 5   # minutes
DEFAULT_BUFFER_FLUSH_SECONDS = 30
DEFAULT_BUFFER_MAX_TRADES = 200

# Reconnection backoff
RECONNECT_MIN_WAIT = 2
RECONNECT_MAX_WAIT = 60


# ---------------------------------------------------------------------------
# Config helpers — price feed (scheduler_config.json)
# ---------------------------------------------------------------------------
SCHEDULER_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                      "scheduler_config.json")


def load_live_price_config():
    """Load live price feed config from scheduler_config.json."""
    defaults = {
        "enabled": False,
        "auto_start": True,
        "ws_url": DEFAULT_WS_URL,
        "flush_enabled": False,
        "flush_interval_minutes": DEFAULT_FLUSH_INTERVAL,
        "flush_on_close": True,
        "flush_watchlists": ["Priority"],
    }
    try:
        with open(SCHEDULER_CONFIG_PATH, "r", encoding="utf-8") as f:
            saved = json.load(f)
        if "live_price_feed" in saved and isinstance(saved["live_price_feed"], dict):
            result = dict(defaults)
            result.update(saved["live_price_feed"])
            return result
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    return defaults


def save_live_price_config(config):
    """Save live price feed config to scheduler_config.json (merges with existing)."""
    try:
        with open(SCHEDULER_CONFIG_PATH, "r", encoding="utf-8") as f:
            full = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        full = {}
    full["live_price_feed"] = config
    with open(SCHEDULER_CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(full, f, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Config helpers — End of Day compute (scheduler_config.json)
# ---------------------------------------------------------------------------

def load_eod_compute_config():
    """Load end-of-day compute config from scheduler_config.json."""
    defaults = {
        "enabled": True,
        "rs": {
            "enabled": True,
            "delay_minutes": 30,
            "universe": "Russell3000",
            "workers": 8,
        },
        "hvc": {
            "enabled": True,
            "delay_minutes": 30,
            "watchlists": ["Priority"],
            "workers": 8,
        },
    }
    try:
        with open(SCHEDULER_CONFIG_PATH, "r", encoding="utf-8") as f:
            saved = json.load(f)
        if "eod_compute" in saved and isinstance(saved["eod_compute"], dict):
            result = dict(defaults)
            for k in ("enabled",):
                if k in saved["eod_compute"]:
                    result[k] = saved["eod_compute"][k]
            if "rs" in saved["eod_compute"] and isinstance(saved["eod_compute"]["rs"], dict):
                result["rs"] = dict(defaults["rs"])
                result["rs"].update(saved["eod_compute"]["rs"])
            if "hvc" in saved["eod_compute"] and isinstance(saved["eod_compute"]["hvc"], dict):
                result["hvc"] = dict(defaults["hvc"])
                result["hvc"].update(saved["eod_compute"]["hvc"])
            return result
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    return defaults


def save_eod_compute_config(config):
    """Save end-of-day compute config to scheduler_config.json (merges with existing)."""
    try:
        with open(SCHEDULER_CONFIG_PATH, "r", encoding="utf-8") as f:
            full = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        full = {}
    full["eod_compute"] = config
    with open(SCHEDULER_CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(full, f, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Config helpers — indicator compute (scheduler_config.json)
# ---------------------------------------------------------------------------

def load_indicator_compute_config():
    """Load indicator computation config from scheduler_config.json."""
    defaults = {
        "enabled": False,
        "interval_minutes": 30,
        "watchlists": ["Priority"],
        "workers": 8,
    }
    try:
        with open(SCHEDULER_CONFIG_PATH, "r", encoding="utf-8") as f:
            saved = json.load(f)
        if "indicator_compute" in saved and isinstance(saved["indicator_compute"], dict):
            result = dict(defaults)
            result.update(saved["indicator_compute"])
            return result
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    return defaults


def save_indicator_compute_config(config):
    """Save indicator computation config to scheduler_config.json (merges)."""
    try:
        with open(SCHEDULER_CONFIG_PATH, "r", encoding="utf-8") as f:
            full = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        full = {}
    full["indicator_compute"] = config
    with open(SCHEDULER_CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(full, f, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Config helpers — sweep scanner (sweep_detection_config.json)
# ---------------------------------------------------------------------------
SWEEP_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "sweep_detection_config.json")


def load_live_config():
    """Load live scanner config from sweep_detection_config.json."""
    defaults = {
        "auto_start": True,
        "detection_interval_minutes": DEFAULT_DETECTION_INTERVAL,
        "ws_url": DEFAULT_WS_URL,
        "min_notional": MIN_SWEEP_NOTIONAL,
        "buffer_flush_seconds": DEFAULT_BUFFER_FLUSH_SECONDS,
        "buffer_max_trades": DEFAULT_BUFFER_MAX_TRADES,
        # Auto-fetch queue
        "auto_fetch_enabled": True,
        "auto_fetch_mode": "periodic",
        "auto_fetch_interval_minutes": 30,
        "auto_fetch_time": "18:00",
        "auto_fetch_lookback_years": 10,
        "auto_fetch_price_data": True,
    }
    try:
        with open(SWEEP_CONFIG_PATH, "r", encoding="utf-8") as f:
            saved = json.load(f)
        if "live_scanner" in saved and isinstance(saved["live_scanner"], dict):
            result = dict(defaults)
            result.update(saved["live_scanner"])
            return result
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    return defaults


def save_live_config(config):
    """Save live scanner config to sweep_detection_config.json (merges with existing)."""
    try:
        with open(SWEEP_CONFIG_PATH, "r", encoding="utf-8") as f:
            full = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        full = {}
    full["live_scanner"] = config
    with open(SWEEP_CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(full, f, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Database helpers (sweep writes)
# ---------------------------------------------------------------------------
def _get_db():
    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), DB_PATH)
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def _batch_write(trades):
    """Write a batch of sweep trade dicts to DB. Returns count of new inserts."""
    if not trades:
        return 0

    conn = _get_db()
    c = conn.cursor()
    inserted = 0
    batch_notional = 0.0
    batch_tickers = set()
    batch_dates = set()
    dirty_pairs = set()

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
                batch_notional += t["notional"]
                batch_tickers.add(t["ticker"])
                batch_dates.add(t["trade_date"])
                dirty_pairs.add((t["ticker"], t["trade_date"]))
        except sqlite3.IntegrityError:
            pass

    if inserted > 0 and batch_dates:
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
        row = c.execute(
            "SELECT COUNT(DISTINCT ticker) FROM sweep_trades WHERE is_darkpool=1 AND is_sweep=1"
        ).fetchone()
        if row:
            c.execute("UPDATE sweep_stats_cache SET tickers_tracked=? WHERE id=1", (row[0],))

    for tk, dt in dirty_pairs:
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

    for tk, dt in dirty_pairs:
        c.execute("""
            INSERT OR REPLACE INTO sweep_fetch_log
            (ticker, trade_date, sweeps_found, total_trades, fetched_at)
            VALUES (?, ?,
                COALESCE((SELECT sweeps_found FROM sweep_fetch_log WHERE ticker=? AND trade_date=?), 0) + ?,
                -1, ?)
        """, (tk, dt, tk, dt, 1, datetime.now(timezone.utc).isoformat()))

    conn.commit()
    conn.close()
    return inserted


# ---------------------------------------------------------------------------
# TickerBars — per-ticker in-memory bar storage (price side)
# ---------------------------------------------------------------------------
class TickerBars:
    """In-memory OHLCV storage for a single ticker for the current trading day."""

    def __init__(self, ticker, trade_date):
        self.ticker = ticker
        self.trade_date = trade_date
        self.minutes = []
        self.hourly = []
        self.daily = None
        self.last_minute_ts = 0

        self._current_hour = None
        self._hour_accum = None

    def add_minute(self, bar, bar_hour):
        self.minutes.append(bar)
        self.last_minute_ts = bar["t"]

        if self._current_hour is not None and self._current_hour != bar_hour:
            if self._hour_accum:
                self.hourly.append(dict(self._hour_accum))
            self._hour_accum = None

        if self._hour_accum is None:
            self._current_hour = bar_hour
            self._hour_accum = {
                "t": bar["t"], "o": bar["o"], "h": bar["h"],
                "l": bar["l"], "c": bar["c"], "v": bar["v"],
            }
        else:
            self._hour_accum["h"] = max(self._hour_accum["h"], bar["h"])
            self._hour_accum["l"] = min(self._hour_accum["l"], bar["l"])
            self._hour_accum["c"] = bar["c"]
            self._hour_accum["v"] += bar["v"]

        if self.daily is None:
            self.daily = {
                "t": bar["t"], "o": bar["o"], "h": bar["h"],
                "l": bar["l"], "c": bar["c"], "v": bar["v"],
            }
        else:
            self.daily["h"] = max(self.daily["h"], bar["h"])
            self.daily["l"] = min(self.daily["l"], bar["l"])
            self.daily["c"] = bar["c"]
            self.daily["v"] += bar["v"]

    def snapshot(self):
        return {
            "trade_date": self.trade_date,
            "hourly": list(self.hourly),
            "current_hour": dict(self._hour_accum) if self._hour_accum else None,
            "daily": dict(self.daily) if self.daily else None,
            "minute_count": len(self.minutes),
        }


# ---------------------------------------------------------------------------
# UnifiedLiveDaemon
# ---------------------------------------------------------------------------
class UnifiedLiveDaemon:
    """Single WebSocket connection handling both price bars (AM.*) and sweep trades (T.*).

    Subscribes to AM.*,T.* on one Polygon delayed WebSocket, dispatches:
      - AM events → price bar aggregation + periodic CSV flush
      - T  events → sweep filter + buffer + DB write + periodic detection
    """

    def __init__(self, price_config=None, sweep_config=None):
        pcfg = price_config or {}
        scfg = sweep_config or {}

        self._api_key = MASSIVE_API_KEY
        self._ws_url = pcfg.get("ws_url") or scfg.get("ws_url") or DEFAULT_WS_URL

        # Price settings
        self._flush_enabled = pcfg.get("flush_enabled", True)
        self._flush_interval = pcfg.get("flush_interval_minutes", DEFAULT_FLUSH_INTERVAL)
        self._flush_on_close = pcfg.get("flush_on_close", True)
        self._flush_watchlists = pcfg.get("flush_watchlists", ["Priority"])
        # Sweep settings
        self._min_notional = scfg.get("min_notional", MIN_SWEEP_NOTIONAL)
        self._detection_interval = scfg.get("detection_interval_minutes", DEFAULT_DETECTION_INTERVAL)
        self._sweep_flush_seconds = scfg.get("buffer_flush_seconds", DEFAULT_BUFFER_FLUSH_SECONDS)
        self._sweep_flush_max = scfg.get("buffer_max_trades", DEFAULT_BUFFER_MAX_TRADES)

        # Auto-fetch queue settings
        self._auto_fetch_enabled = scfg.get("auto_fetch_enabled", True)
        self._auto_fetch_mode = scfg.get("auto_fetch_mode", "periodic")  # periodic|scheduled|manual
        self._auto_fetch_interval = scfg.get("auto_fetch_interval_minutes", 30)
        self._auto_fetch_time = scfg.get("auto_fetch_time", "18:00")  # HH:MM local
        self._auto_fetch_lookback_years = scfg.get("auto_fetch_lookback_years", 10)
        self._auto_fetch_price_data = scfg.get("auto_fetch_price_data", True)
        self._pending_fetch_tickers = set()
        self._pending_fetch_lock = threading.Lock()
        self._known_fetched_tickers = set()   # tickers with ≥10 days in sweep_fetch_log
        self._known_fetched_loaded = False
        self._load_pending_queue()  # restore queue from disk (survives restart)

        # Connection state
        self._ws = None
        self._thread = None
        self._price_flush_thread = None
        self._sweep_flush_thread = None
        self._detection_thread = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

        # ── Price state ──
        self._bars = {}
        self._bars_lock = threading.Lock()
        self._trading_date = None
        self._price_msg_count = 0         # rolling count for msgs/s
        self._price_msg_window_start = 0  # epoch second of current window
        self._flush_tickers = None    # set of safe-name tickers from watchlists
        self._flush_tickers_ts = 0    # last refresh time

        # ── Sweep state ──
        self._sweep_buffer = []
        self._sweep_buffer_lock = threading.Lock()
        self._trade_count = 0             # rolling count for trades/s
        self._trade_window_start = 0      # epoch second of current window
        self._last_trade_ts = 0           # monotonic timestamp of last T.* message

        # ── Inbound message queue ──
        # WebSocket _on_message dumps raw strings here instantly.
        # A separate consumer thread drains + processes them so the
        # WS reader thread never blocks and Polygon's buffer won't overflow.
        self._msg_queue = queue.Queue(maxsize=0)  # unbounded
        self._msg_consumer_thread = None

        # ── Combined status ──
        self._status = {
            "running": False,
            "connected": False,
            "authenticated": False,
            "subscribed": False,
            "started_at": None,
            "reconnect_count": 0,
            "error": None,
            # Price stats
            "price_messages_received": 0,
            "price_messages_per_sec": 0.0,
            "price_tickers_active": 0,
            "price_last_flush_at": None,
            "price_last_message_at": None,
            # Sweep stats
            "trades_received": 0,
            "trades_per_sec": 0.0,
            "sweeps_today": 0,
            "sweeps_buffered": 0,
            "sweeps_written": 0,
            "last_sweep": None,
            "recent_sweeps": deque(maxlen=20),
            "sweep_tickers_active": set(),
            "events_detected": {"clusterbomb": 0, "rare_sweep": 0, "monster_sweep": 0,
                                "etf_daily": 0, "etf_rare_sweep": 0, "etf_ranked": 0},
            "events_today": {},  # cumulative daily detection totals
            "_events_date": None,
            "last_detection_at": None,
            "last_sweep_flush_at": None,
            # Crash tracking
            "crash_count": 0,
            "last_crash_at": None,
            "last_crash_error": None,
            # EOD compute status
            "eod_rs_last_at": None,
            "eod_rs_last_result": None,
            "eod_rs_last_duration_s": 0,
            "eod_rs_last_error": None,
            "eod_rs_last_date": None,
            "eod_hvc_last_at": None,
            "eod_hvc_last_result": None,
            "eod_hvc_last_duration_s": 0,
            "eod_hvc_last_error": None,
            "eod_hvc_last_date": None,
            # Indicator compute status (subprocess)
            "compute_last_at": None,
            "compute_last_result": None,
            "compute_last_duration_s": 0,
            "compute_last_error": None,
            # Auto-fetch queue status
            "auto_fetch_pending": 0,
            "auto_fetch_last_at": None,
            "auto_fetch_last_result": None,
            "auto_fetch_last_tickers": 0,
            "auto_fetch_last_duration_s": 0,
            "auto_fetch_total_fetched": 0,
        }
        self._eod_compute_thread = None
        self._eod_config = load_eod_compute_config()
        self._indicator_compute_thread = None
        self._indicator_compute_config = load_indicator_compute_config()
        self._auto_fetch_thread = None

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def start(self):
        if websocket is None:
            raise RuntimeError("websocket-client not installed. Run: pip install websocket-client")

        with self._lock:
            if self._status["running"]:
                return
            self._stop_event.clear()
            self._status["running"] = True
            self._status["started_at"] = datetime.now(timezone.utc).isoformat()
            self._status["error"] = None
            self._status["connected"] = False
            self._status["authenticated"] = False
            self._status["subscribed"] = False

        self._thread = threading.Thread(target=self._run_loop, daemon=True,
                                        name="unified-live-daemon")
        self._thread.start()
        print(f"[LIVE] Unified daemon started — connecting to {self._ws_url}", flush=True)

    def stop(self):
        print("[LIVE] Stopping unified daemon...", flush=True)
        self._stop_event.set()

        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass

        # Final flushes
        try:
            self._flush_sweep_buffer()
        except Exception as e:
            print(f"[LIVE] Final sweep flush error: {e}", flush=True)
        try:
            self._flush_price_csv()
        except Exception as e:
            print(f"[LIVE] Final price flush error: {e}", flush=True)

        with self._lock:
            self._status["running"] = False
            self._status["connected"] = False
            self._status["authenticated"] = False
            self._status["subscribed"] = False

        print("[LIVE] Unified daemon stopped.", flush=True)

    def get_status(self):
        """Return full combined status dict (JSON-safe)."""
        with self._lock:
            s = dict(self._status)

        # Truthful status: if main thread is dead but status says running, correct it
        if s["running"] and (self._thread is None or not self._thread.is_alive()):
            with self._lock:
                self._status["running"] = False
                self._status["connected"] = False
                if not self._status.get("error"):
                    self._status["error"] = "Main thread died unexpectedly"
            s["running"] = False
            s["connected"] = False
            s["error"] = self._status["error"]

        # Compute uptime
        if s["running"] and s.get("started_at"):
            try:
                started = datetime.fromisoformat(s["started_at"])
                s["uptime_seconds"] = int((datetime.now(timezone.utc) - started).total_seconds())
            except Exception:
                s["uptime_seconds"] = 0
        else:
            s["uptime_seconds"] = 0

        with self._bars_lock:
            s["price_tickers_active"] = len(self._bars)
        s["sweep_tickers_active"] = len(s["sweep_tickers_active"])
        s["recent_sweeps"] = list(s.get("recent_sweeps", []))
        with self._sweep_buffer_lock:
            s["sweeps_buffered"] = len(self._sweep_buffer)
        # Expose flush config for UI
        s["flush_enabled"] = self._flush_enabled
        s["flush_watchlists"] = list(self._flush_watchlists or [])
        # Auto-fetch pending count (live from lock, not status dict)
        with self._pending_fetch_lock:
            s["auto_fetch_pending"] = len(self._pending_fetch_tickers)
        return s

    def get_sweep_status(self):
        """Return sweep-focused status dict matching old LiveSweepDaemon.get_status() shape."""
        full = self.get_status()
        return {
            "running": full["running"],
            "connected": full["connected"],
            "authenticated": full["authenticated"],
            "subscribed": full["subscribed"],
            "started_at": full["started_at"],
            "trades_received": full["trades_received"],
            "trades_per_sec": full["trades_per_sec"],
            "sweeps_today": full["sweeps_today"],
            "sweeps_buffered": full["sweeps_buffered"],
            "sweeps_written": full["sweeps_written"],
            "last_sweep": full["last_sweep"],
            "recent_sweeps": full.get("recent_sweeps", []),
            "tickers_active": full["sweep_tickers_active"],
            "events_detected": full["events_detected"],
            "last_detection_at": full["last_detection_at"],
            "last_flush_at": full["last_sweep_flush_at"],
            "reconnect_count": full["reconnect_count"],
            "error": full["error"],
            "uptime_seconds": full.get("uptime_seconds", 0),
            "crash_count": full.get("crash_count", 0),
            "last_crash_at": full.get("last_crash_at"),
            "last_crash_error": full.get("last_crash_error"),
            # Auto-fetch queue
            "auto_fetch_pending": full.get("auto_fetch_pending", 0),
            "auto_fetch_last_at": full.get("auto_fetch_last_at"),
            "auto_fetch_last_result": full.get("auto_fetch_last_result"),
            "auto_fetch_last_tickers": full.get("auto_fetch_last_tickers", 0),
            "auto_fetch_total_fetched": full.get("auto_fetch_total_fetched", 0),
        }

    def get_price_status(self):
        """Return price-focused status dict matching old LivePriceDaemon.get_status() shape."""
        full = self.get_status()
        return {
            "running": full["running"],
            "connected": full["connected"],
            "authenticated": full["authenticated"],
            "subscribed": full["subscribed"],
            "started_at": full["started_at"],
            "messages_received": full["price_messages_received"],
            "messages_per_sec": full["price_messages_per_sec"],
            "tickers_active": full["price_tickers_active"],
            "last_flush_at": full["price_last_flush_at"],
            "last_message_at": full["price_last_message_at"],
            "reconnect_count": full["reconnect_count"],
            "error": full["error"],
        }

    def update_config(self, price_cfg=None, sweep_cfg=None):
        """Hot-reload config on a running daemon (no restart needed).

        Updates runtime attributes that are read by loop threads.
        Thread-safe for simple types under GIL.
        """
        changed = []
        if price_cfg:
            fe = price_cfg.get("flush_enabled")
            if fe is not None and fe != self._flush_enabled:
                self._flush_enabled = fe
                changed.append(f"flush_enabled={fe}")
            new_flush = price_cfg.get("flush_interval_minutes")
            if new_flush and new_flush != self._flush_interval:
                self._flush_interval = max(1, new_flush)
                changed.append(f"flush_interval={self._flush_interval}m")
            fc = price_cfg.get("flush_on_close")
            if fc is not None and fc != self._flush_on_close:
                self._flush_on_close = fc
                changed.append(f"flush_on_close={fc}")
            fw = price_cfg.get("flush_watchlists")
            if fw is not None and fw != self._flush_watchlists:
                self._flush_watchlists = fw
                self._flush_tickers = None  # force refresh on next flush
                self._flush_tickers_ts = 0
                changed.append(f"flush_watchlists={fw}")
        if sweep_cfg:
            new_mn = sweep_cfg.get("min_notional")
            if new_mn and new_mn != self._min_notional:
                self._min_notional = new_mn
                changed.append(f"min_notional={new_mn}")
            new_di = sweep_cfg.get("detection_interval_minutes")
            if new_di and new_di != self._detection_interval:
                self._detection_interval = max(1, new_di)
                changed.append(f"detection_interval={self._detection_interval}m")
            # Auto-fetch hot-reload
            for key, attr, transform in [
                ("auto_fetch_enabled", "_auto_fetch_enabled", None),
                ("auto_fetch_mode", "_auto_fetch_mode", None),
                ("auto_fetch_interval_minutes", "_auto_fetch_interval",
                 lambda x: max(5, x)),
                ("auto_fetch_time", "_auto_fetch_time", None),
                ("auto_fetch_price_data", "_auto_fetch_price_data", None),
            ]:
                val = sweep_cfg.get(key)
                if val is not None and val != getattr(self, attr):
                    setattr(self, attr,
                            transform(val) if transform else val)
                    changed.append(f"{key}={getattr(self, attr)}")
        if changed:
            print(f"[LIVE] Config hot-reloaded: {', '.join(changed)}", flush=True)
        return changed

    def get_latest_bars(self, ticker):
        ticker = ticker.upper()
        with self._bars_lock:
            tb = self._bars.get(ticker)
            if not tb:
                return None
            return tb.snapshot()

    def get_latest_daily(self, ticker):
        ticker = ticker.upper()
        with self._bars_lock:
            tb = self._bars.get(ticker)
            if not tb or not tb.daily:
                return None
            return dict(tb.daily)

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def _run_loop(self):
        """Top-level wrapper — catches fatal exceptions so status is always correct."""
        try:
            self._run_loop_inner()
        except Exception as e:
            print(f"[LIVE] FATAL: _run_loop crashed: {e}", flush=True)
            _daemon_log("fatal_crash", str(e)[:200])
            traceback.print_exc()
            with self._lock:
                self._status["running"] = False
                self._status["connected"] = False
                self._status["error"] = f"Fatal crash: {e}"
                self._status["crash_count"] = self._status.get("crash_count", 0) + 1
                self._status["last_crash_at"] = datetime.now(timezone.utc).isoformat()
                self._status["last_crash_error"] = str(e)

    def _start_sub_thread(self, attr, target, name):
        """Start (or restart) a sub-thread, storing its reference."""
        t = threading.Thread(target=target, daemon=True, name=name)
        t.start()
        setattr(self, attr, t)

    def _revive_sub_threads(self):
        """Check sub-threads and restart any that have died."""
        threads = [
            ("_msg_consumer_thread", self._msg_consumer_loop, "live-msg-consumer"),
            ("_price_flush_thread", self._price_flush_loop, "live-price-flush"),
            ("_sweep_flush_thread", self._sweep_flush_loop, "live-sweep-flush"),
            ("_detection_thread", self._detection_loop, "live-detection"),
            ("_eod_compute_thread", self._eod_compute_loop, "live-eod-compute"),
            ("_indicator_compute_thread", self._indicator_compute_loop, "live-indicator-compute"),
            ("_heartbeat_thread", self._heartbeat_loop, "live-heartbeat"),
            ("_auto_fetch_thread", self._auto_fetch_loop, "live-auto-fetch"),
        ]
        for attr, target, name in threads:
            t = getattr(self, attr, None)
            if t is None or not t.is_alive():
                print(f"[LIVE] Sub-thread '{name}' is dead — restarting", flush=True)
                self._start_sub_thread(attr, target, name)

    def _run_loop_inner(self):
        self._reconnect_wait = RECONNECT_MIN_WAIT

        _daemon_log("daemon_start", "Unified daemon starting")

        # Start message consumer FIRST — must be ready before WS connects
        self._start_sub_thread("_msg_consumer_thread", self._msg_consumer_loop, "live-msg-consumer")
        # Start background timer threads (store references for health checks)
        self._start_sub_thread("_price_flush_thread", self._price_flush_loop, "live-price-flush")
        self._start_sub_thread("_sweep_flush_thread", self._sweep_flush_loop, "live-sweep-flush")
        self._start_sub_thread("_detection_thread", self._detection_loop, "live-detection")
        self._start_sub_thread("_eod_compute_thread", self._eod_compute_loop, "live-eod-compute")
        self._start_sub_thread("_indicator_compute_thread", self._indicator_compute_loop, "live-indicator-compute")
        self._start_sub_thread("_heartbeat_thread", self._heartbeat_loop, "live-heartbeat")
        self._start_sub_thread("_auto_fetch_thread", self._auto_fetch_loop, "live-auto-fetch")

        while not self._stop_event.is_set():
            # Revive any dead sub-threads before each connection attempt
            self._revive_sub_threads()

            try:
                self._connect_and_run()
            except Exception as e:
                with self._lock:
                    self._status["error"] = str(e)
                    self._status["connected"] = False
                print(f"[LIVE] WebSocket error: {e}", flush=True)
                traceback.print_exc()

            if self._stop_event.is_set():
                break

            with self._lock:
                self._status["reconnect_count"] += 1
            _daemon_log("reconnecting", f"wait={self._reconnect_wait}s")
            print(f"[LIVE] Reconnecting in {self._reconnect_wait}s...", flush=True)
            self._stop_event.wait(timeout=self._reconnect_wait)
            self._reconnect_wait = min(self._reconnect_wait * 2, RECONNECT_MAX_WAIT)

    def _connect_and_run(self):
        self._ws = websocket.WebSocketApp(
            self._ws_url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        self._ws.run_forever(ping_interval=0)

    # ------------------------------------------------------------------
    # WebSocket callbacks
    # ------------------------------------------------------------------

    def _on_open(self, ws):
        print("[LIVE] WebSocket connected, authenticating...", flush=True)
        _daemon_log("ws_connected", "Authenticating...")
        with self._lock:
            self._status["connected"] = True
            self._status["error"] = None
        ws.send(json.dumps({"action": "auth", "params": self._api_key}))

    def _on_message(self, ws, message):
        # Absolute minimum work: shove raw string into queue and return.
        # This keeps the WebSocket reader thread unblocked so Polygon's
        # server-side buffer never overflows (which causes close 1008).
        self._msg_queue.put_nowait(message)

    def _msg_consumer_loop(self):
        """Drain the message queue and dispatch to handlers.

        Runs on its own thread so the WS reader is never blocked by
        JSON parsing, lock acquisitions, or any processing.
        """
        while not self._stop_event.is_set():
            try:
                raw = self._msg_queue.get(timeout=1.0)
            except queue.Empty:
                continue
            try:
                msgs = json.loads(raw)
                if not isinstance(msgs, list):
                    msgs = [msgs]
                for msg in msgs:
                    ev = msg.get("ev")
                    if ev == "status":
                        self._handle_status_msg(self._ws, msg)
                    elif ev == "AM":
                        self._handle_minute_bar(msg)
                    elif ev == "T":
                        self._handle_trade(msg)
            except json.JSONDecodeError:
                pass
            except Exception as e:
                print(f"[LIVE] Message consumer error: {e}", flush=True)

    def _on_error(self, ws, error):
        with self._lock:
            self._status["error"] = str(error)
        _daemon_log("ws_error", str(error)[:200])
        print(f"[LIVE] WebSocket error: {error}", flush=True)

    def _on_close(self, ws, close_status_code, close_msg):
        with self._lock:
            self._status["connected"] = False
            self._status["authenticated"] = False
            self._status["subscribed"] = False
        reason = f"code={close_status_code} msg={close_msg}" if close_status_code else "clean"
        _daemon_log("ws_closed", reason)
        print(f"[LIVE] WebSocket closed ({reason})", flush=True)

    # ------------------------------------------------------------------
    # Status message handler
    # ------------------------------------------------------------------

    def _handle_status_msg(self, ws, msg):
        status = msg.get("status", "")
        message = msg.get("message", "")

        if status == "auth_success":
            print("[LIVE] Authenticated. Subscribing to AM.*,T.*...", flush=True)
            _daemon_log("auth_success", "Subscribing to AM.*,T.*")
            with self._lock:
                self._status["authenticated"] = True
            # Subscribe to BOTH channels in one call
            ws.send(json.dumps({"action": "subscribe", "params": "AM.*,T.*"}))

        elif status == "auth_failed":
            _daemon_log("auth_failed", message)
            print(f"[LIVE] Authentication FAILED: {message}", flush=True)
            with self._lock:
                self._status["error"] = f"Auth failed: {message}"
            ws.close()

        elif status == "success" and "subscribed" in message.lower():
            print(f"[LIVE] {message}", flush=True)
            _daemon_log("subscribed", message)
            with self._lock:
                self._status["subscribed"] = True
            self._reconnect_wait = RECONNECT_MIN_WAIT

        elif status == "error":
            print(f"[LIVE] Server error: {message}", flush=True)
            with self._lock:
                self._status["error"] = message

    # ------------------------------------------------------------------
    # Price handler (AM.* messages)
    # ------------------------------------------------------------------

    def _handle_minute_bar(self, msg):
        now = time.time()
        now_sec = int(now)

        # Cheap per-second counter instead of O(n) list rebuild
        if now_sec != self._price_msg_window_start:
            with self._lock:
                self._status["price_messages_per_sec"] = self._price_msg_count
            self._price_msg_count = 0
            self._price_msg_window_start = now_sec
        self._price_msg_count += 1

        with self._lock:
            self._status["price_messages_received"] += 1
            self._status["price_last_message_at"] = datetime.now(timezone.utc).isoformat()

        ticker = msg.get("sym", "")
        if not ticker:
            return

        bar_start_ms = msg.get("s", 0)
        o = msg.get("o", 0)
        h = msg.get("h", 0)
        l = msg.get("l", 0)
        c = msg.get("c", 0)
        v = msg.get("v", 0)

        if not bar_start_ms or not o:
            return

        bar_ts = bar_start_ms / 1000.0
        bar_dt = datetime.fromtimestamp(bar_ts, tz=timezone.utc)
        trade_date = bar_dt.strftime("%Y-%m-%d")
        bar_hour = bar_dt.hour

        if self._trading_date and self._trading_date != trade_date:
            self._on_day_rollover(trade_date)
        self._trading_date = trade_date

        minute_bar = {
            "t": int(bar_ts), "o": float(o), "h": float(h),
            "l": float(l), "c": float(c), "v": int(v),
        }

        with self._bars_lock:
            if ticker not in self._bars:
                self._bars[ticker] = TickerBars(ticker, trade_date)
            tb = self._bars[ticker]
            if minute_bar["t"] <= tb.last_minute_ts:
                return
            tb.add_minute(minute_bar, bar_hour)

    def _on_day_rollover(self, new_date):
        print(f"[LIVE] Day rollover: {self._trading_date} → {new_date}. "
              f"Flushing {len(self._bars)} tickers...", flush=True)
        self._flush_price_csv()
        with self._bars_lock:
            self._bars.clear()

    # ------------------------------------------------------------------
    # Sweep handler (T.* messages)
    # ------------------------------------------------------------------

    def _handle_trade(self, msg):
        now = time.time()
        now_sec = int(now)
        self._last_trade_ts = now

        # Cheap per-second counter instead of O(n) list rebuild
        if now_sec != self._trade_window_start:
            with self._lock:
                self._status["trades_per_sec"] = self._trade_count
                self._status["trades_received"] += self._trade_count
            self._trade_count = 0
            self._trade_window_start = now_sec
        self._trade_count += 1

        # Filter: dark pool intermarket sweep with significant notional
        exchange = msg.get("x", 0)
        if exchange != FINRA_TRF_EXCHANGE_ID:
            return  # fast reject: not dark pool

        conditions = msg.get("c", [])
        if INTERMARKET_SWEEP_CONDITION not in conditions:
            return  # fast reject: not a sweep

        price = msg.get("p", 0)
        size = msg.get("s", 0)
        notional = price * size
        if notional < self._min_notional:
            return  # fast reject: below threshold

        ticker = msg.get("sym", "")
        sip_ts = msg.get("t", 0)
        trf_id = msg.get("trfi")

        # Polygon WebSocket sends SIP timestamp — detect precision:
        #   Nanoseconds  (2025-2030): ~1.74e18 – 1.90e18
        #   Microseconds (2025-2030): ~1.74e15 – 1.90e15
        #   Milliseconds (2025-2030): ~1.74e12 – 1.90e12
        if sip_ts > 1e17:
            trade_dt = datetime.fromtimestamp(sip_ts / 1e9, tz=timezone.utc)
            sip_ts_ns = sip_ts
        elif sip_ts > 1e14:
            trade_dt = datetime.fromtimestamp(sip_ts / 1e6, tz=timezone.utc)
            sip_ts_ns = sip_ts * 1000
        else:
            trade_dt = datetime.fromtimestamp(sip_ts / 1e3, tz=timezone.utc)
            sip_ts_ns = sip_ts * 1000000

        trade_record = {
            "ticker": ticker,
            "trade_date": trade_dt.strftime("%Y-%m-%d"),
            "trade_time": trade_dt.strftime("%H:%M:%S.%f")[:-3],
            "sip_timestamp": sip_ts_ns,
            "price": price,
            "size": size,
            "notional": round(notional, 2),
            "exchange": exchange,
            "trf_id": trf_id,
            "conditions": json.dumps(conditions),
            "is_sweep": 1,
            "is_darkpool": 1,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }

        # Single lock acquisition for buffer + stats (never do DB writes here)
        with self._sweep_buffer_lock:
            self._sweep_buffer.append(trade_record)

        with self._lock:
            self._status["sweeps_today"] += 1
            self._status["sweep_tickers_active"].add(ticker)
            sweep_entry = {
                "ticker": ticker,
                "notional": round(notional, 2),
                "time": trade_dt.strftime("%H:%M:%S"),
                "price": price,
            }
            self._status["last_sweep"] = sweep_entry
            self._status["recent_sweeps"].appendleft(sweep_entry)
        # Buffer flush is handled ONLY by the timer thread (_sweep_flush_loop)
        # — never block the WebSocket reader thread with DB writes

    # ------------------------------------------------------------------
    # Price CSV flush
    # ------------------------------------------------------------------

    def _price_flush_loop(self):
        interval_s = self._flush_interval * 60
        while not self._stop_event.is_set():
            try:
                self._stop_event.wait(timeout=interval_s)
                if not self._stop_event.is_set():
                    if not self._flush_enabled:
                        continue  # flush disabled — skip but keep loop alive
                    try:
                        self._flush_price_csv()
                    except Exception as e:
                        print(f"[LIVE] Price flush error: {e}", flush=True)
                        traceback.print_exc()
            except Exception as e:
                print(f"[LIVE] CRITICAL: price_flush_loop unexpected error: {e}", flush=True)
                traceback.print_exc()
                self._stop_event.wait(timeout=10)  # cooldown before retry

    def _refresh_flush_tickers(self):
        """Build set of safe-name tickers from flush_watchlists config."""
        try:
            from config import _load_watchlists
            wl_names = self._flush_watchlists or ["Priority"]
            all_wl = _load_watchlists()
            tickers = set()
            for wl_name in wl_names:
                groups = all_wl.get(wl_name, [])
                for _group_name, ticker_list in groups:
                    for display, _api, _atype in ticker_list:
                        safe = display.replace(":", "_").replace("/", "_").upper()
                        tickers.add(safe)
            self._flush_tickers = tickers
            self._flush_tickers_ts = time.time()
            print(f"[LIVE] Flush scope: {len(tickers)} tickers from "
                  f"watchlists {wl_names}", flush=True)
        except Exception as e:
            print(f"[LIVE] Flush scope error: {e}", flush=True)
            self._flush_tickers = set()

    def _get_flush_tickers(self):
        """Return set of safe-name tickers from indicator compute watchlists.

        Only flushes watchlist tickers (small set, fast) — not all cached tickers.
        The broad universe gets its CSVs updated via REST scheduler instead.
        Refreshes every hour.
        """
        if self._flush_tickers is None or (time.time() - self._flush_tickers_ts > 3600):
            self._refresh_flush_tickers()
        return self._flush_tickers

    def _flush_price_csv(self):
        """Flush in-memory bars to CSV files.

        Flushes all tickers that have existing cache CSV files, keeping
        sweep page charts fresh for all tracked tickers.
        """
        flush_filter = self._get_flush_tickers()

        with self._bars_lock:
            if not self._bars:
                return
            tickers_data = {}
            for ticker, tb in self._bars.items():
                # Match websocket ticker against cache filenames
                safe = ticker.replace(":", "_").replace("/", "_").upper()
                if flush_filter is not None and safe not in flush_filter:
                    continue
                snap = tb.snapshot()
                if snap:
                    tickers_data[ticker] = snap

        if not tickers_data:
            return

        t0 = time.time()
        flushed_hourly = 0
        flushed_daily = 0
        total_bars = len(self._bars)

        print(f"[LIVE] Price flush starting: {len(tickers_data)} tickers to write "
              f"(of {total_bars} in memory)...", flush=True)

        os.makedirs(CACHE_DIR, exist_ok=True)

        for ticker, data in tickers_data.items():
            safe = ticker.replace(":", "_").replace("/", "_")
            trade_date = data.get("trade_date")
            if not trade_date:
                continue

            hourly_bars = data.get("hourly", [])
            current_hour = data.get("current_hour")
            all_hourly = list(hourly_bars)
            if current_hour:
                all_hourly.append(current_hour)

            if all_hourly:
                hourly_path = os.path.join(CACHE_DIR, f"{safe}_hour.csv")
                try:
                    self._merge_bars_to_csv(hourly_path, all_hourly, trade_date)
                    flushed_hourly += 1
                except Exception as e:
                    print(f"[LIVE] Hourly flush error for {ticker}: {e}", flush=True)

            daily_bar = data.get("daily")
            if daily_bar:
                daily_path = os.path.join(CACHE_DIR, f"{safe}_day.csv")
                try:
                    self._merge_bars_to_csv(daily_path, [daily_bar], trade_date)
                    flushed_daily += 1
                except Exception as e:
                    print(f"[LIVE] Daily flush error for {ticker}: {e}", flush=True)

        elapsed = time.time() - t0
        with self._lock:
            self._status["price_last_flush_at"] = datetime.now(timezone.utc).isoformat()

        print(f"[LIVE] Price flush: {flushed_hourly} hourly + {flushed_daily} daily "
              f"CSVs in {elapsed:.1f}s ({len(tickers_data)} tickers)", flush=True)

        # No return value needed — indicator compute is now a separate subprocess

    def _merge_bars_to_csv(self, csv_path, bars, trade_date):
        live_rows = []
        for b in bars:
            ts = b["t"]
            live_rows.append({
                "timestamp": pd.Timestamp.utcfromtimestamp(ts).tz_localize(None),
                "open": b["o"], "high": b["h"], "low": b["l"],
                "close": b["c"], "volume": b["v"],
            })

        if not live_rows:
            return

        live_df = pd.DataFrame(live_rows)

        existing_df = None
        if os.path.exists(csv_path):
            try:
                existing_df = pd.read_csv(csv_path, parse_dates=["timestamp"])
                if existing_df["timestamp"].dt.tz is not None:
                    existing_df["timestamp"] = existing_df["timestamp"].dt.tz_localize(None)
            except Exception:
                existing_df = None

        if existing_df is not None and not existing_df.empty:
            trade_date_start = pd.Timestamp(trade_date)
            trade_date_end = trade_date_start + pd.Timedelta(days=1)
            existing_df = existing_df[
                (existing_df["timestamp"] < trade_date_start) |
                (existing_df["timestamp"] >= trade_date_end)
            ]
            merged = pd.concat([existing_df, live_df], ignore_index=True)
        else:
            merged = live_df

        merged = merged.sort_values("timestamp").reset_index(drop=True)
        keep_cols = [c for c in ["timestamp", "open", "high", "low", "close",
                                  "volume", "vwap", "n_trades"] if c in merged.columns]
        merged[keep_cols].to_csv(csv_path, index=False)

    # ------------------------------------------------------------------
    # Auto-fetch: persistent queue (survives server restart)
    # ------------------------------------------------------------------

    def _save_pending_queue(self):
        """Persist pending fetch tickers to disk."""
        with self._pending_fetch_lock:
            data = sorted(self._pending_fetch_tickers)
        try:
            with open(PENDING_QUEUE_PATH, "w", encoding="utf-8") as f:
                json.dump(data, f)
        except Exception as e:
            print(f"[LIVE] Warning: could not save pending queue: {e}", flush=True)

    def _load_pending_queue(self):
        """Load persisted pending fetch tickers from disk."""
        try:
            with open(PENDING_QUEUE_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list) and data:
                with self._pending_fetch_lock:
                    self._pending_fetch_tickers.update(data)
                print(f"[LIVE] Auto-fetch: restored {len(data)} pending ticker(s) from disk",
                      flush=True)
        except (FileNotFoundError, json.JSONDecodeError):
            pass

    # ------------------------------------------------------------------
    # Auto-fetch: known tickers loader
    # ------------------------------------------------------------------

    def _load_known_fetched_tickers(self):
        """Load tickers with ≥10 days in sweep_fetch_log (have real history).
        Tickers with only today's live data (1 entry) are NOT considered known."""
        try:
            conn = sqlite3.connect(DB_PATH, timeout=5)
            rows = conn.execute(
                "SELECT ticker FROM sweep_fetch_log "
                "GROUP BY ticker HAVING COUNT(DISTINCT trade_date) >= 10"
            ).fetchall()
            conn.close()
            self._known_fetched_tickers = set(r[0] for r in rows)
            self._known_fetched_loaded = True
            print(f"[LIVE] Auto-fetch: {len(self._known_fetched_tickers)} "
                  f"known-fetched tickers loaded", flush=True)
        except Exception as e:
            print(f"[LIVE] Auto-fetch: failed to load known tickers: {e}", flush=True)
            self._known_fetched_tickers = set()
            self._known_fetched_loaded = True  # don't retry on every flush

    # ------------------------------------------------------------------
    # Sweep buffer flush
    # ------------------------------------------------------------------

    def _sweep_flush_loop(self):
        while not self._stop_event.is_set():
            try:
                self._stop_event.wait(timeout=self._sweep_flush_seconds)
                if not self._stop_event.is_set():
                    self._flush_sweep_buffer()
            except Exception as e:
                print(f"[LIVE] CRITICAL: sweep_flush_loop unexpected error: {e}", flush=True)
                traceback.print_exc()
                self._stop_event.wait(timeout=10)  # cooldown before retry

    def _flush_sweep_buffer(self):
        with self._sweep_buffer_lock:
            if not self._sweep_buffer:
                return
            batch = list(self._sweep_buffer)
            self._sweep_buffer.clear()
        self._do_sweep_flush(batch)

    def _do_sweep_flush(self, batch=None):
        if batch is None:
            with self._sweep_buffer_lock:
                if not self._sweep_buffer:
                    return
                batch = list(self._sweep_buffer)
                self._sweep_buffer.clear()

        if not batch:
            return

        try:
            inserted = _batch_write(batch)
            with self._lock:
                self._status["sweeps_written"] += inserted
                self._status["last_sweep_flush_at"] = datetime.now(timezone.utc).isoformat()

            if inserted > 0:
                tickers = set(t["ticker"] for t in batch)
                print(f"[LIVE] Sweep flush: {inserted} new sweeps ({len(batch)} total, "
                      f"{len(tickers)} tickers)", flush=True)

                # Auto-fetch: check for new tickers needing historical backfill
                if self._auto_fetch_enabled:
                    if not self._known_fetched_loaded:
                        self._load_known_fetched_tickers()
                    new_tickers = tickers - self._known_fetched_tickers
                    if new_tickers:
                        with self._pending_fetch_lock:
                            before = len(self._pending_fetch_tickers)
                            self._pending_fetch_tickers.update(new_tickers)
                            added = len(self._pending_fetch_tickers) - before
                        if added > 0:
                            self._save_pending_queue()
                            print(f"[LIVE] Auto-fetch: queued {added} new ticker(s): "
                                  f"{sorted(new_tickers)[:10]}"
                                  f"{'...' if len(new_tickers) > 10 else ''}",
                                  flush=True)
                            with self._lock:
                                self._status["auto_fetch_pending"] = \
                                    len(self._pending_fetch_tickers)
        except Exception as e:
            print(f"[LIVE] Sweep flush error: {e}", flush=True)
            traceback.print_exc()
            with self._sweep_buffer_lock:
                self._sweep_buffer = batch + self._sweep_buffer

    # ------------------------------------------------------------------
    # Detection cycle
    # ------------------------------------------------------------------

    def _detection_loop(self):
        self._stop_event.wait(timeout=self._detection_interval * 60)

        while not self._stop_event.is_set():
            try:
                self._run_detection()
            except Exception as e:
                print(f"[LIVE] Detection error: {e}", flush=True)
                traceback.print_exc()
            try:
                self._stop_event.wait(timeout=self._detection_interval * 60)
            except Exception as e:
                print(f"[LIVE] CRITICAL: detection_loop unexpected error: {e}", flush=True)
                traceback.print_exc()
                self._stop_event.wait(timeout=10)  # cooldown before retry

    def _run_detection(self):
        self._flush_sweep_buffer()

        print("[LIVE] Running detection cycle...", flush=True)
        t0 = time.time()

        try:
            from sweep_engine import detect_today
            results = detect_today()
        except Exception as e:
            print(f"[LIVE] Detection failed: {e}", flush=True)
            traceback.print_exc()
            return

        with self._lock:
            self._status["events_detected"] = results
            self._status["last_detection_at"] = datetime.now(timezone.utc).isoformat()
            # Accumulate daily totals (reset on date change)
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            if self._status.get("_events_date") != today:
                self._status["_events_date"] = today
                self._status["events_today"] = {}
            et = self._status["events_today"]
            for k, v in results.items():
                et[k] = et.get(k, 0) + v

        elapsed = time.time() - t0
        # Build stock/ETF delta summary
        stock_n = results.get("clusterbomb", 0) + results.get("rare_sweep", 0) + results.get("monster_sweep", 0)
        etf_n = results.get("etf_daily", 0) + results.get("etf_rare_sweep", 0) + results.get("etf_ranked", 0)
        parts = []
        if stock_n:
            sub = []
            if results.get("clusterbomb"): sub.append(f"{results['clusterbomb']} CB")
            if results.get("rare_sweep"):  sub.append(f"{results['rare_sweep']} rare")
            if results.get("monster_sweep"): sub.append(f"{results['monster_sweep']} monster")
            parts.append(f"Stock +{stock_n} ({', '.join(sub)})")
        if etf_n:
            sub = []
            if results.get("etf_daily"):      sub.append(f"{results['etf_daily']} daily")
            if results.get("etf_rare_sweep"): sub.append(f"{results['etf_rare_sweep']} rare")
            if results.get("etf_ranked"):     sub.append(f"{results['etf_ranked']} ranked")
            parts.append(f"ETF +{etf_n} ({', '.join(sub)})")
        if not parts:
            parts.append("no new events")
        print(f"[LIVE] Detection {elapsed:.1f}s — {' | '.join(parts)}", flush=True)

    # ------------------------------------------------------------------
    # Auto-fetch queue — backfill newly-discovered tickers
    # ------------------------------------------------------------------

    MAX_AUTO_FETCH_BATCH = 20  # cap per cycle to keep batches manageable

    def _auto_fetch_loop(self):
        """Dispatch to periodic, scheduled, or manual (idle) mode."""
        if not self._auto_fetch_enabled or self._auto_fetch_mode == "manual":
            mode = "manual" if self._auto_fetch_enabled else "disabled"
            print(f"[LIVE] Auto-fetch: {mode} — thread idle", flush=True)
            # Stay alive but idle (so _revive_sub_threads doesn't keep restarting)
            while not self._stop_event.is_set():
                self._stop_event.wait(timeout=300)
            return

        # Initial delay: wait for first detection cycle + 60s
        wait_s = (self._detection_interval * 60) + 60
        print(f"[LIVE] Auto-fetch: {self._auto_fetch_mode} mode, "
              f"waiting {wait_s}s for first detection cycle...", flush=True)
        self._stop_event.wait(timeout=wait_s)

        if self._auto_fetch_mode == "scheduled":
            self._auto_fetch_loop_scheduled()
        else:
            self._auto_fetch_loop_periodic()

    def _auto_fetch_loop_periodic(self):
        """Run auto-fetch every N minutes."""
        interval_s = max(self._auto_fetch_interval, 5) * 60
        print(f"[LIVE] Auto-fetch: periodic every {self._auto_fetch_interval}m", flush=True)
        while not self._stop_event.is_set():
            try:
                self._run_auto_fetch()
            except Exception as e:
                print(f"[LIVE] Auto-fetch error: {e}", flush=True)
                traceback.print_exc()
            self._stop_event.wait(timeout=interval_s)

    def _auto_fetch_loop_scheduled(self):
        """Run auto-fetch once daily at configured local time."""
        print(f"[LIVE] Auto-fetch: scheduled daily at {self._auto_fetch_time}", flush=True)
        last_run_date = None
        while not self._stop_event.is_set():
            now = datetime.now()
            today = now.strftime("%Y-%m-%d")
            try:
                target_h, target_m = map(int, self._auto_fetch_time.split(":"))
            except ValueError:
                target_h, target_m = 18, 0
            target = now.replace(hour=target_h, minute=target_m, second=0, microsecond=0)

            if now >= target and last_run_date != today:
                try:
                    self._run_auto_fetch()
                    last_run_date = today
                except Exception as e:
                    print(f"[LIVE] Auto-fetch error: {e}", flush=True)
                    traceback.print_exc()

            # Check every 60s
            self._stop_event.wait(timeout=60)

    def _run_auto_fetch(self):
        """Execute one auto-fetch cycle: drain pending tickers, fetch history,
        run detection, optionally fetch price data."""
        # 1. Drain pending (capped at MAX_AUTO_FETCH_BATCH)
        with self._pending_fetch_lock:
            if not self._pending_fetch_tickers:
                return
            batch = sorted(self._pending_fetch_tickers)[:self.MAX_AUTO_FETCH_BATCH]
            self._pending_fetch_tickers -= set(batch)
        self._save_pending_queue()
        with self._lock:
            self._status["auto_fetch_pending"] = len(self._pending_fetch_tickers)

        # 2. Check manual fetch guard (shared from app.py)
        try:
            from app import _sweep_fetch_lock, _sweep_fetch_progress, _sweep_fetch_cancel
            with _sweep_fetch_lock:
                if _sweep_fetch_progress["running"]:
                    # Re-queue and skip this cycle
                    with self._pending_fetch_lock:
                        self._pending_fetch_tickers.update(batch)
                    self._save_pending_queue()
                    with self._lock:
                        self._status["auto_fetch_pending"] = \
                            len(self._pending_fetch_tickers)
                        self._status["auto_fetch_last_result"] = "skipped"
                    print(f"[LIVE] Auto-fetch: skipped (manual fetch running), "
                          f"{len(batch)} tickers re-queued", flush=True)
                    return
                # Claim the shared fetch lock
                _sweep_fetch_progress.update({
                    "running": True, "completed": 0, "total": 0,
                    "sweeps_found": 0, "current_ticker": "", "current_date": "",
                    "rate": 0.0, "phase": "auto_fetch",
                    "log_lines": [f"Auto-fetch: {len(batch)} new tickers"],
                    "_t0": time.time(),
                })
            _sweep_fetch_cancel.clear()
            cancel_event = _sweep_fetch_cancel
        except ImportError:
            cancel_event = threading.Event()  # standalone mode

        # 3. Compute date range
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(
            days=self._auto_fetch_lookback_years * 365
        )).strftime("%Y-%m-%d")

        print(f"[LIVE] Auto-fetch: {len(batch)} tickers "
              f"({start_date} \u2192 {end_date})", flush=True)
        _daemon_log("auto_fetch_start",
                    f"{len(batch)} tickers: {batch[:5]}")
        t0 = time.time()

        try:
            from sweep_engine import (
                fetch_and_store_sweeps, get_detection_config,
                detect_clusterbombs, detect_rare_sweep_days,
                detect_monster_sweeps, detect_ranked_sweeps,
                detect_ranked_daily,
            )

            # Progress callback \u2014 updates shared fetch progress dict
            def _cb(ticker, date_str, n_sweeps, total_sweeps,
                    completed, total_jobs):
                try:
                    from app import (_sweep_fetch_lock as lk,
                                     _sweep_fetch_progress as fp)
                    with lk:
                        fp["completed"] = completed
                        fp["total"] = total_jobs
                        fp["sweeps_found"] = total_sweeps
                        fp["current_ticker"] = ticker
                        fp["current_date"] = date_str
                        elapsed = time.time() - fp.get("_t0", time.time())
                        fp["rate"] = round(
                            completed / max(elapsed, 0.1), 1)
                        lines = fp["log_lines"]
                        lines.append(
                            f"[{completed}/{total_jobs}] {ticker} {date_str}")
                        if len(lines) > 50:
                            fp["log_lines"] = lines[-50:]
                except ImportError:
                    pass
                if completed == 1 or completed % 50 == 0:
                    print(f"  [AUTO] [{completed}/{total_jobs}] "
                          f"{ticker} {date_str} \u2014 {n_sweeps} sweeps",
                          flush=True)

            # --- Phase 1: Fetch sweep trades ---
            stats = fetch_and_store_sweeps(
                batch, start_date, end_date,
                progress_callback=_cb,
                cancel_event=cancel_event,
            )
            if cancel_event.is_set():
                # Re-queue tickers so they're retried next cycle (not lost)
                with self._pending_fetch_lock:
                    self._pending_fetch_tickers.update(batch)
                self._save_pending_queue()
                with self._lock:
                    self._status["auto_fetch_pending"] = \
                        len(self._pending_fetch_tickers)
                    self._status["auto_fetch_last_result"] = "cancelled"
                print(f"[LIVE] Auto-fetch: cancelled, "
                      f"{len(batch)} tickers re-queued", flush=True)
                return

            # --- Phase 2: Detection ---
            print(f"[LIVE] Auto-fetch: detecting for "
                  f"{len(batch)} tickers...", flush=True)
            try:
                from app import (_sweep_fetch_lock as lk,
                                 _sweep_fetch_progress as fp)
                with lk:
                    fp["phase"] = "detecting"
                    fp["log_lines"].append("Detecting events...")
            except ImportError:
                pass

            cfg = get_detection_config()
            sc = cfg.get("stock", {})
            # Stock detection
            cb_keys = ("min_sweeps", "min_notional", "min_total",
                       "rarity_days", "rare_min_notional")
            detect_clusterbombs(
                tickers=batch,
                **{k: sc[k] for k in cb_keys if k in sc})
            detect_rare_sweep_days(
                min_notional=sc.get("rare_min_notional",
                                    sc.get("min_notional", 1_000_000)),
                rarity_days=sc.get("rarity_days", 20),
                tickers=batch)
            sm = sc.get("monster_min_notional")
            if sm:
                detect_monster_sweeps(
                    monster_min_notional=float(sm), tickers=batch)
            # Stock ranking
            detect_ranked_daily(
                rank_limit=100,
                min_sweeps=sc.get("min_sweeps_daily", 1),
                tickers=batch, exclude_etfs=True, etf_only=False)
            detect_ranked_sweeps(
                rank_limit=100, tickers=batch,
                exclude_etfs=True, etf_only=False)
            # ETF detection (some new tickers may be ETFs)
            ec = cfg.get("etf", {})
            detect_ranked_daily(
                rank_limit=100, min_sweeps=1, tickers=batch,
                exclude_etfs=False, etf_only=True)
            detect_rare_sweep_days(
                min_notional=float(ec.get("rare_min_notional", 1_000_000)),
                rarity_days=int(ec.get("rarity_days", 20)),
                tickers=batch, exclude_etfs=False, etf_only=True)
            detect_ranked_sweeps(
                rank_limit=100, tickers=batch,
                exclude_etfs=False, etf_only=True)

            # --- Phase 3: Price data (OHLCV candles \u2192 CSV cache) ---
            if self._auto_fetch_price_data:
                print(f"[LIVE] Auto-fetch: fetching price data for "
                      f"{len(batch)} tickers...", flush=True)
                try:
                    from app import (_sweep_fetch_lock as lk,
                                     _sweep_fetch_progress as fp)
                    with lk:
                        fp["phase"] = "price_fetch"
                        fp["log_lines"].append(
                            "Fetching price candles...")
                except ImportError:
                    pass
                from data_fetcher import fetch_with_cache
                for i, ticker in enumerate(batch):
                    if cancel_event.is_set():
                        break
                    try:
                        for ts in ("day", "hour", "week"):
                            fetch_with_cache(
                                ticker, ts, asset_type="stock")
                        print(f"  [AUTO] Price: {ticker} "
                              f"({i+1}/{len(batch)})", flush=True)
                    except Exception as pe:
                        print(f"  [AUTO] Price fetch failed for "
                              f"{ticker}: {pe}", flush=True)

            # --- Done ---
            elapsed = time.time() - t0
            self._known_fetched_tickers.update(batch)

            with self._lock:
                self._status["auto_fetch_last_at"] = \
                    datetime.now(timezone.utc).isoformat()
                self._status["auto_fetch_last_result"] = "success"
                self._status["auto_fetch_last_tickers"] = len(batch)
                self._status["auto_fetch_last_duration_s"] = \
                    round(elapsed, 1)
                self._status["auto_fetch_total_fetched"] += len(batch)

            print(f"[LIVE] Auto-fetch: done \u2014 {len(batch)} tickers, "
                  f"{stats.get('sweeps_found', 0)} sweeps, "
                  f"{elapsed:.0f}s", flush=True)
            _daemon_log("auto_fetch_done",
                        f"{len(batch)} tickers, {elapsed:.0f}s")

        except Exception as e:
            elapsed = time.time() - t0
            with self._lock:
                self._status["auto_fetch_last_at"] = \
                    datetime.now(timezone.utc).isoformat()
                self._status["auto_fetch_last_result"] = "error"
                self._status["auto_fetch_last_duration_s"] = \
                    round(elapsed, 1)
            # Re-queue failed tickers for retry next cycle
            with self._pending_fetch_lock:
                self._pending_fetch_tickers.update(batch)
            self._save_pending_queue()
            with self._lock:
                self._status["auto_fetch_pending"] = \
                    len(self._pending_fetch_tickers)
            print(f"[LIVE] Auto-fetch ERROR: {e}", flush=True)
            _daemon_log("auto_fetch_error", str(e)[:200])
            traceback.print_exc()
        finally:
            # Always release the shared fetch lock
            try:
                from app import (_sweep_fetch_lock as lk,
                                 _sweep_fetch_progress as fp)
                with lk:
                    fp["running"] = False
                    fp["phase"] = "done"
            except ImportError:
                pass

    # ------------------------------------------------------------------
    # Heartbeat — periodic one-line status summary
    # ------------------------------------------------------------------

    def _heartbeat_loop(self):
        """Print status every 30 minutes."""
        HEARTBEAT_INTERVAL = 1800    # print summary every 30 minutes
        last_heartbeat = time.time()

        self._stop_event.wait(timeout=60)  # initial grace period
        while not self._stop_event.is_set():
            try:
                now = time.time()
                if now - last_heartbeat >= HEARTBEAT_INTERVAL:
                    self._print_heartbeat()
                    last_heartbeat = now
            except Exception as e:
                print(f"[LIVE] Heartbeat error: {e}", flush=True)
            self._stop_event.wait(timeout=60)

    def _print_heartbeat(self):
        with self._lock:
            s = dict(self._status)
            et = dict(s.get("events_today", {}))
        # Uptime
        started = s.get("started_at")
        if started:
            delta = datetime.now(timezone.utc) - datetime.fromisoformat(started)
            hrs = delta.total_seconds() / 3600
            uptime = f"{hrs:.0f}h" if hrs >= 1 else f"{delta.total_seconds()/60:.0f}m"
        else:
            uptime = "?"
        conn = "connected" if s.get("connected") else "disconnected"
        # Prices
        prices = s.get("price_tickers_active", 0)
        # Sweeps received today
        sweeps = s.get("sweeps_today", 0)
        # Stock events today (cumulative)
        s_cb = et.get("clusterbomb", 0)
        s_rare = et.get("rare_sweep", 0)
        s_mon = et.get("monster_sweep", 0)
        stock_total = s_cb + s_rare + s_mon
        # ETF events today (cumulative)
        e_daily = et.get("etf_daily", 0)
        e_rare = et.get("etf_rare_sweep", 0)
        e_ranked = et.get("etf_ranked", 0)
        etf_total = e_daily + e_rare + e_ranked
        # Format
        stock_str = f"{stock_total}" if not stock_total else f"{s_cb}cb {s_rare}r {s_mon}m"
        etf_str = f"{etf_total}" if not etf_total else f"{e_daily}d {e_rare}r {e_ranked}rk"
        # Auto-fetch status
        with self._pending_fetch_lock:
            af_pending = len(self._pending_fetch_tickers)
        af_total = s.get("auto_fetch_total_fetched", 0)
        af_str = f"{af_pending}q {af_total}done"
        q_depth = self._msg_queue.qsize()
        now = datetime.now().strftime("%H:%M")
        summary = (f"Up {uptime} {conn} | Q: {q_depth} | Prices: {prices:,} | "
                   f"Sweeps: {sweeps:,} | Stock: {stock_str} | "
                   f"ETF: {etf_str} | AF: {af_str}")
        _daemon_log("heartbeat", summary)
        print(f"[{now}] {summary}", flush=True)

    # ------------------------------------------------------------------
    # Periodic indicator computation (subprocess)
    # ------------------------------------------------------------------

    def _indicator_compute_loop(self):
        """Periodically run indicator computation as a subprocess.

        Runs engine.py with --watchlist/--once/--workers flags in a separate
        OS process, completely isolated from the daemon's WebSocket thread.
        """
        cfg = self._indicator_compute_config
        if not cfg.get("enabled", True):
            print("[LIVE] Indicator compute: disabled in config, thread sleeping",
                  flush=True)
            return
        interval_s = max(cfg.get("interval_minutes", 30), 5) * 60
        # Wait for first flush cycle to complete before first compute
        initial_delay = self._flush_interval * 60 + 30
        print(f"[LIVE] Indicator compute: will start in {initial_delay}s, "
              f"then every {interval_s // 60}m", flush=True)
        self._stop_event.wait(timeout=initial_delay)

        while not self._stop_event.is_set():
            try:
                self._run_indicator_compute_subprocess()
            except Exception as e:
                print(f"[LIVE] Indicator compute error: {e}", flush=True)
                traceback.print_exc()
            self._stop_event.wait(timeout=interval_s)

    def _run_indicator_compute_subprocess(self):
        """Run engine.py as a subprocess for indicator computation."""
        cfg = self._indicator_compute_config
        watchlists = cfg.get("watchlists", ["Priority"])
        workers = cfg.get("workers", 8)

        engine_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                   "engine.py")
        args = [sys.executable, engine_path, "--once", "--workers", str(workers)]
        for wl in watchlists:
            args.extend(["--watchlist", wl])

        print(f"[LIVE] Starting indicator compute subprocess: "
              f"{watchlists} w={workers}...", flush=True)
        t0 = time.time()

        try:
            result = subprocess.run(
                args, capture_output=True, timeout=600,
                cwd=os.path.dirname(engine_path),
                encoding="utf-8", errors="replace"
            )
            elapsed = time.time() - t0

            with self._lock:
                self._status["compute_last_at"] = datetime.now(timezone.utc).isoformat()
                self._status["compute_last_duration_s"] = round(elapsed, 1)

            if result.returncode == 0:
                print(f"[LIVE] Indicator compute complete in {elapsed:.1f}s",
                      flush=True)
                with self._lock:
                    self._status["compute_last_result"] = "success"
                    self._status["compute_last_error"] = None
                # Print last few lines of output for visibility
                if result.stdout:
                    for line in result.stdout.strip().split("\n")[-3:]:
                        print(f"  [COMPUTE] {line}", flush=True)
            else:
                error_msg = (result.stderr or "Unknown error")[-200:]
                print(f"[LIVE] Indicator compute FAILED (exit "
                      f"{result.returncode}): {error_msg}", flush=True)
                with self._lock:
                    self._status["compute_last_result"] = "error"
                    self._status["compute_last_error"] = error_msg

        except subprocess.TimeoutExpired:
            elapsed = time.time() - t0
            print("[LIVE] Indicator compute timed out (600s)", flush=True)
            with self._lock:
                self._status["compute_last_at"] = datetime.now(timezone.utc).isoformat()
                self._status["compute_last_duration_s"] = round(elapsed, 1)
                self._status["compute_last_result"] = "error"
                self._status["compute_last_error"] = "Timeout (600s)"

    # ------------------------------------------------------------------
    # End of Day compute — RS + HVC after market close
    # ------------------------------------------------------------------

    def _eod_compute_loop(self):
        """Thread that checks every 60s whether EOD compute should trigger."""
        self._stop_event.wait(timeout=60)
        while not self._stop_event.is_set():
            try:
                self._check_eod_compute()
            except Exception as e:
                print(f"[LIVE] EOD compute check error: {e}", flush=True)
                traceback.print_exc()
            self._stop_event.wait(timeout=60)

    def _check_eod_compute(self):
        """Check if market closed + delay elapsed, then run enabled computations."""
        try:
            cfg = load_eod_compute_config()
        except Exception:
            cfg = self._eod_config
        self._eod_config = cfg

        if not cfg.get("enabled", True):
            return

        from trading_calendar import is_trading_day
        from scheduler import MarketClock

        now = datetime.now(timezone.utc)
        today = now.date()

        if not is_trading_day(today):
            return

        market_close = MarketClock.market_close_utc(today)

        # --- RS ---
        rs_cfg = cfg.get("rs", {})
        if rs_cfg.get("enabled", True):
            rs_delay = rs_cfg.get("delay_minutes", 30)
            rs_trigger_time = market_close + timedelta(minutes=rs_delay)
            rs_last_date = self._status.get("eod_rs_last_date")
            if now >= rs_trigger_time and rs_last_date != today.isoformat():
                self._run_eod_rs(rs_cfg)

        # --- HVC ---
        hvc_cfg = cfg.get("hvc", {})
        if hvc_cfg.get("enabled", True):
            hvc_delay = hvc_cfg.get("delay_minutes", 30)
            hvc_trigger_time = market_close + timedelta(minutes=hvc_delay)
            hvc_last_date = self._status.get("eod_hvc_last_date")
            if now >= hvc_trigger_time and hvc_last_date != today.isoformat():
                self._run_eod_hvc(hvc_cfg)

    def _run_eod_rs(self, rs_cfg):
        """Run RS computation as subprocess."""
        universe = rs_cfg.get("universe", "Russell3000")
        workers = rs_cfg.get("workers", 8)
        today_str = datetime.now(timezone.utc).date().isoformat()

        print(f"[LIVE] EOD: Starting RS computation for {universe}...", flush=True)
        t0 = time.time()

        try:
            engine_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rs_engine.py")
            result = subprocess.run(
                [sys.executable, engine_path, "--universe", universe,
                 "--workers", str(workers), "--once"],
                capture_output=True, timeout=600,
                cwd=os.path.dirname(engine_path),
                encoding="utf-8", errors="replace"
            )
            elapsed = time.time() - t0

            if result.returncode == 0:
                with self._lock:
                    self._status["eod_rs_last_at"] = datetime.now(timezone.utc).isoformat()
                    self._status["eod_rs_last_result"] = "success"
                    self._status["eod_rs_last_duration_s"] = round(elapsed, 1)
                    self._status["eod_rs_last_error"] = None
                    self._status["eod_rs_last_date"] = today_str
                print(f"[LIVE] EOD: RS computation complete in {elapsed:.1f}s", flush=True)
                if result.stdout:
                    for line in result.stdout.strip().split("\n")[-5:]:
                        print(f"  [RS] {line}", flush=True)
            else:
                error_msg = (result.stderr or "Unknown error")[-200:]
                with self._lock:
                    self._status["eod_rs_last_at"] = datetime.now(timezone.utc).isoformat()
                    self._status["eod_rs_last_result"] = "error"
                    self._status["eod_rs_last_duration_s"] = round(elapsed, 1)
                    self._status["eod_rs_last_error"] = error_msg
                    self._status["eod_rs_last_date"] = today_str
                print(f"[LIVE] EOD: RS computation FAILED (exit {result.returncode}): {error_msg}", flush=True)

        except subprocess.TimeoutExpired:
            with self._lock:
                self._status["eod_rs_last_at"] = datetime.now(timezone.utc).isoformat()
                self._status["eod_rs_last_result"] = "error"
                self._status["eod_rs_last_error"] = "Timeout (600s)"
                self._status["eod_rs_last_date"] = today_str
            print("[LIVE] EOD: RS computation timed out after 600s", flush=True)

        except Exception as e:
            with self._lock:
                self._status["eod_rs_last_at"] = datetime.now(timezone.utc).isoformat()
                self._status["eod_rs_last_result"] = "error"
                self._status["eod_rs_last_error"] = str(e)
                self._status["eod_rs_last_date"] = today_str
            print(f"[LIVE] EOD: RS computation error: {e}", flush=True)

    def _run_eod_hvc(self, hvc_cfg):
        """Run HVC/indicator computation as subprocess (matches RS pattern)."""
        watchlists = hvc_cfg.get("watchlists", ["Priority"])
        workers = hvc_cfg.get("workers", 8)
        today_str = datetime.now(timezone.utc).date().isoformat()

        print(f"[LIVE] EOD: Starting HVC computation for {watchlists}...", flush=True)
        t0 = time.time()

        try:
            engine_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                       "engine.py")
            args = [sys.executable, engine_path, "--once", "--workers", str(workers)]
            for wl in watchlists:
                args.extend(["--watchlist", wl])

            result = subprocess.run(
                args, capture_output=True, timeout=600,
                cwd=os.path.dirname(engine_path),
                encoding="utf-8", errors="replace"
            )
            elapsed = time.time() - t0

            if result.returncode == 0:
                with self._lock:
                    self._status["eod_hvc_last_at"] = datetime.now(timezone.utc).isoformat()
                    self._status["eod_hvc_last_result"] = "success"
                    self._status["eod_hvc_last_duration_s"] = round(elapsed, 1)
                    self._status["eod_hvc_last_error"] = None
                    self._status["eod_hvc_last_date"] = today_str
                print(f"[LIVE] EOD: HVC computation complete in {elapsed:.1f}s",
                      flush=True)
                if result.stdout:
                    for line in result.stdout.strip().split("\n")[-5:]:
                        print(f"  [HVC] {line}", flush=True)
            else:
                error_msg = (result.stderr or "Unknown error")[-200:]
                with self._lock:
                    self._status["eod_hvc_last_at"] = datetime.now(timezone.utc).isoformat()
                    self._status["eod_hvc_last_result"] = "error"
                    self._status["eod_hvc_last_duration_s"] = round(elapsed, 1)
                    self._status["eod_hvc_last_error"] = error_msg
                    self._status["eod_hvc_last_date"] = today_str
                print(f"[LIVE] EOD: HVC FAILED (exit {result.returncode}): "
                      f"{error_msg}", flush=True)

        except subprocess.TimeoutExpired:
            with self._lock:
                self._status["eod_hvc_last_at"] = datetime.now(timezone.utc).isoformat()
                self._status["eod_hvc_last_result"] = "error"
                self._status["eod_hvc_last_error"] = "Timeout (600s)"
                self._status["eod_hvc_last_date"] = today_str
            print("[LIVE] EOD: HVC computation timed out after 600s", flush=True)

        except Exception as e:
            elapsed = time.time() - t0
            with self._lock:
                self._status["eod_hvc_last_at"] = datetime.now(timezone.utc).isoformat()
                self._status["eod_hvc_last_result"] = "error"
                self._status["eod_hvc_last_duration_s"] = round(elapsed, 1)
                self._status["eod_hvc_last_error"] = str(e)
                self._status["eod_hvc_last_date"] = today_str
            print(f"[LIVE] EOD: HVC computation error: {e}", flush=True)
            traceback.print_exc()

    def get_eod_status(self):
        """Return EOD compute status dict."""
        with self._lock:
            return {
                "rs": {
                    "last_at": self._status.get("eod_rs_last_at"),
                    "last_result": self._status.get("eod_rs_last_result"),
                    "last_duration_s": self._status.get("eod_rs_last_duration_s", 0),
                    "last_error": self._status.get("eod_rs_last_error"),
                    "last_date": self._status.get("eod_rs_last_date"),
                },
                "hvc": {
                    "last_at": self._status.get("eod_hvc_last_at"),
                    "last_result": self._status.get("eod_hvc_last_result"),
                    "last_duration_s": self._status.get("eod_hvc_last_duration_s", 0),
                    "last_error": self._status.get("eod_hvc_last_error"),
                    "last_date": self._status.get("eod_hvc_last_date"),
                },
            }


# ---------------------------------------------------------------------------
# Standalone test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("Unified Live Daemon — standalone test")
    print("=" * 50)

    pcfg = load_live_price_config()
    scfg = load_live_config()
    print(f"Price config: {json.dumps(pcfg, indent=2)}")
    print(f"Sweep config: {json.dumps(scfg, indent=2)}")

    daemon = UnifiedLiveDaemon(price_config=pcfg, sweep_config=scfg)

    try:
        daemon.start()
        print("Press Ctrl+C to stop...")
        while True:
            time.sleep(10)
            s = daemon.get_status()
            print(f"  AM bars: {s['price_messages_received']}  "
                  f"Trades: {s['trades_received']}  "
                  f"Sweeps: {s['sweeps_today']}  "
                  f"Price tickers: {s['price_tickers_active']}  "
                  f"Connected: {s['connected']}")
    except KeyboardInterrupt:
        daemon.stop()
        print("Done.")
