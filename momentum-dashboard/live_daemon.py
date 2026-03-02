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
import sqlite3
import threading
import time
import traceback
from collections import defaultdict
from datetime import datetime, timezone, timedelta

import pandas as pd

try:
    import websocket
except ImportError:
    websocket = None
    print("WARNING: websocket-client not installed. Run: pip install websocket-client")

from config import MASSIVE_API_KEY, DB_PATH

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")

# Sweep filtering (match sweep_engine.py)
FINRA_TRF_EXCHANGE_ID = 4
INTERMARKET_SWEEP_CONDITION = 14
MIN_SWEEP_NOTIONAL = 500_000  # $500K

DEFAULT_WS_URL = "wss://delayed.polygon.io/stocks"

# Price defaults
DEFAULT_FLUSH_INTERVAL = 5   # minutes

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
        "auto_start": False,
        "ws_url": DEFAULT_WS_URL,
        "flush_interval_minutes": DEFAULT_FLUSH_INTERVAL,
        "flush_on_close": True,
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
# Config helpers — sweep scanner (sweep_detection_config.json)
# ---------------------------------------------------------------------------
SWEEP_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "sweep_detection_config.json")


def load_live_config():
    """Load live scanner config from sweep_detection_config.json."""
    defaults = {
        "auto_start": False,
        "detection_interval_minutes": DEFAULT_DETECTION_INTERVAL,
        "ws_url": DEFAULT_WS_URL,
        "min_notional": MIN_SWEEP_NOTIONAL,
        "buffer_flush_seconds": DEFAULT_BUFFER_FLUSH_SECONDS,
        "buffer_max_trades": DEFAULT_BUFFER_MAX_TRADES,
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
        self._flush_interval = pcfg.get("flush_interval_minutes", DEFAULT_FLUSH_INTERVAL)
        self._flush_on_close = pcfg.get("flush_on_close", True)

        # Sweep settings
        self._min_notional = scfg.get("min_notional", MIN_SWEEP_NOTIONAL)
        self._detection_interval = scfg.get("detection_interval_minutes", DEFAULT_DETECTION_INTERVAL)
        self._sweep_flush_seconds = scfg.get("buffer_flush_seconds", DEFAULT_BUFFER_FLUSH_SECONDS)
        self._sweep_flush_max = scfg.get("buffer_max_trades", DEFAULT_BUFFER_MAX_TRADES)

        # Connection state
        self._ws = None
        self._thread = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

        # ── Price state ──
        self._bars = {}
        self._bars_lock = threading.Lock()
        self._trading_date = None
        self._price_msg_timestamps = []

        # ── Sweep state ──
        self._sweep_buffer = []
        self._sweep_buffer_lock = threading.Lock()
        self._trade_timestamps = []

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
            "sweep_tickers_active": set(),
            "events_detected": {"clusterbomb": 0, "rare_sweep": 0, "monster_sweep": 0},
            "last_detection_at": None,
            "last_sweep_flush_at": None,
        }

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
        with self._bars_lock:
            s["price_tickers_active"] = len(self._bars)
        s["sweep_tickers_active"] = len(s["sweep_tickers_active"])
        with self._sweep_buffer_lock:
            s["sweeps_buffered"] = len(self._sweep_buffer)
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
            "tickers_active": full["sweep_tickers_active"],
            "events_detected": full["events_detected"],
            "last_detection_at": full["last_detection_at"],
            "last_flush_at": full["last_sweep_flush_at"],
            "reconnect_count": full["reconnect_count"],
            "error": full["error"],
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
        reconnect_wait = RECONNECT_MIN_WAIT

        # Start background timer threads
        threading.Thread(target=self._price_flush_loop, daemon=True,
                         name="live-price-flush").start()
        threading.Thread(target=self._sweep_flush_loop, daemon=True,
                         name="live-sweep-flush").start()
        threading.Thread(target=self._detection_loop, daemon=True,
                         name="live-detection").start()

        while not self._stop_event.is_set():
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
            print(f"[LIVE] Reconnecting in {reconnect_wait}s...", flush=True)
            self._stop_event.wait(timeout=reconnect_wait)
            reconnect_wait = min(reconnect_wait * 2, RECONNECT_MAX_WAIT)

    def _connect_and_run(self):
        self._ws = websocket.WebSocketApp(
            self._ws_url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        self._ws.run_forever(ping_interval=60, ping_timeout=30)

    # ------------------------------------------------------------------
    # WebSocket callbacks
    # ------------------------------------------------------------------

    def _on_open(self, ws):
        print("[LIVE] WebSocket connected, authenticating...", flush=True)
        with self._lock:
            self._status["connected"] = True
            self._status["error"] = None
        ws.send(json.dumps({"action": "auth", "params": self._api_key}))

    def _on_message(self, ws, message):
        try:
            msgs = json.loads(message)
            if not isinstance(msgs, list):
                msgs = [msgs]

            for msg in msgs:
                ev = msg.get("ev")
                if ev == "status":
                    self._handle_status_msg(ws, msg)
                elif ev == "AM":
                    self._handle_minute_bar(msg)
                elif ev == "T":
                    self._handle_trade(msg)
        except json.JSONDecodeError:
            pass
        except Exception as e:
            print(f"[LIVE] Message handler error: {e}", flush=True)

    def _on_error(self, ws, error):
        with self._lock:
            self._status["error"] = str(error)
        print(f"[LIVE] WebSocket error: {error}", flush=True)

    def _on_close(self, ws, close_status_code, close_msg):
        with self._lock:
            self._status["connected"] = False
            self._status["authenticated"] = False
            self._status["subscribed"] = False
        reason = f"code={close_status_code} msg={close_msg}" if close_status_code else "clean"
        print(f"[LIVE] WebSocket closed ({reason})", flush=True)

    # ------------------------------------------------------------------
    # Status message handler
    # ------------------------------------------------------------------

    def _handle_status_msg(self, ws, msg):
        status = msg.get("status", "")
        message = msg.get("message", "")

        if status == "auth_success":
            print("[LIVE] Authenticated. Subscribing to AM.*,T.*...", flush=True)
            with self._lock:
                self._status["authenticated"] = True
            # Subscribe to BOTH channels in one call
            ws.send(json.dumps({"action": "subscribe", "params": "AM.*,T.*"}))

        elif status == "auth_failed":
            print(f"[LIVE] Authentication FAILED: {message}", flush=True)
            with self._lock:
                self._status["error"] = f"Auth failed: {message}"
            ws.close()

        elif status == "success" and "subscribed" in message.lower():
            print(f"[LIVE] {message}", flush=True)
            with self._lock:
                self._status["subscribed"] = True

        elif status == "error":
            print(f"[LIVE] Server error: {message}", flush=True)
            with self._lock:
                self._status["error"] = message

    # ------------------------------------------------------------------
    # Price handler (AM.* messages)
    # ------------------------------------------------------------------

    def _handle_minute_bar(self, msg):
        now = time.time()
        self._price_msg_timestamps.append(now)

        with self._lock:
            self._status["price_messages_received"] += 1
            self._status["price_last_message_at"] = datetime.now(timezone.utc).isoformat()

        cutoff = now - 60
        self._price_msg_timestamps = [t for t in self._price_msg_timestamps if t > cutoff]
        with self._lock:
            self._status["price_messages_per_sec"] = round(
                len(self._price_msg_timestamps) / 60.0, 1)

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
        self._trade_timestamps.append(now)

        with self._lock:
            self._status["trades_received"] += 1

        cutoff = now - 60
        self._trade_timestamps = [t for t in self._trade_timestamps if t > cutoff]
        with self._lock:
            self._status["trades_per_sec"] = round(
                len(self._trade_timestamps) / 60.0, 1)

        # Filter: dark pool intermarket sweep with significant notional
        exchange = msg.get("x", 0)
        conditions = msg.get("c", [])
        price = msg.get("p", 0)
        size = msg.get("s", 0)
        notional = price * size

        is_darkpool = (exchange == FINRA_TRF_EXCHANGE_ID)
        is_sweep = (INTERMARKET_SWEEP_CONDITION in conditions)

        if not (is_darkpool and is_sweep and notional >= self._min_notional):
            return

        ticker = msg.get("sym", "")
        sip_ts = msg.get("t", 0)
        trf_id = msg.get("trfi")

        if sip_ts > 1e15:
            trade_dt = datetime.fromtimestamp(sip_ts / 1e9, tz=timezone.utc)
            sip_ts_ns = sip_ts
        elif sip_ts > 1e12:
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

        with self._sweep_buffer_lock:
            self._sweep_buffer.append(trade_record)

        with self._lock:
            self._status["sweeps_today"] += 1
            self._status["sweep_tickers_active"].add(ticker)
            self._status["last_sweep"] = {
                "ticker": ticker,
                "notional": round(notional, 2),
                "time": trade_dt.strftime("%H:%M:%S"),
                "price": price,
            }

        with self._sweep_buffer_lock:
            if len(self._sweep_buffer) >= self._sweep_flush_max:
                self._do_sweep_flush()

    # ------------------------------------------------------------------
    # Price CSV flush
    # ------------------------------------------------------------------

    def _price_flush_loop(self):
        interval_s = self._flush_interval * 60
        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=interval_s)
            if not self._stop_event.is_set():
                try:
                    self._flush_price_csv()
                except Exception as e:
                    print(f"[LIVE] Price flush error: {e}", flush=True)
                    traceback.print_exc()

    def _flush_price_csv(self):
        with self._bars_lock:
            if not self._bars:
                return
            tickers_data = {}
            for ticker, tb in self._bars.items():
                snap = tb.snapshot()
                if snap:
                    tickers_data[ticker] = snap

        if not tickers_data:
            return

        t0 = time.time()
        flushed_hourly = 0
        flushed_daily = 0

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
    # Sweep buffer flush
    # ------------------------------------------------------------------

    def _sweep_flush_loop(self):
        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=self._sweep_flush_seconds)
            if not self._stop_event.is_set():
                self._flush_sweep_buffer()

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
            self._stop_event.wait(timeout=self._detection_interval * 60)

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

        elapsed = time.time() - t0
        print(f"[LIVE] Detection complete in {elapsed:.1f}s — "
              f"{results.get('clusterbomb', 0)} CBs, "
              f"{results.get('rare_sweep', 0)} rare, "
              f"{results.get('monster_sweep', 0)} monsters", flush=True)


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
