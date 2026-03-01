"""
Live Sweep Daemon — WebSocket ingestion of dark pool sweeps.

Connects to Massive.com (Polygon) WebSocket feed, filters for dark pool
intermarket sweeps, batches writes to the same SQLite tables as the backfill
system, and runs incremental detection every N minutes.

Runs as a background daemon thread inside app.py.

Usage (standalone test):
    python live_sweep_daemon.py

Architecture:
    WebSocket (T.*) → filter (exchange==4, cond 14, notional >= $500K)
        → buffer → batch write to sweep_trades + sweep_daily_summary
        → detection cycle every 5 min → clusterbomb_events
"""

import json
import os
import sqlite3
import threading
import time
import traceback
from collections import defaultdict
from datetime import datetime, timezone, timedelta

try:
    import websocket
except ImportError:
    websocket = None
    print("WARNING: websocket-client not installed. Run: pip install websocket-client")

from config import MASSIVE_API_KEY, DB_PATH

# ---------------------------------------------------------------------------
# Constants (match sweep_engine.py)
# ---------------------------------------------------------------------------
FINRA_TRF_EXCHANGE_ID = 4
INTERMARKET_SWEEP_CONDITION = 14
MIN_SWEEP_NOTIONAL = 500_000  # $500K

DEFAULT_WS_URL = "wss://delayed.polygon.io/stocks"
DEFAULT_DETECTION_INTERVAL = 5  # minutes
DEFAULT_BUFFER_FLUSH_SECONDS = 30
DEFAULT_BUFFER_MAX_TRADES = 200

# Reconnection backoff
RECONNECT_MIN_WAIT = 2
RECONNECT_MAX_WAIT = 60


# ---------------------------------------------------------------------------
# Database helpers (lightweight, avoids importing full sweep_engine)
# ---------------------------------------------------------------------------
def _get_db():
    """Open SQLite connection with row_factory."""
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

    # Update sweep_stats_cache
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

    # Update sweep_daily_summary for dirty pairs
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

    # Mark in fetch_log so backfill won't re-fetch today
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
# LiveSweepDaemon
# ---------------------------------------------------------------------------
class LiveSweepDaemon:
    """WebSocket-based live dark pool sweep scanner.

    Connects to Massive.com (Polygon) delayed WebSocket feed, filters for
    dark pool intermarket sweeps, batches writes to DB, and runs incremental
    clusterbomb/rare/monster detection every N minutes.

    Thread-safe status dict readable by API endpoints.
    """

    def __init__(self, config=None):
        cfg = config or {}
        self._api_key = MASSIVE_API_KEY
        self._ws_url = cfg.get("ws_url", DEFAULT_WS_URL)
        self._min_notional = cfg.get("min_notional", MIN_SWEEP_NOTIONAL)
        self._detection_interval = cfg.get("detection_interval_minutes", DEFAULT_DETECTION_INTERVAL)
        self._flush_seconds = cfg.get("buffer_flush_seconds", DEFAULT_BUFFER_FLUSH_SECONDS)
        self._flush_max = cfg.get("buffer_max_trades", DEFAULT_BUFFER_MAX_TRADES)

        # State
        self._ws = None
        self._thread = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

        # Buffer
        self._buffer = []
        self._buffer_lock = threading.Lock()
        self._last_flush = time.time()

        # Stats
        self._status = {
            "running": False,
            "connected": False,
            "authenticated": False,
            "subscribed": False,
            "started_at": None,
            "trades_received": 0,
            "trades_per_sec": 0.0,
            "sweeps_today": 0,
            "sweeps_buffered": 0,
            "sweeps_written": 0,
            "last_sweep": None,
            "tickers_active": set(),
            "events_detected": {"clusterbomb": 0, "rare_sweep": 0, "monster_sweep": 0},
            "last_detection_at": None,
            "last_flush_at": None,
            "reconnect_count": 0,
            "error": None,
        }

        # Rate tracking (rolling window)
        self._trade_timestamps = []

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def start(self):
        """Start the daemon in a background thread."""
        if websocket is None:
            raise RuntimeError("websocket-client not installed. Run: pip install websocket-client")

        with self._lock:
            if self._status["running"]:
                return  # already running

            self._stop_event.clear()
            self._status["running"] = True
            self._status["started_at"] = datetime.now(timezone.utc).isoformat()
            self._status["error"] = None
            self._status["connected"] = False
            self._status["authenticated"] = False
            self._status["subscribed"] = False

        self._thread = threading.Thread(target=self._run_loop, daemon=True,
                                        name="live-sweep-daemon")
        self._thread.start()
        print(f"[LIVE] Daemon started — connecting to {self._ws_url}", flush=True)

    def stop(self):
        """Stop the daemon gracefully."""
        print("[LIVE] Stopping daemon...", flush=True)
        self._stop_event.set()

        # Close WebSocket
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass

        # Flush remaining buffer
        self._flush_buffer()

        with self._lock:
            self._status["running"] = False
            self._status["connected"] = False
            self._status["authenticated"] = False
            self._status["subscribed"] = False

        print("[LIVE] Daemon stopped.", flush=True)

    def get_status(self):
        """Return a JSON-safe status dict."""
        with self._lock:
            s = dict(self._status)
        # Convert set to count
        s["tickers_active"] = len(s["tickers_active"])
        with self._buffer_lock:
            s["sweeps_buffered"] = len(self._buffer)
        return s

    # ------------------------------------------------------------------
    # Main loop (runs in daemon thread)
    # ------------------------------------------------------------------

    def _run_loop(self):
        """Reconnection loop — keeps trying until stop is signalled."""
        reconnect_wait = RECONNECT_MIN_WAIT

        # Start detection timer thread
        detection_thread = threading.Thread(target=self._detection_loop, daemon=True,
                                            name="live-detection")
        detection_thread.start()

        # Start flush timer thread
        flush_thread = threading.Thread(target=self._flush_loop, daemon=True,
                                        name="live-flush")
        flush_thread.start()

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

            # Reconnect with backoff
            with self._lock:
                self._status["reconnect_count"] += 1
            print(f"[LIVE] Reconnecting in {reconnect_wait}s...", flush=True)
            self._stop_event.wait(timeout=reconnect_wait)
            reconnect_wait = min(reconnect_wait * 2, RECONNECT_MAX_WAIT)

            # Reset backoff on successful connection (checked in on_open)

    def _connect_and_run(self):
        """Single WebSocket connection lifecycle."""
        self._ws = websocket.WebSocketApp(
            self._ws_url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        # run_forever blocks until connection closes
        self._ws.run_forever(
            ping_interval=30,
            ping_timeout=10,
        )

    # ------------------------------------------------------------------
    # WebSocket callbacks
    # ------------------------------------------------------------------

    def _on_open(self, ws):
        """Connected — send auth."""
        print("[LIVE] WebSocket connected, authenticating...", flush=True)
        with self._lock:
            self._status["connected"] = True
            self._status["error"] = None
        # Send auth
        ws.send(json.dumps({"action": "auth", "params": self._api_key}))

    def _on_message(self, ws, message):
        """Handle incoming messages."""
        try:
            msgs = json.loads(message)
            if not isinstance(msgs, list):
                msgs = [msgs]

            for msg in msgs:
                ev = msg.get("ev")

                if ev == "status":
                    self._handle_status_msg(ws, msg)
                elif ev == "T":
                    self._handle_trade(msg)

        except json.JSONDecodeError:
            pass
        except Exception as e:
            print(f"[LIVE] Message handler error: {e}", flush=True)

    def _on_error(self, ws, error):
        """WebSocket error."""
        with self._lock:
            self._status["error"] = str(error)
        print(f"[LIVE] WebSocket error: {error}", flush=True)

    def _on_close(self, ws, close_status_code, close_msg):
        """WebSocket closed."""
        with self._lock:
            self._status["connected"] = False
            self._status["authenticated"] = False
            self._status["subscribed"] = False
        reason = f"code={close_status_code} msg={close_msg}" if close_status_code else "clean"
        print(f"[LIVE] WebSocket closed ({reason})", flush=True)

    # ------------------------------------------------------------------
    # Message handlers
    # ------------------------------------------------------------------

    def _handle_status_msg(self, ws, msg):
        """Handle status messages (auth result, subscription confirmation)."""
        status = msg.get("status", "")
        message = msg.get("message", "")

        if status == "auth_success":
            print("[LIVE] Authenticated. Subscribing to T.*...", flush=True)
            with self._lock:
                self._status["authenticated"] = True
            ws.send(json.dumps({"action": "subscribe", "params": "T.*"}))

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

    def _handle_trade(self, msg):
        """Process a single trade message. Filter and buffer qualifying sweeps."""
        # Track all trades for rate calculation
        now = time.time()
        self._trade_timestamps.append(now)

        with self._lock:
            self._status["trades_received"] += 1

        # Update rate (trades in last 60s)
        cutoff = now - 60
        self._trade_timestamps = [t for t in self._trade_timestamps if t > cutoff]
        with self._lock:
            self._status["trades_per_sec"] = round(len(self._trade_timestamps) / 60.0, 1)

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

        # Qualifying sweep — build trade record
        ticker = msg.get("sym", "")
        sip_ts = msg.get("t", 0)  # milliseconds in WebSocket (vs nanoseconds in REST)
        trf_id = msg.get("trfi")

        # Convert ms timestamp to datetime
        if sip_ts > 1e15:
            # Nanoseconds
            trade_dt = datetime.fromtimestamp(sip_ts / 1e9, tz=timezone.utc)
            sip_ts_ns = sip_ts
        elif sip_ts > 1e12:
            # Microseconds
            trade_dt = datetime.fromtimestamp(sip_ts / 1e6, tz=timezone.utc)
            sip_ts_ns = sip_ts * 1000
        else:
            # Milliseconds (most common from WebSocket)
            trade_dt = datetime.fromtimestamp(sip_ts / 1e3, tz=timezone.utc)
            sip_ts_ns = sip_ts * 1000000

        trade_record = {
            "ticker": ticker,
            "trade_date": trade_dt.strftime("%Y-%m-%d"),
            "trade_time": trade_dt.strftime("%H:%M:%S.%f")[:-3],
            "sip_timestamp": sip_ts_ns,  # store as nanoseconds (matches REST)
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

        with self._buffer_lock:
            self._buffer.append(trade_record)

        with self._lock:
            self._status["sweeps_today"] += 1
            self._status["tickers_active"].add(ticker)
            self._status["last_sweep"] = {
                "ticker": ticker,
                "notional": round(notional, 2),
                "time": trade_dt.strftime("%H:%M:%S"),
                "price": price,
            }

        # Check if buffer needs immediate flush
        with self._buffer_lock:
            if len(self._buffer) >= self._flush_max:
                self._do_flush()

    # ------------------------------------------------------------------
    # Buffer flush
    # ------------------------------------------------------------------

    def _flush_loop(self):
        """Timer-based flush loop — runs every flush_seconds."""
        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=self._flush_seconds)
            if not self._stop_event.is_set():
                self._flush_buffer()

    def _flush_buffer(self):
        """Flush buffered trades to DB."""
        with self._buffer_lock:
            if not self._buffer:
                return
            batch = list(self._buffer)
            self._buffer.clear()

        self._do_flush(batch)

    def _do_flush(self, batch=None):
        """Actually write trades to DB. If batch is None, drain from buffer."""
        if batch is None:
            with self._buffer_lock:
                if not self._buffer:
                    return
                batch = list(self._buffer)
                self._buffer.clear()

        if not batch:
            return

        try:
            inserted = _batch_write(batch)
            with self._lock:
                self._status["sweeps_written"] += inserted
                self._status["last_flush_at"] = datetime.now(timezone.utc).isoformat()

            if inserted > 0:
                tickers = set(t["ticker"] for t in batch)
                print(f"[LIVE] Flushed {inserted} new sweeps ({len(batch)} total, "
                      f"{len(tickers)} tickers)", flush=True)
        except Exception as e:
            print(f"[LIVE] Flush error: {e}", flush=True)
            traceback.print_exc()
            # Put trades back in buffer to retry
            with self._buffer_lock:
                self._buffer = batch + self._buffer

    # ------------------------------------------------------------------
    # Detection cycle
    # ------------------------------------------------------------------

    def _detection_loop(self):
        """Run detection every N minutes."""
        # Wait one full interval before first detection
        self._stop_event.wait(timeout=self._detection_interval * 60)

        while not self._stop_event.is_set():
            try:
                self._run_detection()
            except Exception as e:
                print(f"[LIVE] Detection error: {e}", flush=True)
                traceback.print_exc()

            self._stop_event.wait(timeout=self._detection_interval * 60)

    def _run_detection(self):
        """Run incremental detection on today's data."""
        # Flush buffer first to ensure all trades are in DB
        self._flush_buffer()

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

        total = sum(results.values())
        elapsed = time.time() - t0
        print(f"[LIVE] Detection complete in {elapsed:.1f}s — "
              f"{results.get('clusterbomb', 0)} CBs, "
              f"{results.get('rare_sweep', 0)} rare, "
              f"{results.get('monster_sweep', 0)} monsters", flush=True)


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def load_live_config():
    """Load live scanner config from sweep_detection_config.json."""
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               "sweep_detection_config.json")
    defaults = {
        "auto_start": False,
        "detection_interval_minutes": DEFAULT_DETECTION_INTERVAL,
        "ws_url": DEFAULT_WS_URL,
        "min_notional": MIN_SWEEP_NOTIONAL,
        "buffer_flush_seconds": DEFAULT_BUFFER_FLUSH_SECONDS,
        "buffer_max_trades": DEFAULT_BUFFER_MAX_TRADES,
    }

    try:
        with open(config_path, "r", encoding="utf-8") as f:
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
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               "sweep_detection_config.json")

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            full = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        full = {}

    full["live_scanner"] = config

    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(full, f, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Standalone test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("Live Sweep Daemon — standalone test")
    print("=" * 50)

    cfg = load_live_config()
    print(f"Config: {json.dumps(cfg, indent=2)}")

    daemon = LiveSweepDaemon(config=cfg)

    try:
        daemon.start()
        print("Press Ctrl+C to stop...")
        while True:
            time.sleep(10)
            status = daemon.get_status()
            print(f"  Trades: {status['trades_received']}  "
                  f"Sweeps: {status['sweeps_today']}  "
                  f"Rate: {status['trades_per_sec']}/s  "
                  f"Connected: {status['connected']}")
    except KeyboardInterrupt:
        daemon.stop()
        print("Done.")
