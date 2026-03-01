"""
Live Price Daemon — WebSocket ingestion of per-minute aggregate bars.

Connects to Polygon delayed WebSocket feed (AM.* channel), aggregates
minute bars into hourly and daily OHLCV in memory, and periodically
flushes to CSV cache files so charts and other consumers get fresh data.

Runs as a background daemon thread inside app.py.

Usage (standalone test):
    python live_price_daemon.py

Architecture:
    WebSocket (AM.*) → in-memory minute/hourly/daily bars per ticker
        → periodic CSV flush (every 5 min + market close + on stop)
        → serve_tv_history / serve_sweep_chart read merged live data
"""

import json
import os
import threading
import time
import traceback
from datetime import datetime, timezone, timedelta

import pandas as pd

try:
    import websocket
except ImportError:
    websocket = None
    print("WARNING: websocket-client not installed. Run: pip install websocket-client")

from config import MASSIVE_API_KEY

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")

DEFAULT_WS_URL = "wss://delayed.polygon.io/stocks"
DEFAULT_FLUSH_INTERVAL = 5  # minutes
DEFAULT_FLUSH_ON_CLOSE = True

# Reconnection backoff
RECONNECT_MIN_WAIT = 2
RECONNECT_MAX_WAIT = 60


# ---------------------------------------------------------------------------
# Config helpers
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
        "flush_on_close": DEFAULT_FLUSH_ON_CLOSE,
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
# LivePriceDaemon
# ---------------------------------------------------------------------------
class LivePriceDaemon:
    """WebSocket-based live price feed using Polygon AM.* (per-minute aggregates).

    Connects to the Polygon delayed WebSocket, subscribes to AM.* for all stocks,
    aggregates minute bars into hourly and daily OHLCV in memory, and periodically
    flushes to CSV cache files.

    Thread-safe status dict and bar access for API endpoints.
    """

    def __init__(self, config=None):
        cfg = config or {}
        self._api_key = MASSIVE_API_KEY
        self._ws_url = cfg.get("ws_url", DEFAULT_WS_URL)
        self._flush_interval = cfg.get("flush_interval_minutes", DEFAULT_FLUSH_INTERVAL)
        self._flush_on_close = cfg.get("flush_on_close", DEFAULT_FLUSH_ON_CLOSE)

        # State
        self._ws = None
        self._thread = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

        # Per-ticker bar data: {ticker: TickerBars}
        self._bars = {}
        self._bars_lock = threading.Lock()

        # Track current trading date
        self._trading_date = None

        # Stats
        self._status = {
            "running": False,
            "connected": False,
            "authenticated": False,
            "subscribed": False,
            "started_at": None,
            "messages_received": 0,
            "messages_per_sec": 0.0,
            "tickers_active": 0,
            "last_flush_at": None,
            "last_message_at": None,
            "reconnect_count": 0,
            "error": None,
        }

        # Rate tracking (rolling window)
        self._msg_timestamps = []

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def start(self):
        """Start the daemon in a background thread."""
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
                                        name="live-price-daemon")
        self._thread.start()
        print(f"[PRICE] Daemon started — connecting to {self._ws_url}", flush=True)

    def stop(self):
        """Stop the daemon gracefully. Flushes bars to CSV before exit."""
        print("[PRICE] Stopping daemon...", flush=True)
        self._stop_event.set()

        # Close WebSocket
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass

        # Final flush
        try:
            self._flush_to_csv()
        except Exception as e:
            print(f"[PRICE] Final flush error: {e}", flush=True)

        with self._lock:
            self._status["running"] = False
            self._status["connected"] = False
            self._status["authenticated"] = False
            self._status["subscribed"] = False

        print("[PRICE] Daemon stopped.", flush=True)

    def get_status(self):
        """Return a JSON-safe status dict."""
        with self._lock:
            s = dict(self._status)
        with self._bars_lock:
            s["tickers_active"] = len(self._bars)
        return s

    def get_latest_bars(self, ticker):
        """Return today's bars for a ticker.

        Returns dict with 'hourly', 'daily', 'current_hour' keys,
        or None if no data for this ticker.
        """
        ticker = ticker.upper()
        with self._bars_lock:
            tb = self._bars.get(ticker)
            if not tb:
                return None
            return tb.snapshot()

    def get_latest_daily(self, ticker):
        """Return the running daily bar dict for a ticker, or None."""
        ticker = ticker.upper()
        with self._bars_lock:
            tb = self._bars.get(ticker)
            if not tb or not tb.daily:
                return None
            return dict(tb.daily)

    # ------------------------------------------------------------------
    # Main loop (runs in daemon thread)
    # ------------------------------------------------------------------

    def _run_loop(self):
        """Reconnection loop — keeps trying until stop is signalled."""
        reconnect_wait = RECONNECT_MIN_WAIT

        # Start flush timer thread
        flush_thread = threading.Thread(target=self._flush_loop, daemon=True,
                                        name="price-flush")
        flush_thread.start()

        while not self._stop_event.is_set():
            try:
                self._connect_and_run()
            except Exception as e:
                with self._lock:
                    self._status["error"] = str(e)
                    self._status["connected"] = False
                print(f"[PRICE] WebSocket error: {e}", flush=True)
                traceback.print_exc()

            if self._stop_event.is_set():
                break

            # Reconnect with backoff
            with self._lock:
                self._status["reconnect_count"] += 1
            print(f"[PRICE] Reconnecting in {reconnect_wait}s...", flush=True)
            self._stop_event.wait(timeout=reconnect_wait)
            reconnect_wait = min(reconnect_wait * 2, RECONNECT_MAX_WAIT)

    def _connect_and_run(self):
        """Single WebSocket connection lifecycle."""
        self._ws = websocket.WebSocketApp(
            self._ws_url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        self._ws.run_forever(
            ping_interval=30,
            ping_timeout=10,
        )

    # ------------------------------------------------------------------
    # WebSocket callbacks
    # ------------------------------------------------------------------

    def _on_open(self, ws):
        print("[PRICE] WebSocket connected, authenticating...", flush=True)
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
        except json.JSONDecodeError:
            pass
        except Exception as e:
            print(f"[PRICE] Message handler error: {e}", flush=True)

    def _on_error(self, ws, error):
        with self._lock:
            self._status["error"] = str(error)
        print(f"[PRICE] WebSocket error: {error}", flush=True)

    def _on_close(self, ws, close_status_code, close_msg):
        with self._lock:
            self._status["connected"] = False
            self._status["authenticated"] = False
            self._status["subscribed"] = False
        reason = f"code={close_status_code} msg={close_msg}" if close_status_code else "clean"
        print(f"[PRICE] WebSocket closed ({reason})", flush=True)

    # ------------------------------------------------------------------
    # Message handlers
    # ------------------------------------------------------------------

    def _handle_status_msg(self, ws, msg):
        status = msg.get("status", "")
        message = msg.get("message", "")

        if status == "auth_success":
            print("[PRICE] Authenticated. Subscribing to AM.*...", flush=True)
            with self._lock:
                self._status["authenticated"] = True
            ws.send(json.dumps({"action": "subscribe", "params": "AM.*"}))

        elif status == "auth_failed":
            print(f"[PRICE] Authentication FAILED: {message}", flush=True)
            with self._lock:
                self._status["error"] = f"Auth failed: {message}"
            ws.close()

        elif status == "success" and "subscribed" in message.lower():
            print(f"[PRICE] {message}", flush=True)
            with self._lock:
                self._status["subscribed"] = True

        elif status == "error":
            print(f"[PRICE] Server error: {message}", flush=True)
            with self._lock:
                self._status["error"] = message

    def _handle_minute_bar(self, msg):
        """Process a single AM.* message (per-minute aggregate bar)."""
        now = time.time()
        self._msg_timestamps.append(now)

        with self._lock:
            self._status["messages_received"] += 1
            self._status["last_message_at"] = datetime.now(timezone.utc).isoformat()

        # Update rate (messages in last 60s)
        cutoff = now - 60
        self._msg_timestamps = [t for t in self._msg_timestamps if t > cutoff]
        with self._lock:
            self._status["messages_per_sec"] = round(len(self._msg_timestamps) / 60.0, 1)

        ticker = msg.get("sym", "")
        if not ticker:
            return

        # Parse bar data
        bar_start_ms = msg.get("s", 0)  # start timestamp in ms
        o = msg.get("o", 0)
        h = msg.get("h", 0)
        l = msg.get("l", 0)
        c = msg.get("c", 0)
        v = msg.get("v", 0)

        if not bar_start_ms or not o:
            return

        bar_ts = bar_start_ms / 1000.0  # convert to unix seconds
        bar_dt = datetime.fromtimestamp(bar_ts, tz=timezone.utc)
        trade_date = bar_dt.strftime("%Y-%m-%d")
        bar_hour = bar_dt.hour

        # Day rollover check — clear old data if trading date changed
        if self._trading_date and self._trading_date != trade_date:
            self._on_day_rollover(trade_date)
        self._trading_date = trade_date

        minute_bar = {
            "t": int(bar_ts),           # unix seconds
            "o": float(o),
            "h": float(h),
            "l": float(l),
            "c": float(c),
            "v": int(v),
        }

        with self._bars_lock:
            if ticker not in self._bars:
                self._bars[ticker] = TickerBars(ticker, trade_date)

            tb = self._bars[ticker]

            # Dedup: skip if we already have this minute
            if minute_bar["t"] <= tb.last_minute_ts:
                return

            tb.add_minute(minute_bar, bar_hour)

    def _on_day_rollover(self, new_date):
        """Called when trading date changes. Flush then clear yesterday's data."""
        print(f"[PRICE] Day rollover: {self._trading_date} → {new_date}. "
              f"Flushing {len(self._bars)} tickers...", flush=True)
        self._flush_to_csv()
        with self._bars_lock:
            self._bars.clear()

    # ------------------------------------------------------------------
    # CSV flush
    # ------------------------------------------------------------------

    def _flush_loop(self):
        """Timer-based flush loop — runs every flush_interval minutes."""
        interval_s = self._flush_interval * 60
        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=interval_s)
            if not self._stop_event.is_set():
                try:
                    self._flush_to_csv()
                except Exception as e:
                    print(f"[PRICE] Flush error: {e}", flush=True)
                    traceback.print_exc()

    def _flush_to_csv(self):
        """Write accumulated hourly and daily bars to CSV cache files."""
        with self._bars_lock:
            if not self._bars:
                return
            # Take a snapshot of all tickers that have data
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

            # --- Hourly bars ---
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
                    print(f"[PRICE] Hourly flush error for {ticker}: {e}", flush=True)

            # --- Daily bar ---
            daily_bar = data.get("daily")
            if daily_bar:
                daily_path = os.path.join(CACHE_DIR, f"{safe}_day.csv")
                try:
                    self._merge_bars_to_csv(daily_path, [daily_bar], trade_date)
                    flushed_daily += 1
                except Exception as e:
                    print(f"[PRICE] Daily flush error for {ticker}: {e}", flush=True)

        elapsed = time.time() - t0
        with self._lock:
            self._status["last_flush_at"] = datetime.now(timezone.utc).isoformat()

        print(f"[PRICE] Flushed {flushed_hourly} hourly + {flushed_daily} daily "
              f"CSVs in {elapsed:.1f}s ({len(tickers_data)} tickers)", flush=True)

    def _merge_bars_to_csv(self, csv_path, bars, trade_date):
        """Merge live bars into an existing CSV file.

        Approach: load existing CSV, remove any rows for today's date,
        append today's live bars, write back. This is safe because live
        only touches today's data.
        """
        # Build DataFrame from live bars
        live_rows = []
        for b in bars:
            ts = b["t"]
            # Convert unix seconds to pandas Timestamp
            live_rows.append({
                "timestamp": pd.Timestamp.utcfromtimestamp(ts),
                "open": b["o"],
                "high": b["h"],
                "low": b["l"],
                "close": b["c"],
                "volume": b["v"],
            })

        if not live_rows:
            return

        live_df = pd.DataFrame(live_rows)

        # Load existing CSV
        existing_df = None
        if os.path.exists(csv_path):
            try:
                existing_df = pd.read_csv(csv_path, parse_dates=["timestamp"])
            except Exception:
                existing_df = None

        if existing_df is not None and not existing_df.empty:
            # Remove any existing rows for today
            trade_date_start = pd.Timestamp(trade_date)
            trade_date_end = trade_date_start + pd.Timedelta(days=1)
            existing_df = existing_df[
                (existing_df["timestamp"] < trade_date_start) |
                (existing_df["timestamp"] >= trade_date_end)
            ]
            # Append live bars
            merged = pd.concat([existing_df, live_df], ignore_index=True)
        else:
            merged = live_df

        merged = merged.sort_values("timestamp").reset_index(drop=True)
        # Only keep standard columns
        keep_cols = [c for c in ["timestamp", "open", "high", "low", "close",
                                  "volume", "vwap", "n_trades"] if c in merged.columns]
        merged[keep_cols].to_csv(csv_path, index=False)


# ---------------------------------------------------------------------------
# TickerBars — per-ticker in-memory bar storage
# ---------------------------------------------------------------------------
class TickerBars:
    """In-memory OHLCV storage for a single ticker for the current trading day.

    Stores minute bars, aggregates completed hourly bars, and maintains
    a running daily aggregate.
    """

    def __init__(self, ticker, trade_date):
        self.ticker = ticker
        self.trade_date = trade_date
        self.minutes = []           # list of minute bar dicts
        self.hourly = []            # list of completed hourly bar dicts
        self.daily = None           # running daily aggregate dict
        self.last_minute_ts = 0     # last minute bar start ts (unix seconds)

        # Hourly accumulator
        self._current_hour = None    # hour number (0-23)
        self._hour_accum = None      # accumulating hour bar dict

    def add_minute(self, bar, bar_hour):
        """Add a minute bar and update aggregates."""
        self.minutes.append(bar)
        self.last_minute_ts = bar["t"]

        # --- Hourly aggregation ---
        if self._current_hour is not None and self._current_hour != bar_hour:
            # Hour changed — finalize previous hour
            if self._hour_accum:
                self.hourly.append(dict(self._hour_accum))
            self._hour_accum = None

        if self._hour_accum is None:
            # Start new hour
            self._current_hour = bar_hour
            self._hour_accum = {
                "t": bar["t"],
                "o": bar["o"],
                "h": bar["h"],
                "l": bar["l"],
                "c": bar["c"],
                "v": bar["v"],
            }
        else:
            # Update current hour
            self._hour_accum["h"] = max(self._hour_accum["h"], bar["h"])
            self._hour_accum["l"] = min(self._hour_accum["l"], bar["l"])
            self._hour_accum["c"] = bar["c"]
            self._hour_accum["v"] += bar["v"]

        # --- Daily aggregation ---
        if self.daily is None:
            self.daily = {
                "t": bar["t"],  # use first minute's ts as daily bar ts
                "o": bar["o"],
                "h": bar["h"],
                "l": bar["l"],
                "c": bar["c"],
                "v": bar["v"],
            }
        else:
            self.daily["h"] = max(self.daily["h"], bar["h"])
            self.daily["l"] = min(self.daily["l"], bar["l"])
            self.daily["c"] = bar["c"]
            self.daily["v"] += bar["v"]

    def snapshot(self):
        """Return a thread-safe snapshot of current bar state."""
        return {
            "trade_date": self.trade_date,
            "hourly": list(self.hourly),            # completed hours
            "current_hour": dict(self._hour_accum) if self._hour_accum else None,
            "daily": dict(self.daily) if self.daily else None,
            "minute_count": len(self.minutes),
        }


# ---------------------------------------------------------------------------
# Standalone test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("Live Price Daemon — standalone test")
    print("=" * 50)

    cfg = load_live_price_config()
    print(f"Config: {json.dumps(cfg, indent=2)}")

    daemon = LivePriceDaemon(config=cfg)

    try:
        daemon.start()
        print("Press Ctrl+C to stop...")
        while True:
            time.sleep(10)
            status = daemon.get_status()
            print(f"  Messages: {status['messages_received']}  "
                  f"Tickers: {status['tickers_active']}  "
                  f"Rate: {status['messages_per_sec']}/s  "
                  f"Connected: {status['connected']}")
    except KeyboardInterrupt:
        daemon.stop()
        print("Done.")
