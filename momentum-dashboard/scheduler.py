"""
Watchlist Scheduler — Centralized scheduling for data collection and indicator computation.

Imported by app.py (not a separate process). Replaces the simple scheduled_scan() loop
with a priority-aware, per-watchlist scheduler that supports interval, after-close, and
manual scheduling modes.

Collection (I/O-bound) and computation (CPU-bound) are treated as separate phases:
  Phase 1: collect_all() — ThreadPoolExecutor, hitting the API
  Phase 2: compute_all() — ProcessPoolExecutor, crunching indicators

Multiple watchlists can run concurrently up to max_concurrent.
"""

import json
import os
import sys
import threading
import time
import traceback
from datetime import datetime, timedelta, timezone, date
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Optional, List, Any

from config import _load_watchlists
from trading_calendar import is_trading_day
from change_detector import init_db

# ---------- Constants ----------

SCHEDULER_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scheduler_config.json")
TICK_INTERVAL = 15  # seconds between scheduler wake-ups

# US Eastern timezone offsets from UTC
_EST_OFFSET = -5  # hours
_EDT_OFFSET = -4  # hours

# NYSE market hours in ET
_MARKET_OPEN_H, _MARKET_OPEN_M = 9, 30
_MARKET_CLOSE_H, _MARKET_CLOSE_M = 16, 0


# ============================================================
# CONFIG LOAD / SAVE
# ============================================================

def _load_scheduler_config() -> dict:
    """Load scheduler config from JSON file. Returns defaults if missing."""
    try:
        with open(SCHEDULER_CONFIG_PATH, "r", encoding="utf-8") as f:
            config = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        config = {"enabled": True, "max_concurrent": 2, "watchlists": {}}

    # Ensure top-level keys exist
    config.setdefault("enabled", True)
    config.setdefault("max_concurrent", 2)
    config.setdefault("watchlists", {})

    # Auto-discover watchlists not yet in config
    config = _ensure_all_watchlists(config)
    return config


def _save_scheduler_config(config: dict) -> None:
    """Write scheduler config to JSON file."""
    with open(SCHEDULER_CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)


def _default_watchlist_config(name: str) -> dict:
    """Generate default config for a newly discovered watchlist.
    Large index universes default to after_close mode so RS computes daily.
    AllTickers is a special virtual watchlist that discovers every ticker in the system.
    """
    _RS_UNIVERSES = {"Russell3000", "Russell2000", "Russell1000", "SP500"}
    if name == "AllTickers":
        return {
            "enabled": False,
            "mode": "manual",
            "interval_minutes": 1440,
            "priority": 50,
            "collect_workers": 8,
            "compute_workers": 8,
            "market_hours_only": False,
            "after_close_delay_minutes": 60,
        }
    if name in _RS_UNIVERSES:
        return {
            "enabled": True,
            "mode": "after_close",
            "interval_minutes": 15,
            "priority": 1 if name == "Russell3000" else 2,
            "collect_workers": 4,
            "compute_workers": 8,
            "market_hours_only": False,
            "after_close_delay_minutes": 30,
        }
    return {
        "enabled": False,
        "mode": "manual",
        "interval_minutes": 15,
        "priority": 99,
        "collect_workers": 4,
        "compute_workers": 8,
        "market_hours_only": False,
        "after_close_delay_minutes": 30,
    }


def _ensure_all_watchlists(config: dict) -> dict:
    """Add default entries for any watchlists not yet in config, plus AllTickers."""
    try:
        current_wls = _load_watchlists()
    except Exception:
        current_wls = {}

    changed = False
    for wl_name in current_wls:
        if wl_name not in config["watchlists"]:
            config["watchlists"][wl_name] = _default_watchlist_config(wl_name)
            changed = True

    # Ensure the virtual "AllTickers" entry always exists
    if "AllTickers" not in config["watchlists"]:
        config["watchlists"]["AllTickers"] = _default_watchlist_config("AllTickers")
        changed = True

    if changed:
        _save_scheduler_config(config)

    return config


# ============================================================
# MARKET CLOCK
# ============================================================

class MarketClock:
    """Utilities for market-aware time calculations.

    All internal times are UTC. DST is handled by checking the current
    date against known US DST rules (no zoneinfo dependency).
    """

    @staticmethod
    def is_dst(d: date) -> bool:
        """Check if a date falls in US Eastern Daylight Time.

        DST starts: second Sunday in March at 2:00 AM ET
        DST ends:   first Sunday in November at 2:00 AM ET
        """
        if d.month < 3 or d.month > 11:
            return False
        if d.month > 3 and d.month < 11:
            return True

        if d.month == 3:
            # Second Sunday in March
            # Find first Sunday: day 1 + (6 - weekday) % 7
            first_day_weekday = date(d.year, 3, 1).weekday()  # 0=Mon
            first_sunday = 1 + (6 - first_day_weekday) % 7
            second_sunday = first_sunday + 7
            return d.day >= second_sunday

        # d.month == 11
        # First Sunday in November
        first_day_weekday = date(d.year, 11, 1).weekday()
        first_sunday = 1 + (6 - first_day_weekday) % 7
        return d.day < first_sunday

    @staticmethod
    def et_offset(d: date) -> int:
        """Return UTC offset in hours for US Eastern on given date."""
        return _EDT_OFFSET if MarketClock.is_dst(d) else _EST_OFFSET

    @staticmethod
    def utc_to_et(utc_dt: datetime) -> datetime:
        """Convert UTC datetime to US/Eastern (naive, for display only)."""
        offset_h = MarketClock.et_offset(utc_dt.date())
        return utc_dt + timedelta(hours=offset_h)

    @staticmethod
    def et_to_utc(et_hour: int, et_minute: int, d: date) -> datetime:
        """Convert ET time on a specific date to UTC datetime."""
        offset_h = MarketClock.et_offset(d)
        return datetime(d.year, d.month, d.day, et_hour, et_minute, 0,
                        tzinfo=timezone.utc) - timedelta(hours=offset_h)

    @staticmethod
    def market_close_utc(d: date) -> datetime:
        """Get market close time in UTC for a given date."""
        return MarketClock.et_to_utc(_MARKET_CLOSE_H, _MARKET_CLOSE_M, d)

    @staticmethod
    def market_open_utc(d: date) -> datetime:
        """Get market open time in UTC for a given date."""
        return MarketClock.et_to_utc(_MARKET_OPEN_H, _MARKET_OPEN_M, d)

    @staticmethod
    def is_market_open_now() -> bool:
        """Check if NYSE market is currently open (trading day + within hours)."""
        now = datetime.now(timezone.utc)
        today = now.date()

        if not is_trading_day(today):
            return False

        market_open = MarketClock.market_open_utc(today)
        market_close = MarketClock.market_close_utc(today)
        return market_open <= now <= market_close


# ============================================================
# WATCHLIST RUN STATE (ephemeral, per-watchlist)
# ============================================================

class WatchlistRunState:
    """Tracks the runtime state of a single watchlist scan.

    Ephemeral (in-memory only). Resets on process restart.
    """

    def __init__(self, name: str):
        self.name = name
        self.status = "idle"                    # idle | collecting | computing | error
        self.last_started_utc: Optional[str] = None
        self.last_completed_utc: Optional[str] = None
        self.last_duration_seconds: Optional[float] = None
        self.last_error: Optional[str] = None
        self.last_collect_summary: Optional[dict] = None
        self.last_compute_summary: Optional[dict] = None
        self.next_scheduled_utc: Optional[str] = None
        self.last_after_close_date: Optional[str] = None  # "YYYY-MM-DD"
        self.tickers_total = 0
        self.tickers_done = 0
        self._lock = threading.Lock()

    def to_dict(self) -> dict:
        """Serialize to JSON-safe dict for API response."""
        with self._lock:
            return {
                "name": self.name,
                "status": self.status,
                "last_started_utc": self.last_started_utc,
                "last_completed_utc": self.last_completed_utc,
                "last_duration_seconds": self.last_duration_seconds,
                "last_error": self.last_error,
                "last_collect_summary": self.last_collect_summary,
                "last_compute_summary": self.last_compute_summary,
                "next_scheduled_utc": self.next_scheduled_utc,
                "last_after_close_date": self.last_after_close_date,
                "tickers_total": self.tickers_total,
                "tickers_done": self.tickers_done,
            }

    def mark_started(self, phase: str, ticker_count: int):
        with self._lock:
            self.status = phase
            self.last_started_utc = datetime.now(timezone.utc).isoformat()
            self.last_error = None
            self.tickers_total = ticker_count
            self.tickers_done = 0

    def mark_phase(self, phase: str):
        with self._lock:
            self.status = phase
            self.tickers_done = 0

    def mark_completed(self, duration: float, collect_summary: dict, compute_summary: dict):
        with self._lock:
            self.status = "idle"
            self.last_completed_utc = datetime.now(timezone.utc).isoformat()
            self.last_duration_seconds = round(duration, 1)
            self.last_collect_summary = collect_summary
            self.last_compute_summary = compute_summary
            self.tickers_done = self.tickers_total

    def mark_error(self, error_msg: str):
        with self._lock:
            self.status = "error"
            self.last_error = error_msg


# ============================================================
# THE SCHEDULER
# ============================================================

class WatchlistScheduler:
    """Central orchestrator for per-watchlist scheduling.

    Lifecycle:
        1. Created by app.py at startup
        2. start() loads config, launches daemon tick-loop thread
        3. Tick loop runs every TICK_INTERVAL seconds
        4. Each tick: checks due watchlists, dispatches to worker pool
        5. stop() signals the thread to exit
    """

    def __init__(self):
        self._config: dict = {}
        self._states: Dict[str, WatchlistRunState] = {}
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._executor: Optional[ThreadPoolExecutor] = None
        self._running_set: set = set()          # names currently running
        self._running_lock = threading.Lock()    # protects _running_set
        self._manual_queue: List[str] = []
        self._manual_lock = threading.Lock()

    # ---------- Lifecycle ----------

    def start(self):
        """Load config and start the tick loop thread."""
        self._start_time = datetime.now(timezone.utc)
        self._config = _load_scheduler_config()
        max_c = self._config.get("max_concurrent", 2)
        self._executor = ThreadPoolExecutor(max_workers=max(max_c, 1),
                                            thread_name_prefix="sched")

        # Initialize run states for all configured watchlists
        for wl_name in self._config.get("watchlists", {}):
            if wl_name not in self._states:
                self._states[wl_name] = WatchlistRunState(wl_name)

        # Calculate initial next-scheduled times
        self._update_all_next_scheduled()

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._tick_loop, daemon=True,
                                        name="scheduler-tick")
        self._thread.start()

        wl_count = sum(1 for w in self._config.get("watchlists", {}).values() if w.get("enabled"))
        enabled = self._config.get("enabled", True)
        status = "ENABLED" if enabled else "DISABLED"
        print(f"\n[SCHEDULER] === {status} ===", flush=True)
        print(f"[SCHEDULER] Watchlists: {wl_count} enabled, max_concurrent={max_c}", flush=True)
        print(f"[SCHEDULER] Tick interval: {TICK_INTERVAL}s\n", flush=True)

    def stop(self):
        """Signal the tick loop to stop and shut down the executor."""
        self._stop_event.set()
        if self._executor:
            self._executor.shutdown(wait=False)

    def reload_config(self):
        """Re-read scheduler_config.json and update internal state."""
        self._config = _load_scheduler_config()

        # Ensure states exist for any new watchlists
        for wl_name in self._config.get("watchlists", {}):
            if wl_name not in self._states:
                self._states[wl_name] = WatchlistRunState(wl_name)

        # Resize executor if max_concurrent changed
        max_c = self._config.get("max_concurrent", 2)
        if self._executor and self._executor._max_workers != max_c:
            old = self._executor
            self._executor = ThreadPoolExecutor(max_workers=max(max_c, 1),
                                                thread_name_prefix="sched")
            old.shutdown(wait=False)

        self._update_all_next_scheduled()

    # ---------- Config API ----------

    def reload_config(self):
        """Force re-load config from disk and re-discover any new watchlists."""
        self._config = _load_scheduler_config()

    def get_config(self) -> dict:
        """Return current config for API response."""
        if not self._config or not self._config.get("watchlists"):
            self._config = _load_scheduler_config()
        return dict(self._config)

    def save_config(self, new_config: dict):
        """Validate and save new config, then reload."""
        # Basic validation
        if "watchlists" not in new_config:
            raise ValueError("Missing 'watchlists' key in config")

        for name, cfg in new_config.get("watchlists", {}).items():
            mode = cfg.get("mode", "manual")
            if mode not in ("interval", "after_close", "manual"):
                raise ValueError(f"Invalid mode '{mode}' for {name}")
            if mode == "interval":
                mins = cfg.get("interval_minutes", 0)
                if not isinstance(mins, (int, float)) or mins < 1:
                    raise ValueError(f"interval_minutes must be >= 1 for {name}")

        new_config.setdefault("enabled", True)
        new_config.setdefault("max_concurrent", 2)

        _save_scheduler_config(new_config)
        self.reload_config()

    def get_status(self) -> dict:
        """Return current status of all watchlists for the API."""
        # Lazy-load config if not yet started
        if not self._config or not self._config.get("watchlists"):
            self._config = _load_scheduler_config()
        # Ensure states exist for all configured watchlists
        for wl_name in self._config.get("watchlists", {}):
            if wl_name not in self._states:
                self._states[wl_name] = WatchlistRunState(wl_name)

        wl_status = {}
        for name, state in self._states.items():
            d = state.to_dict()
            # Merge in config fields for convenience
            wl_cfg = self._config.get("watchlists", {}).get(name, {})
            d["config"] = wl_cfg
            wl_status[name] = d

        with self._running_lock:
            current = list(self._running_set)

        return {
            "enabled": self._config.get("enabled", True),
            "max_concurrent": self._config.get("max_concurrent", 2),
            "current_watchlists": current,
            "watchlists": wl_status,
        }

    # ---------- Manual trigger ----------

    def trigger_watchlist(self, name: str) -> dict:
        """Queue a manual run for a watchlist."""
        if name not in self._config.get("watchlists", {}):
            return {"ok": False, "error": f"Unknown watchlist: {name}"}

        with self._running_lock:
            if name in self._running_set:
                return {"ok": False, "error": f"{name} is already running"}

        # Ensure state exists
        if name not in self._states:
            self._states[name] = WatchlistRunState(name)

        with self._manual_lock:
            if name not in self._manual_queue:
                self._manual_queue.append(name)

        return {"ok": True, "queued": True}

    def toggle_enabled(self, enabled: bool) -> dict:
        """Toggle the global scheduler enabled flag."""
        self._config["enabled"] = enabled
        _save_scheduler_config(self._config)
        return {"ok": True, "enabled": enabled}

    # ---------- Core tick loop ----------

    def _tick_loop(self):
        """Main scheduler loop. Runs in a daemon thread."""
        while not self._stop_event.is_set():
            try:
                if self._config.get("enabled", True):
                    self._process_tick()
            except Exception as e:
                print(f"[SCHEDULER] Tick error: {e}", flush=True)
                traceback.print_exc()

            self._stop_event.wait(timeout=TICK_INTERVAL)

    def _process_tick(self):
        """Single tick: check what's due and dispatch."""
        max_concurrent = self._config.get("max_concurrent", 2)

        # How many slots are free?
        with self._running_lock:
            running_count = len(self._running_set)
            available_slots = max_concurrent - running_count

        if available_slots <= 0:
            return

        # 1. Check manual queue first (highest priority)
        to_run = []
        with self._manual_lock:
            while self._manual_queue and len(to_run) < available_slots:
                name = self._manual_queue.pop(0)
                with self._running_lock:
                    if name not in self._running_set:
                        to_run.append(name)

        # 2. Check scheduled watchlists
        if len(to_run) < available_slots:
            due = self._get_due_watchlists()
            for name in due:
                if name not in to_run and len(to_run) < available_slots:
                    to_run.append(name)

        # 3. Dispatch — use plain threads (not ThreadPoolExecutor) to avoid
        #    nesting executors, which deadlocks ProcessPoolExecutor on Windows.
        for name in to_run:
            with self._running_lock:
                self._running_set.add(name)
            t = threading.Thread(target=self._run_watchlist_safe, args=(name,),
                                 daemon=True, name=f"scan-{name}")
            t.start()

    def _get_due_watchlists(self) -> List[str]:
        """Determine which watchlists are due to run now.

        Returns list of watchlist names sorted by priority, excluding already-running.
        """
        due = []
        wl_configs = self._config.get("watchlists", {})

        for name, cfg in wl_configs.items():
            if not cfg.get("enabled", False):
                continue

            # Skip if already running
            with self._running_lock:
                if name in self._running_set:
                    continue

            mode = cfg.get("mode", "manual")

            if mode == "interval" and self._is_due_interval(name, cfg):
                due.append((cfg.get("priority", 99), name))
            elif mode == "after_close" and self._is_due_after_close(name, cfg):
                due.append((cfg.get("priority", 99), name))
            # manual mode: never auto-triggers

        # Sort by priority (lower number = higher priority)
        due.sort(key=lambda x: x[0])
        return [name for _, name in due]

    def _is_due_interval(self, name: str, cfg: dict) -> bool:
        """Check if an interval-mode watchlist is due."""
        state = self._states.get(name)
        if not state:
            return True  # Never run → due immediately

        interval_min = cfg.get("interval_minutes", 15)

        # Market hours check
        if cfg.get("market_hours_only", False) and not MarketClock.is_market_open_now():
            return False

        # Startup grace period: wait one full interval before first run
        # This prevents all interval watchlists from firing immediately on restart
        if not state.last_completed_utc:
            now = datetime.now(timezone.utc)
            start_time = getattr(self, '_start_time', now)
            if now < start_time + timedelta(minutes=interval_min):
                return False
            return True

        try:
            last = datetime.fromisoformat(state.last_completed_utc)
            if last.tzinfo is None:
                last = last.replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            return now >= last + timedelta(minutes=interval_min)
        except (ValueError, TypeError):
            return True

    def _is_due_after_close(self, name: str, cfg: dict) -> bool:
        """Check if an after-close-mode watchlist is due."""
        now = datetime.now(timezone.utc)
        today = now.date()

        # Must be a trading day
        if not is_trading_day(today):
            return False

        # Must be after market close + delay
        delay_min = cfg.get("after_close_delay_minutes", 30)
        close_time = MarketClock.market_close_utc(today) + timedelta(minutes=delay_min)
        if now < close_time:
            return False

        # Must not have already run today
        state = self._states.get(name)
        if state and state.last_after_close_date == today.isoformat():
            return False

        return True

    # ---------- Scan execution ----------

    def _run_watchlist_safe(self, name: str):
        """Wrapper that ensures running_set is cleaned up."""
        try:
            self._run_watchlist(name)
        except Exception as e:
            print(f"[SCHEDULER] Error running {name}: {e}", flush=True)
            traceback.print_exc()
            state = self._states.get(name)
            if state:
                state.mark_error(str(e))
        finally:
            with self._running_lock:
                self._running_set.discard(name)
            self._update_next_scheduled(name)

    def _run_watchlist(self, name: str):
        """Execute a full collect + compute cycle for one watchlist."""
        # Lazy import — only collector is called in-process
        from collector import collect_all

        state = self._states.get(name)
        if not state:
            state = WatchlistRunState(name)
            self._states[name] = state

        cfg = self._config.get("watchlists", {}).get(name, {})
        tickers = self._build_ticker_list(name)

        if not tickers:
            print(f"[SCHEDULER] {name}: 0 tickers — skipping", flush=True)
            state.mark_completed(0.0, {"ok": 0}, {"ok": 0})
            return

        t0 = time.time()
        collect_workers = cfg.get("collect_workers", 4)
        compute_workers = cfg.get("compute_workers", 8)

        print(f"\n[SCHEDULER] Starting {name}: {len(tickers)} tickers "
              f"(collect={collect_workers}w, compute={compute_workers}w)", flush=True)

        # Phase 1: Collect (I/O-bound)
        state.mark_started("collecting", len(tickers))
        try:
            collect_results = collect_all(tickers, workers=collect_workers)
        except Exception as e:
            state.mark_error(f"Collection failed: {e}")
            raise

        collect_ok = sum(1 for r in collect_results if r.get("status") == "ok")
        collect_summary = {
            "ok": collect_ok,
            "skipped": sum(1 for r in collect_results if r.get("status") == "no_data"),
            "error": sum(1 for r in collect_results if r.get("status") == "error"),
        }

        # Phase 2: Compute (CPU-bound) — skippable via config
        do_compute = cfg.get("compute", True)
        stdout = ""
        n_ok = 0
        n_skip = len(tickers)
        n_events = 0
        compute_summary = {"ok": 0, "skipped": len(tickers), "error": 0, "events": 0}

        if not do_compute:
            print(f"[SCHEDULER] Skipping compute for {name} (compute=false)", flush=True)
        else:
            # Spawn engine.py as a subprocess so ProcessPoolExecutor runs from
            # the main thread of its own process — avoids Windows deadlock when
            # ProcessPoolExecutor is called from a non-main thread.
            state.mark_phase("computing")
            tickers_file = None
            try:
                import subprocess
                import tempfile
                engine_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "engine.py")

                if name == "AllTickers":
                    # Write tickers to temp file — engine.py reads via --tickers-file
                    tickers_file = tempfile.NamedTemporaryFile(
                        mode='w', suffix='.txt', prefix='alltickers_',
                        dir=os.path.dirname(engine_path), delete=False
                    )
                    tickers_file.write('\n'.join(tickers))
                    tickers_file.close()
                    cmd = [sys.executable, engine_path,
                           "--tickers-file", tickers_file.name,
                           "--workers", str(compute_workers),
                           "--once"]
                else:
                    cmd = [sys.executable, engine_path,
                           "--watchlist", name,
                           "--workers", str(compute_workers),
                           "--once"]

                result = subprocess.run(cmd, capture_output=True, timeout=1800,
                                        cwd=os.path.dirname(engine_path),
                                        encoding='utf-8', errors='replace')
                if result.returncode != 0:
                    err_msg = result.stderr[-500:] if result.stderr else "unknown error"
                    state.mark_error(f"Compute subprocess failed: {err_msg}")
                    raise RuntimeError(f"engine.py exited with code {result.returncode}")
                # Parse summary from stdout
                stdout = result.stdout or ""
                if stdout:
                    print(stdout[-2000:] if len(stdout) > 2000 else stdout, flush=True)
            except subprocess.TimeoutExpired:
                state.mark_error("Compute subprocess timed out (30min)")
                raise
            except Exception as e:
                if "Compute subprocess failed" not in str(e):
                    state.mark_error(f"Computation failed: {e}")
                raise
            finally:
                # Clean up temp tickers file if we created one
                if tickers_file and os.path.exists(tickers_file.name):
                    try:
                        os.unlink(tickers_file.name)
                    except OSError:
                        pass

            # Parse compute results from engine output
            import re
            compute_ok_m = re.search(r'(\d+) computed', stdout or "")
            compute_skip_m = re.search(r'(\d+) skipped', stdout or "")
            events_m = re.search(r'Total events: (\d+)', stdout or "")
            n_ok = int(compute_ok_m.group(1)) if compute_ok_m else len(tickers)
            n_skip = int(compute_skip_m.group(1)) if compute_skip_m else 0
            n_events = int(events_m.group(1)) if events_m else 0
            compute_summary = {
                "ok": n_ok,
                "skipped": n_skip,
                "error": 0,
                "events": n_events,
            }

        # Phase 3: RS Rating (optional, for large universes only — requires compute)
        RS_ELIGIBLE = {"Russell3000", "Russell2000", "Russell1000", "SP500"}
        if do_compute and name in RS_ELIGIBLE:
            try:
                import subprocess as _sp3
                rs_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rs_engine.py")
                if os.path.exists(rs_path):
                    rs_cmd = [sys.executable, rs_path,
                              "--universe", name,
                              "--workers", str(compute_workers),
                              "--once"]
                    print(f"[SCHEDULER] Phase 3: RS Rating for {name}...", flush=True)
                    rs_result = _sp3.run(rs_cmd, capture_output=True, timeout=600,
                                         cwd=os.path.dirname(rs_path),
                                         encoding='utf-8', errors='replace')
                    rs_out = rs_result.stdout or ""
                    if rs_out:
                        # Print last bit of RS output
                        for line in rs_out.strip().splitlines()[-10:]:
                            print(f"  [RS] {line}", flush=True)
                    if rs_result.returncode != 0:
                        rs_err = rs_result.stderr or ""
                        print(f"  [RS] Warning: RS engine exited {rs_result.returncode}: {rs_err[-200:]}", flush=True)
            except Exception as rs_e:
                print(f"  [RS] RS engine failed (non-fatal): {rs_e}", flush=True)

            # Phase 3b: Save report snapshot after RS compute
            try:
                from change_detector import save_rs_report_snapshot, _get_db
                import json as _snap_json
                today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                print(f"[SCHEDULER] Phase 3b: Saving report snapshot for {name}...", flush=True)

                # Generate report data inline (same logic as serve_rs_report)
                conn = _get_db()
                c = conn.cursor()

                # Sector rotation
                c.execute("""SELECT sector, ROUND(AVG(rs_rating), 1) as avg_rs, COUNT(*) as cnt
                    FROM rs_rankings WHERE universe = ? AND sector != '' GROUP BY sector""", (name,))
                current_sectors = {r[0]: {"avg_rs": r[1], "count": r[2]} for r in c.fetchall()}
                c.execute("""SELECT DISTINCT trade_date FROM rs_history
                    WHERE universe = ? ORDER BY trade_date DESC LIMIT 20""", (name,))
                hist_dates = [r[0] for r in c.fetchall()]
                sector_rotation = []
                if len(hist_dates) >= 10:
                    old_date = hist_dates[-1]
                    c.execute("SELECT ticker, sector FROM rs_rankings WHERE universe = ? AND sector != ''", (name,))
                    tk_sec = {r[0]: r[1] for r in c.fetchall()}
                    c.execute("SELECT ticker, rs_rating FROM rs_history WHERE universe = ? AND trade_date = ?",
                              (name, old_date))
                    old_rtg = {r[0]: r[1] for r in c.fetchall()}
                    old_sec = {}
                    for t, s in tk_sec.items():
                        if t in old_rtg: old_sec.setdefault(s, []).append(old_rtg[t])
                    old_avg = {s: round(sum(v)/len(v), 1) for s, v in old_sec.items() if v}
                    for sec, d in current_sectors.items():
                        if sec in old_avg:
                            chg = round(d["avg_rs"] - old_avg[sec], 1)
                            sector_rotation.append({"sector": sec, "avg_rs": d["avg_rs"], "change": chg, "count": d["count"]})
                    sector_rotation.sort(key=lambda x: x["change"], reverse=True)

                def _q(sql, params):
                    c.execute(sql, params)
                    cols = [d[0] for d in c.description]
                    return [dict(zip(cols, r)) for r in c.fetchall()]

                new_monsters = _q("""SELECT ticker, rs_rating, rs_change_20d, sector, price, rs_change_5d,
                    monster_score, price_return_3m, hvc_count, gap_count, gaps_open FROM rs_rankings
                    WHERE universe = ? AND rs_rating >= 90 AND rs_change_20d IS NOT NULL AND rs_change_20d > 0
                    AND (rs_rating - rs_change_20d) < 90 ORDER BY rs_change_20d DESC LIMIT 15""", (name,))
                accelerating = _q("""SELECT ticker, rs_rating, rs_change_5d, rs_change_20d, sector, price,
                    monster_score, hvc_count, gap_count, gaps_open FROM rs_rankings
                    WHERE universe = ? AND rs_rating >= 80 AND rs_change_5d IS NOT NULL AND rs_change_5d > 0
                    ORDER BY rs_change_5d DESC LIMIT 10""", (name,))
                emerging = _q("""SELECT ticker, rs_rating, rs_change_20d, rs_change_5d, sector, price,
                    monster_score, hvc_count, gap_count, gaps_open FROM rs_rankings
                    WHERE universe = ? AND rs_rating BETWEEN 60 AND 79 AND rs_change_20d IS NOT NULL AND rs_change_20d > 0
                    ORDER BY rs_change_20d DESC LIMIT 10""", (name,))
                divergences = _q("""SELECT ticker, rs_rating, price_return_3m, price_return_6m, sector, price,
                    monster_score, hvc_count, gap_count, gaps_open FROM rs_rankings
                    WHERE universe = ? AND price_return_3m IS NOT NULL AND price_return_3m > 30
                    ORDER BY price_return_3m DESC LIMIT 10""", (name,))
                hvc_activity = _q("""SELECT ticker, rs_rating, monster_score, sector, price,
                    hvc_count, gap_count, gaps_open FROM rs_rankings
                    WHERE universe = ? AND hvc_count IS NOT NULL AND hvc_count > 0 AND rs_rating >= 70
                    ORDER BY hvc_count DESC, monster_score DESC LIMIT 10""", (name,))
                conn.close()

                report = {
                    "sector_rotation": {"gaining": sector_rotation[:5],
                        "losing": list(reversed(sector_rotation[-5:])) if len(sector_rotation) > 5 else []},
                    "new_monsters": new_monsters, "accelerating": accelerating,
                    "emerging": emerging, "divergences": divergences,
                    "hvc_activity": hvc_activity, "universe": name,
                }
                save_rs_report_snapshot(name, today_str, report)
                print(f"  [RS] Report snapshot saved for {today_str}", flush=True)
            except Exception as snap_e:
                print(f"  [RS] Report snapshot failed (non-fatal): {snap_e}", flush=True)

        duration = time.time() - t0
        state.mark_completed(duration, collect_summary, compute_summary)

        # Track after_close date
        if cfg.get("mode") == "after_close":
            state.last_after_close_date = datetime.now(timezone.utc).date().isoformat()

        print(f"[SCHEDULER] Completed {name}: "
              f"collect={collect_ok}/{len(tickers)}, "
              f"compute={n_ok}/{len(tickers)}, "
              f"events={n_events}, "
              f"duration={duration:.1f}s", flush=True)

    def _build_ticker_list(self, watchlist_name: str) -> List[str]:
        """Extract deduplicated ticker list for a watchlist (or all sources for AllTickers)."""
        if watchlist_name == "AllTickers":
            return self._discover_all_tickers()

        try:
            all_wls = _load_watchlists()
        except Exception:
            all_wls = {}

        wl_groups = all_wls.get(watchlist_name, [])
        seen = set()
        tickers = []
        for group_name, group_tickers in wl_groups:
            for display, api, atype in group_tickers:
                if display not in seen:
                    seen.add(display)
                    tickers.append(display)
        return tickers

    def _discover_all_tickers(self) -> List[str]:
        """Dynamically discover all tickers from watchlists + DB + cache.

        Mirrors app.py serve_all_tickers() logic — unions 4 sources so we
        capture tickers from old backfills, one-off fetches, and sweep data.
        """
        import sqlite3

        seen = set()

        # 1. Watchlist tickers
        try:
            all_wls = _load_watchlists()
            for _name, groups in all_wls.items():
                for _group_name, group_tickers in groups:
                    for display, _api, _atype in group_tickers:
                        seen.add(display)
        except Exception:
            pass

        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "momentum_dashboard.db")

        # 2. Indicator DB (snapshots table)
        try:
            conn = sqlite3.connect(db_path)
            for row in conn.execute("SELECT DISTINCT ticker FROM snapshots"):
                seen.add(row[0])
            conn.close()
        except Exception:
            pass

        # 3. Sweep DB (sweep_daily_summary)
        try:
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

        return sorted(seen)

    # ---------- Next-scheduled calculation ----------

    def _update_all_next_scheduled(self):
        """Update next_scheduled_utc for all watchlists."""
        for name in self._config.get("watchlists", {}):
            self._update_next_scheduled(name)

    def _update_next_scheduled(self, name: str):
        """Calculate and set the next scheduled run time for a watchlist."""
        state = self._states.get(name)
        if not state:
            return

        cfg = self._config.get("watchlists", {}).get(name, {})
        if not cfg.get("enabled", False):
            state.next_scheduled_utc = None
            return

        mode = cfg.get("mode", "manual")

        if mode == "interval":
            interval_min = cfg.get("interval_minutes", 15)
            if state.last_completed_utc:
                try:
                    last = datetime.fromisoformat(state.last_completed_utc)
                    if last.tzinfo is None:
                        last = last.replace(tzinfo=timezone.utc)
                    next_run = last + timedelta(minutes=interval_min)
                    state.next_scheduled_utc = next_run.isoformat()
                except (ValueError, TypeError):
                    state.next_scheduled_utc = "now"
            else:
                state.next_scheduled_utc = "now"

        elif mode == "after_close":
            # Next market close + delay
            now = datetime.now(timezone.utc)
            today = now.date()
            delay_min = cfg.get("after_close_delay_minutes", 30)

            # Check if today's after-close already happened
            if is_trading_day(today):
                after_close = MarketClock.market_close_utc(today) + timedelta(minutes=delay_min)
                if now < after_close:
                    state.next_scheduled_utc = after_close.isoformat()
                    return
                elif state.last_after_close_date != today.isoformat():
                    state.next_scheduled_utc = after_close.isoformat()
                    return

            # Find next trading day
            check = today + timedelta(days=1)
            for _ in range(10):
                if is_trading_day(check):
                    after_close = MarketClock.market_close_utc(check) + timedelta(minutes=delay_min)
                    state.next_scheduled_utc = after_close.isoformat()
                    return
                check += timedelta(days=1)

            state.next_scheduled_utc = None

        else:
            # manual mode
            state.next_scheduled_utc = None
