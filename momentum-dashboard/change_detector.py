"""
Change detection and event logging.
Compares current indicator state to previous snapshot and logs meaningful changes.
"""

import sqlite3
import json
import time as _time
from datetime import datetime, timezone
from config import DB_PATH, TIMEFRAMES


def _get_db():
    """Get a database connection with WAL mode and retry-friendly timeout."""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _db_write(fn, max_retries=8):
    """Execute a write operation with retry on database locked errors."""
    for attempt in range(max_retries):
        try:
            return fn()
        except sqlite3.OperationalError as e:
            if "locked" in str(e) and attempt < max_retries - 1:
                _time.sleep(0.2 * (2 ** attempt))  # exponential: 0.2, 0.4, 0.8, 1.6...
                continue
            raise


def init_db():
    """Create database tables if they don't exist."""
    conn = _get_db()
    c = conn.cursor()
    
    c.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            ticker TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            mom_color TEXT,
            mom_rising INTEGER,
            sqz_state TEXT,
            band_pos TEXT,
            band_flip INTEGER,
            acc_state TEXT,
            acc_impulse TEXT,
            dev_signal TEXT,
            div_adj INTEGER DEFAULT 0,
            bar_status TEXT DEFAULT 'confirmed'
        )
    """)
    
    c.execute("""
        CREATE TABLE IF NOT EXISTS meta_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            ticker TEXT NOT NULL,
            price REAL,
            price_change_pct REAL,
            above_wma30 INTEGER,
            wma30_value REAL,
            wma30_cross TEXT
        )
    """)
    
    c.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            ticker TEXT NOT NULL,
            timeframe TEXT,
            indicator TEXT NOT NULL,
            old_value TEXT,
            new_value TEXT,
            description TEXT NOT NULL,
            bar_status TEXT DEFAULT 'confirmed',
            div_adj INTEGER DEFAULT 0,
            time_bucket TEXT DEFAULT ''
        )
    """)
    
    c.execute("""
        CREATE TABLE IF NOT EXISTS scan_cache (
            ticker TEXT NOT NULL,
            div_adj INTEGER NOT NULL,
            data_hash TEXT NOT NULL,
            indicator_version TEXT NOT NULL,
            scanned_at TEXT NOT NULL,
            PRIMARY KEY (ticker, div_adj)
        )
    """)
    
    # Indexes for fast querying
    c.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_ticker_tf ON snapshots(ticker, timeframe)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_timestamp ON snapshots(timestamp)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_lookup ON snapshots(div_adj, ticker, timeframe, timestamp)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_meta_lookup ON meta_snapshots(ticker, timestamp)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_events_ticker ON events(ticker)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_events_timeframe ON events(timeframe)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_events_composite ON events(timestamp DESC, bar_status, div_adj)")
    
    # Migration: add div_adj column if missing (existing DBs)
    try:
        c.execute("SELECT div_adj FROM snapshots LIMIT 1")
    except sqlite3.OperationalError:
        c.execute("ALTER TABLE snapshots ADD COLUMN div_adj INTEGER DEFAULT 0")
    
    # Migration: add bar_status column if missing (existing DBs)
    try:
        c.execute("SELECT bar_status FROM snapshots LIMIT 1")
    except sqlite3.OperationalError:
        c.execute("ALTER TABLE snapshots ADD COLUMN bar_status TEXT DEFAULT 'confirmed'")
    
    # Migration: add bar_status column to events if missing (existing DBs)
    try:
        c.execute("SELECT bar_status FROM events LIMIT 1")
    except sqlite3.OperationalError:
        c.execute("ALTER TABLE events ADD COLUMN bar_status TEXT DEFAULT 'confirmed'")

    # Migration: add div_adj column to events if missing (existing DBs)
    try:
        c.execute("SELECT div_adj FROM events LIMIT 1")
    except sqlite3.OperationalError:
        c.execute("ALTER TABLE events ADD COLUMN div_adj INTEGER DEFAULT 0")

    # Migration: add time_bucket column to events if missing (existing DBs)
    try:
        c.execute("SELECT time_bucket FROM events LIMIT 1")
    except sqlite3.OperationalError:
        c.execute("ALTER TABLE events ADD COLUMN time_bucket TEXT DEFAULT ''")
        # Backfill time_bucket for existing events
        c.execute("UPDATE events SET time_bucket = substr(timestamp, 1, 15) WHERE time_bucket = '' OR time_bucket IS NULL")

    # Unique index to prevent duplicate events within the same 10-min bucket
    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_events_dedup ON events(ticker, timeframe, indicator, description, div_adj, time_bucket)")

    c.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_divadj ON snapshots(div_adj)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_events_divadj ON events(div_adj)")

    # HVC events table — persistent log of high volume candle detections
    c.execute("""
        CREATE TABLE IF NOT EXISTS hvc_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            event_date TEXT NOT NULL,
            detected_at TEXT NOT NULL,
            close_price REAL,
            open_price REAL,
            volume INTEGER,
            avg_volume INTEGER,
            volume_ratio REAL,
            candle_dir TEXT,
            price_at_detection REAL,
            gap_type TEXT DEFAULT '',
            gap_level REAL,
            gap_closed INTEGER DEFAULT 0
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_hvc_date ON hvc_events(event_date)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_hvc_ticker ON hvc_events(ticker)")
    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_hvc_unique ON hvc_events(ticker, event_date)")

    # Migration: add gap columns to hvc_events if missing (existing DBs)
    for col, default in [("gap_type", "''"), ("gap_level", "NULL"), ("gap_closed", "0")]:
        try:
            c.execute(f"SELECT {col} FROM hvc_events LIMIT 1")
        except sqlite3.OperationalError:
            c.execute(f"ALTER TABLE hvc_events ADD COLUMN {col} DEFAULT {default}")

    # Migration: add HVC columns to meta_snapshots if missing
    for col, default in [("hvc_triggered", "0"), ("hvc_volume_ratio", "NULL"), ("hvc_candle_dir", "''")]:
        try:
            c.execute(f"SELECT {col} FROM meta_snapshots LIMIT 1")
        except sqlite3.OperationalError:
            c.execute(f"ALTER TABLE meta_snapshots ADD COLUMN {col} DEFAULT {default}")

    # --- RS (Relative Strength) tables ---

    # Current RS state per ticker
    c.execute("""
        CREATE TABLE IF NOT EXISTS rs_rankings (
            ticker TEXT NOT NULL,
            universe TEXT NOT NULL,
            rs_score REAL,
            rs_rating INTEGER,
            rs_rank INTEGER,
            rs_change_5d INTEGER,
            rs_change_20d INTEGER,
            rs_new_high INTEGER DEFAULT 0,
            price_new_high INTEGER DEFAULT 0,
            above_rs_30wma INTEGER,
            sector TEXT DEFAULT '',
            price REAL,
            price_change_pct REAL,
            above_wma30 INTEGER,
            wma30_value REAL,
            computed_at TEXT NOT NULL,
            PRIMARY KEY (ticker, universe)
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_rs_rankings_universe ON rs_rankings(universe, rs_rating DESC)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_rs_rankings_sector ON rs_rankings(universe, sector)")

    # Migrations: add new columns to rs_rankings
    for _col in ["price_return_1m", "price_return_3m", "price_return_6m", "monster_score",
                  "hvc_count", "gap_count", "gaps_open"]:
        try:
            c.execute(f"SELECT {_col} FROM rs_rankings LIMIT 1")
        except Exception:
            _type = "INTEGER" if _col in ("hvc_count", "gap_count", "gaps_open") else "REAL"
            c.execute(f"ALTER TABLE rs_rankings ADD COLUMN {_col} {_type}")

    # Daily RS snapshots for trend analysis
    c.execute("""
        CREATE TABLE IF NOT EXISTS rs_history (
            ticker TEXT NOT NULL,
            universe TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            rs_score REAL,
            rs_rating INTEGER,
            price REAL,
            PRIMARY KEY (ticker, universe, trade_date)
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_rs_history_ticker ON rs_history(ticker, universe, trade_date)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_rs_history_date ON rs_history(trade_date, universe)")

    # Breakout/signal event log
    c.execute("""
        CREATE TABLE IF NOT EXISTS rs_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            universe TEXT NOT NULL,
            event_type TEXT NOT NULL,
            event_date TEXT NOT NULL,
            rs_rating INTEGER,
            price REAL,
            details TEXT DEFAULT '',
            detected_at TEXT NOT NULL
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_rs_events_date ON rs_events(event_date DESC)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_rs_events_type ON rs_events(event_type, event_date DESC)")
    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_rs_events_dedup ON rs_events(ticker, universe, event_type, event_date)")

    # RS report snapshots — archived daily intelligence reports
    c.execute("""
        CREATE TABLE IF NOT EXISTS rs_report_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            universe TEXT NOT NULL,
            report_date TEXT NOT NULL,
            report_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)
    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_rs_report_snap_date ON rs_report_snapshots(universe, report_date)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_rs_report_snap_lookup ON rs_report_snapshots(universe, report_date DESC)")

    conn.commit()
    conn.close()


def check_scan_cache(ticker, div_adj, data_hash, indicator_version):
    """Check if a ticker needs rescanning.
    Returns True if cached results are still valid (skip scan)."""
    conn = _get_db()
    c = conn.cursor()
    c.execute("""
        SELECT data_hash, indicator_version FROM scan_cache
        WHERE ticker = ? AND div_adj = ?
    """, (ticker, div_adj))
    row = c.fetchone()
    conn.close()
    
    if row is None:
        return False  # Never scanned
    
    cached_hash, cached_version = row
    return cached_hash == data_hash and cached_version == indicator_version


def update_scan_cache(ticker, div_adj, data_hash, indicator_version):
    """Record that a ticker was scanned with given data hash and version."""
    def _do():
        conn = _get_db()
        c = conn.cursor()
        c.execute("""
            INSERT OR REPLACE INTO scan_cache 
            (ticker, div_adj, data_hash, indicator_version, scanned_at)
            VALUES (?, ?, ?, ?, ?)
        """, (ticker, div_adj, data_hash, indicator_version, datetime.now(timezone.utc).isoformat()))
        conn.commit()
        conn.close()
    _db_write(_do)


def invalidate_scan_cache():
    """Clear all scan cache entries (e.g. after indicator code change)."""
    conn = _get_db()
    c = conn.cursor()
    c.execute("DELETE FROM scan_cache")
    conn.commit()
    conn.close()


def get_previous_snapshot(ticker, timeframe, div_adj=0):
    """Get the most recent snapshot for a ticker/timeframe pair."""
    conn = _get_db()
    c = conn.cursor()
    
    c.execute("""
        SELECT mom_color, mom_rising, sqz_state, band_pos, band_flip, 
               acc_state, acc_impulse, dev_signal
        FROM snapshots 
        WHERE ticker = ? AND timeframe = ? AND div_adj = ?
        ORDER BY timestamp DESC LIMIT 1
    """, (ticker, timeframe, div_adj))
    
    row = c.fetchone()
    conn.close()
    
    if not row:
        return None
    
    return {
        "mom_color": row[0],
        "mom_rising": bool(row[1]) if row[1] is not None else None,
        "sqz_state": row[2],
        "band_pos": row[3],
        "band_flip": bool(row[4]) if row[4] is not None else None,
        "acc_state": row[5],
        "acc_impulse": row[6],
        "dev_signal": row[7],
    }


def get_previous_meta(ticker):
    """Get the most recent meta snapshot for a ticker."""
    conn = _get_db()
    c = conn.cursor()
    
    c.execute("""
        SELECT above_wma30, wma30_cross
        FROM meta_snapshots
        WHERE ticker = ?
        ORDER BY timestamp DESC LIMIT 1
    """, (ticker,))
    
    row = c.fetchone()
    conn.close()
    
    if not row:
        return None
    
    return {
        "above_wma30": bool(row[0]) if row[0] is not None else None,
        "wma30_cross": row[1],
    }


def save_snapshot(ticker, indicator_results, timestamp=None, div_adj=0):
    """Save current indicator state to the database."""
    if timestamp is None:
        timestamp = datetime.now(timezone.utc).isoformat()
    
    def _do_save():
        conn = _get_db()
        c = conn.cursor()
        
        for tf in TIMEFRAMES:
            if tf not in indicator_results:
                continue
            
            data = indicator_results[tf]
            c.execute("""
                INSERT INTO snapshots 
                (timestamp, ticker, timeframe, mom_color, mom_rising, sqz_state, 
                 band_pos, band_flip, acc_state, acc_impulse, dev_signal, div_adj, bar_status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                timestamp, ticker, tf,
                data.get("mom_color", ""),
                int(data.get("mom_rising", False)) if data.get("mom_rising") is not None else None,
                data.get("sqz_state", ""),
                data.get("band_pos", ""),
                int(data.get("band_flip", False)),
                data.get("acc_state", ""),
                data.get("acc_impulse", ""),
                data.get("dev_signal", ""),
                div_adj,
                data.get("bar_status", "confirmed"),
            ))
        
        # Save meta (including HVC fields)
        meta = indicator_results.get("_meta", {})
        c.execute("""
            INSERT INTO meta_snapshots
            (timestamp, ticker, price, price_change_pct, above_wma30, wma30_value, wma30_cross,
             hvc_triggered, hvc_volume_ratio, hvc_candle_dir)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            timestamp, ticker,
            meta.get("price"),
            meta.get("price_change_pct"),
            int(meta.get("above_wma30")) if meta.get("above_wma30") is not None else None,
            meta.get("wma30_value"),
            meta.get("wma30_cross", ""),
            int(meta.get("hvc_triggered", False)),
            meta.get("hvc_volume_ratio"),
            meta.get("hvc_candle_dir", ""),
        ))
        
        conn.commit()
        conn.close()
    
    _db_write(_do_save)


def _describe_mom_color(color):
    """Human-readable momentum color."""
    return {
        "aqua": "+ve Rising",
        "blue": "+ve Falling",
        "yellow": "-ve Rising",
        "red": "-ve Falling",
    }.get(color, color)


def _describe_sqz_state(state):
    """Human-readable squeeze state."""
    return {
        "green": "No Squeeze",
        "black": "Low Compression",
        "red": "Mid Compression",
        "orange": "High Compression",
    }.get(state, state)


def detect_changes(ticker, current_results, timestamp=None, div_adj=0):
    """
    Compare current indicator state against previous snapshot.
    Log any changes as events.
    
    Returns list of event dicts.
    """
    if timestamp is None:
        timestamp = datetime.now(timezone.utc).isoformat()
    
    events = []
    conn = _get_db()
    c = conn.cursor()
    
    for tf in TIMEFRAMES:
        if tf not in current_results:
            continue

        current = current_results[tf]
        previous = get_previous_snapshot(ticker, tf, div_adj=div_adj)

        if previous is None:
            continue  # First run, no comparison

        # Determine bar status for this timeframe's events
        tf_bar_status = current.get("bar_status", "confirmed")

        # Check each indicator for changes

        # 1. Momentum color change
        if current["mom_color"] and previous["mom_color"] and current["mom_color"] != previous["mom_color"]:
            desc = f"MOM {_describe_mom_color(previous['mom_color'])} → {_describe_mom_color(current['mom_color'])}"
            event = {
                "timestamp": timestamp, "ticker": ticker, "timeframe": tf,
                "indicator": "MOM", "old_value": previous["mom_color"],
                "new_value": current["mom_color"], "description": desc,
                "bar_status": tf_bar_status,
            }
            events.append(event)

        # 2. Squeeze state change
        if current["sqz_state"] and previous["sqz_state"] and current["sqz_state"] != previous["sqz_state"]:
            desc = f"SQZ {_describe_sqz_state(previous['sqz_state'])} → {_describe_sqz_state(current['sqz_state'])}"
            event = {
                "timestamp": timestamp, "ticker": ticker, "timeframe": tf,
                "indicator": "SQZ", "old_value": previous["sqz_state"],
                "new_value": current["sqz_state"], "description": desc,
                "bar_status": tf_bar_status,
            }
            events.append(event)

        # 3. Band position flip
        if current["band_pos"] and previous["band_pos"] and current["band_pos"] != previous["band_pos"]:
            direction = "Bullish" if current["band_pos"] == "↑" else "Bearish"
            desc = f"BAND flipped {direction}"
            event = {
                "timestamp": timestamp, "ticker": ticker, "timeframe": tf,
                "indicator": "BAND", "old_value": previous["band_pos"],
                "new_value": current["band_pos"], "description": desc,
                "bar_status": tf_bar_status,
            }
            events.append(event)

        # 4. Acceleration state change
        if current["acc_state"] and previous["acc_state"] and current["acc_state"] != previous["acc_state"]:
            state = "Accelerating" if current["acc_state"] == "green" else "Decelerating"
            desc = f"ACC → {state}"
            event = {
                "timestamp": timestamp, "ticker": ticker, "timeframe": tf,
                "indicator": "ACC", "old_value": previous["acc_state"],
                "new_value": current["acc_state"], "description": desc,
                "bar_status": tf_bar_status,
            }
            events.append(event)

        # 5. Acceleration impulse fired
        if current["acc_impulse"] and current["acc_impulse"] != "":
            impulse_type = "Accel Impulse ▲" if current["acc_impulse"] == "▲" else "Decel Impulse ▼"
            desc = f"ACC {impulse_type}"
            event = {
                "timestamp": timestamp, "ticker": ticker, "timeframe": tf,
                "indicator": "ACC_IMPULSE", "old_value": "",
                "new_value": current["acc_impulse"], "description": desc,
                "bar_status": tf_bar_status,
            }
            events.append(event)

        # 6. Deviation signal fired
        if current["dev_signal"] and current["dev_signal"] != "" and current["dev_signal"] != previous.get("dev_signal", ""):
            signal_type = "DEV Buy Signal" if current["dev_signal"] == "buy" else "DEV Sell Signal"
            desc = signal_type
            event = {
                "timestamp": timestamp, "ticker": ticker, "timeframe": tf,
                "indicator": "DEV", "old_value": previous.get("dev_signal", ""),
                "new_value": current["dev_signal"], "description": desc,
                "bar_status": tf_bar_status,
            }
            events.append(event)
    
    # 7. 30WMA cross
    meta = current_results.get("_meta", {})
    prev_meta = get_previous_meta(ticker)
    
    if prev_meta and meta.get("above_wma30") is not None and prev_meta.get("above_wma30") is not None:
        if meta["above_wma30"] != prev_meta["above_wma30"]:
            direction = "above" if meta["above_wma30"] else "below"
            desc = f"30WMA crossed {direction}"
            # 30WMA is weekly — use weekly bar status if available, else confirmed
            wma_bar_status = current_results.get("1W", {}).get("bar_status", "confirmed")
            event = {
                "timestamp": timestamp, "ticker": ticker, "timeframe": "W",
                "indicator": "30WMA", "old_value": str(prev_meta["above_wma30"]),
                "new_value": str(meta["above_wma30"]), "description": desc,
                "bar_status": wma_bar_status,
            }
            events.append(event)

    def _commit():
        conn2 = _get_db()
        c2 = conn2.cursor()
        for evt in events:
            # Use time bucket (10-min window) for dedup via INSERT OR IGNORE
            # time_bucket = timestamp truncated to 10-minute intervals
            ts = evt["timestamp"]
            # Truncate to 10-min bucket: "2026-02-23T13:25:xx" -> "2026-02-23T13:2"
            time_bucket = ts[:15] if len(ts) >= 15 else ts[:10]
            c2.execute("""
                INSERT OR IGNORE INTO events (timestamp, ticker, timeframe, indicator, old_value, new_value, description, bar_status, div_adj, time_bucket)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (ts, evt["ticker"], evt["timeframe"], evt["indicator"], evt["old_value"], evt["new_value"], evt["description"], evt.get("bar_status", "confirmed"), div_adj, time_bucket))
        conn2.commit()
        conn2.close()
    
    conn.close()
    if events:
        _db_write(_commit)
    
    return events


def get_recent_events(limit=100, ticker=None, timeframe_min=None, indicator=None, since=None, bar_status=None, div_adj=None):
    """
    Query recent events with optional filters.

    Args:
        limit: Max events to return
        ticker: Filter by ticker
        timeframe_min: Minimum timeframe significance (e.g. '1D' = daily and above)
        indicator: Filter by indicator type
        since: ISO timestamp, only events after this time
        bar_status: Filter by bar status ('confirmed' or 'live')
        div_adj: Filter by dividend adjustment (0 or 1), None = both

    Returns list of event dicts.
    """
    conn = _get_db()
    c = conn.cursor()

    query = "SELECT timestamp, ticker, timeframe, indicator, old_value, new_value, description, bar_status, div_adj FROM events WHERE 1=1"
    params = []

    if ticker:
        query += " AND ticker = ?"
        params.append(ticker)

    if indicator:
        query += " AND indicator = ?"
        params.append(indicator)

    if since:
        query += " AND timestamp >= ?"
        params.append(since)

    if bar_status:
        query += " AND bar_status = ?"
        params.append(bar_status)

    if div_adj is not None:
        query += " AND div_adj = ?"
        params.append(int(div_adj))

    if timeframe_min:
        tf_order = {tf: i for i, tf in enumerate(TIMEFRAMES)}
        min_idx = tf_order.get(timeframe_min, 0)
        valid_tfs = [tf for tf, idx in tf_order.items() if idx >= min_idx]
        valid_tfs.append("W")  # Include weekly for 30WMA
        placeholders = ",".join("?" * len(valid_tfs))
        query += f" AND timeframe IN ({placeholders})"
        params.extend(valid_tfs)

    query += " ORDER BY timestamp DESC LIMIT ?"
    params.append(limit)

    c.execute(query, params)
    rows = c.fetchall()
    conn.close()

    return [
        {
            "timestamp": row[0],
            "ticker": row[1],
            "timeframe": row[2],
            "indicator": row[3],
            "old_value": row[4],
            "new_value": row[5],
            "description": row[6],
            "bar_status": row[7] or "confirmed",
            "div_adj": row[8] if row[8] is not None else 0,
        }
        for row in rows
    ]


def get_latest_full_state(div_adj=0, tickers=None):
    """
    Get the most recent complete state for all tickers (or a subset).
    Returns dict: { ticker: { timeframe: {indicators...}, '_meta': {...} } }
    """
    conn = _get_db()
    c = conn.cursor()

    # Single query: JOIN snapshots against their per-ticker/timeframe max timestamp.
    # Replaces the old N+1 pattern (GROUP BY then 14,500 individual SELECTs).
    ticker_filter = ""
    params = [div_adj]
    if tickers:
        placeholders = ",".join("?" * len(tickers))
        ticker_filter = f" AND s.ticker IN ({placeholders})"
        params.extend(list(tickers))
    params.append(div_adj)
    if tickers:
        params.extend(list(tickers))

    rows = c.execute(f"""
        SELECT s.ticker, s.timeframe, s.timestamp,
               s.mom_color, s.mom_rising, s.sqz_state, s.band_pos, s.band_flip,
               s.acc_state, s.acc_impulse, s.dev_signal, s.bar_status
        FROM snapshots s
        INNER JOIN (
            SELECT ticker, timeframe, MAX(timestamp) as max_ts
            FROM snapshots
            WHERE div_adj = ?{ticker_filter}
            GROUP BY ticker, timeframe
        ) latest ON s.ticker = latest.ticker
                 AND s.timeframe = latest.timeframe
                 AND s.timestamp = latest.max_ts
        WHERE s.div_adj = ?{ticker_filter}
    """, params).fetchall()

    full_state = {}
    for row in rows:
        ticker, tf = row[0], row[1]
        if ticker not in full_state:
            full_state[ticker] = {}
        full_state[ticker][tf] = {
            "mom_color": row[3],
            "mom_rising": bool(row[4]) if row[4] is not None else None,
            "sqz_state": row[5],
            "band_pos": row[6],
            "band_flip": bool(row[7]) if row[7] is not None else False,
            "acc_state": row[8],
            "acc_impulse": row[9],
            "dev_signal": row[10],
            "bar_status": row[11] or "confirmed",
            "snapshot_time": row[2],
        }

    # Meta snapshots — same single-query approach
    meta_params = []
    meta_filter = ""
    if tickers:
        meta_filter = f" AND m.ticker IN ({placeholders})"
        meta_params.extend(list(tickers))
    if tickers:
        meta_params.extend(list(tickers))

    meta_rows = c.execute(f"""
        SELECT m.ticker, m.timestamp,
               m.price, m.price_change_pct, m.above_wma30, m.wma30_value, m.wma30_cross,
               m.hvc_triggered, m.hvc_volume_ratio, m.hvc_candle_dir
        FROM meta_snapshots m
        INNER JOIN (
            SELECT ticker, MAX(timestamp) as max_ts
            FROM meta_snapshots
            {"WHERE ticker IN (" + placeholders + ")" if tickers else ""}
            GROUP BY ticker
        ) latest ON m.ticker = latest.ticker
                 AND m.timestamp = latest.max_ts
        {"WHERE m.ticker IN (" + placeholders + ")" if tickers else ""}
    """, meta_params).fetchall()

    for row in meta_rows:
        ticker = row[0]
        if ticker not in full_state:
            full_state[ticker] = {}
        full_state[ticker]["_meta"] = {
            "price": row[2],
            "price_change_pct": row[3],
            "above_wma30": bool(row[4]) if row[4] is not None else None,
            "wma30_value": row[5],
            "wma30_cross": row[6],
            "hvc_triggered": bool(row[7]) if row[7] is not None else False,
            "hvc_volume_ratio": row[8],
            "hvc_candle_dir": row[9] or "",
            "snapshot_time": row[1],
        }

    conn.close()
    return full_state


def get_state_at_time(before_timestamp, div_adj=0, tickers=None):
    """
    Get the most recent CONFIRMED snapshot for each ticker/timeframe that was taken
    BEFORE the given timestamp. Used for delta comparison.
    Only returns confirmed bar snapshots to avoid comparing against live/unfinished data.
    """
    conn = _get_db()
    c = conn.cursor()

    # Single query with JOIN — replaces N+1 pattern
    ticker_filter = ""
    params = [div_adj, before_timestamp]
    if tickers:
        placeholders = ",".join("?" * len(tickers))
        ticker_filter = f" AND s.ticker IN ({placeholders})"
        params.extend(list(tickers))
    params.extend([div_adj, before_timestamp])
    if tickers:
        params.extend(list(tickers))

    rows = c.execute(f"""
        SELECT s.ticker, s.timeframe,
               s.mom_color, s.mom_rising, s.sqz_state, s.band_pos, s.band_flip,
               s.acc_state, s.acc_impulse, s.dev_signal
        FROM snapshots s
        INNER JOIN (
            SELECT ticker, timeframe, MAX(timestamp) as max_ts
            FROM snapshots
            WHERE div_adj = ? AND timestamp <= ?
              AND (bar_status = 'confirmed' OR bar_status IS NULL)
              {ticker_filter}
            GROUP BY ticker, timeframe
        ) latest ON s.ticker = latest.ticker
                 AND s.timeframe = latest.timeframe
                 AND s.timestamp = latest.max_ts
        WHERE s.div_adj = ? AND s.timestamp <= ?
          AND (s.bar_status = 'confirmed' OR s.bar_status IS NULL)
          {ticker_filter}
    """, params).fetchall()

    old_state = {}
    for row in rows:
        ticker, tf = row[0], row[1]
        if ticker not in old_state:
            old_state[ticker] = {}
        old_state[ticker][tf] = {
            "mom_color": row[2],
            "mom_rising": bool(row[3]) if row[3] is not None else None,
            "sqz_state": row[4],
            "band_pos": row[5],
            "band_flip": bool(row[6]) if row[6] is not None else False,
            "acc_state": row[7],
            "acc_impulse": row[8],
            "dev_signal": row[9],
        }

    conn.close()
    return old_state


# ============================================================
# HVC (High Volume Candle) Events
# ============================================================

def save_hvc_event(ticker, event_date, detected_at, close_price, open_price,
                   volume, avg_volume, volume_ratio, candle_dir,
                   gap_type="", gap_level=None, gap_closed=False):
    """Save an HVC event. Uses INSERT OR IGNORE to prevent duplicates per ticker+date."""
    def _do():
        conn = _get_db()
        c = conn.cursor()
        c.execute("""
            INSERT OR IGNORE INTO hvc_events
            (ticker, event_date, detected_at, close_price, open_price,
             volume, avg_volume, volume_ratio, candle_dir, price_at_detection,
             gap_type, gap_level, gap_closed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (ticker, event_date, detected_at, close_price, open_price,
              volume, avg_volume, volume_ratio, candle_dir, close_price,
              gap_type or "", gap_level, int(gap_closed)))
        conn.commit()
        conn.close()
    _db_write(_do)


def update_hvc_gap_status(ticker, event_date, gap_closed):
    """Update the gap_closed status for an existing HVC event."""
    def _do():
        conn = _get_db()
        c = conn.cursor()
        c.execute("""
            UPDATE hvc_events SET gap_closed = ?
            WHERE ticker = ? AND event_date = ?
        """, (int(gap_closed), ticker, event_date))
        conn.commit()
        conn.close()
    _db_write(_do)


def get_hvc_events_today(date_str=None):
    """Get all HVC events for today (or a specific date).
    Returns list of dicts sorted by volume_ratio descending."""
    if date_str is None:
        from datetime import date
        date_str = date.today().isoformat()
    conn = _get_db()
    c = conn.cursor()
    c.execute("""
        SELECT ticker, event_date, volume, avg_volume, volume_ratio,
               candle_dir, close_price, open_price, detected_at,
               gap_type, gap_level, gap_closed
        FROM hvc_events
        WHERE event_date = ?
        ORDER BY volume_ratio DESC
    """, (date_str,))
    rows = c.fetchall()
    conn.close()
    return [
        {
            "ticker": r[0], "event_date": r[1], "volume": r[2],
            "avg_volume": r[3], "volume_ratio": r[4], "candle_dir": r[5],
            "close_price": r[6], "open_price": r[7], "detected_at": r[8],
            "gap_type": r[9] or "", "gap_level": r[10],
            "gap_closed": bool(r[11]) if r[11] is not None else False,
        }
        for r in rows
    ]


def get_hvc_events_history(since_date, until_date=None):
    """Get HVC events within a date range.
    Returns list of dicts sorted by event_date DESC, volume_ratio DESC."""
    conn = _get_db()
    c = conn.cursor()
    if until_date:
        c.execute("""
            SELECT ticker, event_date, volume, avg_volume, volume_ratio,
                   candle_dir, close_price, open_price, detected_at,
                   gap_type, gap_level, gap_closed
            FROM hvc_events
            WHERE event_date >= ? AND event_date <= ?
            ORDER BY event_date DESC, volume_ratio DESC
        """, (since_date, until_date))
    else:
        c.execute("""
            SELECT ticker, event_date, volume, avg_volume, volume_ratio,
                   candle_dir, close_price, open_price, detected_at,
                   gap_type, gap_level, gap_closed
            FROM hvc_events
            WHERE event_date >= ?
            ORDER BY event_date DESC, volume_ratio DESC
        """, (since_date,))
    rows = c.fetchall()
    conn.close()
    return [
        {
            "ticker": r[0], "event_date": r[1], "volume": r[2],
            "avg_volume": r[3], "volume_ratio": r[4], "candle_dir": r[5],
            "close_price": r[6], "open_price": r[7], "detected_at": r[8],
            "gap_type": r[9] or "", "gap_level": r[10],
            "gap_closed": bool(r[11]) if r[11] is not None else False,
        }
        for r in rows
    ]


def get_hvc_ticker_summary(since_date):
    """Get per-ticker HVC summary for the history explorer.
    Returns dict: {ticker: {count, first_date, first_price, dates, gap_ups, gaps_open}}"""
    conn = _get_db()
    c = conn.cursor()
    c.execute("""
        SELECT ticker, event_date, close_price, volume_ratio, candle_dir,
               gap_type, gap_level, gap_closed
        FROM hvc_events
        WHERE event_date >= ?
        ORDER BY ticker, event_date ASC
    """, (since_date,))
    rows = c.fetchall()
    conn.close()

    summary = {}
    for r in rows:
        tk = r[0]
        if tk not in summary:
            summary[tk] = {
                "count": 0,
                "first_date": r[1],
                "first_price": r[2],
                "latest_ratio": r[3],
                "dates": [],
                "gap_ups": 0,
                "gaps_open": 0,
            }
        summary[tk]["count"] += 1
        summary[tk]["latest_ratio"] = r[3]
        summary[tk]["dates"].append(r[1])
        if r[5] == "up":
            summary[tk]["gap_ups"] += 1
            if not r[7]:  # gap_closed == False
                summary[tk]["gaps_open"] += 1

    return summary


def preload_hvc_for_rs(lookback_days=90):
    """Preload HVC event counts for all tickers in a single query.
    Used by RS engine to enrich rankings with HVC data.
    Returns dict: {ticker: {hvc_count, gap_count, gaps_open, latest_ratio}}"""
    from datetime import datetime, timedelta, timezone
    since = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    conn = _get_db()
    c = conn.cursor()
    c.execute("""
        SELECT ticker, COUNT(*) as cnt,
               SUM(CASE WHEN gap_type = 'up' THEN 1 ELSE 0 END) as gap_ups,
               SUM(CASE WHEN gap_type = 'up' AND gap_closed = 0 THEN 1 ELSE 0 END) as gaps_open,
               MAX(volume_ratio) as max_ratio
        FROM hvc_events
        WHERE event_date >= ?
        GROUP BY ticker
    """, (since,))
    rows = c.fetchall()
    conn.close()
    result = {}
    for r in rows:
        result[r[0]] = {
            "hvc_count": r[1],
            "gap_count": r[2],
            "gaps_open": r[3],
            "max_ratio": r[4],
        }
    return result


def save_rs_report_snapshot(universe, report_date, report_json):
    """Save a report snapshot to the database."""
    import json as _json
    def _write():
        conn = _get_db()
        c = conn.cursor()
        c.execute("""
            INSERT OR REPLACE INTO rs_report_snapshots
            (universe, report_date, report_json, created_at)
            VALUES (?, ?, ?, ?)
        """, (universe, report_date,
              _json.dumps(report_json) if isinstance(report_json, dict) else report_json,
              datetime.now(timezone.utc).isoformat()))
        conn.commit()
        conn.close()
    _db_write(_write)


def get_rs_report_snapshot(universe, report_date):
    """Get a saved report snapshot for a specific date."""
    import json as _json
    conn = _get_db()
    c = conn.cursor()
    c.execute("""
        SELECT report_json, created_at FROM rs_report_snapshots
        WHERE universe = ? AND report_date = ?
    """, (universe, report_date))
    row = c.fetchone()
    conn.close()
    if row:
        return {"report": _json.loads(row[0]), "created_at": row[1], "date": report_date}
    return None


def get_rs_report_dates(universe, limit=90):
    """Get list of available report snapshot dates for a universe."""
    conn = _get_db()
    c = conn.cursor()
    c.execute("""
        SELECT report_date FROM rs_report_snapshots
        WHERE universe = ?
        ORDER BY report_date DESC LIMIT ?
    """, (universe, limit))
    dates = [row[0] for row in c.fetchall()]
    conn.close()
    return dates
