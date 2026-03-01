# Live Sweep Pipeline + Discord Alerting — Design Notes

**Date**: 2026-02-26
**Status**: Research/Design phase — not yet built

---

## Overview

Extend the current batch/backfill sweep system with:
1. **Live WebSocket ingestion** — 15-min delayed dark pool sweeps via Massive.com WebSocket
2. **Real-time detection** — Incremental clusterbomb/monster/rare sweep detection on today's data
3. **Discord alerting** — Webhook notifications with rich embeds + chart snapshots

---

## Current State (Batch System)

- REST API polling: `GET /v3/trades/{ticker}?timestamp=YYYY-MM-DD` (backfill, day by day)
- 8 worker threads, 50k trades/page, cursor pagination
- Filters: `exchange==4` (FINRA TRF/dark pool) + `condition 14` (Intermarket Sweep) + `notional >= $500K`
- Detection: clusterbombs (3+ sweeps, $38M+ total for stocks), rare sweeps (N-day dormancy), monsters ($100M+ single sweep)
- All stored in SQLite: `sweep_trades`, `clusterbomb_events`, `sweep_fetch_log`, `sweep_meta`

---

## Massive.com WebSocket API

### Connection
- Python client: `massive-com/client-python` (`WebSocketClient`)
- Auth: API key (same key as REST)
- Developer tier: **15-minute delayed** data (fine for our use case)
- Subscribe: `["T.*"]` for all stocks, or `["T.MSFT", "T.AAPL"]` for specific tickers

### Trade Message Format
| Field | Description |
|-------|-------------|
| `sym` | Ticker symbol |
| `p` | Price |
| `s` | Size (shares) |
| `x` | Exchange ID (4 = FINRA TRF = dark pool) |
| `c` | Conditions array (14 = Intermarket Sweep) |
| `trfi` | TRF ID (confirms dark pool routing) |
| `t` | SIP timestamp (Unix ms) |
| `q` | Sequence number |
| `i` | Trade ID |
| `z` | Tape designation |

### Dark Pool Sweep Filter (identical to current REST logic)
```python
is_darkpool = (msg['x'] == 4 and msg.get('trfi') is not None)
is_sweep = (14 in msg.get('c', []))
notional = msg['p'] * msg['s']
qualifies = is_darkpool and is_sweep and notional >= 500_000
```

---

## Component 1: Live Sweep Ingestion Daemon

### Architecture
```
WebSocket (T.*)
  → Filter: exchange==4, condition 14, notional >= $500K
  → Buffer qualifying sweeps (batch every ~30s)
  → INSERT into sweep_trades (same schema as backfill)
  → Every 5 min: run detection cycle on today's data
```

### Key Design Decisions
- **Single WebSocket connection** subscribing to `T.*` (all stocks) — one connection covers the full universe
- **In-process filter** — discard 99%+ of trades immediately (only dark pool sweeps kept)
- **Batch writes** — buffer qualifying sweeps, flush to DB every ~30 seconds to reduce SQLite contention
- **Reconnection logic** — auto-reconnect with exponential backoff on disconnect
- **Heartbeat** — WebSocket keepalive to detect stale connections
- **Dedup** — Use `UNIQUE(ticker, sip_timestamp, price, size)` constraint (already exists) to prevent duplicates if backfill and live overlap

### New Database State
```sql
-- Track what's been alerted to avoid duplicate notifications
CREATE TABLE IF NOT EXISTS sweep_alert_log (
    event_type TEXT,        -- 'clusterbomb', 'rare_sweep', 'monster_sweep'
    ticker TEXT,
    event_date TEXT,
    alerted_at TEXT,
    channel TEXT,           -- 'discord', 'email', etc.
    PRIMARY KEY(event_type, ticker, event_date, channel)
);
```

### Detection Cycle (every 5 minutes during market hours)
1. Run `detect_clusterbombs()` on today's data only
2. Run `detect_rare_sweep_days()` on today's data
3. Run `detect_monster_sweeps()` on today's data
4. Diff new events against `sweep_alert_log`
5. Fire alerts for genuinely new events
6. Mark as alerted in `sweep_alert_log`

---

## Component 2: Discord Alert Dispatcher

### Discord Webhook
- **Free** — built-in feature of any Discord server
- No bot token needed, just a webhook URL
- Rate limit: 30 requests/60 seconds per channel (more than enough)
- Supports rich embeds with color, fields, images, timestamps

### Webhook URL Storage
```json
// In scheduler_config.json
{
  "alerts": {
    "enabled": true,
    "discord_webhook_url": "https://discord.com/api/webhooks/...",
    "alert_on": ["clusterbomb", "rare_sweep", "monster_sweep"],
    "min_total_notional": 38000000,
    "quiet_hours": false
  }
}
```

### Discord Embed Format
```
┌──────────────────────────────────────────┐
│ 🟠 MONSTER SWEEP — MSFT                 │  (orange for monster, purple for rare, gold for CB)
│                                          │
│ Total:     $152.3M                       │
│ Sweeps:    7                             │
│ Rank:      #1 / 831 days                 │
│ VWAP:      $415.42                       │
│ Direction: BUY                           │
│                                          │
│ 📅 2026-02-26  ⏰ 15 min delayed         │
│                                          │
│ [Chart image attached]                   │
└──────────────────────────────────────────┘
```

### Chart Snapshot Generation
- Use `mplfinance` (matplotlib finance) for server-side candlestick rendering
- Generate 90-day daily chart with sweep markers overlaid
- Save as PNG, attach to Discord webhook as file upload
- Alternative: if too complex, skip chart initially and add later

---

## Architecture Diagram

```
Massive.com WebSocket (T.*)
         │
         │ 15-min delayed trades
         ▼
┌─────────────────────────────────┐
│ Live Sweep Daemon               │
│ (background thread in app.py    │
│  or standalone process)         │
│                                 │
│ Filter: exchange==4, cond 14    │
│ Buffer → batch write to DB      │
│ Detection cycle every 5 min     │
└──────────┬──────────────────────┘
           │
           │ New events detected
           ▼
┌─────────────────────────────────┐        ┌─────────────────────┐
│ Alert Dispatcher                │───────▶│ Discord Webhook     │
│                                 │        │ (rich embed + chart)│
│ Diff against alert_log          │        └─────────────────────┘
│ Format embed + chart snapshot   │
│ POST to webhook URL             │        ┌─────────────────────┐
│                                 │───────▶│ Email (future)      │
└─────────────────────────────────┘        └─────────────────────┘
           │
           ▼
┌─────────────────────────────────┐
│ momentum_dashboard.db           │
│ sweep_trades (same table)       │
│ clusterbomb_events (same table) │
│ sweep_alert_log (NEW)           │
└─────────────────────────────────┘
```

---

## What Already Exists vs What's New

| Component | Status |
|-----------|--------|
| Sweep filtering logic (exchange==4, condition 14) | ✅ Exists in `sweep_engine.py` |
| DB schema (sweep_trades, clusterbomb_events) | ✅ Exists |
| Detection algorithms (clusterbomb, monster, rare) | ✅ Exists |
| Chart data generation | ✅ Exists |
| WebSocket connection manager | 🆕 To build |
| Incremental detection (today only, diff) | 🆕 To build |
| Discord webhook sender with rich embeds | 🆕 To build |
| Chart snapshot generator (mplfinance) | 🆕 To build |
| Alert log table + dedup | 🆕 To build |
| Config UI for webhook URL + alert preferences | 🆕 To build |

---

## Estimated Build Effort

- **WebSocket daemon + DB writes**: 1 session
- **Incremental detection + alert diffing**: 1 session
- **Discord webhook + rich embeds**: 0.5 session
- **Chart snapshot (mplfinance)**: 0.5 session
- **Config UI + settings panel integration**: 0.5 session

**Total: ~3 sessions**

---

## Dependencies to Install
- `websocket-client` or `polygon` (massive-com/client-python)
- `mplfinance` (for chart snapshots)
- `requests` (already installed — used for webhook POST)

---

## References
- [Massive.com Trades WebSocket](https://massive.com/docs/websocket/stocks/trades)
- [Massive.com Python Client](https://github.com/massive-com/client-python)
- [Discord Webhooks Guide](https://discord.com/developers/docs/resources/webhook)
- [Dark Pool Data at Polygon](https://polygon.io/knowledge-base/article/does-polygon-offer-dark-pool-data)
