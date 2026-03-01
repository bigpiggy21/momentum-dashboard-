# Backtesting Engine — Design Document

## Overview
A signal-based backtesting system that lets the user define multi-condition entry rules
(using the same indicator vocabulary as the screener) and optional exit rules, then runs
them against historical data to produce trade-level and aggregate performance stats.

---

## 1. Signal Definition (Entry Logic)

Reuse the screener's filter model. Each entry signal is a set of AND-joined conditions:

| # | Indicator    | Timeframe | Value         | Example                          |
|---|-------------|-----------|---------------|----------------------------------|
| 1 | sqz_state   | 1W        | orange        | Weekly orange squeeze firing     |
| 2 | above_wma30 | —         | true          | Price above 30-week MA           |
| 3 | band_pos    | 1W        | up-cross      | Crossed above weekly 20 SMA      |
| 4 | mom_color   | 1D        | aqua          | Daily momentum positive & rising |

**Entry trigger:** The bar where ALL conditions become true simultaneously for the first
time (i.e., the last condition to flip is the trigger bar). Use `bar_status = 'confirmed'`
only to avoid look-ahead bias.

### Data source for entry detection
Two approaches (start with Option A, graduate to B):

- **Option A — Snapshot scan:** Walk the `snapshots` table chronologically per ticker.
  At each confirmed bar, check if all conditions are met. Entry = first bar where all
  conditions are true AND at least one was false on the prior bar.

- **Option B — Event-driven:** Use `events` table to find the *last* condition to flip,
  then verify all other conditions were already true at that timestamp. More efficient
  but requires careful cross-timeframe alignment.

---

## 2. Exit Logic

Three exit modes (user selects one):

### A. Fixed holding period (simplest)
- Hold for N bars of a specified timeframe (e.g., 20 daily bars = ~1 month)
- Exit at close of bar N
- Good for: "What happens to price after this signal?"

### B. Signal-based exit (the user's preferred model)
- Exit when a specific indicator condition fires
- Example: "Exit on first 1D deviation sell signal" (`dev_signal = 'sell'` on `1D`)
- Example: "Exit when weekly momentum turns red" (`mom_color = 'red'` on `1W`)
- Can combine multiple exit conditions with OR (exit on whichever fires first)
- Must include a max holding period failsafe (e.g., 252 bars = 1 year)

### C. Trailing stop (future)
- Exit if price drops X% from highest close since entry
- Requires bar-by-bar price tracking from CSV

### Exit data structure
```
exit_rules: [
  { type: "signal", indicator: "dev_signal", timeframe: "1D", value: "sell" },
  { type: "signal", indicator: "mom_color", timeframe: "1W", value: "red" },
  { type: "max_bars", timeframe: "1D", bars: 252 }
]
// Exit on whichever fires first (OR logic)
```

---

## 3. Trade Tracking

Each trade record:
```
{
  ticker: "NVDA",
  entry_date: "2024-03-15",
  entry_price: 878.37,
  entry_bar_status: "confirmed",
  exit_date: "2024-04-22",
  exit_price: 762.00,
  exit_reason: "dev_signal=sell on 1D",
  holding_days: 28,
  return_pct: -13.24,
  max_drawdown_pct: -18.5,   // worst close-to-close during hold
  max_gain_pct: 4.2,         // best close-to-close during hold
  conditions_at_entry: {      // snapshot of all indicators at entry
    sqz_state_1W: "orange",
    mom_color_1D: "aqua",
    above_wma30: true,
    ...
  }
}
```

---

## 4. Aggregate Statistics

### Summary panel
- Total signals found
- Win rate (% of trades with return > 0)
- Average return %
- Median return %
- Average holding period (days)
- Best / worst trade
- Profit factor (gross wins / gross losses)

### Distribution
- Return distribution histogram (buckets: <-20%, -20 to -10%, -10 to 0%, 0 to +10%, etc.)
- Holding period distribution

### By-ticker breakdown
- Which tickers generated the most signals
- Win rate per ticker
- Average return per ticker

---

## 5. Existing Data Infrastructure

### What we have (ready to use)
| Asset              | Location            | Depth          | Notes                           |
|--------------------|---------------------|----------------|---------------------------------|
| OHLCV daily        | cache/*.csv         | ~5 yrs (1250d) | Forward returns calculation     |
| Indicator snapshots| snapshots table     | ~5 yrs daily   | All 14 TFs, confirmed+live bars |
| Event transitions  | events table        | Since DB start  | Sparse, only on state changes   |
| Meta (30WMA, price)| meta_snapshots      | ~5 yrs         | above_wma30, wma30_cross, price |
| HVC events         | hvc_events table    | Full history    | Volume ratio, gap analysis      |
| RS history         | rs_history table    | ~25 days        | RS scores + ratings             |

### What we need to build
1. **Backtest engine** (`backtest_engine.py`) — signal detection + trade simulation
2. **API endpoints** — POST /api/backtest/run, GET /api/backtest/results
3. **Frontend** (`templates/backtest.html`) — signal builder + results display
4. **Route** — /backtest in app.py

### Key implementation notes
- Always filter `bar_status = 'confirmed'` for entry/exit to avoid look-ahead bias
- Use `div_adj = 1` by default (dividend-adjusted) for accurate returns
- Cross-timeframe signals: align by date, not exact timestamp (a 1W bar and a 1D bar
  are "simultaneous" if the 1D bar falls within the 1W bar's date range)
- Re-entry: after an exit, allow re-entry only after all conditions reset (at least one
  goes false) then re-trigger. Prevents duplicate signals on consecutive bars.

---

## 6. UI Design — /backtest page

### Layout: Three sections stacked vertically

```
+------------------------------------------------------------------+
|  HEADER (same as all pages, BACKTESTING = page-active)           |
+------------------------------------------------------------------+
|                                                                    |
|  SIGNAL BUILDER                                                    |
|  +--------------------------------------------------------------+ |
|  | Entry Conditions                                    [+ Add]  | |
|  | +---------+------------+-----------+--------+-------+------+ | |
|  | | Join    | Indicator  | Timeframe | Value  |  TFs  |  x   | | |
|  | +---------+------------+-----------+--------+-------+------+ | |
|  | | IF      | Squeeze    | 1W        | Orange |  [1W] |  x   | | |
|  | | AND     | 30WMA      | —         | Above  |   —   |  x   | | |
|  | | AND     | Band Pos   | 1W        | ↑ cross|  [1W] |  x   | | |
|  | +---------+------------+-----------+--------+-------+------+ | |
|  |                                                               | |
|  | Exit Rules                                          [+ Add]  | |
|  | +---------+------------+-----------+--------+-------+------+ | |
|  | | OR      | Deviation  | 1D        | Sell   |  [1D] |  x   | | |
|  | | OR      | Max Hold   | 1D        | 252 bars       |  x   | | |
|  | +---------+------------+-----------+--------+-------+------+ | |
|  |                                                               | |
|  | Universe: [All watchlists v]  Adj: [DIV v]  Period: [5yr v]  | |
|  |                                                               | |
|  |                              [ Run Backtest ]                 | |
|  +--------------------------------------------------------------+ |
|                                                                    |
|  RESULTS SUMMARY (appears after run)                               |
|  +--------------------------------------------------------------+ |
|  | Signals: 47  |  Win Rate: 68%  |  Avg Return: +12.3%  |     | |
|  | Avg Hold: 34d | Best: +89% SMCI | Worst: -22% INTC   |     | |
|  | Profit Factor: 2.4  |  Median: +8.1%                  |     | |
|  +--------------------------------------------------------------+ |
|  |  [ Return Distribution Histogram ]                            | |
|  |  ████░░░░████████████████░░░░░░██████░░░░                     | |
|  |  -30%    -10%     0%     +10%    +30%    +50%                 | |
|  +--------------------------------------------------------------+ |
|                                                                    |
|  TRADE LOG (sortable table)                                        |
|  +--------------------------------------------------------------+ |
|  | Ticker | Entry Date | Entry $ | Exit Date | Exit $ | Return  | |
|  |        |            |         |           |        | Hold    | |
|  |        |            |         |           |        | Reason  | |
|  +--------------------------------------------------------------+ |
|  | NVDA   | 2024-03-15 | 878.37  | 2024-04-22| 762.00 | -13.2% | |
|  |        |            |         |           |        | 28d     | |
|  |        |            |         |           |        | DEV sell| |
|  +--------------------------------------------------------------+ |
|  | SMCI   | 2024-01-08 | 284.20  | 2024-02-12| 535.10 | +88.3% | |
|  |        |            |         |           |        | 25d     | |
|  |        |            |         |           |        | MaxHold | |
|  +--------------------------------------------------------------+ |
|  | ... (expandable, sortable by any column)                      | |
|  +--------------------------------------------------------------+ |
|                                                                    |
+------------------------------------------------------------------+
```

### Signal Builder panel
- Modeled after the existing screener filter rows
- Dropdowns for indicator, timeframe, value (same options as screener)
- Entry rows joined with AND, exit rows joined with OR
- Universe selector: pick watchlist(s) to test against
- Period selector: 1yr, 2yr, 5yr, All
- "Run Backtest" button → shows spinner → populates results

### Results Summary panel
- Grid of key stats in colored boxes (green for positive, red for negative)
- Histogram rendered with inline divs (no chart library needed — same approach as
  the bollinger heatmap cells, just wider)
- Collapsible "By Ticker" breakdown table

### Trade Log panel
- Sortable table (click column headers)
- Color-coded returns (green/red)
- Exit reason column shows what triggered the exit
- Optional: click a trade row to see full indicator state at entry (expandable detail)

---

## 7. Implementation Phases

### Phase 1 — Fixed holding period (MVP)
- Build `backtest_engine.py` with snapshot-scan approach
- Entry signal builder (reuse screener filter model)
- Fixed N-bar exit only (no signal-based exit yet)
- Summary stats + trade log table
- API: POST /api/backtest/run → returns full results JSON
- Frontend: signal builder + results display

### Phase 2 — Signal-based exits
- Add exit condition builder to UI
- Engine walks forward bar-by-bar checking exit conditions
- Track max drawdown / max gain during hold
- Add exit reason to trade records

### Phase 3 — Advanced features
- Trailing stop exit
- Saved backtest configurations (like saved searches)
- Compare two signal definitions side-by-side
- Equity curve chart (cumulative returns over time)
- Monte Carlo / randomized baseline comparison
- Export trades to CSV

---

## 8. API Design

### POST /api/backtest/run
```json
Request:
{
  "entry": [
    {"indicator": "sqz_state", "timeframe": "1W", "value": "orange"},
    {"indicator": "above_wma30", "timeframe": null, "value": "true"},
    {"indicator": "band_pos", "timeframe": "1W", "value": "↑"}
  ],
  "exit": [
    {"type": "signal", "indicator": "dev_signal", "timeframe": "1D", "value": "sell"},
    {"type": "max_bars", "timeframe": "1D", "bars": 252}
  ],
  "universe": "all",           // or watchlist name
  "div_adj": 1,
  "lookback_years": 5,
  "allow_reentry": false       // if true, same ticker can trigger again after exit
}

Response:
{
  "ok": true,
  "summary": {
    "total_signals": 47,
    "win_rate": 0.68,
    "avg_return_pct": 12.3,
    "median_return_pct": 8.1,
    "avg_holding_days": 34,
    "best": {"ticker": "SMCI", "return_pct": 88.3},
    "worst": {"ticker": "INTC", "return_pct": -22.1},
    "profit_factor": 2.4
  },
  "distribution": [
    {"bucket": "-30 to -20%", "count": 2},
    {"bucket": "-20 to -10%", "count": 5},
    ...
  ],
  "trades": [
    {
      "ticker": "NVDA",
      "entry_date": "2024-03-15",
      "entry_price": 878.37,
      "exit_date": "2024-04-22",
      "exit_price": 762.00,
      "return_pct": -13.24,
      "holding_days": 28,
      "exit_reason": "dev_signal=sell 1D",
      "max_drawdown_pct": -18.5,
      "max_gain_pct": 4.2
    },
    ...
  ]
}
```

---

## 9. File Plan

```
momentum-dashboard/
  backtest_engine.py          # Core engine: signal scan + trade simulation
  templates/backtest.html     # UI: signal builder + results
  app.py                      # Add /backtest route + /api/backtest/run endpoint
  BACKTEST_DESIGN.md          # This file
```

---

## 10. Open Questions (for when we build)

1. **Cross-timeframe alignment:** If entry needs weekly squeeze + daily momentum,
   what does "simultaneous" mean? Proposal: same trading day (the daily bar that
   falls on the weekly bar's close date).

2. **Re-entry policy:** After exit, can the same ticker re-enter immediately if
   conditions are still met? Proposal: require at least one condition to go false
   then re-trigger (prevents consecutive-bar duplicates).

3. **Commission/slippage model:** Ignore for v1, add optional % cost later.

4. **Survivorship bias:** Current universe is today's watchlists. Tickers that
   delisted or were removed won't appear. Acknowledged limitation for v1.

5. **Intraday vs daily:** Signals on hourly timeframes (1H, 4H, 8H) have only
   ~90 days of history. Daily+ timeframes have ~5 years. Might need to restrict
   lookback for intraday entry/exit rules.
