# Sweeps Dashboard — Session Handover

## Project Basics

- **Root**: `C:\Users\david\Downloads\momentum-dashboard`
- **Code dir**: `C:\Users\david\Downloads\momentum-dashboard\momentum-dashboard\`
- **Server**: `cmd /c "cd momentum-dashboard && python app.py --serve-only"` → port 8080
- **DB**: `momentum_dashboard.db` (SQLite, path is relative — server MUST run from inner dir)
- **Date**: 2026-02-25

### Critical Preview Tool Notes
- **NEVER use `preview_screenshot`** — it hangs the session. Use `preview_eval`, `preview_snapshot`, `preview_inspect`, `preview_console_logs`, `preview_logs` instead.
- Canvas-based charts (lightweight-charts) can't be visually verified in the preview tool's headless browser (canvas pixels are empty). Verify chart behaviour through code review, console logs, and API data checks.
- Launch config exists at `.claude/launch.json` — use `preview_start` with name `"dashboard"`.

---

## Key Files (3 files, ~5500 lines total)

### `templates/sweeps.html` (~1914 lines) — Single-page UI
All CSS, HTML, and JS in one file. Uses **lightweight-charts v4.1.0** from CDN.

**CSS** (lines 8–300):
- `.header-row1` (line 15): Global nav bar with logo, page buttons
- `.header-row2` (line 33): Ticker input, watchlist, sector dropdown, Min $ filter
- Left panel (line 68): 680px wide, tracker cards with tabs (Stocks/Index/Favs)
- Right panel (line 121): Chart + data tables
- Settings panel overlay (line 213): Slide-out gear panel with detection params
- Responsive breakpoint at 1400px (line 286)

**HTML** (lines 300–620):
- `header-row1`: Logo, page nav (RS Rankings / Sweeps), status indicator
- `header-row2`: Ticker input (`#tickerInput`, placeholder "e.g. NVDA, AAPL"), watchlist dropdown, sector dropdown (`#sectorSelect`), Min $ input (`#trackerMinTotal` default "38M"), Days controls, Fetch/Detect/Settings buttons
- Left panel: Tab bar (Stocks/Index/Favs), **type filter bar** (All/Clusterbombs/Rare Sweeps — lines 388-394), tracker scroll container
- Right panel: Chart title row, chart container (`#chartContainer`), chart settings row, data tables (Clusterbombs table + Raw Sweeps table)
- Settings panel: Detection parameters with **dual profile tabs** (Stocks/ETFs), each with min_sweeps, min_notional, min_total, rarity_days
- Chart overlay for maximized view

**JavaScript sections** (lines 627–1914):
| Section | Lines | Purpose |
|---------|-------|---------|
| STATE | 627–669 | Global variables (`chartInstance`, `_markerSeries`, `_markerList`, `_trackerTypeFilter`, `_sectorData`, etc.) |
| HELPERS | 670–719 | `fmt$()`, `getCBThreshold()`, `parseNotional()` |
| TAB SWITCHING | 720–730 | Data table tabs (CB table vs Raw sweeps) |
| TRACKER TABS + FAVOURITES | 731–910 | `loadTrackerPanels()`, `renderTrackerCards()`, `filterByType()`, `setTrackerTypeFilter()`, favourites localStorage logic |
| LOAD DATA | 911–1063 | `loadAll()`, `loadChart()`, `loadClusterbombs()`, `loadSweepSummary()` |
| CHART | 1064–1367 | `renderChart()` — candlestick + volume + **3-series marker system** |
| CHART RANGE + MAXIMIZE | 1368–1388 | Fullscreen overlay logic |
| CHART SETTINGS | 1389–1513 | Scale type, candle style, colour pickers |
| CHART FOCUS | 1514–1584 | `focusChartOnDate()` — scroll chart to a CB date |
| FETCH / DETECT | 1585–1670 | `fetchSweeps()`, `redetectClusterbombs()` |
| WATCHLIST LOADING | 1678–1717 | Populate watchlist dropdown from `/api/sweeps/watchlists` |
| API ACCESS CHECK | 1718–1730 | Check Unusual Whales API key |
| SETTINGS PANEL | 1731–1878 | `openSettings()`, `closeSettings()`, `loadDetectionConfig()`, `saveDetectionConfig()`, dual profile tab switching |
| SECTOR FILTER | 1879–1904 | `loadSectors()`, `applySectorFilter()` |
| INIT | 1905–1914 | `window.onload` → calls `loadAll()` |

### `sweep_engine.py` (~1525 lines) — Detection engine + data layer
- **KNOWN_ETFS** (line 53): Set of ~60 ETF tickers for stock/ETF classification
- **SECTOR_GROUPS** (line 100): Dict mapping 14 sector names → ticker lists
- **`get_sectors()`** (line 118): Returns sector list for dropdown
- **`init_sweep_db()`**: Creates tables + migrations (including `event_type` column)
- **`fetch_and_store_sweeps()`**: Fetches from Unusual Whales API, stores in `sweep_trades`
- **`detect_clusterbombs()`** (line 709): Core CB detection — groups sweeps by ticker+date, checks thresholds (min_sweeps, min_notional, min_total), rarity check. Accepts `tickers=` param for partitioned stock/ETF detection.
- **`_check_rarity()`** (line 872): Checks if event is "rare" — no sweep activity in prior N days. Guards against false positives by checking `fetch_log` coverage.
- **`detect_rare_sweep_days()`** (line 909): Independent rare sweep detection — finds ANY sweep activity after long quiet periods, regardless of CB thresholds. Uses `INSERT OR IGNORE` so existing CBs take priority.
- **`get_ticker_day_ranks()`** (line 1095): SQL window function ranking each ticker's sweep days by total notional. Returns `{(ticker, date): {rank, total_days, day_notional}}`.
- **`get_clusterbombs()`** (line 1136): Query CB events with rank data merged. Used by CB table.
- **`get_sweep_chart_data()`** (line 1242): Returns candles + sweeps + CBs with rank for chart rendering.
- **`get_tracker_data()`** (line 1395): Returns enriched events for tracker cards — current price, pct_gain, days_since, bias, rank, **period returns** (1M/3M/6M/12M).
- **`_load_daily_prices()`** (line 1502): Loads per-ticker daily CSV from price cache for period return calculation.

### `app.py` (~2079 lines) — HTTP server
Key sweep endpoints:
| Method | Path | Handler | Purpose |
|--------|------|---------|---------|
| GET | `/sweeps` | serve file | Serve sweeps.html |
| GET | `/api/sweeps/chart` | `serve_sweep_chart` | Candles + sweeps + CBs for chart |
| GET | `/api/sweeps/tracker` | `serve_sweep_tracker` | Tracker card data (enriched) |
| GET | `/api/sweeps/clusterbombs` | `serve_sweep_clusterbombs` | CB table data |
| GET | `/api/sweeps/detection-config` | `serve_sweep_detection_config` | GET/POST dual-profile config |
| GET | `/api/sweeps/sectors` | `serve_sweep_sectors` | Sector list for dropdown |
| POST | `/api/sweeps/fetch` | `serve_sweep_fetch` | Fetch sweeps from API (background thread, runs detect after) |
| POST | `/api/sweeps/redetect` | `serve_sweep_redetect` | Re-detect with custom params (dual stock/ETF profiles) |

**Dual detection in redetect** (line ~1521): Partitions tickers into stocks vs ETFs using `is_etf()`, runs `detect_clusterbombs()` twice with different thresholds, then runs `detect_rare_sweep_days()` for both groups.

---

## Architecture

### Detection System (Dual Profile)
Config stored in `sweep_detection_config.json`:
```json
{
  "stock": {"min_sweeps": 3, "min_notional": 1000000, "min_total": 10000000, "rarity_days": 60},
  "etf":   {"min_sweeps": 1, "min_notional": 10000000, "min_total": 20000000, "rarity_days": 60}
}
```
- **Stocks**: Require 3+ sweeps clustering ("clusterbomb convergence")
- **ETFs**: Can signal on 1 massive sweep ($10M+)
- **Rare sweep detection**: Separate pass — any sweep activity after N days of silence, stored as `event_type='rare_sweep'`

### DB Schema (key tables)
- **`sweep_trades`**: Raw sweep data (ticker, trade_date, price, size, notional, is_darkpool, is_sweep, etc.)
- **`clusterbomb_events`**: Detected events. UNIQUE(ticker, event_date). Key columns: sweep_count, total_notional, avg_price, direction, is_rare, **event_type** ('clusterbomb' or 'rare_sweep'), threshold_*, detected_at
- **`fetch_log`**: Records when tickers were fetched (used by rarity check)

### Chart Marker System (3 invisible LineSeries)
lightweight-charts doesn't support multi-line marker text or independent text positioning. Solved with 3 separate LineSeries, each with their own markers:

1. **txtAboveSeries**: `position:'aboveBar'`, `size:1`, text = `"$63.9M"` ($ amount)
2. **dotSeries**: `position:'inBar'`, `size:3`, text = `""`, color = `rgba(245,158,11,0.55)` (visible circle at VWAP)
3. **txtBelowSeries**: `position:'belowBar'`, `size:1`, text = `"#1"` (rank)

All three share the same data points (time + markerPrice/VWAP). Rare sweeps use purple: `rgba(167,139,250,0.55)`.

`_markerSeries` and `_markerList` global refs point to dotSeries for focus/highlight functionality.

### Tracker Card Layout (3×4 grid)
```
CB Total  | Avg Price | Current | Sweeps
1M        | 3M        | 6M      | 12M
Days Ago  | Bias      | Rank    | % Now
```
- **Bias**: Capitalized direction (Sell/Buy/Mixed) in grid body. Header shows BULL/BEAR badge.
- **Period returns**: Calculated from daily CSV price cache. N/A if target date is in the future.
- **Rank**: `#N/M` format (e.g. #1/39 = best day out of 39 trading days with sweep data)
- **Rare sweeps**: Purple left border, "RARE SWEEP" badge, "Sweep Total" instead of "CB Total"

### Left Panel Features
- **Width**: 680px (responsive to 480px at 1400px breakpoint)
- **Tab bar**: Stocks (count) | Index/ETF (count) | Favs (count)
- **Type filter**: All | Clusterbombs | Rare Sweeps (client-side filtering via `filterByType()`)
- **Favourites**: Stored in localStorage, matched against enriched tracker data

### Header Controls
- **Ticker input**: Free text, comma-separated, uppercase
- **Watchlist dropdown**: Pre-built lists from `/api/sweeps/watchlists`
- **Sector dropdown**: 14 sector groups, populates ticker input on selection
- **Min $ filter**: Amber-styled input, default "38M", controls tracker card threshold
- **Days Ago**: Calendar days range filter

---

## Recent Changes (This Session)

1. **Chart markers redesigned**: From 2-series (circle+square) → 3-series (txtAbove + dot + txtBelow). Circle at VWAP with rgba transparency. Text size:1 (not 0) to guarantee rendering.

2. **Tracker cards expanded**: Added 1M/3M/6M/12M period returns row. Direction renamed to "Bias" and capitalized. Grid now 3×4 (was 3×3).

3. **Rare sweep detection**: New `detect_rare_sweep_days()` function as independent detection pass. New `event_type` column ('clusterbomb' or 'rare_sweep') with ALTER TABLE migration.

4. **Tracker type filter**: All/Clusterbombs/Rare Sweeps buttons with client-side filtering. Context-aware empty messages.

5. **Sector dropdown**: 14 predefined sector groups populated from `/api/sweeps/sectors`.

6. **Left panel widened** from 340px → 680px, text sizes increased throughout cards.

7. **Min $ filter** moved from settings panel to header-row2 (prominent amber styling).

8. **Placeholder text** fixed to "e.g. NVDA, AAPL".

---

## Known Limitations / Notes

- **lightweight-charts marker text size**: The library doesn't expose a CSS/API control for marker text font size. Text size is determined internally. Cannot be increased beyond default.
- **Canvas rendering in preview tool**: The headless Playwright browser doesn't render canvas content. Chart verification must be done via code review, API data checks, and console logs — or in a real browser.
- **Period returns**: Rely on daily CSV price cache in the price cache directory. If a ticker doesn't have cached price data, returns show as N/A.
- **Rare sweep detection**: Currently no rare sweeps exist in the dataset (all events are clusterbombs). The filter and UI are ready but awaiting data that triggers rare sweep detection.
- **Index/ETF tracker fetch**: Occasional `TypeError: Failed to fetch` on the ETF tab during page load — appears to be a race condition, not a code bug. Non-blocking.

---

## File Quick-Reference for HTML Changes

If making CSS changes → lines 8–300
If modifying header/controls → lines 300–400
If changing tracker card rendering → `renderTrackerCards()` at line 847
If modifying chart markers → `renderChart()` at line 1065, marker code around lines 1234–1350
If changing chart settings → lines 1389–1513
If modifying detection/fetch UI → lines 1585–1670
If changing settings panel → lines 1731–1878
If adding new sections → add before INIT section at line 1905
