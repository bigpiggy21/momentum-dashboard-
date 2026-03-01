# CLAUDE.md — Couloir Technologies Momentum Dashboard

## Project Overview

This is a financial momentum dashboard for stock trading that displays TTM Squeeze, momentum, acceleration, and Bollinger Band indicators across 14 timeframes for thousands of tickers. The primary goal is **perfect alignment with TradingView's calculations**. Even small discrepancies matter for trading accuracy.

**Owner:** Alex, based in London. Active trader using technical indicators. Treats TradingView as the gold standard.

**Location:** `C:\Users\david\Downloads\momentum-dashboard\momentum-dashboard\`

## Architecture

### Core Files

| File | Role |
|---|---|
| `app.py` | HTTP server (--serve-only mode), serves dashboard HTML + API endpoints |
| `engine.py` | Indicator computation engine, runs independently with --workers N |
| `collector.py` | Data fetcher, downloads OHLC from Polygon/Massive API into CSV cache |
| `data_fetcher.py` | Data loading, timeframe aggregation, dividend adjustment, calendar alignment |
| `indicators.py` | TTM Squeeze, momentum, acceleration, Bollinger Band calculations |
| `change_detector.py` | SQLite-backed snapshot storage, change detection, scan caching |
| `config.py` | Timeframes, API keys, indicator version, auto-discovers watchlists |
| `templates/dashboard.html` | Single-page dashboard frontend |
| `templates/log.html` | Event log page |

### Watchlist System

Watchlists live in `watchlists/` folder as `*_watchlist.py` files. Auto-discovered by `config._load_watchlists()`. Hot-reloaded on each API request (no server restart needed for new watchlists).

Current watchlists:
- **Debug** (10 tickers) — accuracy testing
- **Leverage** (87 tickers) — core trading universe with LSE leverage ETF mappings
- **Overview** (66 tickers) — broad market ETF coverage
- **SP500** (480 tickers) — from TradingView export
- **Russell1000** (1005 tickers) — from iShares IWB holdings
- **Russell2000** (1939 tickers) — from iShares IWM holdings
- **Russell3000** (2944 tickers) — R1000 + R2000 combined

### Data Flow

```
collector.py → CSV cache files (cache/ folder)
    ↓
engine.py → reads CSV cache → builds timeframes → computes indicators → SQLite snapshots
    ↓
app.py --serve-only → reads SQLite → serves to browser
```

### Key Commands

```bash
# Collect data for a watchlist
python collector.py --watchlist SP500 --workers 4

# Compute indicators (reads from cache, no API calls)
python engine.py --watchlist SP500 --once --workers 8

# Serve dashboard (reads from SQLite)
python app.py --serve-only

# Single ticker lookup
python engine.py --ticker NVDA --once
```

## Indicator Calculations

All indicators target exact TradingView replication.

### TTM Squeeze
- Bollinger Bands: 20-period SMA ± 2.0 stddev
- Keltner Channels: 20-period SMA ± 1.5 × ATR(20)
- Squeeze ON (red dot): BB inside KC
- Squeeze OFF (green dot): BB outside KC
- States: `high` (orange, wide BB), `mid` (red, normal), `low` (yellow, tight)

### Momentum
- Linear regression (20-period) of (close - midline)
- Where midline = (highest(20) + lowest(20)) / 2
- Colors: cyan (positive rising), blue (positive falling), red (negative falling), yellow (negative rising)

### Acceleration
- Momentum of the momentum histogram (second derivative)
- Shows rate of change of squeeze momentum

### Bollinger Band Position
- Where price sits relative to BB bands
- Flip detection for band crossings

### 30-Week Moving Average (30WMA)
- Weekly SMA(30) with amber glow for recent crosses

## Timeframes (14 total)

Hourly: 1H, 4H, 8H
Daily: 1D, 2D, 3D, 5D
Weekly: 1W, 2W, 6W
Monthly: 1M, 3M, 6M, 12M

## Critical Technical Details

### TradingView Alignment

- **Multi-day timeframes (2D, 3D, 5D):** Use epoch-anchored grouping with hardcoded anchor dates per timeframe. NYSE trading calendar computed algorithmically (Easter via Anonymous Gregorian algorithm, weekend adjustment rules). Validated against NYSE 2026-2028.
- **Weekly from daily:** Always rebuilt from daily data (Polygon's weekly bars have aggregation bugs — missing daily lows).
- **Extended hours:** ON (matching TradingView setting). Polygon provides 16 hourly bars per trading day.
- **Dividend adjustment:** Separate toggle (ADJ button). Polygon's `adjusted=true` means split-adjusted only. Dividend adjustment uses Polygon's dividends API with backward proportional factor method.
- **Data source:** Polygon consolidated tape. TradingView uses BATS for some tickers, causing minor OHLC differences at inflection points (documented, accepted).

### Dividend Adjustment Architecture (v15)

Two-pass system: unadjusted + adjusted indicators stored separately.

**Fast adj pass (v15):** Instead of rebuilding all timeframes from scratch:
1. Adjust daily and hourly OHLC directly from unadjusted timeframe data
2. For 2D/3D/5D: adjust OHLC directly (skip expensive calendar lookups)
3. For 1W/2W/6W/1M+: rebuild from adjusted daily (preserves mid-week ex-date accuracy)
4. Non-dividend payers: skip adj pass entirely (empty dividend list)

Vectorised `apply_dividend_adjustment()` builds cumulative factor array, applies once.

### Performance

- **Current benchmark (v15, SP500):** 8.0s/ticker avg (load:0.7 build:1.4 calc:1.4 adj:4.6), 1.4s effective with 8 workers
- **Scan caching:** Fingerprint-based skip logic. If data hash + indicator version unchanged, ticker is skipped (instant).
- **Incremental collection:** After initial fetch, collector only downloads new bars since last timestamp.
- **MAX_LOOKBACK = 150:** Indicator calc trims to last 150 bars per timeframe.
- **Hourly trim: 1200 bars** (v15) — needed for 8H to have 150 bars after ÷8 aggregation.
- **SQLite WAL mode** for parallel worker writes.

### Known Issues / Active Work

1. **8H histograms missing** — fixed in v15 (hourly trim 200→1200), needs verification
2. **Minor value discrepancies** — reported post-v15 scan, needs investigation
3. **Performance target:** Need sub-1s/ticker for 3000+ ticker scaling. Current 8s is too slow.
4. **ACC indicator gaps:** 6W/1M/3M/6M/12M fail with 5-year history (need 92+ bars, insufficient data on long timeframes)

### Indicator Version History

| Version | Changes |
|---|---|
| v15 | Hourly trim 200→1200 for 8H indicators, fast adj pass, vectorised dividend adjustment |
| v14 | (rolled into v15) |
| v13 | Fix NaN propagation in vectorised linreg (cumsum was poisoning all subsequent values) |
| v12 | SQLite WAL mode, write retry for parallel workers, anchor calendar fix for delisted tickers |
| v11 | Fully vectorised linreg (100x faster), MAX_LOOKBACK=150 trim, momentum deduplication |
| v10 | Early raw data trimming, vectorised calendar lookup |
| v9 | Initial vectorisation pass |

## Data Sources

- **Massive.com/Polygon API** — Stocks Starter plan ($29/mo), 5-year history, 15-minute delayed, unlimited API calls
- **Financials add-on** ($29/mo extra) — planned for Monster scan (EPS/revenue/margins)
- **iShares ETF holdings** — Russell 1000/2000 constituent lists (gold standard for index composition)

## Frontend

- Single-page dashboard with sector groups, collapsible rows
- Color-coded indicator cells matching TradingView exactly
- Filter bar: Indicators toggle (sqz/mom/acc/band), Timeframes toggle
- Watchlist dropdown with hot-reload
- Ticker lookup modal
- Watchlist management UI (3-panel editor with paste-to-create)
- Help modal with indicator guide
- ADJ toggle for dividend-adjusted view
- Delta comparison showing changes since last scan

## Roadmap / Planned Features

1. **Performance:** Sub-1s/ticker scanning for 3000+ tickers (parallel/tiered/VPS)
2. **Backtesting engine:** Entry/exit strategies using cached OHLC + indicators
3. **Relative Strength page:** Rankings vs SPY
4. **Volume surge detection:** 300%+ above 20MA
5. **Institutional block trade scanning**
6. **Monster scan criteria:** ≥80% EPS growth, ≥80% sales/revenue growth, ≥10% after-tax margin (requires Financials add-on)
7. **Search/filter engine** across all cached tickers
8. **System info panel** showing data constraints
9. **VPS migration** for production scaling
10. **TradingView import/export** for watchlist compatibility

## Conventions

- TradingView is always the gold standard for validation
- Precision trumps completeness — accept data gaps rather than compromise accuracy
- Cache-first architecture — new indicators never require data refetch
- Indicator version bump forces full recompute of all tickers
- File-based watchlists in `watchlists/` folder for easy management
- Display tickers may differ from API tickers (e.g., "GOLD" displays as GLD API ticker, "BTC" → "X:BTCUSD")

## LSE Leverage ETF Mapping

The Leverage watchlist includes LSE leveraged ETF mappings (3x/5x long/short) for each underlying. Defined in `config.py` as `LSE_MAPPING` dict. Used for cross-referencing trading instruments.

## Session Context (Feb 22, 2026)

This project was built over an intensive 2-day session. The full development history is documented across 24 conversation transcripts covering:
- Initial architecture design and Pine Script analysis
- Windows deployment and API integration
- Visual refinement iterations (v1-v8)
- TTM Squeeze calculation debugging (AND vs OR logic, data cleaning)
- Dividend adjustment discovery and implementation
- Multi-day timeframe alignment (epoch-anchored grouping)
- NYSE trading calendar automation
- Performance optimisation (120s → 8s per ticker)
- Russell index watchlist creation from iShares data
- Vectorised linreg NaN fix
- Watchlist auto-discovery system

The user actively trades using these indicators and needs the dashboard to be production-ready for daily use with thousands of tickers.
