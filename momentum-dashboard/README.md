# Momentum Dashboard

A live technical analysis dashboard that scans your ticker universe across 14 timeframes, calculating TTM squeeze momentum, squeeze state, Bollinger band position, acceleration, deviation signals, and 30WMA stage analysis. Surfaces meaningful changes to reduce noise and highlight opportunities.

## Quick Start

### 1. Install dependencies

```bash
pip install pandas numpy scipy requests
```

### 2. Set your API key

Edit `config.py` and replace `YOUR_API_KEY_HERE` with your Massive.com API key:

```python
MASSIVE_API_KEY = "your_key_here"
```

### 3. Run prototype scan (Semis group only)

```bash
python app.py --prototype
```

This will:
- Fetch hourly, daily, and weekly data for NVDA, AVGO, TSM, AMD, MU, SMCI, ASML, INTC
- Calculate all indicators across 14 timeframes
- Save snapshots and detect changes
- Start the dashboard at http://localhost:8080

### 4. Open the dashboard

Navigate to `http://localhost:8080` in your browser.

## Usage

```bash
# Full scan of all tickers + serve dashboard
python app.py

# Prototype mode (Semis only)
python app.py --prototype

# Scan single ticker
python app.py --ticker NVDA

# Scan only (no web server)
python app.py --scan-only --prototype

# Serve existing data only (no new scan)
python app.py --serve-only

# Custom port
python app.py --port 3000

# No scheduled rescans
python app.py --no-schedule
```

## Dashboard Features

- **Changes Panel**: Chronological feed of all state changes, filterable by timeframe significance, indicator type, and ticker
- **Thematic Groups**: Collapsible sections matching your TradingView watchlist structure
- **Per-Ticker Grid**: Full multi-timeframe state for each instrument showing MOM, SQZ, BAND, ACC, DEV signals
- **30WMA Status**: Stage analysis indicator per ticker
- **Auto-refresh**: Dashboard polls for updates every 60 seconds
- **Scheduled scans**: Background rescans every 15 minutes (configurable)

## Indicator Calculations

All indicators match TradingView Pine Script logic:

| Indicator | What it shows | Colors |
|-----------|--------------|--------|
| **MOM** | TTM Squeeze momentum direction | Aqua (+rising), Blue (+falling), Yellow (-rising), Red (-falling) |
| **SQZ** | Bollinger/Keltner compression | Green (no sqz), Black (low), Red (mid), Orange (high) |
| **BAND** | Price vs 20 SMA | ↑ above, ↓ below, Blue highlight on flip |
| **ACC** | Momentum acceleration (2nd derivative) | Green (accelerating), Red (decelerating), ▲▼ impulses |
| **DEV** | Bollinger Band deviation signal | B (buy), S (sell) |
| **30WMA** | 30-week SMA stage | ↑ above (bullish), ↓ below (bearish) |

## Files

```
momentum-dashboard/
├── app.py              # Main application (orchestrator + web server)
├── config.py           # Configuration (tickers, API key, parameters)
├── data_fetcher.py     # Massive.com API data fetching + timeframe aggregation
├── indicators.py       # Technical indicator calculations
├── change_detector.py  # Change detection + SQLite event logging
├── templates/
│   └── dashboard.html  # Dashboard UI
└── momentum_dashboard.db  # SQLite database (created on first run)
```

## Validating Against TradingView

After running a scan, compare the dashboard values for a ticker against your TradingView indicators on the same timeframe. If values don't match, the most likely causes are:
1. **Timeframe alignment** — TradingView may define bar boundaries differently for custom timeframes
2. **Data differences** — Massive.com vs TradingView data source may have slight OHLC differences
3. **Lookback depth** — Some calculations need sufficient historical bars to warm up

## Next Steps

- [ ] Validate prototype calculations against TradingView
- [ ] Scale to full ticker universe
- [ ] Assess commodity/forex/EU index coverage on Massive.com
- [ ] Add trade/flow data integration (Phase 3)
