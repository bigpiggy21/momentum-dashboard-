# Current Status & Next Steps — Feb 22, 2026 (Night)

## Session Update (Claude Code CLI — Late Evening)

Picked up from chat Claude's handover. Server is running on localhost:8080.

### Completed This Session

1. **Fixed server crash** — `app.py:473` had a globe emoji in a print statement that Windows cp1252 console couldn't encode. Replaced with plain text.

2. **Investigated two indicator issues from SP500 review:**
   - **8H ACC missing on some tickers (TMUS, EA, ZBRA, TEL):** Root cause is sparse hourly data from Polygon. After 8H aggregation these tickers fall below the 90-bar minimum for acceleration calc. TMUS was just 1 bar short (89/90).
   - **All hourly indicators missing (JNPR, WBA, MRO + 8 others):** These are delisted/acquired tickers — Polygon returns 0 bars. Not a code bug.

3. **Applied fix for Issue 1:** Bumped hourly retention from 1200 → 1440 bars in `engine.py:89`. This gives borderline tickers more headroom for 8H ACC. Won't help severely sparse tickers (EA, ZBRA, TEL still ~65 bars).

4. **Removed 11 dead tickers from watchlists + SQLite:**
   - SP500: JNPR (acquired by HPE), PARA (Skydance merger), WBA (went private), DFS (acquired by Capital One), FI (renamed/delisted), RE (acquired by Berkshire), MRO (acquired by ConocoPhillips)
   - Overview: GXG (delisted ETF)
   - Leverage: JJN, NIB, JO (all delisted ETNs)
   - Cleaned all stale rows from momentum_dashboard.db

### Still Outstanding

## Background

Intensive 2-day build sprint. Dashboard is functional with ~472 SP500 tickers scanned and serving. Multiple optimisation rounds brought engine from 120s/ticker down to 8s/ticker.

## Remaining Actions

### 1. Restart Server & Verify Watchlists
The server needs restarting to pick up the new `app.py` with hot-reload watchlist support.
```bash
# Kill existing server, then:
python app.py --serve-only
```
After restart, the dropdown should show: Debug, Leverage, Overview, SP500, Russell1000, Russell2000, Russell3000.

Note: Only SP500, Debug, Leverage, and Overview tickers have cached data. Russell watchlists will show N/A until you run:
```bash
python collector.py --watchlist Russell3000 --workers 4   # ~53 min for 2474 new tickers
python engine.py --watchlist Russell3000 --once --workers 8
```

### 2. Verify v15 Fixes
The v15 scan just completed (480 tickers, 8.0s/ticker avg). Two things to verify:

**a) 8H histograms** — Previously missing across SP500. v15 increased hourly trim from 200→1200 bars so 8H gets 150 bars after aggregation. Check a few tickers in the dashboard — 8H momentum and acceleration should now populate. Also check 4H acceleration.

**b) Minor value discrepancies** — Alex reported seeing wrong values after v15 scan. Not yet investigated. Need to:
- Pick 2-3 tickers and compare dashboard vs TradingView side by side
- Check across timeframes (1D, 1W, 2D, 5D particularly)
- TradingView settings: Extended hours ON, ADJ toggle to match dashboard's ADJ state
- Focus on momentum colors and squeeze states

### 3. Performance — The Big Problem
Current: **8.0s/ticker** average. For Russell 3000 (2944 tickers) with 8 workers = ~49 minutes per full scan.

Target: **Sub-1s/ticker** for practical daily use.

Breakdown of 8.0s:
- load: 0.7s (reading CSV cache from disk)
- build: 1.4s (aggregating 14 timeframes from raw data)
- calc: 1.4s (computing indicators across all timeframes)
- adj: 4.6s (dividend-adjusted second pass)

**Optimisation ideas not yet implemented:**
- **Compute last N values only:** Instead of rolling 150-bar windows, compute only the last 2 values needed (current + previous for delta). Could reduce linreg from 150 calculations to ~2. Expected ~13x reduction in calc time.
- **Skip adj for non-dividend payers:** ~40% of SP500 don't pay dividends. If `fetch_dividends` returns empty, skip entire adj pass. Already partially implemented (check in engine.py) but fetch_dividends itself takes time.
- **Cache dividend lists:** Already cached for 24 hours. But the disk read + parse still costs time per ticker.
- **Parallel CSV loading:** Currently sequential per ticker. Could pre-load in background.
- **Incremental indicator updates:** Only recompute timeframes where new bars arrived since last scan.
- **Tiered scanning:** Scan 1D/1W first (most useful), defer long timeframes.

### 4. TradingView Comparison Audit
Before scaling to Russell 3000, worth doing a careful comparison audit:

Pick 5 diverse tickers (e.g., SPY, AAPL, TLT, MSTR, GLD) and for each:
1. Open TradingView with Extended Hours ON
2. Toggle ADJ to match dashboard
3. Compare across: 1D, 2D, 3D, 5D, 1W, 2W timeframes
4. Check: squeeze state, momentum color, acceleration state
5. Note any mismatches with exact values

This establishes confidence before scaling up.

## File Changes Not Yet Deployed

If you haven't replaced all the files from this session, here's the complete list:

**Project root (replace):**
- `app.py` — hot-reload watchlists, new import structure
- `config.py` — v15, no hardcoded watchlists, auto-discovery only
- `engine.py` — hourly trim 1200, fast adj pass using build_adjusted_timeframes
- `data_fetcher.py` — vectorised apply_dividend_adjustment, build_adjusted_timeframes function, numpy import
- `indicators.py` — v13 NaN-safe linreg with separate mask tracking

**watchlists/ folder (new files):**
- `debug_watchlist.py`
- `leverage_watchlist.py`
- `overview_watchlist.py`
- `sp500_watchlist.py` (already existed)
- `russell1000_watchlist.py`
- `russell2000_watchlist.py`
- `russell3000_watchlist.py`

**Delete from project root (old copies):**
- `sp500_watchlist.py`
- `russell_extra_watchlist.py`
- `russell1000_watchlist.py`
- `russell2000_watchlist.py`
- `russell3000_watchlist.py`

## Project Structure After Cleanup

```
momentum-dashboard/
├── app.py
├── engine.py
├── collector.py
├── data_fetcher.py
├── indicators.py
├── change_detector.py
├── config.py
├── CLAUDE.md
├── momentum_dashboard.db
├── cache/                    # CSV price data cache
├── div_cache/                # Dividend data cache
├── templates/
│   ├── dashboard.html
│   └── log.html
└── watchlists/
    ├── debug_watchlist.py
    ├── leverage_watchlist.py
    ├── overview_watchlist.py
    ├── sp500_watchlist.py
    ├── russell1000_watchlist.py
    ├── russell2000_watchlist.py
    └── russell3000_watchlist.py
```
