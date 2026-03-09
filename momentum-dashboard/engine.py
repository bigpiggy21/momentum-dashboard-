"""
Couloir Technologies — Indicator Engine (Process 2)

Reads OHLC data from CSV cache, computes indicators, detects changes,
and saves snapshots to SQLite. Runs independently of the data collector.

Usage:
  python engine.py                        # Process all watchlists
  python engine.py --watchlist Debug      # Process specific watchlist
  python engine.py --ticker NVDA          # Process single ticker
  python engine.py --once                 # Single pass, no scheduling
  python engine.py --workers 4            # Parallel computation

The engine reads from cache/ and writes to SQLite (momentum_dashboard.db).
It watches for cache changes and only recomputes when data has updated.
"""

import argparse
import os
import sys
import time
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

# Fix Unicode output on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from config import (
    WATCHLISTS, INDICATOR_VERSION, REFRESH_INTERVAL_SECONDS,
)
from data_fetcher import (
    fetch_ticker_data, build_timeframe_data, build_adjusted_timeframes, fetch_dividends,
    get_data_fingerprint,
)
from indicators import calculate_all_indicators, calc_hvc
from change_detector import (
    init_db, detect_changes, save_snapshot,
    check_scan_cache, update_scan_cache,
    save_hvc_event,
)


def get_ticker_info(display_ticker):
    """Look up API ticker and asset type from display name."""
    for wl_name, wl_groups in WATCHLISTS.items():
        for group_name, tickers in wl_groups:
            for display, api, atype in tickers:
                if display == display_ticker:
                    return api, atype
    return display_ticker, "stock"


def compute_ticker(display_ticker):
    """Compute indicators for a single ticker from cached data.
    
    Returns dict with timing info, or skips if data unchanged.
    Does NOT fetch from API — reads only from cache.
    """
    api_ticker, asset_type = get_ticker_info(display_ticker)
    
    # Check if data has changed since last computation
    data_hash = get_data_fingerprint(api_ticker)
    if data_hash:
        unadj_cached = check_scan_cache(display_ticker, 0, data_hash, INDICATOR_VERSION)
        adj_cached = check_scan_cache(display_ticker, 1, data_hash, INDICATOR_VERSION)
        if unadj_cached and adj_cached:
            return {"ticker": display_ticker, "status": "skipped"}
    
    # Load data from cache (fetch_ticker_data will use cache, no API calls
    # since the collector already populated it)
    t0 = time.time()
    raw_data = fetch_ticker_data(display_ticker, api_ticker, asset_type)
    load_time = time.time() - t0
    
    if raw_data["daily"].empty:
        return {"ticker": display_ticker, "status": "no_data"}
    
    # Re-check fingerprint
    data_hash = get_data_fingerprint(api_ticker)
    if data_hash:
        unadj_cached = check_scan_cache(display_ticker, 0, data_hash, INDICATOR_VERSION)
        adj_cached = check_scan_cache(display_ticker, 1, data_hash, INDICATOR_VERSION)
        if unadj_cached and adj_cached:
            return {"ticker": display_ticker, "status": "skipped"}
    
    # --- EARLY TRIM: we only need recent data for indicators ---
    # Hourly: keep last 1440 bars (enough for 8H aggregation + 150 bar indicator lookback)
    # Daily: keep last 5500 bars (~22 years). 12M bollinger needs 20×252=5040 daily bars.
    #        10 years of data = ~2520 bars for full 3M/6M/12M coverage.
    # Weekly: keep last 600 bars (~11.5 years, for 6W aggregation + lookback)
    trimmed_data = {}
    if not raw_data["hourly"].empty:
        # Need enough bars for 8H aggregation (÷8) then 150 bar indicator lookback
        # 180 × 8 = 1440 hourly bars needed for full 8H indicators (inc. acceleration)
        trimmed_data["hourly"] = raw_data["hourly"].iloc[-1440:].reset_index(drop=True) if len(raw_data["hourly"]) > 1440 else raw_data["hourly"]
    else:
        trimmed_data["hourly"] = raw_data["hourly"]

    # Daily: 10+ years needed for 12M bollinger (20 bars × 252 days = 5040) + lookback
    if not raw_data["daily"].empty:
        trimmed_data["daily"] = raw_data["daily"].iloc[-5500:].reset_index(drop=True) if len(raw_data["daily"]) > 5500 else raw_data["daily"]
    else:
        trimmed_data["daily"] = raw_data["daily"]

    if not raw_data["weekly"].empty:
        trimmed_data["weekly"] = raw_data["weekly"].iloc[-600:].reset_index(drop=True) if len(raw_data["weekly"]) > 600 else raw_data["weekly"]
    else:
        trimmed_data["weekly"] = raw_data["weekly"]
    
    # Build timeframes and compute indicators (unadjusted)
    t1 = time.time()
    tf_data = build_timeframe_data(trimmed_data)
    build_time = time.time() - t1
    
    t2 = time.time()
    weekly_for_wma = trimmed_data.get("weekly", None)
    results = calculate_all_indicators(tf_data, weekly_for_wma)
    calc_time = time.time() - t2
    
    price = results.get('_meta', {}).get('price', 'N/A')
    
    # Detect changes and save (unadjusted)
    events = detect_changes(display_ticker, results, div_adj=0)
    save_snapshot(display_ticker, results, div_adj=0)
    if data_hash:
        update_scan_cache(display_ticker, 0, data_hash, INDICATOR_VERSION)

    # Save HVC event if triggered (unadjusted pass only)
    meta = results.get("_meta", {})
    if meta.get("hvc_triggered"):
        try:
            # Use the last daily bar's date as the event date
            daily_df = tf_data.get("1D") if tf_data else None
            event_date = None
            if daily_df is not None and not daily_df.empty:
                last_ts = daily_df["timestamp"].iloc[-1]
                event_date = last_ts.strftime("%Y-%m-%d") if hasattr(last_ts, 'strftime') else str(last_ts)[:10]
            if event_date:
                save_hvc_event(
                    ticker=display_ticker,
                    event_date=event_date,
                    detected_at=datetime.now().isoformat(),
                    close_price=meta.get("price"),
                    open_price=float(daily_df["open"].iloc[-1]) if daily_df is not None else None,
                    volume=meta.get("hvc_volume"),
                    avg_volume=meta.get("hvc_avg_volume"),
                    volume_ratio=meta.get("hvc_volume_ratio"),
                    candle_dir=meta.get("hvc_candle_dir", ""),
                    gap_type=meta.get("hvc_gap_type", ""),
                    gap_level=meta.get("hvc_gap_level"),
                    gap_closed=meta.get("hvc_gap_closed", False),
                )
        except Exception:
            pass  # Don't let HVC event saving break the main flow

    # Dividend-adjusted pass — with smart skip
    t3 = time.time()
    adj_skipped = False
    if asset_type in ("stock", "etf"):
        try:
            dividends = fetch_dividends(api_ticker)
            if not dividends.empty:
                # Smart skip: check if any dividends fall within our data range.
                # If all ex-dates are before our earliest daily bar, the adjustment
                # factors are all 1.0 and adj results are identical to unadjusted.
                earliest_bar = trimmed_data["daily"]["timestamp"].iloc[0]
                # Normalise timezone for comparison
                div_dates = dividends["ex_date"]
                if hasattr(earliest_bar, 'tz') and earliest_bar.tz is not None:
                    earliest_bar = earliest_bar.tz_localize(None)
                div_dates_clean = div_dates.dt.tz_localize(None) if div_dates.dt.tz is not None else div_dates
                relevant_divs = div_dates_clean >= earliest_bar

                if relevant_divs.any():
                    # Has relevant dividends — must compute adjusted pass
                    tf_data_adj = build_adjusted_timeframes(tf_data, dividends)
                    results_adj = calculate_all_indicators(tf_data_adj, weekly_for_wma)
                    detect_changes(display_ticker, results_adj, div_adj=1)
                    save_snapshot(display_ticker, results_adj, div_adj=1)
                else:
                    # All dividends are before our data range — adj == unadj
                    adj_skipped = True
                    save_snapshot(display_ticker, results, div_adj=1)
            else:
                adj_skipped = True
                save_snapshot(display_ticker, results, div_adj=1)
        except Exception:
            save_snapshot(display_ticker, results, div_adj=1)
    else:
        adj_skipped = True
        save_snapshot(display_ticker, results, div_adj=1)
    adj_time = time.time() - t3
    
    if data_hash:
        update_scan_cache(display_ticker, 1, data_hash, INDICATOR_VERSION)
    
    n_events = len(events) if events else 0
    
    return {
        "ticker": display_ticker,
        "status": "ok",
        "price": price,
        "events": n_events,
        "adj_skipped": adj_skipped,
        "load_s": round(load_time, 2),
        "build_s": round(build_time, 2),
        "calc_s": round(calc_time, 2),
        "adj_s": round(adj_time, 2),
        "total_s": round(load_time + build_time + calc_time + adj_time, 2),
    }


def compute_all(tickers, workers=1):
    """Compute indicators for all tickers, optionally in parallel."""
    print(f"\n{'='*60}")
    print(f"INDICATOR ENGINE — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Computing {len(tickers)} tickers with {workers} worker(s)")
    print(f"{'='*60}")
    
    t0 = time.time()
    results = []
    
    if workers <= 1:
        for i, ticker in enumerate(tickers):
            r = compute_ticker(ticker)
            results.append(r)
            if r["status"] == "ok":
                ev = f" ⚡{r['events']}" if r["events"] else ""
                print(f"  [{i+1}/{len(tickers)}] ✓ {ticker}: ${r['price']} "
                      f"({r['load_s']}+{r['build_s']}+{r['calc_s']}+{r['adj_s']}s){ev}")
            elif r["status"] == "skipped":
                print(f"  [{i+1}/{len(tickers)}] ⏭ {ticker}: unchanged")
            else:
                print(f"  [{i+1}/{len(tickers)}] ⏭ {ticker}: {r['status']}")
    else:
        # ProcessPoolExecutor bypasses Python's GIL — each worker gets its
        # own interpreter so pandas/numpy work truly runs in parallel.
        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(compute_ticker, tk): tk for tk in tickers}
            done = 0
            for future in as_completed(futures):
                done += 1
                r = future.result()
                results.append(r)
                if r["status"] == "ok":
                    ev = f" ⚡{r['events']}" if r["events"] else ""
                    print(f"  [{done}/{len(tickers)}] ✓ {r['ticker']}: ${r['price']} "
                          f"({r['total_s']}s){ev}")
                elif r["status"] == "skipped":
                    print(f"  [{done}/{len(tickers)}] ⏭ {r['ticker']}: unchanged")
    
    elapsed = time.time() - t0
    ok = [r for r in results if r["status"] == "ok"]
    skipped = sum(1 for r in results if r["status"] == "skipped")
    
    if ok:
        avg_total = sum(r["total_s"] for r in ok) / len(ok)
        avg_load = sum(r["load_s"] for r in ok) / len(ok)
        avg_build = sum(r["build_s"] for r in ok) / len(ok)
        avg_calc = sum(r["calc_s"] for r in ok) / len(ok)
        avg_adj = sum(r["adj_s"] for r in ok) / len(ok)
        total_events = sum(r["events"] for r in ok)
        
        print(f"\n{'─'*60}")
        print(f"Engine complete: {len(ok)} computed, {skipped} skipped in {elapsed:.1f}s")
        print(f"Avg per ticker: {avg_total:.1f}s (load:{avg_load:.1f} build:{avg_build:.1f} "
              f"calc:{avg_calc:.1f} adj:{avg_adj:.1f})")
        if workers > 1:
            print(f"Effective: {elapsed/max(len(tickers),1):.1f}s/ticker ({workers} workers)")
        print(f"Total events: {total_events}")
        print(f"{'─'*60}")
    else:
        print(f"\n{'─'*60}")
        print(f"Engine complete: {skipped} skipped (all unchanged)")
        print(f"{'─'*60}")
    
    return results


def next_clock_hour():
    """Calculate seconds until the next clock hour (xx:00:00)."""
    now = datetime.now()
    next_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    return (next_hour - now).total_seconds()


def next_quarter_past():
    """Calculate seconds until the next xx:15:00.

    Hourly candles confirm at the top of the hour; scanning at :15
    gives the data provider time to finalise bars.
    """
    now = datetime.now()
    target = now.replace(minute=15, second=0, microsecond=0)
    if now >= target:
        target += timedelta(hours=1)
    return (target - now).total_seconds()


def build_ticker_list(args):
    """Build ticker list from command line args."""
    if args.ticker:
        return [args.ticker]

    if args.tickers_file:
        with open(args.tickers_file, encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]

    seen = set()
    tickers = []
    wl_filter = set(args.watchlist) if args.watchlist else None
    for wl_name, wl_groups in WATCHLISTS.items():
        if wl_filter and wl_name not in wl_filter:
            continue
        for group_name, group_tickers in wl_groups:
            for display, api, atype in group_tickers:
                if display not in seen:
                    seen.add(display)
                    tickers.append(display)
    return tickers


def backfill_hvc(tickers, lookback_days=90):
    """Backfill HVC events from cached daily data.

    Iterates recent daily bars per ticker, runs calc_hvc(), and populates
    hvc_events retroactively. Run once after deploying HVC feature.
    """
    import pandas as pd
    print(f"\n{'='*60}")
    print(f"HVC BACKFILL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Scanning {len(tickers)} tickers, last {lookback_days} days")
    print(f"{'='*60}")

    total_events = 0
    for i, display_ticker in enumerate(tickers):
        api_ticker, asset_type = get_ticker_info(display_ticker)
        try:
            raw_data = fetch_ticker_data(display_ticker, api_ticker, asset_type)
        except Exception:
            continue
        daily = raw_data.get("daily")
        if daily is None or daily.empty or len(daily) < 21:
            continue

        hvc = calc_hvc(daily)
        if hvc.empty:
            continue

        ticker_events = 0
        for idx in range(len(daily)):
            if not hvc["hvc_triggered"].iloc[idx]:
                continue
            bar_ts = daily["timestamp"].iloc[idx]
            event_date = bar_ts.strftime("%Y-%m-%d") if hasattr(bar_ts, 'strftime') else str(bar_ts)[:10]

            # Only backfill within lookback window
            try:
                from datetime import date as date_type
                bar_date = date_type.fromisoformat(event_date)
                if (datetime.now().date() - bar_date).days > lookback_days:
                    continue
            except Exception:
                continue

            try:
                save_hvc_event(
                    ticker=display_ticker,
                    event_date=event_date,
                    detected_at=datetime.now().isoformat(),
                    close_price=float(daily["close"].iloc[idx]),
                    open_price=float(daily["open"].iloc[idx]),
                    volume=int(daily["volume"].iloc[idx]),
                    avg_volume=None,  # Not critical for backfill
                    volume_ratio=round(float(hvc["hvc_volume_ratio"].iloc[idx]), 2),
                    candle_dir=hvc["hvc_candle_dir"].iloc[idx],
                    gap_type=hvc["hvc_gap_type"].iloc[idx],
                    gap_level=round(float(hvc["hvc_gap_level"].iloc[idx]), 2) if not pd.isna(hvc["hvc_gap_level"].iloc[idx]) else None,
                    gap_closed=bool(hvc["hvc_gap_closed"].iloc[idx]),
                )
                ticker_events += 1
            except Exception:
                pass

        if ticker_events:
            total_events += ticker_events
            print(f"  [{i+1}/{len(tickers)}] {display_ticker}: {ticker_events} HVC events")

    print(f"\n{'─'*60}")
    print(f"Backfill complete: {total_events} HVC events inserted")
    print(f"{'─'*60}")


def main():
    parser = argparse.ArgumentParser(description="Couloir Indicator Engine")
    parser.add_argument("--ticker", type=str, help="Compute single ticker")
    parser.add_argument("--watchlist", type=str, action="append", help="Compute specific watchlist(s)")
    parser.add_argument("--tickers-file", type=str, help="File with one ticker per line (used by scheduler for AllTickers)")
    parser.add_argument("--workers", type=int, default=4, help="Parallel workers (default: 4)")
    parser.add_argument("--once", action="store_true", help="Single pass, no scheduling")
    parser.add_argument("--interval", type=int, default=REFRESH_INTERVAL_SECONDS,
                        help=f"Recompute interval in seconds (default: {REFRESH_INTERVAL_SECONDS})")
    parser.add_argument("--align-clock", action="store_true",
                        help="Align computation to clock hours (xx:00:00)")
    parser.add_argument("--align-quarter", action="store_true",
                        help="Align computation to :15 past each hour (xx:15:00)")
    parser.add_argument("--backfill-hvc", action="store_true",
                        help="Backfill HVC events from cached daily data (run once)")
    parser.add_argument("--backfill-days", type=int, default=90,
                        help="Number of days to backfill HVC events (default: 90)")

    args = parser.parse_args()

    init_db()

    tickers = build_ticker_list(args)
    wl_label = args.ticker or (", ".join(args.watchlist) if args.watchlist else "all")
    print(f"⚙️  Couloir Indicator Engine")
    print(f"   Tickers: {len(tickers)} ({wl_label})")
    print(f"   Workers: {args.workers}")
    print(f"   Version: {INDICATOR_VERSION}")

    # HVC backfill mode
    if args.backfill_hvc:
        backfill_hvc(tickers, lookback_days=args.backfill_days)
        return

    # Initial computation
    compute_all(tickers, workers=args.workers)
    
    if args.once:
        return
    
    # Scheduled computation
    while True:
        if args.align_quarter:
            wait = next_quarter_past()
            next_time = (datetime.now() + timedelta(seconds=wait)).strftime('%H:%M:%S')
            print(f"\n⏰ Next computation at {next_time} ({wait:.0f}s)")
            time.sleep(wait)
        elif args.align_clock:
            wait = next_clock_hour()
            next_time = (datetime.now() + timedelta(seconds=wait)).strftime('%H:%M:%S')
            print(f"\n⏰ Next computation at {next_time} ({wait:.0f}s)")
            time.sleep(wait)
        else:
            print(f"\n💤 Sleeping {args.interval}s...")
            time.sleep(args.interval)
        
        try:
            compute_all(tickers, workers=args.workers)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(f"❌ Engine error: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n👋 Engine stopped.")
