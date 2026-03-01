"""
Couloir Technologies — Data Collector (Process 1)

Fetches OHLC data from Polygon API and writes to local CSV cache.
Runs independently of the indicator engine and dashboard server.

Usage:
  python collector.py                     # Collect all watchlists
  python collector.py --watchlist Debug    # Collect specific watchlist
  python collector.py --ticker NVDA       # Collect single ticker
  python collector.py --once              # Single pass, no scheduling
  python collector.py --workers 4         # Parallel collection

The collector writes to cache/ directory. The engine reads from there.
"""

import argparse
import os
import sys
import time
import math
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import (
    WATCHLISTS, MASSIVE_API_KEY,
    REFRESH_INTERVAL_SECONDS,
)
from data_fetcher import fetch_ticker_data, fetch_dividends


def get_ticker_info(display_ticker):
    """Look up API ticker and asset type from display name."""
    for wl_name, wl_groups in WATCHLISTS.items():
        for group_name, tickers in wl_groups:
            for display, api, atype in tickers:
                if display == display_ticker:
                    return api, atype
    return display_ticker, "stock"


def collect_ticker(display_ticker):
    """Fetch fresh data for a single ticker and update cache."""
    api_ticker, asset_type = get_ticker_info(display_ticker)
    
    try:
        t0 = time.time()
        raw = fetch_ticker_data(display_ticker, api_ticker, asset_type)
        fetch_time = time.time() - t0
        
        if raw["daily"].empty:
            return {"ticker": display_ticker, "status": "no_data", "fetch_s": 0}
        
        # Also fetch dividends for stocks/ETFs
        div_time = 0
        if asset_type in ("stock", "etf"):
            try:
                t1 = time.time()
                fetch_dividends(api_ticker)
                div_time = time.time() - t1
            except Exception:
                pass
        
        bars = {
            "hourly": len(raw.get("hourly", [])),
            "daily": len(raw.get("daily", [])),
            "weekly": len(raw.get("weekly", [])),
        }
        
        return {
            "ticker": display_ticker,
            "status": "ok",
            "fetch_s": round(fetch_time, 2),
            "div_s": round(div_time, 2),
            "bars": bars,
        }
    except Exception as e:
        return {"ticker": display_ticker, "status": "error", "error": str(e)}


def collect_all(tickers, workers=1):
    """Collect data for all tickers, optionally in parallel."""
    print(f"\n{'='*60}")
    print(f"DATA COLLECTOR — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Collecting {len(tickers)} tickers with {workers} worker(s)")
    print(f"{'='*60}")
    
    t0 = time.time()
    results = []
    
    if workers <= 1:
        # Serial — simpler, good for debugging
        for i, ticker in enumerate(tickers):
            r = collect_ticker(ticker)
            results.append(r)
            status = "OK" if r["status"] == "ok" else "SKIP" if r["status"] == "no_data" else "ERR"
            timing = f'{r.get("fetch_s", 0):.1f}s' if r["status"] == "ok" else ""
            print(f"  [{i+1}/{len(tickers)}] {status} {ticker} {timing}")
    else:
        # Parallel collection
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(collect_ticker, tk): tk for tk in tickers}
            done = 0
            for future in as_completed(futures):
                done += 1
                r = future.result()
                results.append(r)
                status = "OK" if r["status"] == "ok" else "SKIP" if r["status"] == "no_data" else "ERR"
                timing = f'{r.get("fetch_s", 0):.1f}s' if r["status"] == "ok" else ""
                print(f"  [{done}/{len(tickers)}] {status} {r['ticker']} {timing}")
    
    elapsed = time.time() - t0
    ok = sum(1 for r in results if r["status"] == "ok")
    avg_fetch = sum(r.get("fetch_s", 0) for r in results if r["status"] == "ok") / max(ok, 1)
    
    print(f"\n{'-'*60}")
    print(f"Collection complete: {ok}/{len(tickers)} tickers in {elapsed:.1f}s")
    print(f"Average fetch: {avg_fetch:.1f}s/ticker")
    if workers > 1:
        print(f"Effective throughput: {elapsed/max(len(tickers),1):.1f}s/ticker ({workers} workers)")
    print(f"{'-'*60}")
    
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
    
    seen = set()
    tickers = []
    wl_filter = set(args.watchlist) if args.watchlist else None
    print(f"   SP500 group count: {len(WATCHLISTS.get('SP500', []))}")
    print(f"   Available watchlists: {list(WATCHLISTS.keys())}")
    print(f"   Filter: {wl_filter}")
    for wl_name, wl_groups in WATCHLISTS.items():
        if wl_filter and wl_name not in wl_filter:
            continue
        for group_name, group_tickers in wl_groups:
            for display, api, atype in group_tickers:
                if display not in seen:
                    seen.add(display)
                    tickers.append(display)
    return tickers


def main():
    parser = argparse.ArgumentParser(description="Couloir Data Collector")
    parser.add_argument("--ticker", type=str, help="Collect single ticker")
    parser.add_argument("--watchlist", type=str, action="append", help="Collect specific watchlist(s)")
    parser.add_argument("--workers", type=int, default=1, help="Parallel workers (default: 1)")
    parser.add_argument("--once", action="store_true", help="Single pass, no scheduling")
    parser.add_argument("--interval", type=int, default=REFRESH_INTERVAL_SECONDS,
                        help=f"Refresh interval in seconds (default: {REFRESH_INTERVAL_SECONDS})")
    parser.add_argument("--align-clock", action="store_true",
                        help="Align collection to clock hours (xx:00:00)")
    parser.add_argument("--align-quarter", action="store_true",
                        help="Align collection to :15 past each hour (xx:15:00)")
    
    args = parser.parse_args()
    
    if MASSIVE_API_KEY == "YOUR_API_KEY_HERE":
        print("ERROR: Please set your Massive.com API key in config.py")
        sys.exit(1)
    
    tickers = build_ticker_list(args)
    wl_label = args.ticker or (", ".join(args.watchlist) if args.watchlist else "all")
    print(f"[COLLECTOR] TBD Data Collector")
    print(f"   Tickers: {len(tickers)} ({wl_label})")
    print(f"   Workers: {args.workers}")
    print(f"   Interval: {args.interval}s")
    print(f"   Clock-aligned: {args.align_clock}")
    
    # Initial collection
    collect_all(tickers, workers=args.workers)
    
    if args.once:
        return
    
    # Scheduled collection
    while True:
        if args.align_quarter:
            wait = next_quarter_past()
            next_time = (datetime.now() + timedelta(seconds=wait)).strftime('%H:%M:%S')
            print(f"\nNext collection at {next_time} ({wait:.0f}s)")
            time.sleep(wait)
        elif args.align_clock:
            wait = next_clock_hour()
            next_time = (datetime.now() + timedelta(seconds=wait)).strftime('%H:%M:%S')
            print(f"\nNext collection at {next_time} ({wait:.0f}s)")
            time.sleep(wait)
        else:
            print(f"\nSleeping {args.interval}s...")
            time.sleep(args.interval)
        
        try:
            collect_all(tickers, workers=args.workers)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(f"ERROR: Collection error: {e}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nCollector stopped.")
