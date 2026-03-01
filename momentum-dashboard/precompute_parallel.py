"""Parallel pre-computation of backtest indicators.

Usage:
    python precompute_parallel.py --watchlist SP500 --workers 4 --force
    python precompute_parallel.py --watchlist Russell3000 --workers 4 --force
"""
import argparse
import os
import sys
import time
import sqlite3


# Worker function must be importable by spawned processes
def _compute_one(args_tuple):
    """Worker function — each process gets its own DB connection."""
    ticker, div_adj, force = args_tuple
    try:
        # Workers need to set cwd too for the imports to work
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        import backtest_engine as be
        conn = sqlite3.connect(be.DB_PATH, timeout=60)
        be.init_db(conn)
        n = be.precompute_ticker(conn, ticker, div_adj=div_adj, force=force)
        conn.close()
        return ticker, n
    except Exception as e:
        return ticker, -1


def main():
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    import backtest_engine as be

    parser = argparse.ArgumentParser()
    parser.add_argument("--watchlist", required=True)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--div-adj", type=int, default=0)
    args = parser.parse_args()

    if args.watchlist not in be.WATCHLISTS:
        print(f"Unknown watchlist: {args.watchlist}")
        print(f"Available: {', '.join(be.WATCHLISTS.keys())}")
        sys.exit(1)

    # Build ticker list
    tickers = []
    for group_name, group_tickers in be.WATCHLISTS[args.watchlist]:
        for display, api_ticker, *_ in group_tickers:
            tickers.append(api_ticker)

    total = len(tickers)
    print(f"Pre-computing {total} tickers from '{args.watchlist}' with {args.workers} workers (force={args.force})", flush=True)

    t0 = time.time()
    done = 0
    errors = 0
    skipped = 0

    from concurrent.futures import ProcessPoolExecutor, as_completed

    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        work_items = [(t, args.div_adj, args.force) for t in tickers]
        futures = {pool.submit(_compute_one, item): item[0] for item in work_items}

        for future in as_completed(futures):
            ticker, n = future.result()
            done += 1
            if n < 0:
                errors += 1
            elif n == 0:
                skipped += 1

            if done % 25 == 0 or done == total:
                elapsed = time.time() - t0
                rate = done / elapsed if elapsed > 0 else 0
                eta = (total - done) / rate if rate > 0 else 0
                print(f"  [{done}/{total}] {elapsed:.0f}s elapsed, "
                      f"{rate:.1f} tickers/s, ETA {eta:.0f}s ({eta/60:.1f}min) | "
                      f"{errors} errors, {skipped} skipped", flush=True)

    # Update precompute timestamp
    conn = sqlite3.connect(be.DB_PATH, timeout=30)
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO backtest_meta (key, value) VALUES ('last_precompute', datetime('now'))")
    conn.commit()
    conn.close()

    elapsed = time.time() - t0
    print(f"\nDone: {done} tickers in {elapsed:.0f}s ({elapsed/60:.1f}min)")
    print(f"  Computed: {done - errors - skipped}, Skipped: {skipped}, Errors: {errors}")


if __name__ == "__main__":
    main()
