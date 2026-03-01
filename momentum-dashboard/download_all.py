"""
Overnight bulk download script.
Fetches ALL active US stocks and ETFs from Massive.com API.
Uses same history depth as normal scanner (full plan allowance).

Usage:
    python download_all.py

Resume-friendly: prioritises tickers without cached data.
Progress logged to download_log.txt.
"""

import os
import sys
import time
import json
import requests
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(__file__))

from config import MASSIVE_API_KEY
from data_fetcher import fetch_ticker_data, build_timeframe_data
from indicators import calculate_all_indicators
from change_detector import init_db, detect_changes, save_snapshot

LOG_FILE = os.path.join(os.path.dirname(__file__), "download_log.txt")
TICKER_CACHE = os.path.join(os.path.dirname(__file__), "cache", "all_tickers.json")


def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def fetch_all_tickers():
    """Fetch complete list of active US stock tickers from Massive API."""
    if os.path.exists(TICKER_CACHE):
        mtime = os.path.getmtime(TICKER_CACHE)
        age_hours = (time.time() - mtime) / 3600
        if age_hours < 24:
            with open(TICKER_CACHE, "r") as f:
                data = json.load(f)
            if len(data) > 0:
                log(f"Loading cached ticker list ({len(data)} tickers, {age_hours:.1f}h old)")
                return data

    log("Fetching all active tickers from Massive API...")
    headers = {"Authorization": f"Bearer {MASSIVE_API_KEY}"}
    all_tickers = []
    url = "https://api.massive.com/v3/reference/tickers"
    params = {"market": "stocks", "active": "true", "limit": 1000, "order": "asc", "sort": "ticker"}

    page = 0
    while True:
        page += 1
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            log(f"  Error fetching ticker list page {page}: {e}")
            break

        results = data.get("results", [])
        if not results:
            break

        for t in results:
            all_tickers.append({
                "ticker": t.get("ticker", ""),
                "name": t.get("name", ""),
                "type": t.get("type", ""),
                "primary_exchange": t.get("primary_exchange", ""),
            })

        log(f"  Page {page}: {len(results)} tickers (total: {len(all_tickers)})")
        next_url = data.get("next_url")
        if next_url:
            url = next_url
            params = {}
            time.sleep(0.15)
        else:
            break

    os.makedirs(os.path.dirname(TICKER_CACHE), exist_ok=True)
    with open(TICKER_CACHE, "w") as f:
        json.dump(all_tickers, f)
    log(f"Total tickers fetched: {len(all_tickers)}")
    return all_tickers


def filter_tickers(all_tickers):
    wanted_types = {"CS", "ETF", "ADRC"}
    wanted_exchanges = {"XNYS", "XNAS", "XASE", "ARCX", "BATS"}
    filtered = []
    for t in all_tickers:
        ticker = t["ticker"]
        if t.get("type", "") not in wanted_types:
            continue
        if wanted_exchanges and t.get("primary_exchange", "") not in wanted_exchanges:
            continue
        if any(c in ticker for c in ["/", "^", "#", "+"]):
            continue
        filtered.append(ticker)
    return sorted(set(filtered))


def has_cached_data(ticker):
    safe = ticker.replace(":", "_").replace("/", "_")
    return os.path.exists(os.path.join(os.path.dirname(__file__), "cache", f"{safe}_day.csv"))


def scan_ticker_safe(ticker):
    """Scan a single ticker with error handling. Returns price on success."""
    try:
        raw_data = fetch_ticker_data(ticker, ticker, "stock")

        if raw_data["daily"].empty:
            return None

        tf_data = build_timeframe_data(raw_data)
        results = calculate_all_indicators(tf_data, raw_data.get("weekly", None))
        detect_changes(ticker, results)
        save_snapshot(ticker, results)

        price = results.get("_meta", {}).get("price", "?")
        return price
    except Exception as e:
        return None


def main():
    log("=" * 60)
    log("BULK DOWNLOAD — STARTING")
    log("=" * 60)

    init_db()
    all_tickers = fetch_all_tickers()
    tickers = filter_tickers(all_tickers)
    log(f"Filtered to {len(tickers)} stocks and ETFs")

    already_cached = [t for t in tickers if has_cached_data(t)]
    need_download = [t for t in tickers if not has_cached_data(t)]
    log(f"Already cached: {len(already_cached)}")
    log(f"Need download: {len(need_download)}")

    # New tickers first, then refresh cached
    ordered = need_download + already_cached
    log(f"\nProcessing {len(ordered)} tickers...")
    log("")

    success = 0
    failed = 0
    start_time = time.time()

    for i, ticker in enumerate(ordered):
        elapsed = time.time() - start_time
        rate = (i + 1) / (elapsed / 60) if elapsed > 0 else 0
        remaining = (len(ordered) - i - 1) / rate if rate > 0 else 0

        price = scan_ticker_safe(ticker)

        if price is not None:
            success += 1
            if i % 10 == 0:
                log(f"[{i+1}/{len(ordered)}] {ticker} ${price} ({rate:.1f}/min, ETA: {remaining:.0f}min)")
        else:
            failed += 1
            if i % 50 == 0:
                log(f"[{i+1}/{len(ordered)}] {ticker} — no data ({rate:.1f}/min)")

        time.sleep(0.15)

    total_time = (time.time() - start_time) / 60
    log("")
    log("=" * 60)
    log(f"BULK DOWNLOAD COMPLETE")
    log(f"  Total time: {total_time:.1f} minutes")
    log(f"  Success: {success}")
    log(f"  Failed: {failed}")
    log("=" * 60)


if __name__ == "__main__":
    main()
