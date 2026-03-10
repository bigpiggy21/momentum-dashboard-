#!/usr/bin/env python3
"""
Daily Darkpool Sweep Report — generates a .txt table of today's sweep activity.

Usage:
    python3 daily_report.py                              # today, top 10, all assets
    python3 daily_report.py --date 2026-03-10            # specific date
    python3 daily_report.py --top 5                      # top 5 only
    python3 daily_report.py --top 20 --asset stock       # top 20 stocks only
    python3 daily_report.py --top 10 --asset etf         # top 10 ETFs only
    python3 daily_report.py --min-total 50               # min $50M total notional
    python3 daily_report.py --no-file                    # print only, don't save
"""

import argparse
import json
import os
import sys
from datetime import datetime, date

import requests

BASE_URL = "http://localhost:8080"
REPORT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reports")


def fetch_events(report_date, asset_class, min_total_m):
    """Fetch events from the tracker API."""
    params = {
        "date_from": report_date,
        "date_to": report_date,
        "limit": 500,
        "offset": 0,
        "min_total": int(min_total_m * 1_000_000),
    }
    if asset_class in ("stock", "etf"):
        params["asset_class"] = asset_class  # API expects 'stock' or 'etf'
    else:
        params["asset_class"] = "all"

    try:
        r = requests.get(f"{BASE_URL}/api/sweeps/tracker", params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        return data.get("events", [])
    except Exception as e:
        print(f"Error fetching events: {e}")
        sys.exit(1)


def classify_asset(ticker, etf_set):
    return "etf" if ticker in etf_set else "stock"


def load_etf_set():
    """Load ETF ticker set from cache file."""
    cache_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "cache", "etf_tickers.json")
    try:
        with open(cache_path, "r") as f:
            return set(json.load(f))
    except (FileNotFoundError, json.JSONDecodeError):
        return set()


def fmt_notional(val):
    if val is None:
        return "   --"
    if abs(val) >= 1e9:
        return f"${val/1e9:.1f}B"
    if abs(val) >= 1e6:
        return f"${val/1e6:.1f}M"
    if abs(val) >= 1e3:
        return f"${val/1e3:.0f}K"
    return f"${val:.0f}"


def fmt_rank(val):
    if val is None or val == 0:
        return "  --"
    return f"  #{val}"


def fmt_pct(val):
    if val is None:
        return "    --"
    return f"{val:+.1f}%"


def fmt_flag(val):
    return " Y" if val else "  "


def build_table(events, top_n, label, show_cb_monster=False):
    """Build a formatted text table from events."""
    if not events:
        return f"  No events.\n"

    # Sort by total notional descending, take top N
    events.sort(key=lambda e: e.get("total_notional", 0) or 0, reverse=True)
    events = events[:top_n]

    lines = []

    if show_cb_monster:
        header = (f"  {'Ticker':<8} {'Total $':>9}  {'Swps':>4}  "
                  f"{'Daily':>5}  {'Single':>6}  {'Rare':>4}  "
                  f"{'CB':>3}  {'Mon':>3}  {'% Gain':>7}")
        sep = "  " + "-" * 72
    else:
        header = (f"  {'Ticker':<8} {'Total $':>9}  {'Swps':>4}  "
                  f"{'Daily':>5}  {'Single':>6}  {'Rare':>4}  {'% Gain':>7}")
        sep = "  " + "-" * 58

    lines.append(sep)
    lines.append(header)
    lines.append(sep)

    for ev in events:
        ticker = ev.get("ticker", "???")
        total = fmt_notional(ev.get("total_notional"))
        sweeps = f"{ev.get('sweep_count', 0):4d}"
        daily = fmt_rank(ev.get("daily_rank"))
        single = fmt_rank(ev.get("sweep_rank"))
        is_rare = fmt_flag(ev.get("event_type") == "rare_sweep" or ev.get("is_rare"))
        pct = fmt_pct(ev.get("pct_gain"))

        if show_cb_monster:
            is_cb = fmt_flag(ev.get("event_type") == "clusterbomb")
            is_mon = fmt_flag(ev.get("is_monster") or ev.get("event_type") == "monster_sweep")
            lines.append(f"  {ticker:<8} {total:>9}  {sweeps}  "
                         f"{daily:>5}  {single:>6}  {is_rare:>4}  "
                         f"{is_cb:>3}  {is_mon:>3}  {pct:>7}")
        else:
            lines.append(f"  {ticker:<8} {total:>9}  {sweeps}  "
                         f"{daily:>5}  {single:>6}  {is_rare:>4}  {pct:>7}")

    lines.append(sep)
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description="Daily Darkpool Sweep Report")
    parser.add_argument("--date", default=date.today().strftime("%Y-%m-%d"),
                        help="Report date (YYYY-MM-DD, default: today)")
    parser.add_argument("--top", type=int, default=10,
                        help="Top N events per category (default: 10)")
    parser.add_argument("--asset", choices=["stock", "etf", "all"], default="all",
                        help="Asset class filter (default: all)")
    parser.add_argument("--min-total", type=float, default=0,
                        help="Min total notional in $M (default: 0 = show all)")
    parser.add_argument("--no-file", action="store_true",
                        help="Print only, don't save to file")
    args = parser.parse_args()

    # Parse date for display
    try:
        dt = datetime.strptime(args.date, "%Y-%m-%d")
        day_name = dt.strftime("%A")
    except ValueError:
        print(f"Invalid date format: {args.date}")
        sys.exit(1)

    # Fetch all events for the date
    all_events = fetch_events(args.date, args.asset, args.min_total)

    # Split into stocks and ETFs
    etf_set = load_etf_set()
    stock_events = [e for e in all_events if classify_asset(e["ticker"], etf_set) == "stock"]
    etf_events = [e for e in all_events if classify_asset(e["ticker"], etf_set) == "etf"]

    # Build report
    output = []
    output.append("")
    output.append("=" * 76)
    output.append(f"  DARKPOOL SWEEP REPORT -- {args.date} ({day_name})")
    output.append(f"  Top {args.top} | Filter: {args.asset.upper()}"
                  + (f" | Min ${args.min_total:.0f}M" if args.min_total > 0 else ""))
    output.append("=" * 76)

    if args.asset in ("stock", "all"):
        output.append("")
        output.append(f"  STOCKS ({len(stock_events)} events)")
        output.append(build_table(stock_events, args.top, "Stocks", show_cb_monster=True))

    if args.asset in ("etf", "all"):
        output.append(f"  ETFs ({len(etf_events)} events)")
        output.append(build_table(etf_events, args.top, "ETFs", show_cb_monster=False))

    report_text = "\n".join(output)
    print(report_text)

    # Save to file
    if not args.no_file:
        os.makedirs(REPORT_DIR, exist_ok=True)
        filename = f"sweep_report_{args.date}.txt"
        filepath = os.path.join(REPORT_DIR, filename)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(report_text + "\n")
        print(f"\n  Saved to: {filepath}")


if __name__ == "__main__":
    main()
