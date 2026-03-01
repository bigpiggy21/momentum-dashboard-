"""
Smart automated backtest strategy search.
Uses direct engine calls (no HTTP) and analytical funnel approach.
Target: 65%+ win rate, minimized drawdowns and holding time.
Base: HVC triggered, bullish candle.
"""
import sys, io, os, time, json
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stdout.reconfigure(line_buffering=True)

# Direct engine import — no HTTP overhead
from backtest_engine import run_backtest

RESULTS = []
RUN_N = 0

def bt(label, payload):
    """Run backtest directly and collect results."""
    global RUN_N
    RUN_N += 1
    t0 = time.time()
    data = run_backtest(payload)
    elapsed = time.time() - t0

    if not data.get("ok") or not data.get("trades"):
        print(f"  [{RUN_N:>3}] {label}: 0 trades ({elapsed:.1f}s)")
        return None

    s = data.get("summary", {})
    n = s.get("total_signals", 0)
    wr = s.get("win_rate", 0) * 100
    avg_ret = s.get("avg_return_pct", 0)
    med_ret = s.get("median_return_pct", 0)
    avg_hold = s.get("avg_holding_days", 0)
    pf = s.get("profit_factor", 0)
    exp = s.get("expectancy", 0)
    pr = s.get("payoff_ratio", 0)

    trades = data.get("trades", [])
    mdds = [abs(t.get("max_drawdown_pct", 0)) for t in trades if t.get("max_drawdown_pct") is not None]
    avg_mdd = sum(mdds) / len(mdds) if mdds else 0

    sig_flag = " $" if data.get("signals_cached") else ""
    r = {"label": label, "signals": n, "win_rate": wr, "avg_return": avg_ret,
         "median_return": med_ret, "avg_hold": avg_hold, "profit_factor": pf,
         "expectancy": exp, "payoff_ratio": pr, "avg_mdd": avg_mdd, "elapsed": elapsed}
    RESULTS.append(r)

    flag = " ***" if wr >= 65 else (" **" if wr >= 60 else "")
    print(f"  [{RUN_N:>3}] {label:<72} {n:>5} sig  WR={wr:>5.1f}%  avg={avg_ret:>+7.1f}%  "
          f"hold={avg_hold:>5.0f}d  PF={pf:>5.2f}  MDD={avg_mdd:>5.1f}%{sig_flag} ({elapsed:.1f}s){flag}")
    return r


def make_payload(entry, exit_rules, stop_loss=None, maintain=None, lookback=5, dedup=30):
    return {
        "entry": entry, "exit": exit_rules, "universe": "all", "div_adj": 0,
        "lookback_years": lookback, "dedup": dedup, "direction": "long",
        "stop_loss_pct": stop_loss, "maintain": maintain,
    }


def leaderboard(title="LEADERBOARD", min_sig=10, min_wr=55, top_n=20):
    print(f"\n{'='*150}")
    print(f"  {title}")
    print(f"{'='*150}")
    scored = []
    for r in RESULTS:
        if r["signals"] < min_sig:
            continue
        if r["win_rate"] < min_wr:
            continue
        # Score: WR matters most, then PF, penalise MDD and long holds
        score = r["win_rate"]*2 + r["profit_factor"]*8 - r["avg_mdd"]*0.5 - r["avg_hold"]*0.05
        scored.append((score, r))
    scored.sort(key=lambda x: x[0], reverse=True)
    print(f"  {'#':>3} {'Score':>6}  {'Label':<72} {'Sig':>5} {'WR%':>6} {'AvgRet':>8} {'MedRet':>8} {'Hold':>6} {'PF':>6} {'MDD':>6} {'PayR':>6}")
    print(f"  {'-'*144}")
    for i, (score, r) in enumerate(scored[:top_n]):
        print(f"  {i+1:>3} {score:>6.1f}  {r['label']:<72} {r['signals']:>5} {r['win_rate']:>5.1f}% "
              f"{r['avg_return']:>+7.1f}% {r['median_return']:>+7.1f}% {r['avg_hold']:>5.0f}d "
              f"{r['profit_factor']:>5.2f} {r['avg_mdd']:>5.1f}% {r['payoff_ratio']:>5.2f}")
    print()
    return scored


# ═══════════════════════════════════════════════════════════════
# BUILDING BLOCKS
# ═══════════════════════════════════════════════════════════════

# Entry conditions
E = {
    "hvc_bull":      {"indicator": "hvc_triggered", "value": "bull", "within_bars": 0},
    "hvc_gap_up":    {"indicator": "hvc_triggered", "value": "gap_up", "within_bars": 0},
    "wma_cross_w0":  {"indicator": "wma30_cross", "value": "crossed_above", "within_bars": 0},
    "wma_cross_w5":  {"indicator": "wma30_cross", "value": "crossed_above", "within_bars": 5},
    "wma_cross_w10": {"indicator": "wma30_cross", "value": "crossed_above", "within_bars": 10},
    "above_wma":     {"indicator": "above_wma30", "value": "true", "within_bars": 0},
    "mom_blue":      {"indicator": "mom_color", "timeframe": "1D", "value": "blue", "within_bars": 0},
    "mom_blue_w5":   {"indicator": "mom_color", "timeframe": "1D", "value": "blue", "within_bars": 5},
    "mom_aqua":      {"indicator": "mom_color", "timeframe": "1D", "value": "aqua", "within_bars": 0},
    "mom_aqua_w5":   {"indicator": "mom_color", "timeframe": "1D", "value": "aqua", "within_bars": 5},
    "mom_blue+":     {"indicator": "mom_color", "timeframe": "1D", "value": "blue+", "within_bars": 0},
    "mom_blue+_w5":  {"indicator": "mom_color", "timeframe": "1D", "value": "blue+", "within_bars": 5},
    "mom_blue_1W":   {"indicator": "mom_color", "timeframe": "1W", "value": "blue", "within_bars": 0},
    "mom_aqua_1W":   {"indicator": "mom_color", "timeframe": "1W", "value": "aqua", "within_bars": 0},
    "mom_blue+_1W":  {"indicator": "mom_color", "timeframe": "1W", "value": "blue+", "within_bars": 0},
    "mom_rising":    {"indicator": "mom_rising", "value": "true", "within_bars": 0},
    "sqz_fired":     {"indicator": "sqz_state", "timeframe": "1D", "value": "fired", "within_bars": 0},
    "sqz_fired_w5":  {"indicator": "sqz_state", "timeframe": "1D", "value": "fired", "within_bars": 5},
    "sqz_fired_1W":  {"indicator": "sqz_state", "timeframe": "1W", "value": "fired", "within_bars": 0},
    "sqz_green":     {"indicator": "sqz_state", "timeframe": "1D", "value": "green", "within_bars": 0},
    "band_flip":     {"indicator": "band_flip", "timeframe": "1D", "value": "1", "within_bars": 0},
    "band_flip_w5":  {"indicator": "band_flip", "timeframe": "1D", "value": "1", "within_bars": 5},
    "dev_buy_1M":    {"indicator": "dev_signal", "timeframe": "1M", "value": "buy", "within_bars": 0},
}

# Exit rules
X = {
    "hold20":     [{"type": "max_bars", "timeframe": "1D", "bars": 20}],
    "hold50":     [{"type": "max_bars", "timeframe": "1D", "bars": 50}],
    "hold100":    [{"type": "max_bars", "timeframe": "1D", "bars": 100}],
    "trail10":    [{"type": "trailing_stop", "pct": 10}],
    "trail15":    [{"type": "trailing_stop", "pct": 15}],
    "trail20":    [{"type": "trailing_stop", "pct": 20}],
    "trail25":    [{"type": "trailing_stop", "pct": 25}],
    "dev_sell":   [{"type": "signal", "indicator": "dev_signal", "timeframe": "1M", "value": "sell"}],
    "mom_red":    [{"type": "signal", "indicator": "mom_color", "timeframe": "1D", "value": "red"}],
    "mom_yel":    [{"type": "signal", "indicator": "mom_color", "timeframe": "1D", "value": "yellow"}],
    "mom_red_1W": [{"type": "signal", "indicator": "mom_color", "timeframe": "1W", "value": "red"}],
    "sqz_red":    [{"type": "signal", "indicator": "sqz_state", "timeframe": "1D", "value": "red"}],
    # Combos
    "dev+trail15":  [{"type": "signal", "indicator": "dev_signal", "timeframe": "1M", "value": "sell"}, {"type": "trailing_stop", "pct": 15}],
    "dev+trail20":  [{"type": "signal", "indicator": "dev_signal", "timeframe": "1M", "value": "sell"}, {"type": "trailing_stop", "pct": 20}],
    "dev+trail25":  [{"type": "signal", "indicator": "dev_signal", "timeframe": "1M", "value": "sell"}, {"type": "trailing_stop", "pct": 25}],
    "h100+trail15": [{"type": "max_bars", "timeframe": "1D", "bars": 100}, {"type": "trailing_stop", "pct": 15}],
    "h100+trail20": [{"type": "max_bars", "timeframe": "1D", "bars": 100}, {"type": "trailing_stop", "pct": 20}],
    "h50+trail15":  [{"type": "max_bars", "timeframe": "1D", "bars": 50}, {"type": "trailing_stop", "pct": 15}],
    "mred1W+tr15":  [{"type": "signal", "indicator": "mom_color", "timeframe": "1W", "value": "red"}, {"type": "trailing_stop", "pct": 15}],
    "mred1W+tr20":  [{"type": "signal", "indicator": "mom_color", "timeframe": "1W", "value": "red"}, {"type": "trailing_stop", "pct": 20}],
}


# ═══════════════════════════════════════════════════════════════
# PHASE 1: BASELINE — HVC bull with a reference exit to measure
# win rates across different secondaries quickly.
# Use dev_sell as reference (highest WR from your screenshot).
# ═══════════════════════════════════════════════════════════════

print("\n" + "="*80)
print("PHASE 1: MEASURE LIFT — Which secondary conditions improve WR the most?")
print("  Using dev_sell_1M as reference exit (best WR from your findings)")
print("="*80)

# Baseline: HVC bull alone
bt("BASELINE: HVC_bull | dev_sell", make_payload([E["hvc_bull"]], X["dev_sell"]))
bt("BASELINE: HVC_gap_up | dev_sell", make_payload([E["hvc_gap_up"]], X["dev_sell"]))

# Now test every secondary — all share the same entry cache approach
# We group these so they run fast
print("\n--- HVC bull + secondary (dev_sell exit) ---")
for name, cond in E.items():
    if name.startswith("hvc"):
        continue  # skip base entries
    bt(f"HVC_bull + {name} | dev_sell",
       make_payload([E["hvc_bull"], cond], X["dev_sell"]))

print("\n--- HVC gap_up + secondary (dev_sell exit) ---")
for name, cond in E.items():
    if name.startswith("hvc"):
        continue
    bt(f"HVC_gap_up + {name} | dev_sell",
       make_payload([E["hvc_gap_up"], cond], X["dev_sell"]))

leaderboard("PHASE 1: SECONDARY CONDITION LIFT (dev_sell exit)", min_wr=50)


# ═══════════════════════════════════════════════════════════════
# PHASE 2: TOP ENTRIES × EXIT MATRIX
# Take the top ~8 entry combos and test all exit strategies.
# Signal cache means exit variations are nearly free.
# ═══════════════════════════════════════════════════════════════

print("\n" + "="*80)
print("PHASE 2: TOP ENTRIES × EXIT VARIATIONS")
print("="*80)

# Identify top entries from Phase 1 (by win rate)
phase1 = [(r["win_rate"], r["label"]) for r in RESULTS if r["signals"] >= 10]
phase1.sort(reverse=True)

# Extract the top entry combo names (everything before " | ")
top_entries_labels = []
seen = set()
for wr, label in phase1:
    entry_part = label.split(" | ")[0]
    if entry_part not in seen and "BASELINE" not in label:
        seen.add(entry_part)
        top_entries_labels.append(entry_part)
    if len(top_entries_labels) >= 10:
        break

print(f"Top entries from Phase 1: {top_entries_labels}\n")

def parse_entry(label):
    """Reconstruct entry conditions from a label like 'HVC_bull + wma_cross_w10'."""
    parts = label.replace("HVC_bull + ", "").replace("HVC_gap_up + ", "")
    entry = []
    if "HVC_gap_up" in label:
        entry.append(E["hvc_gap_up"])
    else:
        entry.append(E["hvc_bull"])
    if parts != label:  # has secondary
        sec_name = parts.strip()
        if sec_name in E:
            entry.append(E[sec_name])
    return entry

for entry_label in top_entries_labels:
    entry = parse_entry(entry_label)
    if len(entry) == 0:
        continue
    print(f"\n--- {entry_label} × all exits ---")
    for exit_name, exit_rules in X.items():
        if exit_name == "dev_sell":
            continue  # already tested in Phase 1
        bt(f"{entry_label} | {exit_name}", make_payload(entry, exit_rules))

leaderboard("PHASE 2: FULL ENTRY × EXIT MATRIX", min_wr=55)


# ═══════════════════════════════════════════════════════════════
# PHASE 3: STOP LOSS & POSITION REVIEW TUNING
# Take top ~10 combos and test stop loss + maintain variants.
# ═══════════════════════════════════════════════════════════════

print("\n" + "="*80)
print("PHASE 3: STOP LOSS TUNING ON TOP CANDIDATES")
print("="*80)

# Get top 10 from current leaderboard
top10 = leaderboard("Pre-Phase 3 standings", min_wr=58, top_n=10)

for _, r in top10[:8]:
    label = r["label"]
    entry_part = label.split(" | ")[0]
    exit_part = label.split(" | ")[1] if " | " in label else "dev_sell"
    entry = parse_entry(entry_part)
    exit_rules = X.get(exit_part, X["dev_sell"])

    for sl in [8, 10, 15, 20]:
        bt(f"{label} SL={sl}%", make_payload(entry, exit_rules, stop_loss=sl))

leaderboard("PHASE 3: WITH STOP LOSSES", min_wr=58)


# ═══════════════════════════════════════════════════════════════
# PHASE 4: POSITION REVIEW ON BEST
# ═══════════════════════════════════════════════════════════════

print("\n" + "="*80)
print("PHASE 4: POSITION REVIEW ON TOP CANDIDATES")
print("="*80)

top5 = leaderboard("Pre-Phase 4", min_wr=60, top_n=5)

for _, r in top5[:5]:
    label = r["label"]
    # Strip any existing SL from label
    base_label = label.split(" SL=")[0]
    entry_part = base_label.split(" | ")[0]
    exit_part = base_label.split(" | ")[1] if " | " in base_label else "dev_sell"
    entry = parse_entry(entry_part)
    exit_rules = X.get(exit_part, X["dev_sell"])
    sl = None
    if "SL=" in label:
        sl = int(label.split("SL=")[1].replace("%",""))

    # Position review: check if above entry after 10 trading days
    for review_days in [10, 20, 30]:
        maintain = {"after_days": review_days, "conditions": [{"type": "above_entry"}]}
        bt(f"{label} +review{review_days}d(above)",
           make_payload(entry, exit_rules, stop_loss=sl, maintain=maintain))

    # Position review: 5% min gain after 30 days
    maintain = {"after_days": 30, "conditions": [{"type": "min_gain", "pct": 5}]}
    bt(f"{label} +review30d(5%gain)",
       make_payload(entry, exit_rules, stop_loss=sl, maintain=maintain))


# ═══════════════════════════════════════════════════════════════
# FINAL RESULTS
# ═══════════════════════════════════════════════════════════════
leaderboard("FINAL LEADERBOARD — ALL RESULTS", min_wr=55, top_n=30)
print(f"Total backtests run: {RUN_N}")
print("Done.")
