# Backtesting Engine — Research & Planning

## 1. Current Architecture

The backtester uses a **two-phase design**:

1. **Pre-computation** (`precompute_ticker()`): Loads daily OHLC, builds all 11 timeframes (1H→12M), computes indicators at every historical bar, stores in `backtest_indicators` SQLite table.
2. **Query engine** (`run_backtest()`): Matches signal conditions from the pre-computed table, resolves higher-TF bars to daily dates, calculates forward returns with exit rules.

### Trade Execution Model
- Signal confirmed at **close of signal_date**
- Entry at **open of next trading day** (signal_idx + 1)
- Exit signals also resolve to next-day open

---

## 2. Look-Ahead Bias Audit

### Verdict: Execution timing is clean, but SIGNAL SELECTION has look-ahead bias in multi-timeframe conditions

| Area | Status | Detail |
|------|--------|--------|
| Entry timing | OK | Next-day open after signal close |
| Exit timing | OK | Next-day open after exit signal confirmed |
| Higher-TF resolution | OK | `_resolve_bar_end_date()` finds last daily bar of the TF period |
| Intrabar data | OK | No live/forming bars used in backtest |
| Position review | OK | Uses close price at specific day count |
| **Multi-TF signal selection** | **BIASED** | **See below — this is the critical issue** |

### CRITICAL: Multi-Timeframe Signal Selection Bias + Early Entry

**Example**: Backtest condition = "HVC bullish on daily AND deviation buy on monthly"

What actually happens:
1. Monthly deviation buy is pre-computed using the **full month's OHLC** (including the final close on March 31)
2. HVC triggers on March 5 (daily — correctly confirmed at daily close)
3. Backtester matches both signals — **enters on March 6 open** (next day after HVC)
4. But the monthly deviation buy won't be confirmed until March 31

**This is a double bias:**
- **Entry timing bias**: Enters on March 6 using a monthly signal that won't confirm for 25 more days. The trade gets the full benefit of the move that CAUSES the monthly bar to close as a deviation buy.
- **Survivorship bias in selection**: Only includes trades where the monthly signal happened to confirm. All cases where HVC fired mid-month but the monthly deviation reversed are invisible — never counted as losses.

**In live trading on March 5**, you'd see:
- HVC confirmed (daily) ✓
- Monthly deviation buy **flashing/unconfirmed** — the month's bar is still forming
- You might enter based on the unconfirmed signal, but many of those entries will fail when the monthly bar reverses

The backtest shows a misleadingly high win rate because it only enters trades where it already knows the monthly outcome.

### How to fix — Both approaches needed

**Approach A — Confirmed signals only mode**
- When matching "deviation buy on 1M", only use the **PREVIOUS completed month's** signal
- A monthly deviation buy in February (confirmed Feb 28) can be matched with an HVC on any day in March
- This eliminates all look-ahead but changes strategy semantics — you're checking last month, not current month
- Clean, simple, no new data required

**Approach B — Unconfirmed signal mode (requires new data)**
- At each daily close, calculate what the incomplete higher-TF bar's indicators currently show
- Store these "rolling" or "intra-period" signals: "On March 5, the forming monthly bar currently shows a deviation buy"
- Enter when HVC fires AND the unconfirmed monthly deviation buy is showing
- Track whether the monthly signal confirmed or reversed
- Add maintain/exit logic: "If the unconfirmed signal disappears after X days, exit the trade"
- This models what a live trader actually sees and does

**Data gap for Approach B**: We do NOT currently store intra-period indicator calculations. The pre-computed table only has the final confirmed bar. To support Approach B, we would need to:
- For every daily close, compute all higher-TF indicators using the incomplete bar (daily closes accumulated within the period so far)
- Store these as a separate dataset (e.g., `backtest_unconfirmed_signals` table)
- This significantly increases storage and compute time (every daily bar generates signals for ~10 higher TFs)

**Approach B also needs its own exit logic:**
- "If unconfirmed signal disappears within X days → exit"
- "If unconfirmed signal confirms → switch to normal hold logic"
- "If still unconfirmed after X% of the bar period → exit"

### Affected timeframes
This bias exists for ALL timeframes above 1D when combined with daily signals:
- **1W**: Signal confirmed Friday, but daily entry could be Monday-Thursday (1-4 days early)
- **2W**: Up to 9 days early
- **1M**: Up to ~21 trading days early
- **3M**: Up to ~63 trading days early
- **6M, 12M**: Extreme look-ahead — entering months before confirmation

The bias severity scales with timeframe length. A daily + weekly combo has mild bias. A daily + 12M combo is essentially entering with full future knowledge.

### Other Edge Cases
- **Missing bars / gaps**: Code returns None if entry date not found — safe but could miss valid trades
- **Trading calendar**: Algorithmic NYSE calendar should be spot-checked for edge years
- **Multi-condition rounding**: "Within N bars" uses `_tf_to_calendar_days()` approximation — could accumulate error across conditions

---

## 3. Confirmed vs Unconfirmed Signals — The Real Question

### What the backtester does today
The backtester only uses **confirmed (closed) bars**. This is correct for historical backtesting — you can't act on a signal until the bar closes.

### What the LIVE system does
The live system (`indicators.py`) has `_is_bar_confirmed()` which distinguishes "confirmed" vs "live" bars. The dashboard shows live/forming signals to the user.

### The user's concern: Weekly deviation buy on a Wednesday
**Example**: A stock flashes a weekly deviation buy signal mid-week (Wednesday). The signal won't be confirmed until Friday close. Currently:
- **Live dashboard**: Shows the unconfirmed signal (correct for monitoring)
- **Backtester**: Only sees the confirmed Friday-close signal (correct for backtesting)
- **No bias here** — the backtester doesn't act on mid-week signals

### But there IS value in recording unconfirmed signals
**Why**: Understanding when signals first appeared (even unconfirmed) could improve:
- **Entry timing research**: "Do stocks that flash a deviation buy on Monday tend to confirm by Friday?"
- **Signal reliability**: "What % of unconfirmed signals actually confirm?"
- **Early entry strategies**: "If we enter on the unconfirmed signal, does the risk/reward improve vs waiting for confirmation?"

### Possible implementation
- Add a `first_seen_date` column to `backtest_indicators` for each signal
- Record when a signal first appears on an incomplete bar
- The confirmed signal retains the existing `date` (bar close date)
- Backtester can optionally use `first_seen_date` for early-entry testing

---

## 4. Sweep Data Integration — Design Sketch

### The goal
Integrate dark pool sweep events as backtesting signals:
- "If darkpool clusterbomb of X size, X trades, on X watchlist, on X group, from X period → do X things to the trade"

### Data available
- `sweep_trades` table: Individual sweep trades (ticker, date, time, price, size, notional)
- `clusterbomb_events` table: Detected clusterbomb events (aggregated sweep days)
- `sweep_fetch_log`: Which ticker+dates have been fetched

### Integration approach

#### Option A: Sweep as standalone entry signal
```
IF clusterbomb(ticker, date)
   AND notional >= min_threshold
   AND trade_count >= min_trades
   AND ticker IN watchlist
   AND group IN allowed_groups
THEN enter trade at next-day open
```

#### Option B: Sweep as confirmation filter
```
IF bollinger_deviation_buy(ticker, date, timeframe)
   AND clusterbomb_within(ticker, date, lookback_days=5)
THEN enter trade (only when both align)
```

#### Option C: Sweep as position sizing modifier
```
IF entry_signal(ticker, date)
   AND clusterbomb(ticker, date)
THEN increase position size by X%
```

### Required changes
1. **New signal type** in `backtest_indicators` or separate sweep signal table
2. **Pre-computation step** for sweep-based signals (aggregate daily sweep stats per ticker)
3. **Condition builder** in the backtest UI for sweep parameters (min notional, min trades, etc.)
4. **Cross-referencing** sweep dates with indicator signal dates in the query engine

### Complexity estimate
- Sweep as standalone signal: Medium (new signal type + UI)
- Sweep as confirmation filter: High (cross-table joins + lookback logic)
- Sweep as position sizer: Medium (modifier on existing trades)

---

## 5. External Backtesting Frameworks

### NautilusTrader
- **Pros**: Event-driven with nanosecond resolution, strict look-ahead bias prevention by design, institutional-grade, same code for backtest + live trading, Rust core = very fast
- **Cons**: Steep learning curve, complex build chain (Rust + Cython), overkill if not going live, requires paradigm shift from current vectorized approach
- **Fit**: Good if planning live deployment; heavy for research-only use

### Backtrader
- **Pros**: Easy to learn, event-driven, good multi-timeframe support, large community, can feed pre-computed signals as custom data
- **Cons**: Moderate speed, look-ahead protection requires developer discipline, project maintenance has slowed
- **Fit**: Good middle ground — quick to prototype, adequate for this use case

### VectorBT
- **Pros**: Fastest for optimization (NumPy/Pandas vectorized), great for parameter sweeps across many assets
- **Cons**: Not event-driven (harder to model dark pool event triggers), look-ahead protection is developer responsibility
- **Fit**: Good for bulk signal research, less ideal for event-driven sweep integration

### Zipline-reloaded
- **Pros**: Good academic pedigree, pipeline system for factor research
- **Cons**: Slow, heavyweight, maintenance uncertain
- **Fit**: Not recommended

### Recommendation
The **current custom engine is solid** — no major bias issues found. For sweep integration, extending the existing engine is likely easier than migrating to an external framework. Consider Backtrader or NautilusTrader only if:
- You need formal look-ahead guarantees (NautilusTrader)
- You want faster prototyping of new strategies (Backtrader)
- You plan to go live with automated execution (NautilusTrader)

---

## 6. Approach B — Compute & Storage Estimates

### Scope: Russell 3000, daily TFs and above, unconfirmed signals

**Current dataset (confirmed bars only):**
- ~609 rows per ticker per year (all TFs: 1D through 12M)
- R3000 × 10yr = **18.3M rows** (~3.7 GB)
- R3000 × 20yr = **36.5M rows** (~7.3 GB)

**Approach B adds (unconfirmed snapshots at each daily close):**
- For each daily close, compute what incomplete higher-TF bars currently show
- Limited to 1D and above (skip sub-daily: 1H, 4H, 8H)

| Timeframe | Unconfirmed days/bar | Bars/yr | New rows/yr |
|-----------|---------------------|---------|-------------|
| 2D | 1 | 126 | 126 |
| 3D | 2 | 84 | 168 |
| 5D | 4 | 50 | 200 |
| 1W | 4 | 52 | 208 |
| 2W | 9 | 26 | 234 |
| 1M | ~20 | 12 | 240 |
| 3M | ~62 | 4 | 248 |
| 6M | ~125 | 2 | 250 |
| 12M | ~251 | 1 | 251 |
| **Total** | | | **~1,925/ticker/yr** |

**Combined totals:**

| | Confirmed (current) | + Unconfirmed | Combined | Multiplier |
|---|---|---|---|---|
| Rows/ticker/year | 609 | 1,925 | 2,534 | ~4x |
| **R3000 × 10yr** | 18.3M | 57.8M | **76.0M rows** | |
| **R3000 × 20yr** | 36.5M | 115.5M | **152.0M rows** | |
| Storage 10yr | ~3.7 GB | ~11.6 GB | **~15.2 GB** | |
| Storage 20yr | ~7.3 GB | ~23.1 GB | **~30.4 GB** | |

**Compute time:**

| | Current | With Approach B |
|---|---|---|
| Per ticker | ~2-5s | ~15-30s |
| R3000 × 10yr | ~4-6 hours | **~15-25 hours** |
| R3000 × 20yr | ~8-12 hours | **~30-50 hours** |

**Optimisation note:** The 2D/3D/5D unconfirmed signals add many rows for very little value (1-4 days of look-ahead). Restricting to 1W and above roughly halves the new rows and compute while covering the meaningful bias cases.

**Verdict:** ~4x increase in storage and compute. ~30 GB and ~2 days compute for the full 20yr R3000 dataset. One-time pre-compute, query forever. Doable.

---

## 7. Prioritised Next Steps

### Phase 1: Fix look-ahead bias — Approach A (confirmed signals only)
- [ ] Add "confirmed only" mode to backtest query engine
- [ ] When matching higher-TF signals, only use the PREVIOUS completed bar's signal
- [ ] No new data required — pure query-side fix
- [ ] Priority: **HIGH** — this is needed before any backtest results can be trusted

### Phase 2: Sweep signal pre-computation
- [ ] Build daily sweep aggregates per ticker (total notional, trade count, max single sweep, clusterbomb flag)
- [ ] Store as new rows in `backtest_indicators` or a dedicated `backtest_sweep_signals` table
- [ ] Ensure sweep data aligns with existing daily price dates

### Phase 3: Backtest condition builder for sweeps
- [ ] Add sweep-related conditions to the backtest UI (min notional, min trades, clusterbomb Y/N)
- [ ] Support sweep as entry signal, confirmation filter, or position modifier
- [ ] Cross-reference sweep dates with indicator signal dates

### Phase 4: Unconfirmed signal tracking — Approach B
- [ ] Build rolling intra-period indicator computation (daily close → synthetic incomplete higher-TF bar)
- [ ] Store in `backtest_unconfirmed_signals` table (limit to 1W+ TFs to keep storage manageable)
- [ ] Add backtest mode: "unconfirmed signal entry" with maintain/exit logic
- [ ] Exit rules: "signal disappears → exit", "signal confirms → hold", "X% of bar period elapsed unconfirmed → exit"

### Phase 5: Framework migration (only if needed)
- [ ] Evaluate whether current engine limitations justify migration
- [ ] If yes, prototype sweep strategy in Backtrader or NautilusTrader
- [ ] Compare results with current engine for validation
