"""
Technical indicator calculations.
Translates TradingView Pine Script logic into Python/pandas.
"""

import numpy as np
import pandas as pd
from scipy import stats


def sma(series, length):
    return series.rolling(window=length, min_periods=length).mean()

def ema(series, length):
    return series.ewm(span=length, adjust=False, min_periods=length).mean()

def rma(series, length):
    """Wilder's smoothing (RMA) - same as Pine Script's ta.rma / ta.atr."""
    return series.ewm(alpha=1/length, adjust=False, min_periods=length).mean()

def stdev(series, length):
    return series.rolling(window=length, min_periods=length).std(ddof=0)

def true_range(df):
    prev_close = df["close"].shift(1)
    tr1 = df["high"] - df["low"]
    tr2 = (df["high"] - prev_close).abs()
    tr3 = (df["low"] - prev_close).abs()
    return pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

def highest(series, length):
    return series.rolling(window=length, min_periods=length).max()

def lowest(series, length):
    return series.rolling(window=length, min_periods=length).min()

def linreg(series, length, offset=0):
    """Fully vectorised rolling linear regression — zero Python loops.
    
    Uses cumsum-based rolling window sums for O(n) computation.
    Produces identical results to TradingView's linreg() function.
    NaN values in any window produce NaN output for that window only.
    """
    values = series.values.astype(float)
    n = length
    out = np.full(len(values), np.nan)
    
    if len(values) < n:
        return pd.Series(out, index=series.index)
    
    # Constants for x = 0, 1, ..., n-1
    sum_x = n * (n - 1) / 2.0
    sum_x2 = n * (n - 1) * (2 * n - 1) / 6.0
    denom = n * sum_x2 - sum_x * sum_x
    
    # Track NaN positions: a window is invalid if it contains any NaN
    nan_mask = np.isnan(values).astype(float)
    cs_nan = np.concatenate(([0.0], np.cumsum(nan_mask)))
    roll_nan_count = cs_nan[n:] - cs_nan[:-n]  # NaN count per window
    
    # Replace NaN with 0 for cumsum computation (won't affect valid windows)
    safe_values = np.where(np.isnan(values), 0.0, values)
    
    # Rolling sum of y values
    cs_y = np.concatenate(([0.0], np.cumsum(safe_values)))
    roll_sum_y = cs_y[n:] - cs_y[:-n]
    
    # Rolling sum of (local_index * y)
    idx = np.arange(len(values), dtype=float)
    cs_xy = np.concatenate(([0.0], np.cumsum(idx * safe_values)))
    roll_sum_xy_raw = cs_xy[n:] - cs_xy[:-n]
    
    starts = np.arange(len(values) - n + 1, dtype=float)
    roll_sum_xy = roll_sum_xy_raw - starts * roll_sum_y
    
    # Compute slope and value
    slope = (n * roll_sum_xy - sum_x * roll_sum_y) / denom
    intercept = (roll_sum_y - slope * sum_x) / n
    vals = intercept + slope * (n - 1 - offset)
    
    # Invalidate windows that contained any NaN
    vals[roll_nan_count > 0] = np.nan
    
    out[n - 1:] = vals
    
    return pd.Series(out, index=series.index)

def change(series, periods=1):
    return series - series.shift(periods)


def linreg_scalar(y_array, length=20, offset=0):
    """Scalar linear regression for a single window of `length` values.

    y_array: numpy array of exactly `length` float values.
    Returns the linreg endpoint value (float), or NaN if any value is NaN.
    Matches TradingView's linreg() semantics for a single window.
    """
    n = length
    if len(y_array) < n or np.any(np.isnan(y_array)):
        return np.nan

    sum_x = n * (n - 1) / 2.0
    sum_x2 = n * (n - 1) * (2 * n - 1) / 6.0
    denom = n * sum_x2 - sum_x * sum_x

    x_vals = np.arange(n, dtype=float)
    sum_y = np.sum(y_array)
    sum_xy = np.sum(x_vals * y_array)

    slope = (n * sum_xy - sum_x * sum_y) / denom
    intercept = (sum_y - slope * sum_x) / n
    return intercept + slope * (n - 1 - offset)


def calc_momentum(df, length=20):
    close, high, low = df["close"], df["high"], df["low"]
    hh = highest(high, length)
    ll = lowest(low, length)
    midline = (hh + ll) / 2
    sma_close = sma(close, length)
    basis = (midline + sma_close) / 2
    delta = close - basis
    mom = linreg(delta, length, 0)
    mom_rising = mom > mom.shift(1)
    mom_color = pd.Series("", index=df.index)
    mom_color[(mom > 0) & mom_rising] = "aqua"
    mom_color[(mom > 0) & ~mom_rising] = "blue"
    mom_color[(mom <= 0) & mom_rising] = "yellow"
    mom_color[(mom <= 0) & ~mom_rising] = "red"
    return pd.DataFrame({"mom": mom, "mom_rising": mom_rising, "mom_color": mom_color}, index=df.index)


def calc_momentum_pivot(df, length=20):
    """Calculate the TTM momentum pivot price for each bar.

    The pivot price is the hypothetical close where momentum would equal the
    previous bar's momentum (i.e., momentum would be flat / zero change).

    Uses analytical solution — the relationship mom(X) is linear in X because
    changing close[i] only affects delta[i] in the linreg window.

    Returns DataFrame with:
      - mom_pivot_price: the pivot price level (REAL)
    """
    close = df["close"].values.astype(float)
    high = df["high"].values.astype(float)
    low = df["low"].values.astype(float)
    n = length

    # Compute full momentum series to get mom values
    mom_df = calc_momentum(df, length)
    mom_vals = mom_df["mom"].values.astype(float)

    # Pre-compute rolling components (vectorised)
    hh = highest(pd.Series(high), n).values
    ll = lowest(pd.Series(low), n).values
    midline = (hh + ll) / 2.0
    sma_close = sma(pd.Series(close), n).values
    basis = (midline + sma_close) / 2.0
    delta = close - basis

    pivot = np.full(len(close), np.nan)

    # Need 2*n bars (n for inner calcs + n for linreg) + 1 for prev mom
    min_idx = 2 * n

    for i in range(min_idx, len(close)):
        if np.isnan(mom_vals[i]) or np.isnan(mom_vals[i - 1]):
            continue

        mom_prev = mom_vals[i - 1]
        mom_curr = mom_vals[i]

        # Extract 20-bar delta window ending at bar i
        win_start = i - n + 1
        if win_start < 0:
            continue
        delta_window = delta[win_start:i + 1].copy()

        if len(delta_window) < n or np.any(np.isnan(delta_window)):
            continue

        # Sensitivity: how much does mom change per unit change in delta[i]?
        # linreg is linear, so perturb by 1.0 and measure
        mom_base = linreg_scalar(delta_window, n, 0)
        delta_perturbed = delta_window.copy()
        delta_perturbed[-1] += 1.0
        mom_pert = linreg_scalar(delta_perturbed, n, 0)
        sensitivity = mom_pert - mom_base  # dmom / ddelta_at_i

        if abs(sensitivity) < 1e-15:
            continue

        # How delta[i] changes per unit change in close:
        # sma_new = sma_orig + dX/n → basis_new = basis_orig + dX/(2n)
        # delta_new = (close+dX) - (basis + dX/(2n)) = delta_orig + dX*(2n-1)/(2n)
        ddelta_dclose = (2 * n - 1) / (2.0 * n)  # = 39/40 for n=20

        dmom_dclose = sensitivity * ddelta_dclose

        # Solve: mom_prev = mom_curr + dmom_dclose * (X - close[i])
        X = close[i] + (mom_prev - mom_curr) / dmom_dclose
        pivot[i] = X

    return pd.DataFrame({"mom_pivot_price": pivot}, index=df.index)


def calc_consecutive_streaks(df, length=20, precomputed_mom=None):
    """Calculate consecutive streak counts for momentum direction and candle colour.

    Note: consec_price_green/red count per-bar green/red candles. The "Price Run"
    backtesting condition uses a separate fixed-window lookback (comparing current
    close vs open of N bars back) which is computed at query time, not here.

    Returns DataFrame with:
      - consec_mom_falling: consecutive bars where mom is falling (incl. current)
      - consec_mom_rising: consecutive bars where mom is rising (incl. current)
      - consec_price_green: consecutive green candles (close > open)
      - consec_price_red: consecutive red candles (close < open)
    """
    if precomputed_mom is not None:
        mom_rising_raw = precomputed_mom["mom_rising"]
    else:
        mom_data = calc_momentum(df, length)
        mom_rising_raw = mom_data["mom_rising"]

    close_vals = df["close"].values.astype(float)
    open_vals = df["open"].values.astype(float)

    nn = len(df)
    c_falling = np.zeros(nn, dtype=int)
    c_rising = np.zeros(nn, dtype=int)
    c_green = np.zeros(nn, dtype=int)
    c_red = np.zeros(nn, dtype=int)

    for i in range(nn):
        mr = mom_rising_raw.iloc[i] if hasattr(mom_rising_raw, 'iloc') else mom_rising_raw[i]
        if pd.isna(mr):
            continue

        # Momentum streaks
        if not bool(mr):  # falling
            c_falling[i] = 1 + (c_falling[i - 1] if i > 0 else 0)
        else:  # rising
            c_rising[i] = 1 + (c_rising[i - 1] if i > 0 else 0)

        # Price colour streaks (per-bar)
        if close_vals[i] > open_vals[i]:
            c_green[i] = 1 + (c_green[i - 1] if i > 0 else 0)
        elif close_vals[i] < open_vals[i]:
            c_red[i] = 1 + (c_red[i - 1] if i > 0 else 0)

    return pd.DataFrame({
        "consec_mom_falling": c_falling,
        "consec_mom_rising": c_rising,
        "consec_price_green": c_green,
        "consec_price_red": c_red,
    }, index=df.index)


def calc_pivot_wick(df, pivot_prices):
    """Detect failed pivot reclaim — wick crosses pivot but body confirmed on other side.

    pivot_prices: Series or array of pivot price per bar.

    Returns DataFrame with:
      - pivot_wick_above: 1 if high > pivot AND open,close,low all < pivot
      - pivot_wick_below: 1 if low < pivot AND open,close,high all > pivot
    """
    h = df["high"].values.astype(float)
    l = df["low"].values.astype(float)
    o = df["open"].values.astype(float)
    c = df["close"].values.astype(float)
    pvt = np.asarray(pivot_prices, dtype=float)

    wick_above = np.zeros(len(df), dtype=int)
    wick_below = np.zeros(len(df), dtype=int)

    valid = ~np.isnan(pvt)
    wick_above[valid] = (
        (h[valid] > pvt[valid]) &
        (o[valid] < pvt[valid]) &
        (c[valid] < pvt[valid]) &
        (l[valid] < pvt[valid])
    ).astype(int)

    wick_below[valid] = (
        (l[valid] < pvt[valid]) &
        (o[valid] > pvt[valid]) &
        (c[valid] > pvt[valid]) &
        (h[valid] > pvt[valid])
    ).astype(int)

    return pd.DataFrame({
        "pivot_wick_above": wick_above,
        "pivot_wick_below": wick_below,
    }, index=df.index)


def calc_squeeze(df, length=20, bb_mult=2.0, kc_high=1.0, kc_mid=1.5, kc_low=2.0):
    close = df["close"]
    bb_basis = sma(close, length)
    dev = bb_mult * stdev(close, length)
    bb_upper = bb_basis + dev
    bb_lower = bb_basis - dev
    kc_basis = sma(close, length)
    kc_atr = sma(true_range(df), length)
    kc_upper_high = kc_basis + kc_atr * kc_high
    kc_lower_high = kc_basis - kc_atr * kc_high
    kc_upper_mid = kc_basis + kc_atr * kc_mid
    kc_lower_mid = kc_basis - kc_atr * kc_mid
    kc_upper_low = kc_basis + kc_atr * kc_low
    kc_lower_low = kc_basis - kc_atr * kc_low
    # Squeeze uses OR logic (matching Beardy_Fred Pine Script):
    # squeeze is on if EITHER BB lower is inside KC lower OR BB upper is inside KC upper
    low_sqz = (bb_lower >= kc_lower_low) | (bb_upper <= kc_upper_low)
    mid_sqz = (bb_lower >= kc_lower_mid) | (bb_upper <= kc_upper_mid)
    high_sqz = (bb_lower >= kc_lower_high) | (bb_upper <= kc_upper_high)
    sqz_state = pd.Series("green", index=df.index)
    sqz_state[low_sqz] = "black"
    sqz_state[mid_sqz] = "red"
    sqz_state[high_sqz] = "orange"
    return pd.DataFrame({"sqz_state": sqz_state}, index=df.index)


def calc_band(df, length=20):
    close = df["close"]
    sma20 = sma(close, length)
    band_pos = pd.Series("down", index=df.index)
    band_pos[close > sma20] = "up"
    band_flip = band_pos != band_pos.shift(1)
    # Convert to arrow symbols for display
    band_display = band_pos.replace({"up": "↑", "down": "↓"})
    return pd.DataFrame({"band_pos": band_display, "band_flip": band_flip}, index=df.index)


def calc_acceleration(df, length=20, precomputed_mom=None):
    if precomputed_mom is not None:
        mom_data = precomputed_mom
    else:
        mom_data = calc_momentum(df, length)
    mom = mom_data["mom"]
    rate_length = 20
    mom_change = change(mom, 1)
    mom_slope = linreg(mom_change, rate_length, 0)
    mom_accel = change(mom_slope, 1)
    avg_slope = sma(mom_slope.abs(), 50)
    norm_decel = pd.Series(0.0, index=df.index)
    mask = avg_slope != 0
    norm_decel[mask] = mom_slope[mask] / avg_slope[mask]
    decel_signal = (norm_decel <= -1.0) & (norm_decel.shift(1) > -1.0)
    accel_signal = (mom_accel >= 1.0) & (mom_accel.shift(1) < 1.0)
    acc_state = pd.Series("red", index=df.index)
    acc_state[mom_accel > 0] = "green"
    acc_impulse = pd.Series("", index=df.index)
    acc_impulse[accel_signal] = "▲"
    acc_impulse[decel_signal] = "▼"
    return pd.DataFrame({"acc_state": acc_state, "acc_impulse": acc_impulse}, index=df.index)


def calc_deviation_signal(df, length=20, bb_mult=2.0):
    close, open_ = df["close"], df["open"]
    basis = sma(close, length)
    dev = bb_mult * stdev(close, length)
    upper = basis + dev
    lower = basis - dev
    buy = (open_ < lower) & (close > lower)
    sell = (open_ > upper) & (close < upper)
    dev_signal = pd.Series("", index=df.index)
    dev_signal[buy] = "buy"
    dev_signal[sell] = "sell"
    return pd.DataFrame({"dev_signal": dev_signal}, index=df.index)


def calc_hvc(df, sma_length=20, threshold=3.0):
    """High Volume Candle detection with gap analysis.

    HVC criteria: volume >= threshold × SMA(volume, sma_length).
    Gap criteria (only evaluated when HVC triggers):
    - Gap up: today's open > yesterday's high
    - Gap down: today's open < yesterday's low
    - Gap level: previous bar's high (gap up) or low (gap down)
    - Gap closed: did price revisit the gap level in subsequent bars?

    Based on TradingView MTF HVC indicator logic.

    Returns DataFrame with:
    - hvc_triggered: bool — True if bar qualifies as HVC
    - hvc_volume_ratio: float — volume / SMA(volume), e.g. 4.82 means 482%
    - hvc_candle_dir: str — "bull" if close >= open, "bear" otherwise
    - hvc_gap_type: str — "up" / "down" / "" (empty = no gap)
    - hvc_gap_level: float — the gap boundary price (prev high for gap up, prev low for gap down)
    - hvc_gap_closed: bool — True if gap was subsequently filled
    """
    empty = pd.DataFrame({
        "hvc_triggered": pd.Series(False, index=df.index),
        "hvc_volume_ratio": pd.Series(np.nan, index=df.index),
        "hvc_candle_dir": pd.Series("", index=df.index),
        "hvc_gap_type": pd.Series("", index=df.index),
        "hvc_gap_level": pd.Series(np.nan, index=df.index),
        "hvc_gap_closed": pd.Series(False, index=df.index),
    })

    if "volume" not in df.columns or df["volume"].isna().all():
        return empty

    vol = df["volume"].astype(float)
    vol_sma = sma(vol, sma_length)

    ratio = vol / vol_sma
    triggered = (ratio >= threshold) & (vol > vol_sma)
    triggered = triggered.fillna(False)

    candle_dir = pd.Series("", index=df.index)
    candle_dir[df["close"] >= df["open"]] = "bull"
    candle_dir[df["close"] < df["open"]] = "bear"

    # Gap detection — only meaningful when HVC triggers
    gap_type = pd.Series("", index=df.index)
    gap_level = pd.Series(np.nan, index=df.index)
    gap_closed = pd.Series(False, index=df.index)

    prev_high = df["high"].shift(1)
    prev_low = df["low"].shift(1)

    # Gap up: open > previous bar's high
    is_gap_up = df["open"] > prev_high
    # Gap down: open < previous bar's low
    is_gap_down = df["open"] < prev_low

    gap_type[is_gap_up] = "up"
    gap_type[is_gap_down] = "down"
    gap_level[is_gap_up] = prev_high[is_gap_up]
    gap_level[is_gap_down] = prev_low[is_gap_down]

    # Check if gap was closed: for each HVC+gap bar, scan subsequent bars
    for i in range(len(df)):
        if not triggered.iloc[i]:
            continue
        if gap_type.iloc[i] == "up":
            # Gap closed if any subsequent bar's low <= gap level (prev high)
            lvl = gap_level.iloc[i]
            subsequent = df.iloc[i + 1:]
            if not subsequent.empty and (subsequent["low"] <= lvl).any():
                gap_closed.iloc[i] = True
        elif gap_type.iloc[i] == "down":
            # Gap closed if any subsequent bar's high >= gap level (prev low)
            lvl = gap_level.iloc[i]
            subsequent = df.iloc[i + 1:]
            if not subsequent.empty and (subsequent["high"] >= lvl).any():
                gap_closed.iloc[i] = True

    return pd.DataFrame({
        "hvc_triggered": triggered,
        "hvc_volume_ratio": ratio,
        "hvc_candle_dir": candle_dir,
        "hvc_gap_type": gap_type,
        "hvc_gap_level": gap_level,
        "hvc_gap_closed": gap_closed,
    })


def calc_30wma(weekly_df, current_close):
    if weekly_df is None or weekly_df.empty or len(weekly_df) < 30:
        return {"wma30_value": None, "above_wma30": None, "wma30_cross": ""}
    wma30 = sma(weekly_df["close"], 30)
    if wma30.isna().iloc[-1]:
        return {"wma30_value": None, "above_wma30": None, "wma30_cross": ""}
    current_wma = wma30.iloc[-1]
    above = current_close > current_wma
    cross = ""
    if len(wma30) >= 2 and not wma30.isna().iloc[-2]:
        prev_above = weekly_df["close"].iloc[-2] > wma30.iloc[-2]
        if above and not prev_above:
            cross = "crossed_above"
        elif not above and prev_above:
            cross = "crossed_below"
    return {"wma30_value": round(current_wma, 2), "above_wma30": above, "wma30_cross": cross}


def _is_bar_confirmed(tf, df):
    """Determine if the last bar in a timeframe is confirmed (closed) or live (still forming).
    
    A bar is confirmed when:
    - Weekend/holiday: all bars are confirmed (market closed, no live data)
    - Market hours: check if bar's expected close has passed
    
    Uses trading calendar for accurate determination.
    """
    from datetime import datetime, timezone, date as date_type
    from trading_calendar import is_trading_day
    
    if df.empty or len(df) < 2:
        return True
    
    now = datetime.now(timezone.utc)
    today = now.date()
    
    # If today is not a trading day, all bars are confirmed
    if not is_trading_day(today):
        return True
    
    # During trading hours: infer bar duration from gap between last two bars
    last_ts = df['timestamp'].iloc[-1]
    prev_ts = df['timestamp'].iloc[-2]
    
    if last_ts.tzinfo is None:
        last_ts = last_ts.tz_localize('UTC')
    if prev_ts.tzinfo is None:
        prev_ts = prev_ts.tz_localize('UTC')
    
    bar_duration = last_ts - prev_ts
    expected_close = last_ts + bar_duration
    
    return now >= expected_close


def calculate_all_indicators(timeframe_data, weekly_data):
    results = {}
    
    # Maximum lookback needed by any indicator:
    # Acceleration = momentum(20) + change(1) + linreg(20) + change(1) + sma(50) = ~92 bars
    # Need extra warm-up for SMA(50) in acc to stabilise from different starting points.
    # 110 verified identical to 150 across 31 tickers (vs 100 which had 5 edge-case diffs).
    MAX_LOOKBACK = 110
    
    for tf, df in timeframe_data.items():
        tf_result = {"mom_color": "", "mom_rising": None, "sqz_state": "", "band_pos": "", "band_flip": False, "acc_state": "", "acc_impulse": "", "dev_signal": "", "bar_status": "confirmed"}
        
        if df.empty or len(df) < 2:
            results[tf] = tf_result
            continue
        
        # Bar status needs the full df for timestamp comparison
        tf_result["bar_status"] = "confirmed" if _is_bar_confirmed(tf, df) else "live"
        
        # Trim to last MAX_LOOKBACK bars for indicator computation
        # This is the key optimisation: 1256 daily bars -> 150 bars = ~8x less work
        if len(df) > MAX_LOOKBACK:
            df = df.iloc[-MAX_LOOKBACK:].reset_index(drop=True)
        
        # Each indicator calculated independently — show what we can
        # Momentum needs ~40 bars (20 for inner calcs + 20 for linreg)
        mom_data = None
        if len(df) >= 40:
            try:
                mom_data = calc_momentum(df)
                if not mom_data.empty and not pd.isna(mom_data["mom"].iloc[-1]):
                    tf_result["mom_color"] = mom_data["mom_color"].iloc[-1]
                    tf_result["mom_rising"] = bool(mom_data["mom_rising"].iloc[-1])
            except Exception as e:
                pass
        
        # Squeeze needs ~21 bars
        if len(df) >= 21:
            try:
                sqz = calc_squeeze(df)
                if not sqz.empty:
                    tf_result["sqz_state"] = sqz["sqz_state"].iloc[-1]
            except Exception as e:
                pass
        
        # Band needs ~21 bars
        if len(df) >= 21:
            try:
                band = calc_band(df)
                if not band.empty:
                    tf_result["band_pos"] = band["band_pos"].iloc[-1]
                    tf_result["band_flip"] = bool(band["band_flip"].iloc[-1])
            except Exception as e:
                pass
        
        # Acceleration needs ~90 bars (momentum + slope + avg)
        if len(df) >= 90:
            try:
                acc = calc_acceleration(df, precomputed_mom=mom_data)
                if not acc.empty:
                    tf_result["acc_state"] = acc["acc_state"].iloc[-1]
                    tf_result["acc_impulse"] = acc["acc_impulse"].iloc[-1]
            except Exception as e:
                pass
        
        # Deviation needs ~21 bars
        if len(df) >= 21:
            try:
                dev = calc_deviation_signal(df)
                if not dev.empty:
                    tf_result["dev_signal"] = dev["dev_signal"].iloc[-1]
            except Exception as e:
                pass
        
        results[tf] = tf_result
    
    current_price = None
    price_change_pct = None
    if "1D" in timeframe_data and not timeframe_data["1D"].empty:
        daily = timeframe_data["1D"]
        current_price = daily["close"].iloc[-1]
        if len(daily) >= 2:
            prev_close = daily["close"].iloc[-2]
            if prev_close != 0:
                price_change_pct = round(((current_price - prev_close) / prev_close) * 100, 2)
    
    wma30_data = calc_30wma(weekly_data, current_price) if current_price else {"wma30_value": None, "above_wma30": None, "wma30_cross": ""}

    # HVC — daily timeframe only, uses full (untrimmed) daily data for gap analysis accuracy
    hvc_meta = {"hvc_triggered": False, "hvc_volume_ratio": None, "hvc_candle_dir": "",
                "hvc_volume": None, "hvc_avg_volume": None,
                "hvc_gap_type": "", "hvc_gap_level": None, "hvc_gap_closed": False}
    if "1D" in timeframe_data and not timeframe_data["1D"].empty and len(timeframe_data["1D"]) >= 21:
        try:
            hvc = calc_hvc(timeframe_data["1D"])
            if not hvc.empty and hvc["hvc_triggered"].iloc[-1]:
                daily = timeframe_data["1D"]
                vol_sma_val = sma(daily["volume"].astype(float), 20)
                hvc_meta["hvc_triggered"] = True
                hvc_meta["hvc_volume_ratio"] = round(float(hvc["hvc_volume_ratio"].iloc[-1]), 2)
                hvc_meta["hvc_candle_dir"] = hvc["hvc_candle_dir"].iloc[-1]
                hvc_meta["hvc_volume"] = int(daily["volume"].iloc[-1])
                hvc_meta["hvc_avg_volume"] = int(vol_sma_val.iloc[-1]) if not pd.isna(vol_sma_val.iloc[-1]) else None
                hvc_meta["hvc_gap_type"] = hvc["hvc_gap_type"].iloc[-1]
                hvc_meta["hvc_gap_level"] = round(float(hvc["hvc_gap_level"].iloc[-1]), 2) if not pd.isna(hvc["hvc_gap_level"].iloc[-1]) else None
                hvc_meta["hvc_gap_closed"] = bool(hvc["hvc_gap_closed"].iloc[-1])
        except Exception:
            pass

    results["_meta"] = {"price": round(current_price, 2) if current_price else None, "price_change_pct": price_change_pct, **wma30_data, **hvc_meta}
    return results
