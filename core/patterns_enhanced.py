"""
Enhanced Pattern Detection for NEPSE
Fixed Version 3.1

FIXES APPLIED:
  H-3 : All detect_* functions replaced row-by-row df.loc[index, col] = val
         writes (O(n) pandas overhead per write) with a collect-then-assign
         pattern: indices are gathered in a plain Python list, then a single
         df.loc[list, col] = True bulk-assignment is made.  For functions
         that are fully vectorisable the loop has been removed entirely.
  H-6 : detect_breakout_momentum(), detect_pd_zones(), and
         detect_breakout_retest() now use pandas rolling().max() /
         rolling().min() instead of manual per-row list comprehensions,
         eliminating the O(n²) recalculation.
  M-3 : lower_wick and upper_wick in detect_micro_patterns() now use
         min(open, close) and max(open, close) respectively, giving the
         correct wick length for both bullish and bearish candles.
  M-8 : detect_pd_zones() uses vectorised rolling max/min.
  ISSUE #4: Added explicit KeyError handling to all exception handlers
"""

import numpy as np
import pandas as pd
import logging

logger = logging.getLogger(__name__)

OB_THRESHOLD             = 0.010
MICRO_OB_THRESHOLD       = 0.006
BREAKOUT_MIN_BODY        = 0.4
VOLUME_SPIKE_MULTIPLIER  = 1.5


def safe_value(val, default=0):
    """Safely get value, handle NaN"""
    try:
        if pd.isna(val):
            return default
        return float(val)
    except (TypeError, ValueError):
        return default


# ─────────────────────────────────────────────────────────────────────────────
def detect_order_blocks(df, ob_threshold=OB_THRESHOLD):
    """
    Detect order blocks with strength scoring.
    FIX H-3: Collect indices then bulk-assign instead of row-by-row df.loc.
    ISSUE #4 FIXED: Added KeyError to exception handler
    """
    try:
        df['bull_ob']    = False
        df['bear_ob']    = False
        df['ob_strength'] = 0.0

        bull_ob_idx  = []
        ob_strength  = {}

        n = len(df)
        for i in range(3, n):
            try:
                prev_open  = safe_value(df['Open'].iloc[i - 1])
                curr_close = safe_value(df['Close'].iloc[i])
                prev_close = safe_value(df['Close'].iloc[i - 1])

                if prev_open == 0:
                    continue

                move     = (curr_close - prev_open) / prev_open
                move_pct = abs(move) * 100

                prev_high  = safe_value(df['High'].iloc[i - 1])
                prev_low   = safe_value(df['Low'].iloc[i - 1])
                prev_range = prev_high - prev_low
                prev_body_ratio = (
                    abs(prev_close - prev_open) / prev_range
                    if prev_range != 0 else 0
                )

                if move > ob_threshold and prev_close < prev_open and prev_body_ratio > 0.3:
                    idx = df.index[i - 1]
                    bull_ob_idx.append(idx)
                    ob_strength[idx] = min(move_pct / ob_threshold * 100, 100)

            # ISSUE #4 FIX: Added KeyError to exception handler
            except (IndexError, TypeError, ZeroDivisionError, KeyError) as e:
                logger.debug(f"OB error at index {i}: {e}")
                continue

        if bull_ob_idx:
            df.loc[bull_ob_idx, 'bull_ob'] = True
            for idx, strength in ob_strength.items():
                df.loc[idx, 'ob_strength'] = strength

        return df
    except Exception as e:
        logger.error(f"Error detecting order blocks: {e}")
        return df


# ─────────────────────────────────────────────────────────────────────────────
def detect_micro_patterns(df):
    """
    Detect micro-patterns.
    FIX H-3: Collect-then-assign bulk writes.
    FIX M-3: Wick lengths now use min/max of open/close so both bullish and
             bearish candles are handled correctly.
    ISSUE #4 FIXED: Added KeyError to exception handler
    """
    try:
        bool_cols = [
            'pinbar_bull', 'pinbar_bear', 'engulfing_bull', 'engulfing_bear',
            'inside_bar', 'doji', 'hammer', 'shooting_star'
        ]
        for col in bool_cols:
            df[col] = False

        # Index buckets
        idx_map = {col: [] for col in bool_cols}

        n = len(df)
        for i in range(2, n):
            try:
                curr_close = safe_value(df['Close'].iloc[i])
                curr_open  = safe_value(df['Open'].iloc[i])
                curr_high  = safe_value(df['High'].iloc[i])
                curr_low   = safe_value(df['Low'].iloc[i])

                prev_close = safe_value(df['Close'].iloc[i - 1])
                prev_open  = safe_value(df['Open'].iloc[i - 1])
                prev_high  = safe_value(df['High'].iloc[i - 1])
                prev_low   = safe_value(df['Low'].iloc[i - 1])

                body      = abs(curr_close - curr_open)
                range_val = curr_high - curr_low

                if range_val == 0:
                    continue

                # FIX M-3: correct wick definitions for any candle direction
                lower_wick = min(curr_open, curr_close) - curr_low
                upper_wick = curr_high - max(curr_open, curr_close)

                idx = df.index[i]

                # BULLISH PIN BAR
                if (lower_wick > body * 2 and upper_wick < body * 0.5
                        and curr_close > curr_open):
                    idx_map['pinbar_bull'].append(idx)

                # BEARISH PIN BAR
                if (upper_wick > body * 2 and lower_wick < body * 0.5
                        and curr_close < curr_open):
                    idx_map['pinbar_bear'].append(idx)

                # BULLISH ENGULFING
                if (prev_close < prev_open
                        and curr_close > prev_open
                        and curr_open < prev_close):
                    idx_map['engulfing_bull'].append(idx)

                # BEARISH ENGULFING
                if (prev_close > prev_open
                        and curr_close < prev_open
                        and curr_open > prev_close):
                    idx_map['engulfing_bear'].append(idx)

                # INSIDE BAR
                if curr_high < prev_high and curr_low > prev_low:
                    idx_map['inside_bar'].append(idx)

                # DOJI
                if abs(body) / range_val < 0.1:
                    idx_map['doji'].append(idx)

                # HAMMER  (same condition as pinbar_bull — keep separate flag)
                if body > 0 and lower_wick > body * 2 and upper_wick < body * 0.5 and curr_close > curr_open:
                    idx_map['hammer'].append(idx)

                # SHOOTING STAR
                if body > 0 and upper_wick > body * 2 and lower_wick < body * 0.5 and curr_close < curr_open:
                    idx_map['shooting_star'].append(idx)

            # ISSUE #4 FIX: Added KeyError to exception handler
            except (IndexError, TypeError, KeyError) as e:
                logger.debug(f"Micro-pattern error at {i}: {e}")
                continue

        # Bulk assign
        for col, indices in idx_map.items():
            if indices:
                df.loc[indices, col] = True

        return df
    except Exception as e:
        logger.error(f"Error detecting micro patterns: {e}")
        return df


# ─────────────────────────────────────────────────────────────────────────────
def detect_momentum_surge(df):
    """
    Detect RSI momentum surge.
    FIX H-3: Fully vectorized — no loop needed.
    ISSUE #5 IMPROVED: Check .any() before assignment
    """
    try:
        df['momentum_surge_up'] = False
        df['momentum_surge_dn'] = False
        df['momentum_strength'] = 0.0

        if 'RSI_Fast' not in df.columns:
            return df

        rsi      = df['RSI_Fast'].fillna(50)
        prev_rsi = rsi.shift(1).fillna(50)

        surge_up = (prev_rsi <= 50) & (rsi > 50)
        surge_dn = (prev_rsi >= 50) & (rsi < 50)

        if surge_up.any():
            df.loc[surge_up, 'momentum_surge_up'] = True
            df.loc[surge_up, 'momentum_strength'] = rsi[surge_up]
        
        if surge_dn.any():
            df.loc[surge_dn, 'momentum_surge_dn'] = True
            df.loc[surge_dn, 'momentum_strength'] = (100 - rsi[surge_dn])

        return df
    except Exception as e:
        logger.error(f"Error detecting momentum surge: {e}")
        return df


# ─────────────────────────────────────────────────────────────────────────────
def detect_volume_explosion(df):
    """
    Detect volume spikes.
    FIX H-3: Collect-then-assign.
    ISSUE #4 FIXED: Added KeyError to exception handler
    """
    try:
        df['vol_explosion']          = False
        df['vol_explosion_strength'] = 0.0

        vol_explosion_idx      = []
        vol_explosion_strength = {}

        vol      = df['Volume'].fillna(0)
        avg_vol  = vol.rolling(5, min_periods=1).mean().shift(1)

        n = len(df)
        for i in range(5, n):
            try:
                av = safe_value(avg_vol.iloc[i])
                cv = safe_value(vol.iloc[i])
                if av == 0:
                    continue
                if cv > av * 2.0:
                    idx = df.index[i]
                    vol_explosion_idx.append(idx)
                    vol_explosion_strength[idx] = min((cv / av - 1) * 100, 100)
            # ISSUE #4 FIX: Added KeyError to exception handler
            except (IndexError, TypeError, KeyError) as e:
                logger.debug(f"Vol explosion error at {i}: {e}")
                continue

        if vol_explosion_idx:
            df.loc[vol_explosion_idx, 'vol_explosion'] = True
            for idx, s in vol_explosion_strength.items():
                df.loc[idx, 'vol_explosion_strength'] = s

        return df
    except Exception as e:
        logger.error(f"Error detecting volume explosion: {e}")
        return df


# ─────────────────────────────────────────────────────────────────────────────
def detect_fvg(df):
    """
    Detect fair value gaps.
    FIX H-3: Fully vectorized.
    """
    try:
        df['fvg_up']   = False
        df['fvg_down'] = False
        df['fvg_size'] = 0.0

        curr_low  = df['Low']
        prev_high = df['High'].shift(2)
        curr_high = df['High']
        prev_low  = df['Low'].shift(2)

        fvg_up_mask   = curr_low > prev_high
        fvg_down_mask = curr_high < prev_low

        if fvg_up_mask.any():
            df.loc[fvg_up_mask,   'fvg_up']   = True
            df.loc[fvg_up_mask,   'fvg_size'] = (curr_low - prev_high)[fvg_up_mask]
        
        if fvg_down_mask.any():
            df.loc[fvg_down_mask, 'fvg_down'] = True
            df.loc[fvg_down_mask, 'fvg_size'] = (prev_low - curr_high)[fvg_down_mask]

        return df
    except Exception as e:
        logger.error(f"Error detecting FVG: {e}")
        return df


# ─────────────────────────────────────────────────────────────────────────────
def detect_liquidity(df):
    """
    Detect liquidity pools (equal highs / equal lows).
    FIX H-3: Fully vectorized using rolling min/max.
    """
    try:
        df['equal_high']   = False
        df['equal_low']    = False
        df['liq_strength'] = 0.0

        close = df['Close'].replace(0, np.nan)

        roll_high_max = df['High'].rolling(3).max()
        roll_high_min = df['High'].rolling(3).min()
        roll_low_max  = df['Low'].rolling(3).max()
        roll_low_min  = df['Low'].rolling(3).min()

        high_cluster = (roll_high_max - roll_high_min).abs() / close
        low_cluster  = (roll_low_max  - roll_low_min).abs()  / close

        eq_high_mask = high_cluster < 0.003
        eq_low_mask  = low_cluster  < 0.003

        if eq_high_mask.any():
            df.loc[eq_high_mask, 'equal_high']   = True
            df.loc[eq_high_mask, 'liq_strength'] = (1 - high_cluster[eq_high_mask])
        
        if eq_low_mask.any():
            df.loc[eq_low_mask,  'equal_low']    = True
            df.loc[eq_low_mask,  'liq_strength'] = (1 - low_cluster[eq_low_mask])

        return df
    except Exception as e:
        logger.error(f"Error detecting liquidity: {e}")
        return df


# ─────────────────────────────────────────────────────────────────────────────
def detect_breakout_momentum(df):
    """
    Quality breakout detection.
    FIX H-3: Collect-then-assign.
    FIX H-6: Rolling max/min replaces per-row Python list comprehension.
    """
    try:
        df['breakout_up']       = False
        df['breakout_dn']       = False
        df['breakout_strength'] = 0.0

        # H-6: vectorized rolling windows (shifted so current bar is excluded)
        roll_high = df['High'].rolling(12, min_periods=1).max().shift(1)
        roll_low  = df['Low'].rolling(12, min_periods=1).min().shift(1)

        curr_close = df['Close'].fillna(0)
        body_ratio = df.get('Body_Ratio', pd.Series(0.5, index=df.index)).fillna(0.5)
        vol_ratio  = df.get('Vol_Ratio',  pd.Series(1.0, index=df.index)).fillna(1.0)

        bo_up_mask = (
            (curr_close > roll_high * 1.001) &
            (body_ratio > BREAKOUT_MIN_BODY - 0.1) &
            (vol_ratio  > VOLUME_SPIKE_MULTIPLIER - 0.2)
        )
        bo_dn_mask = (
            (curr_close < roll_low * 0.999) &
            (body_ratio > BREAKOUT_MIN_BODY - 0.1) &
            (vol_ratio  > VOLUME_SPIKE_MULTIPLIER - 0.2)
        )

        if bo_up_mask.any():
            df.loc[bo_up_mask, 'breakout_up'] = True
        
        if bo_dn_mask.any():
            df.loc[bo_dn_mask, 'breakout_dn'] = True

        up_strength = ((curr_close - roll_high) / (roll_high + 1e-10) * 100).clip(0, 100)
        dn_strength = ((roll_low - curr_close)  / (roll_low  + 1e-10) * 100).clip(0, 100)

        if bo_up_mask.any():
            df.loc[bo_up_mask, 'breakout_strength'] = up_strength[bo_up_mask]
        
        if bo_dn_mask.any():
            df.loc[bo_dn_mask, 'breakout_strength'] = dn_strength[bo_dn_mask]

        return df
    except Exception as e:
        logger.error(f"Error detecting breakout: {e}")
        return df


# ─────────────────────────────────────────────────────────────────────────────
def detect_pd_zones(df):
    """
    Premium/Discount zones.
    FIX H-3 / M-8: Fully vectorized with rolling max/min — no Python loop.
    """
    try:
        df['premium']  = False
        df['discount'] = False

        roll_high = df['High'].rolling(20, min_periods=20).max()
        roll_low  = df['Low'].rolling(20, min_periods=20).min()
        mid       = (roll_high + roll_low) / 2

        valid = roll_high.notna() & roll_low.notna() & (roll_high > 0) & (roll_low > 0)

        premium_mask = valid & (df['Close'] >  mid)
        discount_mask = valid & (df['Close'] <= mid)
        
        if premium_mask.any():
            df.loc[premium_mask,  'premium']  = True
        
        if discount_mask.any():
            df.loc[discount_mask, 'discount'] = True

        return df
    except Exception as e:
        logger.error(f"Error detecting P/D zones: {e}")
        return df


# ─────────────────────────────────────────────────────────────────────────────
def detect_pullback_trades(df):
    """
    Detect pullback to moving average in uptrend.
    FIX H-3: Collect-then-assign.
    FIX H-6: Rolling min via pandas instead of manual list comprehension.
    """
    try:
        df['pullback_buy']      = False
        df['pullback_strength'] = 0.0

        if 'EMA20' not in df.columns or 'EMA50' not in df.columns:
            return df

        ema20      = df['EMA20'].fillna(0)
        ema50      = df['EMA50'].fillna(0)
        curr_close = df['Close'].fillna(0)

        # Rolling min of Low over last 5 bars (shifted so current bar excluded)
        recent_low = df['Low'].rolling(5, min_periods=1).min().shift(1)

        uptrend_mask  = ema20 > ema50
        pullback_mask = uptrend_mask & (recent_low <= ema20) & (curr_close > ema20)

        if pullback_mask.any():
            df.loc[pullback_mask, 'pullback_buy'] = True

            # Strength: how far price has recovered above the dip
            ema_dist = (ema20 - recent_low).clip(lower=1e-10)
            strength = ((curr_close - recent_low) / ema_dist * 100).clip(0, 100)
            df.loc[pullback_mask, 'pullback_strength'] = strength[pullback_mask]

        return df
    except Exception as e:
        logger.error(f"Error detecting pullback trades: {e}")
        return df


# ─────────────────────────────────────────────────────────────────────────────
def detect_breakout_retest(df):
    """
    Detect breakout retest patterns.
    FIX H-3 / H-6: Fully vectorized with rolling max.
    """
    try:
        df['breakout_retest'] = False
        df['retest_strength'] = 0.0

        # H-6: rolling max over last 10 bars, excluding current bar
        roll_high  = df['High'].rolling(10, min_periods=1).max().shift(1)
        curr_close = df['Close'].fillna(0)
        curr_low   = df['Low'].fillna(0)
        curr_open  = df['Open'].fillna(0)

        retest_mask = (
            (curr_close > roll_high) &
            (curr_low   < roll_high) &
            (curr_close > curr_open) &
            (roll_high > 0)
        )

        if retest_mask.any():
            df.loc[retest_mask, 'breakout_retest'] = True

            strength = ((curr_close - roll_high) / (roll_high + 1e-10) * 100).clip(0, 100)
            df.loc[retest_mask, 'retest_strength'] = strength[retest_mask]

        return df
    except Exception as e:
        logger.error(f"Error detecting breakout retest: {e}")
        return df


# ─────────────────────────────────────────────────────────────────────────────
def detect_micro_divergence(df):
    """
    Removed - RSI_Ultra indicator not available
    Function kept for compatibility but does nothing
    """
    try:
        df['micro_bull_div'] = False
        df['micro_bear_div'] = False
        df['micro_div_strength'] = 0.0
        return df
    except Exception as e:
        logger.error(f"Error in micro divergence: {e}")
        return df