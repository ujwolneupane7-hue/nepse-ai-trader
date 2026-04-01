"""
Final Optimized Strategy
Ultra-strict Grade C filtering

FINAL VERSION:
- [OK] Slippage simulation for entry, stop, and target
- [OK] Safe data access
- [OK] Type checking
"""

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

# Slippage settings
ENTRY_SLIPPAGE = 0.002  # 0.2%
EXIT_SLIPPAGE = 0.003   # 0.3%


def safe_get_float(row, key, default=None):
    """Safely get float value from row"""
    try:
        val = row.get(key) if isinstance(row, pd.Series) else row[key]
        if pd.isna(val):
            return default
        return float(val)
    except (TypeError, ValueError, KeyError):
        return default


def safe_compare(val1, val2, operator="gt"):
    """Safely compare values"""
    try:
        if pd.isna(val1) or pd.isna(val2):
            return False
        
        v1 = float(val1)
        v2 = float(val2)
        
        if operator == "gt":
            return v1 > v2
        elif operator == "gte":
            return v1 >= v2
        elif operator == "lt":
            return v1 < v2
        elif operator == "lte":
            return v1 <= v2
        else:
            return False
    except (TypeError, ValueError):
        return False


def compute_score_lean(row):
    """
    Compute signal score and confluence.
    IMPROVED: Explicit NaN checking for clarity and safety
    """
    try:
        score = 0
        confluence = 0

        # ================================
        # 1. TREND (Primary Direction)
        # ================================
        ema20 = row.get('EMA20')
        ema50 = row.get('EMA50')

        # EXPLICIT NaN CHECK for clarity
        if pd.notna(ema20) and pd.notna(ema50) and ema20 > ema50:
            score += 0.25
            confluence += 1

        # ================================
        # 2. MOMENTUM (Early Confirmation)
        # ================================
        rsi = row.get('RSI_Fast')
        if pd.notna(rsi) and rsi > 58:  # EXPLICIT NaN check
            score += 0.20
            confluence += 1

        macd = row.get('MACD')
        macd_signal = row.get('MACD_Signal')
        if pd.notna(macd) and pd.notna(macd_signal) and macd > macd_signal:  # EXPLICIT checks
            score += 0.18
            confluence += 1

        # ================================
        # 3. VOLUME (Participation)
        # ================================
        vol_ratio = row.get('Vol_Ratio')
        if pd.notna(vol_ratio):  # EXPLICIT NaN check
            if vol_ratio > 1.5:
                score += 0.18
                confluence += 1
            elif vol_ratio > 1.2:
                score += 0.10
                confluence += 0.5

        # ================================
        # 4. VOLATILITY (Quality Filter)
        # IMPROVED: Explicit bounds checking
        # ================================
        atr = row.get('ATR')
        close = row.get('Close')

        if pd.notna(atr) and pd.notna(close) and close > 0.001:  # EXPLICIT checks + bounds
            volatility = atr / close
            if 0.008 < volatility < 0.05:
                score += 0.08
                confluence += 0.5

        # ================================
        # 5. TREND STRENGTH (Confirmation)
        # ================================
        adx = row.get('ADX')
        if pd.notna(adx) and adx > 18:  # EXPLICIT NaN check
            score += 0.08
            confluence += 0.5

        # ================================
        # 6. CANDLE STRUCTURE (Entry Quality)
        # ================================
        body = row.get('Body_Ratio')
        close_pos = row.get('Close_Pos')

        if pd.notna(body) and pd.notna(close_pos) and body > 0.4 and close_pos > 0.6:  # EXPLICIT checks
            score += 0.05
            confluence += 0.5

        # ================================
        # 7. MARKET STRUCTURE (NEW)
        # ================================
        hh = row.get('HH', False)
        hl = row.get('HL', False)
        
        if hh and hl:
            score += 0.10
            confluence += 1

        # ================================
        # 8. BREAKOUT (NEW - NEPSE EDGE)
        # ================================
        breakout = row.get('Breakout', False)
        if breakout and pd.notna(vol_ratio) and vol_ratio > 1.5:  # Added NaN check
            score += 0.15
            confluence += 1

        # ================================
        # 9. CONFLUENCE BONUS
        # ================================
        if confluence >= 4:
            score += 0.05

        return round(min(score, 1.0), 2), confluence

    except Exception as e:
        logger.error(f"Error in compute_score_lean: {e}")
        return 0, 0


def get_grade_final_optimized(score, rr, confluence):
    """
    Stricter grading system
    FIXED: Proper type checking
    """
    try:
        # Validate inputs
        try:
            score = float(score)
            rr = float(rr)
            confluence = float(confluence)
        except (TypeError, ValueError):
            return "F"
        
        # REJECT
        if score < 0.48 or rr < 1.5 or confluence < 1.8:
            return "F"
        
        # GRADE D
        if score < 0.58 or rr < 1.55 or confluence < 2.0:
            return "D"
        
        # GRADE C
        if score < 0.68 or rr < 1.75 or confluence < 2.3:
            return "C"
        
        # GRADE B
        if score < 0.78 or rr < 2.05 or confluence < 2.8:
            return "B"
        
        # GRADE A
        return "A"
    
    except Exception as e:
        logger.error(f"Error getting grade: {e}")
        return "F"


def apply_slippage(price, slippage_pct, direction="buy"):
    """Apply slippage to price"""
    try:
        price = float(price)
        slippage = price * slippage_pct
        
        if direction == "buy":
            return price + slippage  # Worse price for buy
        else:
            return price - slippage  # Worse price for sell
    except Exception as e:
        logger.error(f"Error applying slippage: {e}")
        return price


def get_trade_levels_final_optimized(row):
    """
    Conservative stops + structure-based targets
    FIXED: Uses ONLY available indicators (ATR, not ATR_Fast)
    FIXED: Uses fixed RR ratios (not Volatility-based)
    """
    try:
        close = safe_get_float(row, 'Close')
        if not close:
            return 0, 0, 0, 0
        
        entry = close
        
        # [OK] USE ATR (14-period) - Available in indicators_lean.py
        atr = safe_get_float(row, 'ATR')
        if not atr:
            atr = close * 0.01  # Fallback: 1% of close
        
        # Wider stops for stability
        stop_dist = max(1.4 * atr, entry * 0.013)
        stop = entry - stop_dist
        
        risk = entry - stop
        if risk <= 0:
            return 0, 0, 0, 0

        # [OK] USE FIXED RR RATIOS (Volatility not available)
        rr_target = 1.95  # Fixed ratio for consistency

        target = entry + (risk * rr_target)
        rr = (target - entry) / risk if risk != 0 else 0

        logger.debug(f"Optimized Levels - Entry: {entry:.2f} | Stop: {stop:.2f} | Target: {target:.2f} | RR: {rr:.2f}")
        return entry, stop, target, rr

    except Exception as e:
        logger.error(f"Error calculating optimized levels: {e}")
        return 0, 0, 0, 0


def get_trade_levels_final_optimized_with_slippage(row, entry_slip=0.002, exit_slip=0.003):
    """
    Calculate trade levels WITH SLIPPAGE SIMULATION
    FIXED: Uses ONLY available indicators (ATR, not ATR_Fast)
    FIXED: Uses fixed RR ratios (not Volatility-based)
    """
    try:
        close = safe_get_float(row, 'Close')
        if not close:
            return 0, 0, 0, 0
        
        entry = close
        
        # [OK] USE ATR (14-period) - Available in indicators_lean.py
        atr = safe_get_float(row, 'ATR')
        if not atr:
            atr = close * 0.01  # Fallback: 1% of close
        
        # Apply entry slippage (worse price for buy)
        actual_entry = apply_slippage(entry, entry_slip, "buy")
        
        # Wider stops for stability
        stop_dist = max(1.4 * atr, entry * 0.013)
        stop = entry - stop_dist
        
        # Apply slippage to stop loss (worse for trader - further away)
        actual_stop = apply_slippage(stop, exit_slip, "sell")
        
        risk = actual_entry - actual_stop
        if risk <= 0:
            return 0, 0, 0, 0

        # [OK] USE FIXED RR RATIOS (Volatility not available)
        # Conservative fixed targets based on market conditions
        rr_target = 1.95  # Fixed ratio for consistency

        raw_target = actual_entry + (risk * rr_target)
        
        # Apply exit slippage to target (worse price for sell)
        target = apply_slippage(raw_target, exit_slip, "sell")
        
        actual_rr = (target - actual_entry) / risk if risk != 0 else 0

        logger.debug(f"Levels (with slippage) - Entry: {actual_entry:.2f} | Stop: {actual_stop:.2f} | Target: {target:.2f} | RR: {actual_rr:.2f}")
        return actual_entry, actual_stop, target, actual_rr

    except Exception as e:
        logger.error(f"Error calculating levels with slippage: {e}")
        return 0, 0, 0, 0