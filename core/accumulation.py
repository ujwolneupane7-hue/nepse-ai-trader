"""
Accumulation/Distribution Pattern Detection
Identifies institutional buying/selling patterns
ADJUSTED: Better integration with final optimized system
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)


def detect_accumulation(df):
    """
    Detect accumulation/distribution patterns
    
    Accumulation = tight range + rising volume (institutional buying)
    Distribution = tight range + falling price (institutional selling)
    
    Returns: "ACCUMULATION", "DISTRIBUTION", or "NONE"
    """
    try:
        if df.empty or len(df) < 20:
            return "NONE"

        recent = df.tail(15)

        high = recent['High'].max()
        low = recent['Low'].min()

        # ── SAFETY CHECKS ──
        if low == 0 or low < 0 or pd.isna(low):
            return "NONE"
            
        range_pct = (high - low) / low

        avg_vol = recent['Volume'].mean()
        
        if avg_vol == 0 or pd.isna(avg_vol):
            return "NONE"

        # ── TIGHT RANGE + RISING VOLUME = ACCUMULATION ──
        current_vol = recent['Volume'].iloc[-1]
        
        if pd.isna(current_vol) or current_vol < 0:
            return "NONE"
        
        if range_pct < 0.03 and current_vol > avg_vol * 1.2:
            vol_ratio = current_vol / avg_vol
            logger.debug(f"Accumulation detected - Range: {range_pct:.4f} | Vol ratio: {vol_ratio:.2f}x")
            return "ACCUMULATION"

        # ── TIGHT RANGE + FALLING PRICE = DISTRIBUTION ──
        if range_pct < 0.03 and recent['Close'].iloc[-1] < recent['Open'].iloc[-1]:
            logger.debug(f"Distribution detected - Range: {range_pct:.4f}")
            return "DISTRIBUTION"

        return "NONE"
    except Exception as e:
        logger.error(f"Error detecting accumulation: {e}")
        return "NONE"


def get_accumulation_strength(df):
    """
    Get strength of accumulation pattern (0-1)
    Can be used as bonus in scoring
    """
    try:
        if df.empty or len(df) < 20:
            return 0.0
        
        pattern = detect_accumulation(df)
        
        if pattern == "NONE":
            return 0.0
        
        recent = df.tail(15)
        avg_vol = recent['Volume'].mean()
        
        if avg_vol == 0 or pd.isna(avg_vol):
            return 0.0
        
        current_vol = recent['Volume'].iloc[-1]
        
        if pd.isna(current_vol) or current_vol < 0:
            return 0.0
        
        # Normalize to 0-1
        vol_ratio = current_vol / avg_vol
        strength = min((vol_ratio - 1) / 2, 1.0)
        
        logger.debug(f"Accumulation strength: {strength:.2f}")
        return max(strength, 0.0)
    except Exception as e:
        logger.error(f"Error getting accumulation strength: {e}")
        return 0.0


def is_accumulation_favorable(df, direction="bullish"):
    """
    Check if accumulation pattern is favorable for trading
    
    direction: "bullish" or "bearish"
    Returns: True if favorable, False otherwise
    """
    try:
        pattern = detect_accumulation(df)
        
        if direction == "bullish":
            return pattern == "ACCUMULATION"
        elif direction == "bearish":
            return pattern == "DISTRIBUTION"
        else:
            return False
    except Exception as e:
        logger.error(f"Error checking accumulation favorability: {e}")
        return False