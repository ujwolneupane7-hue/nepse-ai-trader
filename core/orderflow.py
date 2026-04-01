"""
Order Flow Analysis
Measures bullish/bearish pressure based on candle structure
ADJUSTED: Better integration and robust validation
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)


def orderflow_score(row):
    """
    Calculate orderflow intensity
    
    Positive = bullish pressure | Negative = bearish pressure
    Range: -1 to +1
    """
    try:
        if pd.isna(row.get('Close')) or pd.isna(row.get('Open')):
            return 0
        
        body = abs(row['Close'] - row['Open'])
        
        if pd.isna(row.get('High')) or pd.isna(row.get('Low')):
            return 0
        
        range_ = row['High'] - row['Low']

        if range_ == 0 or range_ < 0:
            return 0

        dominance = body / range_

        # ── BULLISH PRESSURE (close > open) ──
        if row['Close'] > row['Open']:
            return min(dominance, 1.0)

        # ── BEARISH PRESSURE (close < open) ──
        if row['Close'] < row['Open']:
            return max(-dominance, -1.0)
        
        # ── DOJI (close == open) ──
        return 0
    except Exception as e:
        logger.error(f"Error calculating orderflow score: {e}")
        return 0


def normalize_orderflow_score(score):
    """
    Convert orderflow score (-1 to +1) to normalized score (0 to 1)
    Used for filtering and comparison
    
    Returns: 0.0-1.0
    """
    try:
        if pd.isna(score):
            return 0.5
        
        # Convert -1 to +1 range into 0 to 1 range
        normalized = (score + 1) / 2
        return max(0.0, min(1.0, normalized))
    except Exception as e:
        logger.error(f"Error normalizing orderflow: {e}")
        return 0.5


def get_orderflow_strength(df, period=5):
    """
    Get recent orderflow strength (average of last N candles)
    
    Returns: -1.0 to +1.0 (negative = bearish, positive = bullish)
    """
    try:
        if df.empty or len(df) < period:
            return 0
        
        recent = df.tail(period)
        scores = []
        
        for _, row in recent.iterrows():
            score = orderflow_score(row)
            if not pd.isna(score):
                scores.append(score)
        
        if not scores:
            return 0
        
        avg_score = sum(scores) / len(scores)
        return max(-1.0, min(1.0, avg_score))
    except Exception as e:
        logger.error(f"Error getting orderflow strength: {e}")
        return 0


def validate_orderflow(row, required_direction="bullish", min_strength=0.2):
    """
    Validate orderflow matches direction
    
    required_direction: "bullish" or "bearish"
    min_strength: Minimum absolute orderflow strength required (0-1)
    
    Returns: True if orderflow matches requirement
    """
    try:
        if pd.isna(row.get('Close')) or pd.isna(row.get('Open')):
            return False
        
        score = orderflow_score(row)
        
        if required_direction == "bullish":
            return score > min_strength
        elif required_direction == "bearish":
            return score < -min_strength
        else:
            return False
    except Exception as e:
        logger.error(f"Error validating orderflow: {e}")
        return False


def get_orderflow_confirmation(df, direction="bullish", periods=3):
    """
    Check if multiple candles confirm orderflow direction
    
    direction: "bullish" or "bearish"
    periods: Number of recent candles to check
    
    Returns: Percentage of candles with matching orderflow (0.0-1.0)
    """
    try:
        if df.empty or len(df) < periods:
            return 0.0
        
        recent = df.tail(periods)
        matching = 0
        
        for _, row in recent.iterrows():
            if validate_orderflow(row, direction, min_strength=0.1):
                matching += 1
        
        confirmation_rate = matching / periods
        logger.debug(f"Orderflow {direction} confirmation: {confirmation_rate:.1%}")
        return confirmation_rate
    except Exception as e:
        logger.error(f"Error getting orderflow confirmation: {e}")
        return 0.0