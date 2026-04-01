"""
Market Regime Detection
"""

import logging

logger = logging.getLogger(__name__)


def detect_regime(df):
    """Detect market regime"""
    try:
        if len(df) < 30:
            return "UNKNOWN"

        recent = df.tail(20)

        close_mean = recent['Close'].mean()
        if close_mean == 0 or close_mean < 0:
            return "UNKNOWN"
            
        vol = (recent['High'] - recent['Low']).mean() / close_mean

        first_close = recent['Close'].iloc[0]
        last_close = recent['Close'].iloc[-1]
        
        if first_close == 0 or first_close < 0:
            trend = 0
        else:
            trend = (last_close - first_close) / first_close

        if vol < 0.004:
            regime = "LOW_VOL"
        elif abs(trend) > 0.03:
            regime = "TRENDING"
        else:
            regime = "RANGING"

        logger.debug(f"Regime: {regime} | Vol: {vol:.4f} | Trend: {trend:.4f}")
        return regime
    except Exception as e:
        logger.error(f"Error detecting regime: {e}")
        return "UNKNOWN"