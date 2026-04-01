"""
Multi-Timeframe Analysis
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)


def resample_tf(df, timeframe):
    """Resample to different timeframe"""
    try:
        if df.empty:
            return pd.DataFrame()

        df = df.copy()
        
        if 'time' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['time']):
            df['time'] = pd.to_datetime(df['time'], unit='s')
        elif 'time' not in df.columns:
            logger.warning("No 'time' column")
            return pd.DataFrame()
            
        df = df.set_index('time')

        ohlc = df.groupby('Stock').resample(timeframe).agg({
            'Open':'first',
            'High':'max',
            'Low':'min',
            'Close':'last',
            'Volume':'sum'
        }).dropna().reset_index()

        logger.debug(f"Resampled to {timeframe}: {len(ohlc)} candles")
        return ohlc
    except Exception as e:
        logger.error(f"Error resampling: {e}")
        return pd.DataFrame()


def get_trend(df):
    """Calculate trend"""
    try:
        if len(df) < 2:
            return 0

        lookback = min(10, len(df) - 1)
        
        past_price = df['Close'].iloc[-lookback - 1]
        current_price = df['Close'].iloc[-1]
        
        if past_price == 0 or past_price < 0:
            return 0
            
        trend = (current_price - past_price) / past_price
        logger.debug(f"Trend ({lookback} periods): {trend:.4f}")
        return trend
    except Exception as e:
        logger.error(f"Error calculating trend: {e}")
        return 0


def get_trend_smooth(df, lookback=3):
    """Get smoothed trend"""
    try:
        if len(df) < lookback + 1:
            return 0
        
        recent_trends = []
        for i in range(lookback):
            idx = -(i+2)
            if len(df) + idx < 0:
                continue
            
            past_price = df['Close'].iloc[idx]
            current_price = df['Close'].iloc[-(i+1)]
            
            if past_price == 0 or past_price < 0:
                continue
                
            trend = (current_price - past_price) / past_price
            recent_trends.append(trend)
        
        if not recent_trends:
            return 0
            
        smoothed = sum(recent_trends) / len(recent_trends)
        logger.debug(f"Smoothed trend: {smoothed:.4f}")
        return smoothed
    except Exception as e:
        logger.error(f"Error calculating smoothed trend: {e}")
        return 0