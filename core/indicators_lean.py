# core/indicators_lean.py

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

# ================================
# BASIC INDICATOR CALCULATIONS
# ================================

def calc_rsi(series, period=7):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
    rs = gain / (loss + 1e-10)
    return 100 - (100 / (1 + rs))


def calc_macd(series, fast=12, slow=26):
    ema_fast = series.ewm(span=fast).mean()
    ema_slow = series.ewm(span=slow).mean()
    return ema_fast - ema_slow


def calc_macd_signal(macd, signal=9):
    return macd.ewm(span=signal).mean()


def calculate_atr(df, period=14):
    high_low = df['High'] - df['Low']
    high_close = np.abs(df['High'] - df['Close'].shift())
    low_close = np.abs(df['Low'] - df['Close'].shift())

    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df['ATR'] = tr.rolling(period).mean()
    return df


def calc_adx(df, period=14):
    plus_dm = df['High'].diff()
    minus_dm = df['Low'].diff() * -1

    plus_dm = np.where((plus_dm > minus_dm) & (plus_dm > 0), plus_dm, 0)
    minus_dm = np.where((minus_dm > plus_dm) & (minus_dm > 0), minus_dm, 0)

    tr = calculate_atr(df.copy(), period)['ATR']

    plus_di = 100 * (pd.Series(plus_dm).rolling(period).sum() / (tr + 1e-10))
    minus_di = 100 * (pd.Series(minus_dm).rolling(period).sum() / (tr + 1e-10))

    dx = (abs(plus_di - minus_di) / (plus_di + minus_di + 1e-10)) * 100
    return dx.rolling(period).mean()


# ================================
# CANDLE STRUCTURE
# ================================

def calc_body_ratio(df):
    body = abs(df['Close'] - df['Open'])
    range_ = df['High'] - df['Low'] + 1e-10
    return body / range_


def calc_hl_ratio(df):
    return (df['High'] - df['Low']) / (df['Close'] + 1e-10)


def calc_close_position(df):
    return (df['Close'] - df['Low']) / ((df['High'] - df['Low']) + 1e-10)


# ================================
# MAIN LEAN INDICATOR FUNCTION
# ================================

def add_indicators_lean(df):
    """
    Lean Indicator Set (11 indicators)
    Optimized for speed, clarity, and real-time trading
    """
    try:
        if df.empty or len(df) < 50:
            logger.warning("Not enough data for indicators")
            return df

        # ========= TREND =========
        df['EMA20'] = df['Close'].ewm(span=20).mean()
        df['EMA50'] = df['Close'].ewm(span=50).mean()

        # ========= MOMENTUM =========
        df['RSI_Fast'] = calc_rsi(df['Close'], period=7)

        df['MACD'] = calc_macd(df['Close'])
        df['MACD_Signal'] = calc_macd_signal(df['MACD'])

        # ========= VOLUME =========
        df['Vol_MA'] = df['Volume'].rolling(20).mean()
        df['Vol_Ratio'] = df['Volume'] / (df['Vol_MA'] + 1e-10)

        # ========= VOLATILITY =========
        df = calculate_atr(df, period=14)

        # ========= TREND STRENGTH =========
        df['ADX'] = calc_adx(df, period=14)

        # ========= CANDLE STRUCTURE =========
        df['Body_Ratio'] = calc_body_ratio(df)
        df['Close_Pos'] = calc_close_position(df)

        # ========= MARKET STRUCTURE =========
        df['HH'] = df['High'] > df['High'].shift(1)
        df['HL'] = df['Low'] > df['Low'].shift(1)

        # ========= BREAKOUT =========
        df['Recent_High'] = df['High'].rolling(20).max()
        df['Breakout'] = df['Close'] > df['Recent_High'].shift(1)

        logger.debug(f"Lean indicators added: {len(df)} rows")
        return df

    except Exception as e:
        logger.error(f"Indicator error: {e}")
        return df