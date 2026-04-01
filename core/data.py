"""
Official NEPSE Data Fetching
Use for historical/batch data
ADJUSTED: Better error handling and fallback

FIX M-9: preprocess() now validates OHLCV ordering:
         High >= Close >= Low  and  High >= Open >= Low
"""

import pandas as pd
import requests
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

OFFICIAL_NEPSE_URL = "https://www.nepalstock.com/api/nots/nepse-data"


def fetch_nepse_data(timeout=10):
    """
    Fetch latest data from official NEPSE API

    Args:
        timeout: Request timeout in seconds

    Returns:
        DataFrame with OHLCV data or empty DataFrame on error
    """

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate"
    }

    try:
        logger.debug(f"Fetching from {OFFICIAL_NEPSE_URL}")

        r = requests.get(OFFICIAL_NEPSE_URL, headers=headers, timeout=timeout, verify=certifi.where())
        r.raise_for_status()

        data = r.json()

        if not data:
            logger.warning("Empty response from NEPSE API")
            return pd.DataFrame()

        df = pd.DataFrame(data)

        if df.empty:
            logger.warning("No data in NEPSE response")
            return pd.DataFrame()

        # Rename columns
        df.rename(columns={
            "symbol": "Stock",
            "openPrice": "Open",
            "highPrice": "High",
            "lowPrice": "Low",
            "closePrice": "Close",
            "totalTradedQuantity": "Volume"
        }, inplace=True)

        # Select required columns
        required_cols = ['Stock', 'Open', 'High', 'Low', 'Close', 'Volume']
        available_cols = [c for c in required_cols if c in df.columns]

        if len(available_cols) < len(required_cols):
            logger.warning(f"Missing columns. Found: {available_cols}")
            return pd.DataFrame()

        df = df[required_cols]

        # Validate data
        df = preprocess(df)

        logger.info(f"NEPSE Official API: {len(df)} stocks fetched")
        return df

    except requests.exceptions.Timeout:
        logger.error(f"Timeout fetching from NEPSE API ({timeout}s)")
        return pd.DataFrame()
    except requests.exceptions.ConnectionError:
        logger.error("Connection error to NEPSE API")
        return pd.DataFrame()
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error from NEPSE API: {e}")
        return pd.DataFrame()
    except ValueError as e:
        logger.error(f"JSON decode error from NEPSE API: {e}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error fetching from NEPSE API: {e}")
        return pd.DataFrame()


def preprocess(df):
    """
    Preprocess data for analysis.

    Removes invalid rows and ensures data quality, including OHLCV
    ordering constraints added in FIX M-9.
    """
    try:
        if df.empty:
            return df

        original_len = len(df)

        # ── Remove NaN values ──
        df = df.dropna()
        logger.debug(f"Dropped NaN values: {original_len - len(df)} rows")

        # ── Remove invalid prices ──
        df = df[df['Close'] > 0]
        df = df[df['Open'] > 0]
        df = df[df['High'] > 0]
        df = df[df['Low'] > 0]

        # ── Remove invalid volumes ──
        df = df[df['Volume'] > 0]

        # ── Ensure High >= Low >= 0 ──
        df = df[df['High'] >= df['Low']]
        df = df[df['Low'] >= 0]

        # ── FIX M-9: Validate OHLCV ordering ──
        # High must be >= both Close and Open
        df = df[df['High'] >= df['Close']]
        df = df[df['High'] >= df['Open']]
        # Low must be <= both Close and Open
        df = df[df['Low'] <= df['Close']]
        df = df[df['Low'] <= df['Open']]

        # ── Stock symbol validation ──
        df = df[df['Stock'].notna()]
        df['Stock'] = df['Stock'].astype(str).str.upper().str.strip()
        df = df[df['Stock'].str.len() > 0]

        # ── Remove duplicates ──
        df = df.drop_duplicates(subset=['Stock'])

        removed = original_len - len(df)
        if removed > 0:
            logger.debug(f"Removed invalid rows: {removed}")

        logger.info(f"Preprocessed data: {len(df)} valid stocks")
        return df
    except Exception as e:
        logger.error(f"Preprocessing error: {e}")
        return pd.DataFrame()


def validate_data(df):
    """
    Validate data integrity.

    Returns: True if valid, False otherwise
    """
    try:
        if df.empty:
            return False

        # Check for required columns
        required = ['Stock', 'Open', 'High', 'Low', 'Close', 'Volume']
        if not all(c in df.columns for c in required):
            return False

        # Check for data quality
        if (df[required[1:]] <= 0).any().any():
            return False

        # Check High >= Low
        if (df['High'] < df['Low']).any():
            return False

        # FIX M-9: Check OHLCV ordering
        if (df['High'] < df['Close']).any():
            return False
        if (df['High'] < df['Open']).any():
            return False
        if (df['Low'] > df['Close']).any():
            return False
        if (df['Low'] > df['Open']).any():
            return False

        return True
    except Exception as e:
        logger.error(f"Data validation error: {e}")
        return False


def get_stock_history(stock_symbol, days=100):
    """
    Get historical data for a specific stock.

    Note: NEPSE official API doesn't provide direct history.
    This function is a placeholder for future implementation.
    """
    try:
        logger.info(f"Getting history for {stock_symbol} ({days} days)")
        # This would require integration with a historical data API.
        # For now, return empty (use live data only).
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"History fetch error: {e}")
        return pd.DataFrame()