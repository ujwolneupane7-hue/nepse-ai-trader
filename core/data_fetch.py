"""
Data Fetching from NEPSE
FINAL VERSION: API response validation, data validation
"""

import pandas as pd
import requests
import logging
from io import StringIO
import certifi

logger = logging.getLogger(__name__)

NEPSEAPI_REST_URL = "https://nepseapi.surajrimal.dev"
MEROLAGANI_URL = "https://merolagani.com/LatestMarket.aspx"
SHARESANSAR_URL = "https://www.sharesansar.com/live-trading"


def validate_api_response(data):
    """Validate API response structure"""
    try:
        if not isinstance(data, dict):
            return None, "Response is not a dictionary"
        
        # Check for expected structure
        if "data" not in data:
            if isinstance(data, list):
                return data, None  # Direct list format
            else:
                return None, "Response missing 'data' field"
        
        items = data.get("data")
        
        if not isinstance(items, list):
            return None, f"'data' is not a list: {type(items)}"
        
        if len(items) == 0:
            return None, "Empty data list"
        
        # Validate first item has expected fields
        first = items[0]
        expected_fields = ["symbol", "ltp", "totalVolume"]
        missing = [f for f in expected_fields if f not in first and f.lower() not in str(first).lower()]
        
        if missing:
            return None, f"Missing fields in response: {missing}"
        
        return items, None
    except Exception as e:
        return None, str(e)


def validate_stock_data(data):
    """Validate fetched data"""
    try:
        # Check required fields
        required = ["Stock", "Close", "Volume"]
        if not all(k in data for k in required):
            return False
        
        # Validate stock symbol
        if not isinstance(data["Stock"], str) or len(data["Stock"]) == 0:
            return False
        
        # Validate prices
        try:
            close = float(data["Close"])
            if close <= 0:
                return False
        except (TypeError, ValueError):
            return False
        
        # Validate volume
        try:
            volume = float(data["Volume"])
            if volume < 0:
                return False
        except (TypeError, ValueError):
            return False
        
        return True
    except Exception as e:
        logger.error(f"Validation error: {e}")
        return False


def fetch_nepseapi_rest():
    """Fetch from NepseAPI REST - WITH VALIDATION"""
    try:
        r = requests.get(
            f"{NEPSEAPI_REST_URL}/api/v1/market/live",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=8,
            verify=certifi.where()
        )
        r.raise_for_status()
        data = r.json()
        
        # [OK] Validate response structure
        items, error = validate_api_response(data)
        if error:
            logger.error(f"Invalid API response: {error}")
            return pd.DataFrame()
        
        if not items:
            return pd.DataFrame()

        rows = []
        for item in items:
            try:
                symbol = (item.get("symbol") or item.get("Symbol") or "").upper().strip()
                price = float(item.get("ltp") or item.get("lastTradedPrice") or item.get("close") or 0)
                volume = float(item.get("totalVolume") or item.get("volume") or 0)
                
                stock_data = {"Stock": symbol, "Close": price, "Volume": volume}
                
                # [OK] VALIDATE
                if validate_stock_data(stock_data):
                    rows.append(stock_data)
                else:
                    logger.debug(f"Skipped invalid stock: {symbol}")
            except (TypeError, ValueError) as e:
                logger.debug(f"Error parsing stock: {e}")
                continue

        if not rows:
            logger.warning("No valid stocks fetched from NepseAPI")
            return pd.DataFrame()
        
        logger.info(f"NepseAPI: {len(rows)} valid stocks fetched")
        return pd.DataFrame(rows)
        
    except requests.exceptions.Timeout:
        logger.error("NepseAPI timeout")
        return pd.DataFrame()
    except requests.exceptions.ConnectionError:
        logger.error("Connection error to NepseAPI")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"NepseAPI Error: {e}")
        return pd.DataFrame()


def fetch_merolagani():
    """Fetch from Merolagani - WITH VALIDATION"""
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(MEROLAGANI_URL, headers=headers, timeout=10, verify=certifi.where())
        r.raise_for_status()
        tables = pd.read_html(StringIO(r.text))
        
        if not tables:
            return pd.DataFrame()

        df = max(tables, key=len)
        
        col_map = {}
        for col in df.columns:
            c = str(col).lower().strip()
            if any(k in c for k in ["symbol", "scrip"]):
                col_map[col] = "Stock"
            elif any(k in c for k in ["ltp", "last traded price"]):
                col_map[col] = "Close"
            elif "volume" in c:
                col_map[col] = "Volume"

        if len(col_map) < 3:
            return pd.DataFrame()
        
        df = df.rename(columns=col_map)[["Stock", "Close", "Volume"]]
        df["Stock"] = df["Stock"].astype(str).str.upper().str.strip()
        df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
        df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce")
        
        # [OK] VALIDATE
        df = df.dropna()
        df = df[df["Close"] > 0]
        df = df[df["Volume"] >= 0]
        
        logger.info(f"Merolagani: {len(df)} valid stocks fetched")
        return df
    except Exception as e:
        logger.error(f"Merolagani Error: {e}")
        return pd.DataFrame()


def fetch_sharesansar():
    """Fetch from Sharesansar - WITH VALIDATION"""
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(SHARESANSAR_URL, headers=headers, timeout=10, verify=certifi.where())
        r.raise_for_status()
        tables = pd.read_html(StringIO(r.text))
        
        if not tables:
            return pd.DataFrame()
        
        df = tables[0]
        
        if not all(c in df.columns for c in ["Symbol", "LTP", "Volume"]):
            return pd.DataFrame()
        
        df = df.rename(columns={"Symbol": "Stock", "LTP": "Close", "Volume": "Volume"})[["Stock", "Close", "Volume"]]
        df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
        df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce")
        
        # [OK] VALIDATE
        df = df.dropna()
        df = df[df["Close"] > 0]
        df = df[df["Volume"] >= 0]
        
        logger.info(f"Sharesansar: {len(df)} valid stocks fetched")
        return df
    except Exception as e:
        logger.error(f"Sharesansar Error: {e}")
        return pd.DataFrame()


def fetch_data():
    """
    Fetch data with fallback mechanism
    FINAL VERSION: Better error handling
    """
    try:
        # Try NepseAPI REST first
        df = fetch_nepseapi_rest()
        if not df.empty:
            logger.debug("Using NepseAPI REST")
            return df, "NEPSEAPI_REST"
        
        # Try Merolagani
        df = fetch_merolagani()
        if not df.empty:
            logger.debug("Using Merolagani")
            return df, "MEROLAGANI"
        
        # Try Sharesansar
        df = fetch_sharesansar()
        if not df.empty:
            logger.debug("Using Sharesansar")
            return df, "SHARESANSAR"
        
        logger.warning("All data sources failed")
        return pd.DataFrame(), "NONE"
    except Exception as e:
        logger.error(f"Data fetch error: {e}")
        return pd.DataFrame(), "ERROR"