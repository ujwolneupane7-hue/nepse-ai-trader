"""
Liquidity Zone Detection
FINAL VERSION: Fixed threshold issue (return 0.1 for empty zones)
"""

import numpy as np
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def liquidity_heatmap(df, bins=15):
    """
    Detect liquidity zones where volume concentrates
    Returns: List of (price_low, price_high) tuples representing zones
    """
    try:
        if df.empty or len(df) < 20:
            return []

        prices = df['Close'].dropna()
        volumes = df['Volume'].dropna()

        if len(prices) < 20 or len(volumes) < 20:
            return []

        # Remove invalid values
        valid_mask = (prices > 0) & (volumes > 0)
        prices = prices[valid_mask]
        volumes = volumes[valid_mask]
        
        if len(prices) < 10:
            return []

        try:
            # Create histogram of prices weighted by volume
            hist, edges = np.histogram(prices, bins=bins, weights=volumes)
        except Exception as e:
            logger.debug(f"Histogram error: {e}")
            return []
        
        mean_vol = np.mean(hist)
        if mean_vol <= 0 or pd.isna(mean_vol):
            return []

        # Find zones where volume exceeds average
        zones = []
        for i in range(len(hist)):
            if hist[i] > mean_vol and not pd.isna(hist[i]):
                zone = (edges[i], edges[i+1])
                zones.append(zone)

        logger.debug(f"Liquidity zones detected: {len(zones)}")
        return zones
    except Exception as e:
        logger.error(f"Error calculating liquidity heatmap: {e}")
        return []


def get_liquidity_score(df, current_price):
    """
    Score current price for liquidity (0-1)
    FIXED: Better scoring with distance calculation
    """
    try:
        if df.empty or pd.isna(current_price):
            return 0.5  # Neutral
        
        zones = liquidity_heatmap(df, bins=15)
        
        if not zones:
            return 0.1  # [OK] FIXED: Was 0.5, now 0.1 for empty zones
        
        # Check if current price is in a liquidity zone
        for zone_low, zone_high in zones:
            if zone_low <= current_price <= zone_high:
                logger.debug(f"Price in liquidity zone: {zone_low:.2f}-{zone_high:.2f}")
                return 1.0  # In liquidity zone
        
        # Find closest zone distance
        distances = []
        for zone_low, zone_high in zones:
            if current_price < zone_low:
                distances.append(zone_low - current_price)
            elif current_price > zone_high:
                distances.append(current_price - zone_high)
        
        if distances:
            closest_distance = min(distances)
            # Inversely proportional to distance
            # 1.0 at 0 distance, 0.1 at 10x distance
            score = max(0.1, 1.0 - (closest_distance / (closest_distance + 0.1)))
            return score
        
        return 0.1
    except Exception as e:
        logger.error(f"Error scoring liquidity: {e}")
        return 0.5


def detect_liquidity_pool(df):
    """
    Detect if current price is in a major liquidity pool
    Returns: True if in liquidity pool, False otherwise
    """
    try:
        if df.empty:
            return False
        
        current_price = df['Close'].iloc[-1]
        
        if pd.isna(current_price) or current_price <= 0:
            return False
        
        zones = liquidity_heatmap(df, bins=15)
        
        if not zones:
            return False
        
        # Check if current price is in any liquidity zone
        for zone_low, zone_high in zones:
            if zone_low <= current_price <= zone_high:
                logger.debug(f"Liquidity pool detected at {current_price:.2f}")
                return True
        
        return False
    except Exception as e:
        logger.error(f"Error detecting liquidity pool: {e}")
        return False


def get_nearest_liquidity_level(df, direction="up"):
    """
    Get nearest liquidity level above or below current price
    direction: "up" or "down"
    Returns: Price level or None
    """
    try:
        if df.empty:
            return None
        
        current_price = df['Close'].iloc[-1]
        
        if pd.isna(current_price) or current_price <= 0:
            return None
        
        zones = liquidity_heatmap(df, bins=15)
        
        if not zones:
            return None
        
        if direction == "up":
            # Find closest zone above current price
            above_zones = [(z[0], z[1]) for z in zones if z[0] > current_price]
            if above_zones:
                return min(above_zones, key=lambda z: z[0])[0]
        
        elif direction == "down":
            # Find closest zone below current price
            below_zones = [(z[0], z[1]) for z in zones if z[1] < current_price]
            if below_zones:
                return max(below_zones, key=lambda z: z[1])[1]
        
        return None
    except Exception as e:
        logger.error(f"Error getting nearest liquidity level: {e}")
        return None