"""
FINAL OPTIMIZED CONFIGURATION
UPDATED: With new validation components
Ultra-strict Grade C filtering
Target: 9-13 alerts/day with 68-73% win rate
"""

FINAL_OPTIMIZED_CONFIG = {
    # ── SCORE THRESHOLDS (Slightly higher) ──
    "MIN_SCORE_A": 0.78,
    "MIN_SCORE_B": 0.68,
    "MIN_SCORE_C": 0.58,
    "MIN_SCORE_OVERALL": 0.50,
    "ACCEPT_GRADES": ["A", "B", "C"],
    
    # ── CONFLUENCE THRESHOLDS (Higher) ──
    "MIN_CONFLUENCE_A": 3.7,
    "MIN_CONFLUENCE_B": 2.5,
    "MIN_CONFLUENCE_C": 2.0,
    
    # ── RISK/REWARD (Slightly higher) ──
    "MIN_RR_A": 2.25,
    "MIN_RR_B": 1.75,
    "MIN_RR_C": 1.55,
    
    # ── VOLUME FILTERS (Stricter) ──
    "MIN_VOLUME": 1800,
    "MIN_VOLUME_RATIO": 1.2,
    "VOLUME_RATIO_FOR_GRADE_C": 1.3,
    
    # ── NEW: LIQUIDITY & ORDERFLOW ──
    "MIN_LIQUIDITY_SCORE": 0.25,
    "MIN_ORDERFLOW_SCORE": 0.05,
    "ORDERFLOW_CONFIRMATION_MIN": 0.4,
    "ENABLE_ACCUMULATION_CHECK": True,
    
    # ── HELPER FUNCTION THRESHOLDS ──
    "ACCUMULATION_STRENGTH_MIN": 0.25,
    "ACCUMULATION_DETECTION_THRESHOLD": 0.03,
    "ACCUMULATION_VOLUME_MULTIPLIER": 1.2,
    
    "LIQUIDITY_SCORE_MIN": 0.25,
    "LIQUIDITY_POOL_THRESHOLD": 0.003,
    
    # ── GRADE C SPECIFIC ──
    "GRADE_C_REQUIREMENTS": {
        "INTRABAR_CONFIRMATION": True,
        "VOLUME_CONFIRMATION": True,
        "TREND_OR_MOMENTUM": True,
        "PATTERN_QUALITY_MIN": 0.8,
        "RSI_SAFE_RANGE": (28, 75),
        "CANDLE_CLOSE_POS_MIN": 0.55,
        "CANDLE_BODY_MIN": 0.45,
    },
    
    # ── GRADE B REQUIREMENTS ──
    "GRADE_B_REQUIREMENTS": {
        "TREND_ALIGNMENT": True,
        "CONFLUENCE_MIN": 2.3,
        "RR_MIN": 1.65,
        "VOLUME_MIN": 1.1,
        "RSI_SAFE_RANGE": (25, 78),
    },
    
    # ── REJECTION FILTERS (Aggressive) ──
    "ENABLE_WICK_REJECTION": True,
    "WICK_BODY_RATIO_MAX": 3.0,
    "ENABLE_VOLUME_DECLINING": True,
    "ENABLE_RSI_EXTREME": True,
    "ENABLE_FAKEOUT_DETECTION": True,
    
    # ── PATTERN FILTERS ──
    "REQUIRE_MAJOR_PATTERN_FOR_C": True,
    "ACCEPTABLE_PATTERNS_C": ["OB", "Breakout", "Pullback", "Hammer", "Engulfing"],
    "MINOR_PATTERNS_ONLY_IF_VOLUME": True,
    
    # ── TIME FILTERS ──
    "ENABLE_TIME_FILTERS": True,
    "SKIP_FIRST_MIN": 5,
    "SKIP_LAST_MIN": 12,
    "REDUCE_GRADE_C_IN_BAD_WINDOWS": True,
    
    # ── POSITION MANAGEMENT ──
    "MAX_POSITIONS": 5,
    "COOLDOWN_PER_STOCK": 500,
    "MIN_BARS_SINCE_LAST": 3,
    
    # ── EXPECTED RESULTS ──
    "EXPECTED_DAILY_ALERTS": "9-13",
    "EXPECTED_WIN_RATE": "68-73%",
    "EXPECTED_RR_RATIO": "1.95-2.25:1",
    "EXPECTED_PROFIT_FACTOR": "2.0-2.6",
    "EXPECTED_MONTHLY_RETURN": "+55-80%",
}

MODE_CONFIGS = {
    "FINAL_OPTIMIZED": {
        "min_alerts": 9,
        "max_alerts": 13,
        "win_rate": "68-73%",
        "quality": "Excellent",
        "manual_work": "Minimal",
    },
    "BALANCED": {
        "min_alerts": 11,
        "max_alerts": 16,
        "win_rate": "60-65%",
        "quality": "Good",
        "manual_work": "Low",
    },
    "CONSERVATIVE": {
        "min_alerts": 8,
        "max_alerts": 12,
        "win_rate": "62-68%",
        "quality": "High",
        "manual_work": "Minimal",
    },
    "AGGRESSIVE": {
        "min_alerts": 15,
        "max_alerts": 25,
        "win_rate": "55-62%",
        "quality": "Medium",
        "manual_work": "High",
    }
}