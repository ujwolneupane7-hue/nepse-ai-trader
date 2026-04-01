# portfolio.py
"""
Portfolio Configuration
Track your existing NEPSE holdings and give them priority in scanning
"""

# ════════════════════════════════════════════════════════════════
# YOUR STOCKS - Add your holdings here
# ════════════════════════════════════════════════════════════════

PORTFOLIO_STOCKS = [
    "HURJA",
    "NIMB",
    "PRVU",
    "UPPER",
    # Add more stocks here
]

# ════════════════════════════════════════════════════════════════
# DETAILED POSITIONS - Entry prices and quantities
# ════════════════════════════════════════════════════════════════

PORTFOLIO_POSITIONS = {
    "HURJA": {
        "shares": 600,
        "entry_price": 277.00,
    },
    "NIMB": {
        "shares": 2090,
        "entry_price": 205.00,
    },
    "PRVU": {
        "shares": 555,
        "entry_price": 206.00,
    },
    "UPPER": {
        "shares": 700,
        "entry_price": 208.10,
    },
    # Add more positions here
}

# ═════════════���══════════════════════════════════════════════════
# CONFIGURATION (Optional - leave as default)
# ════════════════════════════════════════════════════════════════

PORTFOLIO_PRIORITY_BOOST = False

PORTFOLIO_ALERT_CONFIG = {
    "highlight": True,
    "early_warning": True,
    "reduced_score_threshold": 0.48,
}