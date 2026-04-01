# test_alerts.py
import os
import sys

# Add the core directory to path so we can import the alerts module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'core'))

from alerts import RateLimitedAlertBuilder, send_system_message

# Get credentials from environment variables
BOT_TOKEN = os.getenv("NEPSE_BOT_TOKEN", "")
CHAT_ID   = os.getenv("NEPSE_CHAT_ID", "")

# Check if credentials are set
if not BOT_TOKEN or not CHAT_ID:
    print("[ERROR] ERROR: Set NEPSE_BOT_TOKEN and NEPSE_CHAT_ID")
    print("\nHow to set them:")
    print("  export NEPSE_BOT_TOKEN='your_token_here'")
    print("  export NEPSE_CHAT_ID='your_chat_id_here'")
    sys.exit(1)

print("[OK] Credentials found")
print(f"   BOT_TOKEN length: {len(BOT_TOKEN)} chars")
print(f"   CHAT_ID: {CHAT_ID}")
print()

# ════════════════════════════════════════════════════════════════
# TEST 1: Send a Simple System Message
# ════════════════════════════════════════════════════════════════
print("=" * 70)
print("[TEST-1] Sending simple system message...")
print("=" * 70)

success = send_system_message(
    BOT_TOKEN, 
    CHAT_ID,
    "Alert System Test",
    "[OK] Your Telegram alert system is working!\n\nThis is a test message from NEPSE Trading System."
)

if success:
    print("[OK] SUCCESS - Check your Telegram!")
else:
    print("[ERROR] FAILED - Check the error above")

print()

# ════════════════════════════════════════════════════════════════
# TEST 2: Send a Detailed Trade Alert
# ════════════════════════════════════════════════════════════════
print("=" * 70)
print("[TEST-2] Sending detailed trade alert...")
print("=" * 70)

alert_builder = RateLimitedAlertBuilder(BOT_TOKEN, CHAT_ID)

# Sample trade data (like what the scanner would send)
test_trade_data = {
    "grade": "A",
    "score": 0.82,
    "stock": "NABIL",
    "entry": 1250.50,
    "target": 1350.00,
    "stop": 1200.00,
    "rr": 2.0,
    "confluence": "High",
    "patterns": ["Order Block", "Momentum Surge", "Liquidity Pool"],
    "liquidity_score": 0.78,
    "orderflow_score": 0.45,
    "accumulation": "Strong",
    "regime": "Bullish",
    "sector": "Finance",
    "sector_strength": 0.68
}

try:
    alert_builder.send_detailed_alert(test_trade_data)
    print("[OK] SUCCESS - Check your Telegram for formatted alert!")
except Exception as e:
    print(f"[ERROR] FAILED - Error: {e}")

print()

# ════════════════════════════════════════════════════════════════
# TEST 3: Test Rate Limiting (send 5 alerts quickly)
# ════════════════════════════════════════════════════════════════
print("=" * 70)
print("[TEST-3] Testing rate limiting (5 alerts)...")
print("=" * 70)

import time
start = time.time()

for i in range(5):
    alert_builder.send_detailed_alert({
        **test_trade_data,
        "stock": f"TEST{i:02d}",
        "score": 0.50 + (i * 0.10),
        "entry": 1000 + (i * 50)
    })
    print(f"  Sent alert {i+1}/5")

elapsed = time.time() - start
print(f"\nSent 5 alerts in {elapsed:.2f} seconds")
print(f"Expected: ~0.2s (25 msg/sec limit)")
if elapsed > 0:
    print(f"Actual rate: {5/elapsed:.1f} msg/sec")

print()
print("=" * 70)
print("[OK] ALL TESTS COMPLETE - Check your Telegram!")
print("=" * 70)