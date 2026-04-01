#!/usr/bin/env python
"""
Test that system works with ONLY original indicators_lean.py
No added indicators
"""

import pandas as pd
import sys
sys.path.insert(0, '.')

from core.indicators_lean import add_indicators_lean

print("\n" + "="*70)
print("TESTING: Minimal indicators_lean.py (11 indicators only)")
print("="*70 + "\n")

# Sample data
df = pd.DataFrame({
    'Stock': ['NABIL'] * 60,
    'Open':   [1250 + i*2 for i in range(60)],
    'High':   [1260 + i*2 for i in range(60)],
    'Low':    [1240 + i*2 for i in range(60)],
    'Close':  [1255 + i*2 for i in range(60)],
    'Volume': [100000 + i*1000 for i in range(60)],
})

# Apply indicators
df = add_indicators_lean(df)

# Check ONLY these 11 exist
required = [
    'EMA20', 'EMA50',           # 2
    'RSI_Fast',                 # 1
    'MACD', 'MACD_Signal',      # 2
    'Vol_MA', 'Vol_Ratio',      # 2
    'ATR', 'ADX',               # 2
    'Body_Ratio', 'Close_Pos',  # 2
    'HH', 'HL', 'Recent_High', 'Breakout'  # 4
]

print("REQUIRED INDICATORS (11):\n")
all_present = True
for ind in required:
    present = ind in df.columns
    status = "✅" if present else "❌"
    print(f"  {status} {ind}")
    if not present:
        all_present = False

print("\n" + "="*70)

# Check for MISSING indicators (should not exist)
missing_indicators = ['EMA5', 'EMA12', 'ATR_Fast', 'RSI_Ultra', 'Volatility']

print("\nCHECK: Missing indicators should NOT be present:\n")
no_extra = True
for ind in missing_indicators:
    present = ind in df.columns
    status = "❌ EXISTS (BAD)" if present else "✅ Not present"
    print(f"  {status} {ind}")
    if present:
        no_extra = False

print("\n" + "="*70)

if all_present and no_extra:
    print("\n✅ SUCCESS: System uses ONLY original indicators_lean.py")
    print(f"   - All {len(required)} required indicators present")
    print(f"   - No extra/missing indicators added")
    print("\n🚀 READY TO LAUNCH\n")
    sys.exit(0)
else:
    print("\n❌ FAILED")
    if not all_present:
        print("   - Missing some required indicators")
    if not no_extra:
        print("   - Extra indicators found (should not exist)")
    print("\n⛔ DO NOT LAUNCH\n")
    sys.exit(1)