#!/usr/bin/env python
"""Detailed test of scan_final_optimized.py for missing indicator references"""

import pandas as pd
import sys
sys.path.insert(0, '.')

from core.indicators_lean import add_indicators_lean
from core.patterns_enhanced import (
    detect_order_blocks, detect_micro_patterns, detect_momentum_surge,
    detect_volume_explosion, detect_fvg, detect_liquidity,
    detect_breakout_momentum, detect_pd_zones, detect_pullback_trades,
    detect_breakout_retest, detect_micro_divergence
)
from core.scan_final_optimized import count_patterns_final

# Sample data WITH Stock column
df = pd.DataFrame({
    'Stock':  ['NABIL'] * 60,
    'Open':   [1250 + i*2 for i in range(60)],
    'High':   [1260 + i*2 for i in range(60)],
    'Low':    [1240 + i*2 for i in range(60)],
    'Close':  [1255 + i*2 for i in range(60)],
    'Volume': [100000 + i*1000 for i in range(60)],
})

print("\n" + "="*70)
print("DETAILED TEST: scan_final_optimized.py")
print("="*70 + "\n")

# Add indicators
print("[STEP 1] Adding indicators...")
df = add_indicators_lean(df)
print(f"✅ Indicators added: {len(df.columns)} columns")
print(f"   Columns: {list(df.columns)[:10]}...\n")

# Add patterns
print("[STEP 2] Detecting patterns...")
try:
    # Don't group by Stock - apply directly to full df
    df = detect_order_blocks(df)
    print("  ✅ detect_order_blocks")
    df = detect_micro_patterns(df)
    print("  ✅ detect_micro_patterns")
    df = detect_momentum_surge(df)
    print("  ✅ detect_momentum_surge")
    df = detect_volume_explosion(df)
    print("  ✅ detect_volume_explosion")
    df = detect_fvg(df)
    print("  ✅ detect_fvg")
    df = detect_liquidity(df)
    print("  ✅ detect_liquidity")
    df = detect_breakout_momentum(df)
    print("  ✅ detect_breakout_momentum")
    df = detect_pd_zones(df)
    print("  ✅ detect_pd_zones")
    df = detect_pullback_trades(df)
    print("  ✅ detect_pullback_trades")
    df = detect_breakout_retest(df)
    print("  ✅ detect_breakout_retest")
    df = detect_micro_divergence(df)
    print("  ✅ detect_micro_divergence")
    print(f"\n✅ All patterns detected: {len(df.columns)} total columns\n")
except Exception as e:
    print(f"\n❌ Pattern detection failed: {e}\n")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test count_patterns_final
print("[STEP 3] Testing count_patterns_final()...")
row = df.iloc[-1]

try:
    pattern_count, patterns = count_patterns_final(row)
    print(f"✅ Function works")
    print(f"   Pattern count: {pattern_count}")
    print(f"   Patterns detected: {patterns}\n")
except Exception as e:
    print(f"❌ FAILED: {e}\n")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Check for missing indicators
print("[STEP 4] Checking for missing indicator references...")
print()

import inspect

# Check count_patterns_final
source = inspect.getsource(count_patterns_final)
missing = ['EMA5', 'EMA12', 'ATR_Fast', 'RSI_Ultra', 'Volatility']

print("Checking count_patterns_final() function code:")
found_issues = False
for ind in missing:
    if ind in source:
        print(f"  ❌ References {ind}")
        found_issues = True

if not found_issues:
    print(f"  ✅ No missing indicator references\n")
else:
    print("\n⛔ FAILED: Missing indicators found\n")
    sys.exit(1)

# Check indicators available in row
print("Checking available indicators in DataFrame row:")
required = [
    'EMA20', 'EMA50', 'RSI_Fast', 'Vol_Ratio', 'Volume', 'Vol_MA',
    'momentum_surge_up', 'vol_explosion', 'bull_ob', 'breakout_up',
    'hammer', 'engulfing_bull', 'pinbar_bull', 'pullback_buy',
    'fvg_up', 'equal_low', 'breakout_retest', 'micro_bull_div'
]

all_present = True
missing_indicators = []
for ind in required:
    present = ind in row.index
    status = "✅" if present else "❌"
    if present:
        print(f"  {status} {ind}")
    else:
        print(f"  {status} {ind} (MISSING)")
        missing_indicators.append(ind)
        all_present = False

print()

if not all_present:
    print(f"⛔ FAILED: Missing {len(missing_indicators)} indicators: {missing_indicators}\n")
    sys.exit(1)

print("="*70)
print("✅ ALL CHECKS PASSED")
print("="*70)
print("\nSystem is ready with corrected scan_final_optimized.py\n")
print("Summary:")
print(f"  - Total indicators: {len(df.columns)}")
print(f"  - Total patterns: {len([c for c in df.columns if 'bull' in c or 'bear' in c or 'breakout' in c])}")
print(f"  - Patterns detected in latest row: {patterns}\n")