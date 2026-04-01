import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("NEPSE_API_KEY")

print("\n" + "="*70)
print("[NEPSE TRADING SYSTEM] - LIVE SCAN RESULTS")
print("="*70)

# Test health (no auth)
print("\n[1] System Health Check")
print("-"*70)
try:
    r = requests.get("http://localhost:5000/health")
    data = r.json()
    print(f"✅ Status: {data.get('status')}")
    print(f"   Mode: {data.get('mode')}")
    print(f"   Market Active: {data.get('market_active')}")
    print(f"   Data Fresh: {data.get('data_fresh')}")
    print(f"   Current Equity: Rs. {data.get('equity', 0):.2f}")
    print(f"   Open Positions: {data.get('positions_open', 0)}")
except Exception as e:
    print(f"❌ Error: {e}")

# Get scan results
print("\n[2] Live Scan Results")
print("-"*70)
try:
    headers = {"X-API-Key": API_KEY}
    r = requests.get("http://localhost:5000/scan", headers=headers)
    data = r.json()
    
    if data.get('status') == 'success':
        alerts = data.get('alerts', [])
        count = data.get('count', 0)
        
        print(f"✅ Found {count} alerts")
        
        if count > 0:
            print("\n📊 Alert Details:")
            print("-"*70)
            for i, alert in enumerate(alerts, 1):
                print(f"\n{i}. {alert.get('Stock', 'N/A')} [{alert.get('Grade', '?')}]")
                print(f"   Score: {alert.get('Score', 0):.2f}/1.0")
                print(f"   R:R: {alert.get('RR', 0):.2f}:1")
                print(f"   Confluence: {alert.get('Confluence', 0):.1f}")
                print(f"   Patterns: {alert.get('Patterns', 0)}")
                print(f"   Liquidity: {alert.get('Liquidity', 'N/A')}")
                print(f"   Orderflow: {alert.get('Orderflow', 'N/A')}")
        else:
            print("\n⏳ No alerts generated yet.")
            print("   (System will generate alerts during NEPSE market hours)")
    else:
        print(f"❌ Error: {data.get('message', 'Unknown error')}")
except Exception as e:
    print(f"❌ Error: {e}")

# Get trading statistics
print("\n[3] Trading Statistics (Last 7 Days)")
print("-"*70)
try:
    headers = {"X-API-Key": API_KEY}
    r = requests.get("http://localhost:5000/stats", headers=headers)
    data = r.json()
    stats = data.get('stats', {})
    
    print(f"✅ Total Trades: {stats.get('total_trades', 0)}")
    print(f"   Wins: {stats.get('wins', 0)}")
    print(f"   Losses: {stats.get('losses', 0)}")
    print(f"   Win Rate: {stats.get('winrate', 0):.1f}%")
    print(f"   Total P&L: Rs. {stats.get('total_pnl', 0):.2f}")
    print(f"   Avg P&L: Rs. {stats.get('avg_pnl', 0):.2f}")
    print(f"   Profit Factor: {stats.get('profit_factor', 0):.2f}")
except Exception as e:
    print(f"❌ Error: {e}")

print("\n" + "="*70)
print("✅ Check complete!")
print("="*70 + "\n")