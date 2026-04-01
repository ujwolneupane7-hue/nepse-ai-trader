"""
Backtest Trading System Performance
[OK] FIXED: Issue #13 - Backtest coverage validation
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)

try:
    from core.strategy_final_optimized import (
        compute_score_lean,
        get_trade_levels_final_optimized
    )
    STRATEGY_AVAILABLE = True
except ImportError:
    logger.error("Cannot import strategy functions")
    STRATEGY_AVAILABLE = False


def run_backtest(df, score_threshold=0.50, rr_threshold=1.55):
    """
    Backtest strategy on historical data
    [OK] FIXED: Proper validation of indicator coverage
    """
    try:
        # [OK] Check if strategy functions are available
        if not STRATEGY_AVAILABLE:
            logger.error("Strategy functions not available")
            return {
                "error": "Strategy functions not available",
                "final_balance": 100000,
                "trades": 0,
                "winrate": 0
            }
        
        if df.empty or len(df) < 50:
            logger.warning("Insufficient data for backtest")
            return {
                "error": "Insufficient data",
                "final_balance": 100000,
                "trades": 0,
                "winrate": 0
            }
        
        balance = 100000
        initial_balance = 100000
        trades = []
        open_pos = None
        equity_curve = [initial_balance]
        valid_candles = 0  # [OK] Track coverage

        df = df.copy()

        for i in range(50, len(df)):
            row = df.iloc[i]
            current_equity = balance

            # ── SKIP IF INDICATORS NOT READY ──
            if pd.isna(row.get('ATR')) or pd.isna(row.get('RSI_Fast')):
                continue
            
            valid_candles += 1  # [OK] Count valid candles

            # ── ENTRY LOGIC ──
            if open_pos is None:
                try:
                    score, confluence = compute_score_lean(row)
                except Exception as e:
                    logger.debug(f"Score calculation error: {e}")
                    continue

                if score >= score_threshold:
                    try:
                        entry, stop, target, rr = get_trade_levels_final_optimized(row)
                    except Exception as e:
                        logger.debug(f"Trade level calculation error: {e}")
                        continue

                    if rr >= rr_threshold:
                        open_pos = {
                            "entry": entry,
                            "stop": stop,
                            "target": target,
                            "score": score,
                            "index": i,
                            "entry_price": row['Close']
                        }
                        logger.debug(f"Entry: {row.get('Stock', 'Unknown')} | Score: {score:.2f} | RR: {rr:.2f}")

            # ── EXIT LOGIC ──
            if open_pos is not None:
                price = row['Close']

                # Stop loss hit
                if price <= open_pos['stop']:
                    exit_price = open_pos['stop']
                    pnl = exit_price - open_pos['entry']
                    result = "LOSS"
                    
                    balance += pnl
                    trades.append({
                        'pnl': pnl,
                        'result': result,
                        'entry': open_pos['entry'],
                        'exit': exit_price,
                        'bars': i - open_pos['index'],
                        'return_pct': (pnl / open_pos['entry'] * 100)
                    })
                    logger.debug(f"Exit (SL): PnL: {pnl:.2f} | Result: {result}")
                    open_pos = None

                # Target hit
                elif price >= open_pos['target']:
                    exit_price = open_pos['target']
                    pnl = exit_price - open_pos['entry']
                    result = "WIN"
                    
                    balance += pnl
                    trades.append({
                        'pnl': pnl,
                        'result': result,
                        'entry': open_pos['entry'],
                        'exit': exit_price,
                        'bars': i - open_pos['index'],
                        'return_pct': (pnl / open_pos['entry'] * 100)
                    })
                    logger.debug(f"Exit (TP): PnL: {pnl:.2f} | Result: {result}")
                    open_pos = None
            
            equity_curve.append(balance)

        # ── CALCULATE STATISTICS ──
        total_trades = len(trades)
        
        # [OK] FIX #13: Check coverage
        if valid_candles < 20:
            logger.warning(f"Backtest only used {valid_candles} valid candles - results may be unreliable")
        
        if total_trades == 0:
            logger.warning("No trades executed in backtest")
            return {
                "error": "No trades",
                "total_trades": 0,
                "valid_candles": valid_candles,  # [OK] Return coverage info
                "initial_balance": initial_balance,
                "final_balance": balance
            }
        
        wins = sum(1 for t in trades if t['result'] == 'WIN')
        losses = total_trades - wins
        winrate = (wins / total_trades * 100) if total_trades > 0 else 0
        total_pnl = sum(t['pnl'] for t in trades)
        avg_pnl = total_pnl / total_trades if total_trades > 0 else 0
        
        # Profit factor calculation
        win_sum = sum(t['pnl'] for t in trades if t['pnl'] > 0)
        loss_sum = abs(sum(t['pnl'] for t in trades if t['pnl'] < 0))
        profit_factor = (win_sum / loss_sum) if loss_sum > 0 else 0
        
        # Drawdown calculation
        peak = initial_balance
        max_drawdown = 0
        max_drawdown_pct = 0
        
        for equity in equity_curve:
            if equity > peak:
                peak = equity
            drawdown = peak - equity
            drawdown_pct = (drawdown / peak * 100) if peak > 0 else 0
            if drawdown > max_drawdown:
                max_drawdown = drawdown
                max_drawdown_pct = drawdown_pct
        
        return {
            "success": True,
            "final_balance": balance,
            "initial_balance": initial_balance,
            "total_pnl": round(total_pnl, 2),
            "return_pct": round((total_pnl / initial_balance * 100), 2),
            "trades": total_trades,
            "wins": wins,
            "losses": losses,
            "winrate": round(winrate, 2),
            "avg_pnl": round(avg_pnl, 2),
            "best_trade": round(max([t['pnl'] for t in trades]), 2) if trades else 0,
            "worst_trade": round(min([t['pnl'] for t in trades]), 2) if trades else 0,
            "profit_factor": round(profit_factor, 2),
            "max_drawdown": round(max_drawdown, 2),
            "max_drawdown_pct": round(max_drawdown_pct, 2),
            "valid_candles": valid_candles,  # [OK] Return coverage info
            "total_candles": len(df),  # [OK] Return total candles
            "trades_detail": trades[:20]
        }
    except Exception as e:
        logger.error(f"Backtest error: {e}")
        return {
            "error": str(e),
            "final_balance": 100000,
            "trades": 0,
            "winrate": 0
        }