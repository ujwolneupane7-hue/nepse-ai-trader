"""
Advanced Backtest Engine with Equity Curve Tracking
ADJUSTED: Updated to use final optimized strategy
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)

try:
    from core.strategy_final_optimized import (
        compute_score_lean,
        get_trade_levels_final_optimized
    )
except ImportError:
    logger.error("Cannot import strategy functions")
    compute_score_lean = None
    get_trade_levels_final_optimized = None


def run_backtest_engine(df, strategy_func=None, score_threshold=0.50):
    """
    Advanced backtest with full equity curve
    
    Args:
        df: DataFrame with OHLCV and indicators
        strategy_func: Optional custom strategy function
        score_threshold: Minimum signal score
    
    Returns:
        Dictionary with backtest results including equity curve
    """
    try:
        if df.empty or len(df) < 50:
            logger.warning("Insufficient data for backtest")
            return {
                "error": "Insufficient data",
                "final_balance": 100000,
                "trades": 0,
                "equity_curve": [100000]
            }
        
        balance = 100000
        initial_balance = 100000
        equity_curve = [balance]
        open_pos = None
        trades = []
        daily_stats = []

        for i in range(50, len(df)):
            slice_df = df.iloc[:i+1]
            row = slice_df.iloc[-1]

            # ── GET SIGNAL ──
            signal = None
            
            if strategy_func:
                try:
                    signal = strategy_func(slice_df)
                except Exception as e:
                    logger.debug(f"Strategy function error: {e}")
            else:
                # Use final optimized strategy
                if compute_score_lean and get_trade_levels_final_optimized:
                    try:
                        score, confluence = compute_score_lean(row)
                        if score >= score_threshold:
                            entry, stop, target, rr = get_trade_levels_final_optimized(row)
                            if rr >= 1.55:
                                signal = (entry, stop, target, rr)
                    except Exception as e:
                        logger.debug(f"Signal generation error: {e}")

            # ── ENTRY LOGIC ──
            if open_pos is None and signal:
                try:
                    entry, stop, target, rr = signal
                    open_pos = {
                        "entry": entry,
                        "stop": stop,
                        "target": target,
                        "entry_index": i,
                        "entry_price": row['Close']
                    }
                    logger.debug(f"Entry at index {i} | RR: {rr:.2f}")
                except Exception as e:
                    logger.debug(f"Entry error: {e}")

            # ── EXIT LOGIC ──
            if open_pos:
                price = row['Close']

                exit_price = None
                result = None
                
                if price <= open_pos['stop']:
                    exit_price = open_pos['stop']
                    result = "LOSS"
                elif price >= open_pos['target']:
                    exit_price = open_pos['target']
                    result = "WIN"

                # If exit condition met
                if result:
                    pnl = exit_price - open_pos['entry']
                    balance += pnl
                    trades.append({
                        'pnl': pnl,
                        'result': result,
                        'entry': open_pos['entry'],
                        'exit': exit_price,
                        'bars': i - open_pos['entry_index'],
                        'return_pct': (pnl / open_pos['entry'] * 100)
                    })
                    
                    logger.debug(f"Exit | PnL: {pnl:.2f} | Result: {result}")
                    open_pos = None

            equity_curve.append(balance)

        # ── CALCULATE STATISTICS ──
        total_trades = len(trades)
        
        if total_trades == 0:
            logger.warning("No trades executed in backtest engine")
            return {
                "error": "No trades",
                "final_balance": balance,
                "initial_balance": initial_balance,
                "trades": 0,
                "winrate": 0,
                "equity_curve": equity_curve
            }
        
        wins = sum(1 for t in trades if t['result'] == 'WIN')
        losses = total_trades - wins
        winrate = (wins / total_trades) if total_trades > 0 else 0
        total_pnl = sum(t['pnl'] for t in trades)
        
        # Profit factor
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
            "winrate": round(winrate * 100, 2),
            "max_drawdown": round(max_drawdown, 2),
            "max_drawdown_pct": round(max_drawdown_pct, 2),
            "profit_factor": round(profit_factor, 2),
            "best_trade": round(max([t['pnl'] for t in trades]), 2) if trades else 0,
            "worst_trade": round(min([t['pnl'] for t in trades]), 2) if trades else 0,
            "avg_trade": round(total_pnl / total_trades, 2) if total_trades > 0 else 0,
            "equity_curve": equity_curve,
            "trades_detail": trades[:20]
        }
    except Exception as e:
        logger.error(f"Backtest engine error: {e}")
        return {
            "error": str(e),
            "final_balance": 100000,
            "trades": 0,
            "equity_curve": [100000]
        }