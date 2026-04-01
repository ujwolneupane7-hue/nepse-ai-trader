"""
Trade Journal Management
FIX M-5: trade_journal is now protected by a threading.Lock to prevent
          data races when log_trade() and get_journal_stats() are called
          from multiple threads concurrently.
"""

import logging
import threading

logger = logging.getLogger(__name__)

_journal_lock = threading.Lock()
trade_journal = []


def log_trade(stock, entry, exit_price, pnl):
    """Log trade to journal (thread-safe)"""
    try:
        result = "WIN" if pnl > 0 else "LOSS"

        trade_entry = {
            "stock": stock,
            "entry": entry,
            "exit": exit_price,
            "pnl": round(pnl, 2),
            "result": result
        }

        with _journal_lock:
            trade_journal.append(trade_entry)

        logger.info(f"Journal: {stock} | PnL: {pnl:.2f} | Result: {result}")
    except Exception as e:
        logger.error(f"Error logging trade: {e}")


def get_journal():
    """Get a snapshot of the trade journal (thread-safe)"""
    with _journal_lock:
        return list(trade_journal)


def clear_journal():
    """Clear journal (thread-safe)"""
    global trade_journal
    with _journal_lock:
        trade_journal = []
    logger.info("Journal cleared")


def get_journal_stats():
    """
    Get journal statistics (thread-safe).
    Takes a snapshot under lock and computes stats outside the lock to
    avoid holding it during computation.
    """
    try:
        with _journal_lock:
            snapshot = list(trade_journal)

        if not snapshot:
            return {}

        total_trades = len(snapshot)
        wins = sum(1 for t in snapshot if t['result'] == 'WIN')
        losses = total_trades - wins
        total_pnl = sum(t['pnl'] for t in snapshot)
        avg_pnl = total_pnl / total_trades if total_trades > 0 else 0

        return {
            "total_trades": total_trades,
            "wins": wins,
            "losses": losses,
            "win_rate": (wins / total_trades * 100) if total_trades > 0 else 0,
            "total_pnl": total_pnl,
            "avg_pnl": avg_pnl
        }
    except Exception as e:
        logger.error(f"Error getting journal stats: {e}")
        return {}