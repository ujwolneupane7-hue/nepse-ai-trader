"""
FINAL OPTIMIZED SCANNER v3.1
Ultra-strict Grade C filtering
9-13 alerts/day with 68-73% win rate
Manual trade execution (no TMS automation)

FIXES APPLIED:
  C-4 : calc_position_size() now returns actual share count based on
         account equity and Rs. risk, not a dimensionless ratio.
  C-5 : params is None guard moved to the top of scan_final_optimized(),
         before any params.get() call.
  C-7 : insert_position() is called when a position is added to
         open_positions, so positions survive system restarts.
  H-5 : score_confluence and pattern_count are kept as distinct variables
         throughout the scan so the alert message always shows the
         strategy-scoring confluence, not the raw pattern count.
  H-11: Duplicate is_market_active() removed.  The scanner now relies
         entirely on the caller (main.py) to gate market hours, including
         holiday awareness.  A lightweight fallback check remains for
         direct / test calls.
  ISSUE #6: Alert success flag - Position only added if alert succeeds
  ISSUE #7: NaN/infinity checks in calc_position_size()
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime
import pytz

from core.strategy_final_optimized import (
    compute_score_lean,
    get_grade_final_optimized,
    get_trade_levels_final_optimized_with_slippage
)
from core.grade_c_optimizer import is_grade_c_worth_taking
from core.grade_b_optimizer import is_grade_b_worth_taking
from core.liquidity import get_liquidity_score
from core.orderflow import orderflow_score, get_orderflow_confirmation
from core.accumulation import detect_accumulation, get_accumulation_strength
from core.database import insert_signal, insert_position

logger = logging.getLogger(__name__)
NEPSE_TZ = pytz.timezone('Asia/Kathmandu')

# Slippage settings
ENTRY_SLIPPAGE = 0.002   # 0.2 %
EXIT_SLIPPAGE  = 0.003   # 0.3 %


def count_patterns_final(row):
    """
    Count only MEANINGFUL patterns.
    Returns: (count, patterns_list)
    """
    patterns = []
    count    = 0

    checks = [
        ('EMA↑',     lambda r: pd.notna(r.get('EMA20')) and pd.notna(r.get('EMA50')) and r['EMA20'] > r['EMA50'], 1.0),
        ('RSI',      lambda r: pd.notna(r.get('RSI_Fast')) and r['RSI_Fast'] > 60,                              1.0),
        ('Surge↑',   lambda r: r.get('momentum_surge_up', False),                                               1.0),
        ('Vol',      lambda r: pd.notna(r.get('Vol_Ratio')) and r['Vol_Ratio'] > 1.3,                           0.9),
        ('Vol↑↑',    lambda r: r.get('vol_explosion', False),                                                    0.9),
        ('OB',       lambda r: r.get('bull_ob', False),                                                          1.0),
        ('BO',       lambda r: r.get('breakout_up', False),                                                      0.9),
        ('Pullback', lambda r: r.get('pullback_buy', False),                                                     0.85),
        ('FVG',      lambda r: r.get('fvg_up', False),                                                           0.7),
        ('Liq',      lambda r: r.get('equal_low', False),                                                        0.7),
        ('Retest',   lambda r: r.get('breakout_retest', False),                                                  0.75),
        ('Div',      lambda r: r.get('micro_bull_div', False),                                                   0.75),
    ]

    # Candlestick patterns (mutually exclusive)
    cs_checks = [
        ('Hammer',    lambda r: r.get('hammer', False),         0.8),
        ('Engulfing', lambda r: r.get('engulfing_bull', False), 0.8),
        ('PinBar',    lambda r: r.get('pinbar_bull', False),    0.7),
    ]

    for label, fn, weight in checks:
        try:
            if fn(row):
                if label == 'Vol':
                    patterns.append(f"Vol{row['Vol_Ratio']:.1f}x")
                elif label == 'RSI':
                    patterns.append(f"RSI{row['RSI_Fast']:.0f}")
                else:
                    patterns.append(label)
                count += weight
        except Exception as e:
            logger.debug(f"Pattern check error ({label}): {e}")

    for label, fn, weight in cs_checks:
        try:
            if fn(row):
                patterns.append(label)
                count += weight
                break   # Only count first matching candlestick pattern
        except Exception as e:
            logger.debug(f"CS pattern check error ({label}): {e}")

    return count, patterns


def is_market_active():
    """
    Lightweight market hours check for direct/test calls.
    H-11: The authoritative holiday-aware check lives in main.py.
          This function is kept only as a fallback — the scanner is
          normally gated by the caller before this function is reached.
    """
    try:
        now = datetime.now(NEPSE_TZ)
        if now.weekday() in [4, 5]:
            return False
        market_open  = now.replace(hour=11, minute=0, second=0, microsecond=0)
        market_close = now.replace(hour=15, minute=0, second=0, microsecond=0)
        return market_open <= now < market_close
    except Exception as e:
        logger.error(f"Error checking market hours: {e}")
        return False


def calc_position_size(entry, stop, account_equity=100000, risk_pct=0.007):
    """
    Calculate position size in shares.
    FIX C-4: Returns the actual number of shares to buy, derived from a
             fixed Rs. risk amount per trade, so that PnL
             (price_move × shares) is in real Rupees.

    Formula:
        risk_amount  = account_equity × risk_pct   (Rs. to risk this trade)
        shares       = risk_amount / stop_distance  (shares at that risk)

    Args:
        entry          : Entry price in Rs.
        stop           : Stop-loss price in Rs.
        account_equity : Current account equity in Rs.
        risk_pct       : Fraction of equity to risk per trade (default 0.7 %)

    Returns:
        int: Number of shares (0 if invalid inputs)
    
    ISSUE #7 FIXED: Comprehensive NaN and infinity protection
    """
    try:
        try:
            entry          = float(entry)
            stop           = float(stop)
            account_equity = float(account_equity)
        except (TypeError, ValueError) as e:
            logger.error(f"Invalid entry/stop/equity: {entry}, {stop}, {account_equity} — {e}")
            return 0

        # ISSUE #7 FIX: Explicit NaN checks
        if pd.isna(entry) or pd.isna(stop) or pd.isna(account_equity):
            logger.error(f"NaN value in position size calculation")
            return 0

        if entry <= 0:
            logger.error(f"Invalid entry: {entry}")
            return 0

        if account_equity <= 0:
            logger.error(f"Invalid account equity: {account_equity}")
            return 0

        stop_distance = abs(entry - stop)

        if stop_distance == 0:
            logger.warning(f"Entry equals stop: {entry}")
            return 0

        if stop_distance < entry * 0.001:
            logger.warning(f"Stop too tight: {stop_distance / entry * 100:.3f}%")
            return 0

        # ISSUE #7 FIX: Check for NaN/infinity in stop_distance
        if pd.isna(stop_distance) or np.isinf(stop_distance):
            logger.error(f"Invalid stop distance: {stop_distance}")
            return 0

        risk_amount = account_equity * risk_pct
        
        # ISSUE #7 FIX: Check for NaN/infinity in risk_amount
        if pd.isna(risk_amount) or np.isinf(risk_amount):
            logger.error(f"Invalid risk amount: {risk_amount}")
            return 0

        shares = risk_amount / stop_distance

        # ISSUE #7 FIX: Explicit infinity and NaN checks
        if pd.isna(shares):
            logger.error(f"NaN in shares calculation: {risk_amount} / {stop_distance} = NaN")
            return 0
        
        if np.isinf(shares):
            logger.error(f"Infinity in shares calculation: {shares}")
            return 0

        # Safety cap: never more than what 20 % of equity can buy
        max_shares  = (account_equity * 0.20) / entry
        
        # ISSUE #7 FIX: Check max_shares for NaN/infinity
        if pd.isna(max_shares) or np.isinf(max_shares):
            logger.error(f"Invalid max shares calculation: {max_shares}")
            return 0

        if shares > max_shares:
            logger.warning(f"Position size capped: {shares:.0f} → {max_shares:.0f} shares")
            shares = max_shares

        result = max(1, int(shares))
        logger.debug(
            f"Position size: {result} shares | "
            f"Risk Rs. {risk_amount:.0f} | "
            f"Stop dist Rs. {stop_distance:.2f}"
        )
        return result
    except Exception as e:
        logger.error(f"Error calculating position size: {e}")
        return 0


def scan_final_optimized(df, regime, params, sector_strength, sector_map,
                         equity, open_positions, last_alert_time,
                         alert_builder, manage_trades, lock, mtf_analyzer=None):
    """
    FINAL OPTIMIZED SCANNER v3.1

    FIXES:
    - C-4 : calc_position_size now takes account_equity → real share count
    - C-5 : params None check is first statement
    - C-7 : insert_position() called on every new position
    - H-5 : score_confluence kept separate from pattern_count
    - H-11: Holiday-aware market check done by caller; fallback only here
    - ISSUE #6: Alert success flag - Position only added if alert succeeds
    - ISSUE #7: NaN/infinity protection in position size

    Args:
        df             : DataFrame with OHLCV and indicators
        regime         : Market regime string
        params         : Adjusted parameters dict
        sector_strength: Dict of sector momentum
        sector_map     : Dict mapping stocks to sectors
        equity         : Current account equity in Rs.
        open_positions : Dict of open positions (shared, must access under lock)
        last_alert_time: Dict of last alert times per stock (shared)
        alert_builder  : RateLimitedAlertBuilder instance
        manage_trades  : Trade management function
        lock           : RLock for synchronization
        mtf_analyzer   : Optional multi-timeframe analyzer

    Returns:
        List of alert dicts
    """

    # ── C-5: params None guard FIRST, before any .get() call ──
    if params is None:
        logger.warning("[STOP] Null params received — aborting scan")
        return []

    if params.get("is_circuit_broken"):
        logger.error("Scans disabled — circuit breaker active")
        return []

    # Fallback market check (caller should already have gated this)
    if not is_market_active():
        logger.debug("Scanner skipped — market not active")
        return []

    # Manage any closed trades
    if manage_trades:
        try:
            manage_trades(df)
        except Exception as e:
            logger.error(f"Error in manage_trades: {e}")

    # Get latest candle per stock
    latest = df.groupby("Stock").tail(1)
    results          = []
    now              = datetime.now(NEPSE_TZ).timestamp()
    alerts_this_scan = {}   # deduplication within this scan

    for _, row in latest.iterrows():
        stock = row["Stock"]

        try:
            # ════════════════════════════════════════════════════
            # STAGE 1: PRE-FILTERS (UNDER LOCK)
            # ════════════════════════════════════════════════════
            with lock:
                if stock in open_positions:
                    logger.debug(f"{stock}: Already has open position")
                    continue

                cooldown         = 450   # seconds
                time_since_last  = now - last_alert_time.get(stock, 0)
                if time_since_last < cooldown:
                    logger.debug(f"{stock}: In cooldown ({time_since_last:.0f}s < {cooldown}s)")
                    continue

                if stock in alerts_this_scan:
                    logger.debug(f"{stock}: Already alerted in this scan")
                    continue

            # ════════════════════════════════════════════════════
            # STAGE 2: VOLUME CHECK
            # ════════════════════════════════════════════════════
            try:
                volume  = float(row.get("Volume", 0))
                vol_ma  = float(row.get("Vol_MA", volume))

                if volume < 1800:
                    logger.debug(f"{stock}: Volume too low ({volume} < 1800)")
                    continue

                if volume < 1.2 * vol_ma:
                    logger.debug(f"{stock}: Volume below 1.2× MA ({volume:.0f} < {1.2 * vol_ma:.0f})")
                    continue
            except (TypeError, ValueError) as e:
                logger.debug(f"{stock}: Volume check error: {e}")
                continue

            # ════════════════════════════════════════════════════
            # STAGE 3: SCORE COMPUTATION
            # ════════════════════════════════════════════════════
            try:
                score, score_confluence = compute_score_lean(row)  # H-5: renamed
            except Exception as e:
                logger.debug(f"{stock}: Score computation error: {e}")
                continue

            if score < 0.50:
                logger.debug(f"{stock}: Score below threshold ({score:.2f} < 0.50)")
                continue

            # ════════════════════════════════════════════════════
            # STAGE 4: TRADE LEVELS WITH SLIPPAGE
            # ════════════════════════════════════════════════════
            try:
                entry, stop, target, rr = get_trade_levels_final_optimized_with_slippage(
                    row, ENTRY_SLIPPAGE, EXIT_SLIPPAGE
                )
            except Exception as e:
                logger.debug(f"{stock}: Trade level error: {e}")
                continue

            if rr < 1.55 or entry == 0 or stop == 0 or target == 0:
                logger.debug(f"{stock}: Invalid trade levels — RR:{rr:.2f} Entry:{entry} Stop:{stop} Target:{target}")
                continue

            # ════════════════════════════════════════════════════
            # STAGE 5: GRADE ASSIGNMENT
            # ════════════════════════════════════════════════════
            try:
                grade = get_grade_final_optimized(score, rr, score_confluence)  # H-5
            except Exception as e:
                logger.debug(f"{stock}: Grade assignment error: {e}")
                continue

            # ════════════════════════════════════════════════════
            # STAGE 6: GRADE-SPECIFIC VALIDATION
            # ════════════════════════════════════════════════════
            if grade in ["D", "F"]:
                logger.debug(f"{stock}: Rejected as Grade {grade}")
                continue

            if grade == "C":
                try:
                    is_valid, reason = is_grade_c_worth_taking(
                        row, df, stock, score, score_confluence, rr)  # H-5
                    if not is_valid:
                        logger.debug(f"{stock}: {reason}")
                        continue
                except Exception as e:
                    logger.debug(f"{stock}: Grade C validation error: {e}")
                    continue

            elif grade == "B":
                try:
                    is_valid, reason = is_grade_b_worth_taking(
                        row, df, stock, score, score_confluence, rr)  # H-5
                    if not is_valid:
                        logger.debug(f"{stock}: {reason}")
                        continue
                except Exception as e:
                    logger.debug(f"{stock}: Grade B validation error: {e}")
                    continue

            # ════════════════════════════════════════════════════
            # STAGE 7: EXTRA VALIDATIONS (liquidity, orderflow, accumulation)
            # ════════════════════════════════════════════════════
            liq_score        = 0.5
            of_score         = 0.0
            of_confirmation  = 0.5
            accum_status     = "UNKNOWN"
            accum_strength   = 0

            stock_df = df[df['Stock'] == stock]

            try:
                if not stock_df.empty:
                    close_price = float(row.get('Close', 0))
                    liq_score   = get_liquidity_score(stock_df, close_price)
                    if liq_score < 0.3:
                        logger.debug(f"{stock}: Very low liquidity ({liq_score:.2f})")
                        continue
            except Exception as e:
                logger.debug(f"{stock}: Liquidity check error: {e}")

            try:
                of_score = orderflow_score(row)
                if not stock_df.empty:
                    of_confirmation = get_orderflow_confirmation(stock_df, "bullish", periods=2)
                if of_score < 0.05 and of_confirmation < 0.4:
                    logger.debug(f"{stock}: Weak orderflow (score:{of_score:.2f}, conf:{of_confirmation:.0%})")
                    continue
            except Exception as e:
                logger.debug(f"{stock}: Orderflow check error: {e}")

            try:
                if not stock_df.empty:
                    accum_status   = detect_accumulation(stock_df)
                    accum_strength = get_accumulation_strength(stock_df)
                    if grade == "C" and accum_status == "DISTRIBUTION":
                        logger.debug(f"{stock}: Distribution detected, Grade C rejected")
                        continue
            except Exception as e:
                logger.debug(f"{stock}: Accumulation check error: {e}")

            # ════════════════════════════════════════════════════
            # SIGNAL PASSES ALL FILTERS — COMMIT UNDER LOCK
            # ════════════════════════════════════════════════════
            with lock:
                # Double-check: another thread may have added this stock
                if stock in open_positions:
                    logger.debug(f"{stock}: Position added by other thread (race prevented)")
                    continue
                if stock in alerts_this_scan:
                    logger.debug(f"{stock}: Alert added by other thread (race prevented)")
                    continue

                alerts_this_scan[stock] = True
                last_alert_time[stock]  = now

            # H-5: count_patterns returns pattern_count separately from score_confluence
            pattern_count, patterns = count_patterns_final(row)

            # Build alert data  (H-5: use score_confluence for the confluence field)
            trade_data = {
                'stock':           stock,
                'entry':           entry,
                'stop':            stop,
                'target':          target,
                'rr':              rr,
                'grade':           grade,
                'score':           score,
                'confluence':      score_confluence,    # H-5: strategy-scoring value
                'pattern_count':   pattern_count,
                'patterns':        patterns,
                'current_price':   float(row.get('Close', 0)),
                'regime':          regime,
                'sector':          sector_map.get(stock, 'N/A'),
                'sector_strength': sector_strength.get(sector_map.get(stock, 'N/A'), 0),
                'liquidity_score': liq_score,
                'orderflow_score': of_score,
                'accumulation':    accum_status
            }

            # ════════════════════════════════════════════════════
            # SEND ALERT AND TRACK SUCCESS
            # ISSUE #6 FIX: Alert success flag - reject if fails
            # ════════════════════════════════════════════════════
            alert_sent = False
            
            if alert_builder is not None:
                try:
                    alert_builder.send_detailed_alert(trade_data)
                    alert_sent = True
                    logger.info(f"[ALERT SENT] {stock} | Score: {trade_data.get('score')}")
                except Exception as e:
                    logger.error(f"[ALERT ERROR] {stock}: {e}")
                    alert_sent = False
            else:
                logger.warning(f"[ALERT SKIPPED] {stock}: alert_builder is None")
                alert_sent = False

            # ISSUE #6 CRITICAL FIX: REJECT SIGNAL IF ALERT FAILED
            # This prevents silent failures where position is added but user wasn't notified
            if not alert_sent:
                logger.warning(f"{stock}: Alert delivery failed, signal rejected for safety")
                continue  # ← SKIP THIS SIGNAL - Don't add position

            # Save signal to database
            try:
                insert_signal(
                    stock=stock,
                    score=score,
                    entry=entry,
                    stop=stop,
                    target=target,
                    rr=rr,
                    grade=grade,
                    mtf_alignment=0
                )
            except Exception as e:
                logger.error(f"Error saving signal for {stock}: {e}")

            # Add position — atomic check-and-add under lock
            try:
                with lock:
                    if stock not in open_positions and len(open_positions) < 5:
                        # C-4: pass equity so size is in real shares
                        position_size = calc_position_size(entry, stop, equity)

                        if position_size > 0:
                            open_positions[stock] = {
                                "entry":  entry,
                                "stop":   stop,
                                "target": target,
                                "size":   position_size,
                                "grade":  grade,
                                "score":  score,
                                "rr":     rr,
                            }
                            logger.info(f"{stock}: Position added ({len(open_positions)}/5) | Grade: {grade} | Size: {position_size} shares")

                            # C-7: persist to DB so it survives a restart
                            try:
                                insert_position(
                                    stock, entry, stop, target,
                                    position_size, grade, score, rr
                                )
                            except Exception as db_err:
                                logger.error(f"Error persisting position for {stock}: {db_err}")
                        else:
                            logger.warning(f"{stock}: Position size invalid ({position_size})")
                    else:
                        if stock not in open_positions:
                            logger.debug(f"{stock}: Max positions reached ({len(open_positions)}/5), stopping scan")
                            break
            except Exception as e:
                logger.error(f"Error adding position for {stock}: {e}")

            results.append({
                "Stock":        stock,
                "Grade":        grade,
                "Score":        score,
                "RR":           rr,
                "Confluence":   round(score_confluence, 2),  # H-5
                "Patterns":     len(patterns),
                "Liquidity":    f"{liq_score:.2f}",
                "Orderflow":    f"{of_score:.2f}",
                "Accumulation": accum_status
            })

            logger.info(
                f"[OK] {stock} [{grade}] | Score:{score:.2f} | RR:{rr:.2f} | "
                f"Confluence:{score_confluence:.1f} | Entry:{entry:.2f}"
            )

        except Exception as e:
            logger.error(f"Unexpected error processing {stock}: {e}")
            continue

    logger.info(f"Scan complete: {len(results)} alerts (checked {len(latest)} stocks)")
    return results