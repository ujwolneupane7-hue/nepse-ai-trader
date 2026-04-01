"""
GRADE B OPTIMIZER
Standard but solid trading signals
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)

try:
    from core.liquidity import get_liquidity_score
    from core.orderflow import get_orderflow_confirmation
    ENHANCED_VALIDATION = True
except ImportError:
    logger.warning("Enhanced validation modules not available")
    ENHANCED_VALIDATION = False


def is_grade_b_worth_taking(row, df, stock, score, confluence, rr):
    """
    GATE-KEEPER FOR GRADE B SIGNALS
    More permissive than Grade C but still maintains quality
    """
    try:
        acceptance_criteria = []
        
        # ── CRITERIA 1: TREND ALIGNMENT ──
        try:
            # Only use EMA20/EMA50 since EMA5/EMA12 don't exist
            ema20 = row.get('EMA20')
            ema50 = row.get('EMA50')
            
            if pd.notna(ema20) and pd.notna(ema50):
                if ema20 > ema50:
                    acceptance_criteria.append(("ema_perfect", True, "EMA20 > EMA50 (trend up)"))
                else:
                    acceptance_criteria.append(("trend", False, "No uptrend"))
            else:
                acceptance_criteria.append(("trend", None, "EMA data incomplete"))
        except Exception as e:
            logger.debug(f"{stock}: Trend check error: {e}")
            acceptance_criteria.append(("trend", None, "Trend check skipped"))
        
        # ── CRITERIA 2: CONFLUENCE ──
        if confluence >= 2.3:
            acceptance_criteria.append(("confluence", True, f"Good confluence ({confluence:.1f})"))
        else:
            acceptance_criteria.append(("confluence", False, f"Low confluence ({confluence:.1f})"))
        
        # ── CRITERIA 3: RISK/REWARD ──
        if rr >= 1.65:
            acceptance_criteria.append(("rr", True, f"Good RR ({rr:.2f})"))
        else:
            acceptance_criteria.append(("rr", False, f"Low RR ({rr:.2f})"))
        
        # ── CRITERIA 4: VOLUME ──
        try:
            vol_ratio = row.get('Vol_Ratio', 1.0)
            if vol_ratio > 1.1:
                acceptance_criteria.append(("volume", True, f"Volume confirmed ({vol_ratio:.2f}x)"))
            else:
                acceptance_criteria.append(("volume", False, f"Low volume ({vol_ratio:.2f}x)"))
        except Exception as e:
            logger.debug(f"{stock}: Volume check error: {e}")
            acceptance_criteria.append(("volume", None, "Volume check skipped"))
        
        # ── CRITERIA 5: RSI SAFETY ──
        try:
            rsi = row.get('RSI_Fast', 50)
            if 25 < rsi < 78:
                acceptance_criteria.append(("rsi", True, f"RSI safe ({rsi:.0f})"))
            else:
                acceptance_criteria.append(("rsi", False, f"RSI extreme ({rsi:.0f})"))
        except Exception as e:
            logger.debug(f"{stock}: RSI check error: {e}")
            acceptance_criteria.append(("rsi", None, "RSI check skipped"))
        
        # ── CRITERIA 6: LIQUIDITY (NEW) ──
        if ENHANCED_VALIDATION:
            try:
                liq_score = get_liquidity_score(df, row['Close'])
                if liq_score > 0.3:
                    acceptance_criteria.append(("liquidity", True, f"Good liquidity ({liq_score:.2f})"))
                else:
                    acceptance_criteria.append(("liquidity", False, f"Poor liquidity ({liq_score:.2f})"))
            except Exception as e:
                logger.debug(f"{stock}: Liquidity check error: {e}")
                acceptance_criteria.append(("liquidity", None, "Liquidity check skipped"))
        
        # ── CRITERIA 7: ORDERFLOW (NEW) ──
        if ENHANCED_VALIDATION:
            try:
                of_conf = get_orderflow_confirmation(df, "bullish", periods=2)
                if of_conf > 0.4:
                    acceptance_criteria.append(("orderflow", True, f"Bullish orderflow ({of_conf:.0%})"))
                else:
                    acceptance_criteria.append(("orderflow", False, f"Weak orderflow ({of_conf:.0%})"))
            except Exception as e:
                logger.debug(f"{stock}: Orderflow check error: {e}")
                acceptance_criteria.append(("orderflow", None, "Orderflow check skipped"))
        
        # ── FINAL DECISION ──
        passed_checks = sum(1 for _, passed, _ in acceptance_criteria if passed is True)
        total_valid_checks = len([c for c in acceptance_criteria if c[1] is not None])
        
        # Grade B MUST pass these critical checks
        trend_pass = acceptance_criteria[0][1]
        confluence_pass = acceptance_criteria[1][1]
        rr_pass = acceptance_criteria[2][1]
        
        if trend_pass is False:
            return False, f"REJECT Grade B: No trend alignment"
        
        if confluence_pass is False:
            return False, f"REJECT Grade B: Insufficient confluence"
        
        if rr_pass is False:
            return False, f"REJECT Grade B: Insufficient RR"
        
        # Need at least 60% of validation checks to pass
        if total_valid_checks > 0:
            pass_rate = passed_checks / total_valid_checks
            if pass_rate < 0.6:
                return False, f"REJECT Grade B: Only {pass_rate:.0%} checks passed ({passed_checks}/{total_valid_checks})"
        
        return True, f"ACCEPT Grade B: {passed_checks}/{total_valid_checks} checks passed"
    
    except Exception as e:
        logger.error(f"Error validating Grade B: {e}")
        return False, str(e)