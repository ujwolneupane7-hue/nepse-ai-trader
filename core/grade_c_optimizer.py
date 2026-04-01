"""
ULTRA-STRICT GRADE C FILTERING
With enhanced validation from new core files
[OK] FIXED: NaN handling in Body_Ratio and Close_Pos (Issue #7)
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)

try:
    from core.liquidity import get_liquidity_score, detect_liquidity_pool
    from core.orderflow import validate_orderflow, get_orderflow_confirmation
    from core.accumulation import detect_accumulation, get_accumulation_strength
    ENHANCED_VALIDATION = True
except ImportError:
    logger.warning("Enhanced validation modules not available")
    ENHANCED_VALIDATION = False


def is_grade_c_worth_taking(row, df, stock, score, confluence, rr):
    """
    GATE-KEEPER FOR GRADE C SIGNALS
    Only the best Grade C's pass through
    [OK] FIXED: All validations with error handling and NaN handling
    """
    try:
        acceptance_criteria = []
        
        # ── CRITERIA 1: STRONG VOLUME CONFIRMATION ──
        try:
            vol_ratio = row.get('Vol_Ratio', 0)
            
            if pd.isna(vol_ratio):
                vol_ratio = 0
            else:
                vol_ratio = float(vol_ratio)
            
            if vol_ratio > 1.5:
                acceptance_criteria.append(("volume_strong", True, f"Strong volume: {vol_ratio:.2f}x"))
            elif vol_ratio > 1.3:
                acceptance_criteria.append(("volume_moderate", True, f"Moderate volume: {vol_ratio:.2f}x"))
            else:
                acceptance_criteria.append(("volume_weak", False, f"Weak volume: {vol_ratio:.2f}x"))
        except Exception as e:
            logger.debug(f"{stock}: Volume check error: {e}")
            acceptance_criteria.append(("volume", None, "Volume check skipped"))
        
        # ── CRITERIA 2: INTRABAR CONFIRMATION ──
        try:
            prev_row = df.iloc[-2] if len(df) > 1 else None
            candle_confirmed = False
            
            if prev_row is not None:
                body_ratio = row.get('Body_Ratio', 0)
                close_pos = row.get('Close_Pos', 0)
                
                # [OK] FIX: Handle NaN values
                if pd.isna(body_ratio):
                    body_ratio = 0
                else:
                    body_ratio = float(body_ratio)
                
                if pd.isna(close_pos):
                    close_pos = 0
                else:
                    close_pos = float(close_pos)
                
                if body_ratio > 0.5 and close_pos > 0.65:
                    candle_confirmed = True
                    acceptance_criteria.append(("candle_confirmed", True, "Strong candle close"))
                else:
                    acceptance_criteria.append(("candle_confirmed", False, f"Weak candle (body:{body_ratio:.1%}, pos:{close_pos:.1%})"))
            else:
                acceptance_criteria.append(("candle_confirmed", None, "Not enough data"))
        except Exception as e:
            logger.debug(f"{stock}: Candle check error: {e}")
            acceptance_criteria.append(("candle_confirmed", None, "Candle check skipped"))
        
        # ── CRITERIA 3: TREND ALIGNMENT ──
        try:
            # Only use EMA20/EMA50 since EMA5/EMA12 don't exist
            ema20 = row.get('EMA20')
            ema50 = row.get('EMA50')
            
            if pd.notna(ema20) and pd.notna(ema50):
                if ema20 > ema50:
                    acceptance_criteria.append(("ema_perfect", True, "EMA20 > EMA50 (trend up)"))
                else:
                    acceptance_criteria.append(("ema_none", False, "EMA20 < EMA50 (no uptrend)"))
            else:
                acceptance_criteria.append(("ema", None, "EMA data incomplete"))
        except Exception as e:
            logger.debug(f"{stock}: Trend check error: {e}")
            acceptance_criteria.append(("ema", None, "Trend check skipped"))
        
        # ── CRITERIA 4: MOMENTUM CONFIRMATION ──
        try:
            rsi_fast = row.get('RSI_Fast')
            if pd.notna(rsi_fast):
                rsi_fast = float(rsi_fast)
                if rsi_fast > 60:
                    acceptance_criteria.append(("momentum_strong", True, f"Strong momentum (RSI:{rsi_fast:.0f})"))
                elif rsi_fast > 52:
                    acceptance_criteria.append(("momentum_moderate", True, f"Moderate momentum (RSI:{rsi_fast:.0f})"))
                else:
                    acceptance_criteria.append(("momentum_weak", False, f"Weak momentum (RSI:{rsi_fast:.0f})"))
            else:
                acceptance_criteria.append(("momentum", None, "RSI data missing"))
        except Exception as e:
            logger.debug(f"{stock}: Momentum check error: {e}")
            acceptance_criteria.append(("momentum", None, "Momentum check skipped"))
        
        # ── CRITERIA 5: PATTERN QUALITY ──
        pattern_quality = check_pattern_quality(row)
        acceptance_criteria.append(pattern_quality)
        
        # ── CRITERIA 6: LIQUIDITY VALIDATION (NEW) ──
        if ENHANCED_VALIDATION:
            try:
                liq_score = get_liquidity_score(df, row['Close'])
                if liq_score > 0.5:
                    acceptance_criteria.append(("liquidity", True, f"Good liquidity ({liq_score:.2f})"))
                else:
                    acceptance_criteria.append(("liquidity", False, f"Poor liquidity ({liq_score:.2f})"))
            except Exception as e:
                logger.debug(f"{stock}: Liquidity check error: {e}")
                acceptance_criteria.append(("liquidity", None, "Liquidity check skipped"))
        
        # ── CRITERIA 7: ORDERFLOW VALIDATION (NEW) ──
        if ENHANCED_VALIDATION:
            try:
                of_valid = validate_orderflow(row, "bullish", min_strength=0.15)
                of_conf = get_orderflow_confirmation(df, "bullish", periods=3)
                
                if of_valid and of_conf > 0.5:
                    acceptance_criteria.append(("orderflow", True, f"Strong bullish orderflow ({of_conf:.0%})"))
                elif of_valid:
                    acceptance_criteria.append(("orderflow", True, f"Bullish orderflow ({of_conf:.0%})"))
                else:
                    acceptance_criteria.append(("orderflow", False, f"Weak orderflow ({of_conf:.0%})"))
            except Exception as e:
                logger.debug(f"{stock}: Orderflow check error: {e}")
                acceptance_criteria.append(("orderflow", None, "Orderflow check skipped"))
        
        # ── CRITERIA 8: ACCUMULATION CHECK (NEW) ──
        if ENHANCED_VALIDATION:
            try:
                accum = detect_accumulation(df)
                accum_strength = get_accumulation_strength(df)
                
                if accum == "ACCUMULATION" and accum_strength > 0.3:
                    acceptance_criteria.append(("accumulation", True, f"Institutional buying ({accum_strength:.2f})"))
                elif accum == "DISTRIBUTION":
                    acceptance_criteria.append(("accumulation", False, "Institutional selling"))
                else:
                    acceptance_criteria.append(("accumulation", None, "No institutional activity"))
            except Exception as e:
                logger.debug(f"{stock}: Accumulation check error: {e}")
                acceptance_criteria.append(("accumulation", None, "Accumulation check skipped"))
        
        # ── CRITERIA 9: NO REJECTIONS ──
        is_rejected, rejection_reason = check_for_rejections(row, df, stock)
        if is_rejected:
            acceptance_criteria.append(("rejection", False, rejection_reason))
        else:
            acceptance_criteria.append(("rejection", True, "No rejection signals"))
        
        # ── FINAL DECISION ──
        passed_checks = sum(1 for _, passed, _ in acceptance_criteria if passed is True)
        total_valid_checks = len([c for c in acceptance_criteria if c[1] is not None])
        
        volume_pass = acceptance_criteria[0][1]
        candle_pass = acceptance_criteria[1][1]
        no_rejection = acceptance_criteria[-1][1]
        
        # Grade C MUST pass these critical checks
        if volume_pass is False:
            return False, f"REJECT Grade C: Insufficient volume"
        
        if candle_pass is False:
            return False, f"REJECT Grade C: Candle not confirmed"
        
        if no_rejection is False:
            return False, f"REJECT Grade C: Rejection signal detected"
        
        # Need at least 50% of validation checks to pass
        if total_valid_checks > 0:
            pass_rate = passed_checks / total_valid_checks
            if pass_rate < 0.5:
                return False, f"REJECT Grade C: Only {pass_rate:.0%} checks passed ({passed_checks}/{total_valid_checks})"
        
        return True, f"ACCEPT Grade C: {passed_checks}/{total_valid_checks} checks passed"
    
    except Exception as e:
        logger.error(f"Error validating Grade C: {e}")
        return False, str(e)


def check_pattern_quality(row):
    """Evaluate quality of detected patterns"""
    try:
        pattern_score = 0
        pattern_list = []
        
        if row.get('bull_ob', False):
            pattern_score += 1.0
            pattern_list.append("Order Block")
        
        if row.get('breakout_up', False):
            pattern_score += 1.0
            pattern_list.append("Breakout")
        
        if row.get('breakout_retest', False):
            pattern_score += 0.8
            pattern_list.append("Breakout Retest")
        
        if row.get('hammer', False):
            pattern_score += 0.7
            pattern_list.append("Hammer")
        elif row.get('engulfing_bull', False):
            pattern_score += 0.75
            pattern_list.append("Engulfing")
        elif row.get('pinbar_bull', False):
            pattern_score += 0.6
            pattern_list.append("Pin Bar")
        
        if row.get('pullback_buy', False):
            pattern_score += 0.7
            pattern_list.append("Pullback")
        
        if row.get('fvg_up', False):
            pattern_score += 0.5
            pattern_list.append("FVG")
        
        if row.get('equal_low', False):
            pattern_score += 0.5
            pattern_list.append("Liquidity")
        
        if pattern_score >= 0.9:
            return ("pattern_strong", True, f"Strong pattern: {', '.join(pattern_list)} (score:{pattern_score:.1f})")
        elif pattern_score >= 0.7:
            return ("pattern_moderate", True, f"Moderate pattern: {', '.join(pattern_list)} (score:{pattern_score:.1f})")
        elif pattern_score >= 0.5:
            return ("pattern_weak", True, f"Acceptable pattern: {', '.join(pattern_list)} (score:{pattern_score:.1f})")
        else:
            return ("pattern_none", False, f"Weak pattern(s) only (score:{pattern_score:.1f})")
    
    except Exception as e:
        logger.error(f"Error checking pattern quality: {e}")
        return ("pattern_error", False, str(e))


def check_for_rejections(row, df, stock):
    """
    Scan for obvious rejection signals.
    IMPROVED: Proper Stock column validation and handling
    """
    try:
        # IMPROVED: Validate Stock column exists
        if 'Stock' not in df.columns:
            logger.error(f"DataFrame missing Stock column")
            return False, "Stock column missing from DataFrame"
        
        # IMPROVED: Get stock-specific data safely
        stock_df = df[df['Stock'] == stock]
        
        if stock_df.empty:
            logger.warning(f"{stock}: No data available for rejection check")
            return False, "No data for stock"
        
        if len(stock_df) < 3:
            return False, "Insufficient data for rejection check"
        
        recent = stock_df.tail(5)
        
        # ── High wick rejection ──
        try:
            body = abs(row['Close'] - row['Open'])
            wick = row['High'] - max(row['Close'], row['Open'])
            
            if body > 0 and wick / body > 3.0:
                return True, "Wick rejection (wicked out)"
        except Exception as e:
            logger.debug(f"{stock}: Wick check error: {e}")
        
        # ── Close near stop ──
        try:
            if row['Close'] <= row['Open'] * 0.98:
                return True, "Closes near/below open (reversal risk)"
        except Exception as e:
            logger.debug(f"{stock}: Close check error: {e}")
        
        # ── Volume declining ──
        try:
            if len(recent) > 1:
                prev_vol = recent.iloc[-2]['Volume']
                curr_vol = row['Volume']
                
                if curr_vol < prev_vol * 0.7:
                    return True, "Volume declining into signal (weakness)"
        except Exception as e:
            logger.debug(f"{stock}: Volume decline check error: {e}")
        
        # ── Price at extreme RSI ──
        try:
            rsi = row.get('RSI_Fast', 50)
            # IMPROVED: Explicit NaN check
            if pd.notna(rsi):
                rsi = float(rsi)
                if rsi > 78 or rsi < 28:
                    return True, f"RSI at extreme ({rsi:.0f}) - reversal risk"
        except Exception as e:
            logger.debug(f"{stock}: RSI extreme check error: {e}")
        
        # ── Failed breakout ──
        try:
            if row.get('breakout_up', False):
                recent_high = recent.iloc[:-1]['High'].max()
                if row['High'] > recent_high and row['Close'] < row['Open']:
                    return True, "Breakout with close below open (fakeout)"
        except Exception as e:
            logger.debug(f"{stock}: Breakout check error: {e}")
        
        # ── Gap without follow-through ──
        try:
            if len(recent) > 1:
                prev_close = recent.iloc[-2]['Close']
                if prev_close > 0:
                    gap_pct = abs(row['Open'] - prev_close) / prev_close
                    
                    if gap_pct > 0.02 and row['Volume'] < recent.iloc[-2]['Volume'] * 0.8:
                        return True, f"Gap ({gap_pct*100:.1f}%) without volume follow-through"
        except Exception as e:
            logger.debug(f"{stock}: Gap check error: {e}")
        
        return False, "No obvious rejections"
    
    except Exception as e:
        logger.error(f"Error checking rejections: {e}")
        return False, str(e)