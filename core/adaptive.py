"""
Adaptive Parameters Based on Market Conditions
FINAL VERSION: Proper circuit breaker with instance persistence
FIX: Circuit breaker required as parameter (Issue #5)
"""

import logging
from datetime import datetime
import pytz

logger = logging.getLogger(__name__)
NEPSE_TZ = pytz.timezone('Asia/Kathmandu')

adaptive_state = {
    "score_threshold": 0.50,
    "rr_threshold": 1.6,
    "is_circuit_broken": False
}


def clamp(val, min_val, max_val):
    """Clamp value between min and max"""
    return max(min_val, min(val, max_val))


class CircuitBreaker:
    """Circuit breaker for system safety - PERSISTENT INSTANCE"""
    
    def __init__(self, max_drawdown=0.10, lookback_periods=20):
        self.max_drawdown = max_drawdown
        self.lookback_periods = lookback_periods
        self.is_active = False
        self.activation_time = None
        self.reset_after_hours = 4  # Reset after 4 hours
        logger.info(f"CircuitBreaker initialized: max_drawdown={max_drawdown*100:.1f}%, lookback={lookback_periods}")
    
    def check(self, equity_curve):
        """Check if circuit breaker should activate"""
        if len(equity_curve) < self.lookback_periods:
            return False
        
        try:
            peak = max(equity_curve[-self.lookback_periods:])
            current = equity_curve[-1]
            
            if peak > 0:
                drawdown = (peak - current) / peak
                
                if drawdown > self.max_drawdown:
                    if not self.is_active:
                        self.is_active = True
                        self.activation_time = datetime.now(NEPSE_TZ)
                        logger.error(f"[STOP] CIRCUIT BREAKER ACTIVATED: {drawdown*100:.2f}% drawdown")
                        return True
                    
                    # Check reset time
                    if self.activation_time:
                        elapsed = datetime.now(NEPSE_TZ) - self.activation_time
                        if elapsed.total_seconds() > self.reset_after_hours * 3600:
                            logger.warning(f"Circuit breaker auto-reset after {self.reset_after_hours} hours")
                            self.is_active = False
                            self.activation_time = None
                    
                    return True
            
            # No circuit break condition
            if self.is_active:
                logger.info("Circuit breaker deactivated - drawdown recovered")
            
            self.is_active = False
            self.activation_time = None
            return False
        except Exception as e:
            logger.error(f"Error checking circuit breaker: {e}")
            return False


def adjust_parameters(regime, equity_curve, circuit_breaker):
    """
    Adjust trading parameters based on regime and performance
    [OK] FIXED: circuit_breaker is REQUIRED parameter (not optional)
    This ensures persistent state across calls
    """
    global adaptive_state
    
    # [OK] FIX: circuit_breaker is now REQUIRED (not created here)
    if circuit_breaker is None:
        logger.error("Circuit breaker not initialized!")
        return {
            "score_threshold": 0.50,
            "rr_threshold": 1.6,
            "is_circuit_broken": True
        }
    
    # [OK] Check with PERSISTENT instance
    if circuit_breaker.check(equity_curve):
        logger.error("Circuit breaker active - no new trades")
        return {
            "score_threshold": 0.50,
            "rr_threshold": 1.6,
            "is_circuit_broken": True
        }

    base = {
        "score_threshold": 0.50,
        "rr_threshold": 1.6,
        "is_circuit_broken": False
    }

    adaptive_state.update(base)

    # ── PERFORMANCE ANALYSIS ──
    if len(equity_curve) >= 5:
        recent = equity_curve[-5:]
        performance = recent[-1] - recent[0]
    else:
        performance = 0

    # ── REGIME-BASED ADAPTATION ──
    if regime == "TRENDING":
        adaptive_state["score_threshold"] -= 0.05
        adaptive_state["rr_threshold"] += 0.1
        logger.debug("Adaptation: TRENDING regime - slightly relaxed filters")

    elif regime == "RANGING":
        adaptive_state["score_threshold"] += 0.05
        adaptive_state["rr_threshold"] -= 0.1
        logger.debug("Adaptation: RANGING regime - tighter filters")

    elif regime == "LOW_VOL":
        adaptive_state["score_threshold"] += 0.08
        logger.debug("Adaptation: LOW_VOL regime - strict filters")

    # ── PERFORMANCE FEEDBACK ──
    if performance < 0:
        adaptive_state["score_threshold"] += 0.05
        logger.debug("Adaptation: Negative performance - stricter filters")
    elif performance > 0:
        adaptive_state["score_threshold"] -= 0.02
        logger.debug("Adaptation: Positive performance - relaxed slightly")

    # ── SAFETY CLAMPS ──
    adaptive_state["score_threshold"] = clamp(adaptive_state["score_threshold"], 0.45, 0.80)
    adaptive_state["rr_threshold"] = clamp(adaptive_state["rr_threshold"], 1.4, 2.5)

    return adaptive_state


def apply_rl_action(params, action):
    """Apply RL action"""
    try:
        params = params.copy() if params else {}

        if action == 0:
            params["score_threshold"] = params.get("score_threshold", 0.50) + 0.05
        elif action == 1:
            pass
        elif action == 2:
            params["score_threshold"] = params.get("score_threshold", 0.50) - 0.05

        params["score_threshold"] = clamp(params.get("score_threshold", 0.50), 0.45, 0.80)
        params["rr_threshold"] = clamp(params.get("rr_threshold", 1.6), 1.4, 2.5)

        return params
    except Exception as e:
        logger.error(f"Error applying RL action: {e}")
        return params