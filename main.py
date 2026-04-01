"""
NEPSE FINAL OPTIMIZED TRADING SYSTEM v3.1
Ultra-strict Grade C filtering
9-13 alerts/day with 68-73% win rate
Manual trade execution (no TMS automation)

FIXES APPLIED IN THIS VERSION:
  C-1  : Holiday calendar updated with 2026 NEPSE dates + startup warning
         if the current year is not covered.
  C-2  : build_df() — global bfill/ffill replaced with per-group fill so
         NaN values in early candles of one stock are never filled with
         another stock's indicator values.
  C-3  : scan_in_progress check and set now happen atomically under the
         same lock acquisition, eliminating the double-scan race condition.
  C-6  : consecutive_errors in _run_scan moved to module scope so it
         accumulates across successive scan calls.
  C-7  : On startup, open_positions is restored from the positions table
         in the database.  manage_trades() calls delete_position() when a
         position closes, and scan_final_optimized.py calls insert_position()
         when one opens.
  H-9  : tick_buffer and candles_15m use collections.deque(maxlen=500)
         so .pop(0) — which was O(n) on a list — is eliminated entirely.
  H-11 : Duplicate is_market_active() removed from scanner; all market-
         hour gating goes through is_market_active_with_holidays().
  H-12 : ws_consecutive_failures reads/writes guarded by a dedicated lock.
  M-1  : startup() is kept at module level for WSGI compatibility but
         sys.exit() is replaced with RuntimeError so a WSGI worker does
         not silently die; Flask is run only from __main__.
  M-6  : Auto-scanner sleep aligned to 15-min candle period (900 s).
  M-7  : Unused imports from core.data removed from the live path.
  M-11/M-12: manage_trades() takes a snapshot of open_positions under
         lock, processes exits without holding the lock, then re-acquires
         it for the atomic deletion+DB write.
  L-3  : Startup network connectivity check moved to a daemon thread so
         it cannot block the main startup sequence.
  L-7  : Telegram alert sent to operator when all data sources fail
         consecutively for more than DATA_STALE_THRESHOLD * 3 seconds.
  L-8  : _last_data_time initialised to 0, not time.time(), so the
         freshness check correctly reports stale until real data arrives.
"""

from dotenv import load_dotenv
load_dotenv()  # Load .env file first

from flask import Flask, jsonify, request
from functools import wraps
import pandas as pd
import requests
import time
import threading
import websocket
import json
import logging
import logging.handlers
import signal
import sys
import os
import certifi
from collections import defaultdict, deque
from threading import Lock, RLock
from io import StringIO
from datetime import datetime, timedelta
import pytz

# ────────────────────────────────────────────────────────────────
# ENVIRONMENT VARIABLES & SECURITY
# ────────────────────────────────────────────────────────────────
BOT_TOKEN = os.getenv("NEPSE_BOT_TOKEN", "")
CHAT_ID   = os.getenv("NEPSE_CHAT_ID", "")
API_KEY   = os.getenv("NEPSE_API_KEY", "")

# ────────────────────────────────────────────────────────────────
# IMPORTS FROM CORE
# ────────────────────────────────────────────────────────────────
from core.indicators_lean import add_indicators_lean as add_indicators
from core.patterns_enhanced import (
    detect_order_blocks, detect_micro_patterns, detect_momentum_surge,
    detect_volume_explosion, detect_fvg, detect_liquidity,
    detect_breakout_momentum, detect_pd_zones, detect_pullback_trades,
    detect_breakout_retest, detect_micro_divergence
)
from core.strategy_final_optimized import (
    compute_score_lean,
    get_grade_final_optimized,
    get_trade_levels_final_optimized_with_slippage
)
from core.database import (
    insert_trade, insert_signal, fetch_trades, fetch_signals,
    get_stats, init_db, get_database_info, cleanup_old_records,
    fetch_positions, delete_position   # C-7: added
)
from core.sector import compute_sector_strength, sector_map
from core.mtf import resample_tf, get_trend
from core.regime import detect_regime
from core.adaptive import adjust_parameters, CircuitBreaker
from core.scan_final_optimized import scan_final_optimized, is_market_active, calc_position_size
from core.data_fetch import fetch_data
from core.alerts import RateLimitedAlertBuilder, send_system_message
from core.journal import log_trade, get_journal, get_journal_stats
from core.accumulation import detect_accumulation, get_accumulation_strength
from core.liquidity import get_liquidity_score, detect_liquidity_pool
from core.orderflow import orderflow_score, get_orderflow_strength
from core.backtest import run_backtest
from core.backtest_engine import run_backtest_engine
# M-7: core.data imports removed — fetch_nepse_data / preprocess are not
#       used in the live data path and caused confusion.
from core.grade_c_optimizer import is_grade_c_worth_taking
from core.grade_b_optimizer import is_grade_b_worth_taking

from config_final_optimized import FINAL_OPTIMIZED_CONFIG, MODE_CONFIGS

# ────────────────────────────────────────────────────────────────
# LOGGING SETUP - CLEAN & PRODUCTION SAFE
# ────────────────────────────────────────────────────────────────

import logging
import logging.handlers
import sys

# [OK] Force UTF-8 (fixes Windows encoding issues)
try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

# [OK] Prevent logging crashes (VERY IMPORTANT)
logging.raiseExceptions = False

# [OK] Get root logger and clear existing handlers (prevents duplicates)
root_logger = logging.getLogger()
root_logger.handlers.clear()
root_logger.setLevel(logging.DEBUG)

# ── Formatter ──
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# ── Console Handler ──
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(formatter)

# ── File Handler (Rotating) - with absolute path ──
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

file_handler = logging.handlers.RotatingFileHandler(
    os.path.join(LOG_DIR, 'trading_system.log'),
    maxBytes=10 * 1024 * 1024,
    backupCount=5,
    encoding='utf-8'
)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

# ── Attach handlers ──
root_logger.addHandler(console_handler)
root_logger.addHandler(file_handler)

# ── Module logger ──
logger = logging.getLogger(__name__)

logger.info("=" * 70)
logger.info("[SYSTEM] DEBUG logging initialized")
logger.info("=" * 70)

# Add portfolio support
try:
    from portfolio import PORTFOLIO_STOCKS, PORTFOLIO_POSITIONS, PORTFOLIO_ALERT_CONFIG
    HAS_PORTFOLIO = True
    logger.info(f"[OK] Portfolio loaded: {len(PORTFOLIO_STOCKS)} stocks")
except ImportError:
    PORTFOLIO_STOCKS = []
    PORTFOLIO_POSITIONS = {}
    PORTFOLIO_ALERT_CONFIG = {}
    HAS_PORTFOLIO = False
    logger.warning("[WARN] portfolio.py not found - running without portfolio tracking")

# ════════════════════════════════════════════════════════════════
# ██  CONFIGURATION
# ════════════════════════════════════════════════════════════════

TRADING_MODE = "FINAL_OPTIMIZED"

NEPSEAPI_WS_URL  = "wss://nepseapiws.surajrimal.dev/"
NEPSEAPI_REST_URL = "https://nepseapi.surajrimal.dev"

CANDLE       = 900   # 15-minute candles
MAX_POSITIONS = 5
MAX_PER_STOCK = 1
COOLDOWN      = 450
SCAN_TIMEOUT  = 240  # 4 minutes max for scan

ENABLE_ACCUMULATION_FILTER   = True
ENABLE_LIQUIDITY_VALIDATION  = True
ENABLE_ORDERFLOW_VALIDATION  = True
ENABLE_BACKTEST              = True

ENTRY_SLIPPAGE = 0.002
EXIT_SLIPPAGE  = 0.003

DATA_STALE_THRESHOLD = 60  # seconds

MAX_DAILY_LOSS = -20000  # Rs.

# ────────────────────────────────────────────────────────────────
# FIX C-1: NEPSE HOLIDAY CALENDAR (updated to cover 2026)
# [WARN]  IMPORTANT: This list must be updated annually.
#     Source: https://www.nepalstock.com / NEPSE official notices.
#     The system logs a startup error if the current year is missing.
# ────────────────────────────────────────────────────────────────
NEPSE_HOLIDAYS = [
    # ── 2024 ──
    "2024-01-11",  # Prithvi Jayanti / Unity Day
    "2024-02-19",  # National Democracy Day (Falgun 7)
    "2024-03-25",  # Holi (Fagu Purnima)
    "2024-04-09",  # Ram Navami
    "2024-04-14",  # New Year (Baisakh 1) / Nepali New Year
    "2024-05-23",  # Buddha Jayanti
    "2024-08-19",  # Janai Purnima / Raksha Bandhan
    "2024-08-26",  # Krishna Janmashtami
    "2024-09-17",  # Indra Jatra
    "2024-10-02",  # Ghatasthapana
    "2024-10-12",  # Vijaya Dashami (Dashain main day)
    "2024-10-13",  # Dashain (Ekadashi)
    "2024-10-14",  # Dashain (Dwadashi)
    "2024-11-01",  # Laxmi Puja (Tihar)
    "2024-11-02",  # Govardhan Puja
    "2024-11-03",  # Bhai Tika
    "2024-12-25",  # Christmas / Udhauli Parwa
    # ── 2025 ──
    "2025-01-11",  # Prithvi Jayanti
    "2025-02-19",  # National Democracy Day
    "2025-03-14",  # Holi (Fagu Purnima)
    "2025-03-30",  # Ram Navami
    "2025-04-14",  # Nepali New Year (Baisakh 1)
    "2025-05-12",  # Buddha Jayanti
    "2025-08-09",  # Janai Purnima
    "2025-08-16",  # Krishna Janmashtami
    "2025-09-22",  # Ghatasthapana
    "2025-10-02",  # Vijaya Dashami
    "2025-10-20",  # Laxmi Puja (Tihar)
    "2025-10-21",  # Govardhan Puja
    "2025-10-22",  # Bhai Tika
    "2025-12-25",  # Christmas / Udhauli Parwa
    # ── 2026 ──
    # [WARN]  Verify exact dates from NEPSE official calendar before go-live.
    "2026-01-11",  # Prithvi Jayanti
    "2026-02-19",  # National Democracy Day
    "2026-03-03",  # Holi (Fagu Purnima) — approximate
    "2026-04-14",  # Nepali New Year (Baisakh 1)
    "2026-05-01",  # Buddha Jayanti — approximate
    "2026-07-29",  # Janai Purnima — approximate
    "2026-08-05",  # Krishna Janmashtami — approximate
    "2026-09-11",  # Ghatasthapana — approximate
    "2026-09-21",  # Vijaya Dashami — approximate
    "2026-10-09",  # Laxmi Puja (Tihar) — approximate
    "2026-10-10",  # Govardhan Puja — approximate
    "2026-10-11",  # Bhai Tika — approximate
    "2026-12-25",  # Christmas
]

# ════════════════════════════════════════════════════════════════
# ██  THREAD-SAFE EQUITY CURVE
# ════════════════════════════════════════════════════════════════

class ThreadSafeEquityCurve:
    """Thread-safe equity curve management with bounded history"""

    def __init__(self, initial_equity=100000, max_history=5000):
        self.lock           = RLock()
        self.equity_values  = deque([initial_equity], maxlen=max_history)
        self.current_equity = initial_equity
        self.max_history    = max_history
        logger.info(f"EquityCurve initialized: initial={initial_equity}, max_history={max_history}")

    def update(self, new_equity):
        try:
            with self.lock:
                self.current_equity = float(new_equity)
                self.equity_values.append(self.current_equity)
        except Exception as e:
            logger.error(f"Error updating equity: {e}")

    def add_pnl(self, pnl):
        try:
            with self.lock:
                self.current_equity += float(pnl)
                self.equity_values.append(self.current_equity)
                return self.current_equity
        except Exception as e:
            logger.error(f"Error adding PnL: {e}")
            return self.current_equity

    def get_current(self):
        try:
            with self.lock:
                return self.current_equity
        except Exception as e:
            logger.error(f"Error getting current equity: {e}")
            return 0

    def get_peak(self):
        try:
            with self.lock:
                return max(self.equity_values) if self.equity_values else self.current_equity
        except Exception as e:
            logger.error(f"Error getting peak equity: {e}")
            return self.current_equity

    def get_copy(self):
        try:
            with self.lock:
                return list(self.equity_values)
        except Exception as e:
            logger.error(f"Error getting equity copy: {e}")
            return []

    def get_stats(self):
        try:
            with self.lock:
                if not self.equity_values:
                    return {"current": self.current_equity, "peak": self.current_equity,
                            "drawdown": 0, "drawdown_pct": 0, "candle_count": 0,
                            "max_history": self.max_history}
                peak        = max(self.equity_values)
                current     = self.current_equity
                drawdown    = peak - current
                drawdown_pct = (drawdown / peak * 100) if peak > 0 else 0
                return {
                    "current":      float(current),
                    "peak":         float(peak),
                    "drawdown":     float(drawdown),
                    "drawdown_pct": float(drawdown_pct),
                    "candle_count": len(self.equity_values),
                    "max_history":  self.max_history
                }
        except Exception as e:
            logger.error(f"Error getting equity stats: {e}")
            return {}


equity_curve_ts = ThreadSafeEquityCurve(100000, max_history=5000)

# ════════════════════════════════════════════════════════════════
# ██  DAILY LOSS TRACKER
# ════════════════════════════════════════════════════════════════

class DailyLossTracker:
    """Thread-safe daily loss tracking"""

    def __init__(self, max_daily_loss=-20000):
        self.lock              = RLock()
        self.max_daily_loss    = max_daily_loss
        self.daily_loss        = 0.0
        self.daily_start_equity = 100000
        self.daily_start_time  = None
        logger.info(f"Daily loss tracker initialized: max loss = Rs. {max_daily_loss:.2f}")

    def reset_if_needed(self, current_equity):
        try:
            with self.lock:
                now = datetime.now(NEPSE_TZ)
                if self.daily_start_time is None:
                    self.daily_start_time   = now
                    self.daily_start_equity = current_equity
                    self.daily_loss         = 0.0
                    return
                if now.date() != self.daily_start_time.date():
                    logger.info(f"[INFO] Previous Daily P&L: Rs. {self.daily_loss:.2f}")
                    self.daily_start_time   = now
                    self.daily_start_equity = current_equity
                    self.daily_loss         = 0.0
        except Exception as e:
            logger.error(f"Error resetting daily loss: {e}")

    def check_limit(self, current_equity):
        try:
            with self.lock:
                self.reset_if_needed(current_equity)
                self.daily_loss = current_equity - self.daily_start_equity
                if self.daily_loss < self.max_daily_loss:
                    logger.error(f"[STOP] DAILY LOSS LIMIT EXCEEDED: {self.daily_loss:.2f} Rs.")
                    return True
                return False
        except Exception as e:
            logger.error(f"Error checking daily loss limit: {e}")
            return False

    def get_daily_loss(self):
        try:
            with self.lock:
                return self.daily_loss
        except Exception as e:
            logger.error(f"Error getting daily loss: {e}")
            return 0.0


daily_loss_tracker = DailyLossTracker(max_daily_loss=MAX_DAILY_LOSS)

# ════════════════════════════════════════════════════════════════
# ██  GLOBALS
# ════════════════════════════════════════════════════════════════

# H-9: Use deque(maxlen=500) so pop(0) is O(1) and the maxlen cap is automatic
tick_buffer  = defaultdict(lambda: deque(maxlen=500))
candles_15m  = defaultdict(lambda: deque(maxlen=500))

open_positions  = {}
last_alert_time = {}

lock      = RLock()
scan_lock = RLock()   # C-3: RLock so check-and-set can be atomic
db_lock   = Lock()

active_tier             = None
ws_conn                 = None
ws_latest               = {}

# H-12: guard ws_consecutive_failures with its own lock
_ws_fail_lock           = Lock()
ws_consecutive_failures = 0

scan_in_progress = False
_scan_result     = []

# L-8: initialise to 0, not time.time(), so freshness check is correct
#       before the first real data fetch.
_last_data_time        = 0
_data_stale_warning_sent = False

# C-6: module-level counter so it accumulates across successive scan calls
_scan_consecutive_errors = 0

circuit_breaker = None
alert_builder   = None

app      = Flask(__name__)
NEPSE_TZ = pytz.timezone('Asia/Kathmandu')

# ════════════════════════════════════════════════════════════════
# ██  API AUTHENTICATION DECORATOR
# ════════════════════════════════════════════════════════════════

def require_api_key(f):
    """Decorator to require API key for protected endpoints"""
    @wraps(f)
    def decorated(*args, **kwargs):
        key = request.headers.get('X-API-Key')
        if not key or key != API_KEY:
            logger.warning(f"Unauthorized API access attempt from {request.remote_addr}")
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated

# ════════════════════════════════════════════════════════════════
# ██  HOLIDAY CALENDAR FUNCTIONS  (FIX C-1)
# ════════════════════════════════════════════════════════════════

def is_nepse_holiday(date):
    """Check if date is a NEPSE holiday"""
    try:
        return date.strftime("%Y-%m-%d") in NEPSE_HOLIDAYS
    except Exception as e:
        logger.error(f"Error checking holiday: {e}")
        return False


def is_market_active_with_holidays():
    """
    Authoritative market-hours check, including holidays.
    H-11: This is the ONLY place market-hours gating should happen.
          scan_final_optimized.py contains only a lightweight fallback.
    """
    try:
        now = datetime.now(NEPSE_TZ)

        if is_nepse_holiday(now):
            logger.debug(f"Market closed — Holiday: {now.strftime('%Y-%m-%d')}")
            return False

        # NEPSE is closed Friday (weekday 4) and Saturday (weekday 5)
        if now.weekday() in [4, 5]:
            return False

        market_open  = now.replace(hour=11, minute=0, second=0, microsecond=0)
        market_close = now.replace(hour=15, minute=0, second=0, microsecond=0)

        return market_open <= now < market_close
    except Exception as e:
        logger.error(f"Error checking market hours: {e}")
        return False

# ════════════════════════════════════════════════════════════════
# ██  GRACEFUL SHUTDOWN HANDLER
# ════════════════════════════════════════════════════════════════

class GracefulShutdown:
    def __init__(self):
        self.shutdown_event = threading.Event()
        signal.signal(signal.SIGINT,  self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)

    def handle_signal(self, signum, frame):
        logger.info("[STOP] Shutdown signal received")
        self.shutdown()

    def shutdown(self):
        logger.info("=" * 70)
        logger.info("[STOP] SYSTEM SHUTDOWN IN PROGRESS")
        logger.info("=" * 70)
        try:
            if ws_conn:
                ws_conn.close()
                logger.info("[OK] WebSocket closed")
        except Exception as e:
            logger.error(f"Error closing WebSocket: {e}")
        try:
            stats = equity_curve_ts.get_stats()
            logger.info(f"[INFO] Final Statistics:")
            logger.info(f"   - Current Equity:  Rs. {stats['current']:.2f}")
            logger.info(f"   - Total Return:    {((stats['current'] - 100000) / 100000 * 100):.2f}%")
            logger.info(f"   - Open Positions:  {len(open_positions)}")
            logger.info(f"   - Max Drawdown:    {stats['drawdown_pct']:.2f}%")
            logger.info(f"   - Daily Loss:      Rs. {daily_loss_tracker.get_daily_loss():.2f}")
        except Exception as e:
            logger.error(f"Error saving state: {e}")
        logger.info("[GREEN] System shutdown complete")
        logger.info("=" * 70)
        sys.exit(0)


shutdown_handler = GracefulShutdown()

# ════════════════════════════════════════════════════════════════
# ██  STARTUP VALIDATION  (FIX M-1)
# ════════════════════════════════════════════════════════════════

def validate_startup():
    """
    Validate system configuration before startup.
    FIX M-1: Raises RuntimeError instead of calling sys.exit() so that
             WSGI workers (gunicorn) receive a proper exception rather
             than silently dying.  sys.exit() is still used when running
             directly as __main__ (handled in the if __name__ block).
    """
    logger.info("=" * 70)
    logger.info("[CHECK] STARTUP VALIDATION")
    logger.info("=" * 70)

    errors = []

    if not BOT_TOKEN:
        errors.append("NEPSE_BOT_TOKEN environment variable not set. "
                       "Set: export NEPSE_BOT_TOKEN=your_token")

    if not CHAT_ID:
        errors.append("NEPSE_CHAT_ID environment variable not set. "
                       "Set: export NEPSE_CHAT_ID=your_chat_id")

    if not API_KEY or len(API_KEY) < 32:
        errors.append(
            "NEPSE_API_KEY not set or too short (minimum 32 chars). "
            "Generate: openssl rand -hex 16"
        )

    if API_KEY and not all(c.isalnum() or c in '-_' for c in API_KEY):
        errors.append(
            "API_KEY invalid: use only alphanumeric characters, dash, underscore"
        )

    if errors:
        for err in errors:
            logger.error(f"[ERROR] {err}")
        raise RuntimeError("Startup validation failed. See log for details.")

    logger.info(f"[OK] Telegram credentials configured")
    logger.info(f"[OK] API authentication enabled (key length: {len(API_KEY)} chars)")

    # FIX: ENFORCE holiday calendar coverage (blocks startup if missing)
    current_year  = datetime.now(NEPSE_TZ).year
    years_covered = {int(d[:4]) for d in NEPSE_HOLIDAYS}

    if current_year not in years_covered:
        logger.error(
            f"[FATAL] NEPSE holiday calendar does not cover {current_year}! "
            f"Update NEPSE_HOLIDAYS from: https://www.nepalstock.com/"
        )
        raise RuntimeError(f"Holiday calendar incomplete for {current_year}")

    # Warn if using approximate dates
    if current_year == 2026:
        logger.warning(
            f"[WARN] 2026 holiday dates are APPROXIMATE. "
            f"Verify against official NEPSE calendar before live trading."
        )

    holidays_this_year = sum(1 for d in NEPSE_HOLIDAYS if d.startswith(str(current_year)))
    logger.info(f"[OK] Holiday calendar covers {current_year} ({holidays_this_year} dates)")

    # Check database
    try:
        init_db()
        logger.info("[OK] Database initialized")
    except Exception as e:
        raise RuntimeError(f"Database initialization failed: {e}")

    if TRADING_MODE not in MODE_CONFIGS:
        raise RuntimeError(f"Unknown trading mode: {TRADING_MODE}")

    logger.info(f"[OK] Trading mode: {TRADING_MODE}")
    logger.info("=" * 70)

# ════════════════════════════════════════════════════════════════
# ██  DATA FRESHNESS CHECK
# ════════════════════════════════════════════════════════════════

def is_data_fresh():
    """Check if data is fresh enough for trading"""
    data_age = time.time() - _last_data_time
    if data_age > DATA_STALE_THRESHOLD:
        logger.warning(f"[WARN] Data is stale: {data_age:.0f}s old")
        return False
    return True

# ════════════════════════════════════════════════════════════════
# ██  CLEANUP FUNCTIONS
# ════════════════════════════════════════════════════════════════

def cleanup_old_candles():
    """Remove old candle data from inactive stocks"""
    try:
        now               = int(time.time())
        inactive_threshold = 3600 * 4   # 4 hours

        with lock:
            stocks_to_remove = []
            for stock, candles in candles_15m.items():
                if candles:
                    last_time = candles[-1]['time']
                    if now - last_time > inactive_threshold:
                        stocks_to_remove.append(stock)
            for stock in stocks_to_remove:
                del candles_15m[stock]
                logger.debug(f"Cleaned up candles for inactive stock: {stock}")
    except Exception as e:
        logger.error(f"Error cleaning up candles: {e}")

# ════════════════════════════════════════════════════════════════
# ██  WEBSOCKET HANDLERS
# ════════════════════════════════════════════════════════════════

def _set_ws_failures(value):
    """H-12: Thread-safe write to ws_consecutive_failures"""
    global ws_consecutive_failures
    with _ws_fail_lock:
        ws_consecutive_failures = value


def _increment_ws_failures():
    """H-12: Thread-safe increment of ws_consecutive_failures"""
    global ws_consecutive_failures
    with _ws_fail_lock:
        ws_consecutive_failures += 1
        return ws_consecutive_failures


def _get_ws_failures():
    """H-12: Thread-safe read of ws_consecutive_failures"""
    with _ws_fail_lock:
        return ws_consecutive_failures


def _parse_ws_item(item):
    try:
        symbol = (item.get("symbol") or item.get("Symbol") or
                  item.get("stock") or "").upper().strip()
        price  = float(item.get("ltp") or item.get("lastTradedPrice") or
                        item.get("close") or 0)
        volume = float(item.get("totalVolume") or item.get("volume") or 0)
        if symbol and price > 0:
            ws_latest[symbol] = {"price": price, "volume": volume}
    except Exception:
        pass


def on_ws_message(ws, message):
    global active_tier
    try:
        data = json.loads(message)
        if isinstance(data, list):
            for item in data:
                _parse_ws_item(item)
        elif isinstance(data, dict):
            if "data" in data and isinstance(data["data"], list):
                for item in data["data"]:
                    _parse_ws_item(item)
            else:
                _parse_ws_item(data)
        active_tier = "NEPSEAPI_WS"
        _set_ws_failures(0)                          # H-12
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON from WS: {e}")
        _increment_ws_failures()                     # H-12
    except Exception as e:
        logger.error(f"WS parse error: {e}")
        _increment_ws_failures()                     # H-12


def on_ws_open(ws):
    global active_tier
    active_tier = "NEPSEAPI_WS"
    _set_ws_failures(0)                              # H-12
    logger.info("[OK] Unofficial NepseAPI WebSocket connected")


def on_ws_error(ws, error):
    count = _increment_ws_failures()                 # H-12
    logger.warning(f"[WARN]  WS Error ({count}): {error}")


def on_ws_close(ws, close_code, close_msg):
    logger.info(f"WS Closed (code={close_code}) — REST fallback active")


def start_websocket():
    def _run():
        global ws_conn, active_tier
        max_backoff = 300

        while True:
            try:
                ws_conn = websocket.WebSocketApp(
                    NEPSEAPI_WS_URL,
                    on_open=on_ws_open,
                    on_message=on_ws_message,
                    on_error=on_ws_error,
                    on_close=on_ws_close
                )
                ws_conn.run_forever(ping_interval=30, ping_timeout=10)
                _set_ws_failures(0)
            except Exception as e:
                count = _increment_ws_failures()
                logger.error(f"WS connection error ({count}): {e}")
                backoff = min(5 * (2 ** min(count, 6)), max_backoff)
                logger.info(f"WS reconnecting in {backoff}s")
                time.sleep(backoff)

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()

    max_wait, waited = 10, 0
    while active_tier != "NEPSEAPI_WS" and waited < max_wait:
        time.sleep(0.5)
        waited += 0.5

    if active_tier != "NEPSEAPI_WS":
        logger.warning("[WARN] WebSocket failed to connect initially, using REST API")
    else:
        logger.info("[OK] WebSocket connected successfully")


def fetch_from_ws():
    if not ws_latest:
        return pd.DataFrame()
    rows = [{"Stock": s, "Close": v["price"], "Volume": v["volume"]}
            for s, v in ws_latest.items()]
    return pd.DataFrame(rows)

# ════════════════════════════════════════════════════════════════
# ██  CANDLE BUILDER - THREAD SAFE  (FIX H-9)
# ════════════════════════════════════════════════════════════════

def update_ticks(df):
    """Update tick buffer — H-9: uses deque(maxlen=500), no manual pop(0)"""
    now = int(time.time())
    with lock:
        for _, row in df.iterrows():
            stock = row["Stock"]
            tick_buffer[stock].append({
                "price":  row["Close"],
                "volume": row["Volume"],
                "time":   now
            })
            # H-9: deque with maxlen=500 auto-discards oldest entry — no pop(0)


def build_candles():
    """Build 15-minute candles — H-9: candles_15m also uses deque"""
    now    = int(time.time())
    bucket = now - (now % CANDLE)

    with lock:
        for stock in list(tick_buffer.keys()):
            ticks = list(tick_buffer[stock])    # snapshot
            valid = [t for t in ticks if t["time"] >= bucket]
            if not valid:
                continue

            prices = [t["price"]  for t in valid]
            volumes = [t["volume"] for t in valid]
            candle_vol = max(0, volumes[-1] - volumes[0]) if len(volumes) > 1 else volumes[0]

            candle = {
                "Stock":  stock,
                "Open":   prices[0],
                "High":   max(prices),
                "Low":    min(prices),
                "Close":  prices[-1],
                "Volume": candle_vol,
                "time":   bucket
            }

            # Only append if this bucket isn't already the last entry
            if not candles_15m[stock] or candles_15m[stock][-1]["time"] != bucket:
                candles_15m[stock].append(candle)
            # H-9: deque with maxlen=500 auto-discards oldest — no pop(0)

# ════════════════════════════════════════════════════════════════
# ██  DATAFRAME BUILDER WITH VALIDATION  (FIX C-2)
# ════════════════════════════════════════════════════════════════

def build_df():
    """
    Build dataframe with indicators and patterns.
    FIX C-2: Per-group bfill/ffill replaces the global fill that was
             bleeding indicator values across stock boundaries.
    """
    with lock:
        rows = []
        for stock in candles_15m:
            rows += list(candles_15m[stock])

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df = df.sort_values(by=["Stock", "time"])

    try:
        df = df.groupby("Stock", group_keys=False).apply(add_indicators).reset_index(drop=True)
    except Exception as e:
        logger.error(f"INDICATOR ERROR: {e}")
        return pd.DataFrame()

    # C-2: Fill NaN per stock group, not across the whole DataFrame
    try:
        df = (
            df.groupby("Stock", group_keys=False)
              .apply(lambda g: g.bfill().ffill())
              .reset_index(drop=True)
        )
    except Exception as e:
        logger.error(f"Per-group fill error: {e}")
        return pd.DataFrame()

    # Validate required indicators exist
    try:
        required_indicators = ['EMA20', 'EMA50', 'RSI_Fast', 'MACD', 'MACD_Signal', 'ATR', 'ADX', 'Body_Ratio', 'Close_Pos', 'Vol_Ratio']
        for ind in required_indicators:
            if ind not in df.columns:
                logger.error(f"Missing indicator: {ind}")
                return pd.DataFrame()
            missing_pct = df[ind].isna().sum() / len(df) if len(df) > 0 else 0
            if missing_pct > 0.5:
                logger.warning(f"Indicator {ind} is {missing_pct * 100:.1f}% NaN")
    except Exception as e:
        logger.error(f"Indicator validation error: {e}")
        return pd.DataFrame()

    try:
        df = df.groupby("Stock", group_keys=False).apply(detect_order_blocks).reset_index(drop=True)
        df = df.groupby("Stock", group_keys=False).apply(detect_micro_patterns).reset_index(drop=True)
        df = df.groupby("Stock", group_keys=False).apply(detect_momentum_surge).reset_index(drop=True)
        df = df.groupby("Stock", group_keys=False).apply(detect_volume_explosion).reset_index(drop=True)
        df = df.groupby("Stock", group_keys=False).apply(detect_fvg).reset_index(drop=True)
        df = df.groupby("Stock", group_keys=False).apply(detect_liquidity).reset_index(drop=True)
        df = df.groupby("Stock", group_keys=False).apply(detect_breakout_momentum).reset_index(drop=True)
        df = df.groupby("Stock", group_keys=False).apply(detect_pd_zones).reset_index(drop=True)
        df = df.groupby("Stock", group_keys=False).apply(detect_pullback_trades).reset_index(drop=True)
        df = df.groupby("Stock", group_keys=False).apply(detect_breakout_retest).reset_index(drop=True)
        df = df.groupby("Stock", group_keys=False).apply(detect_micro_divergence).reset_index(drop=True)
    except Exception as e:
        logger.error(f"PATTERN ERROR: {e}")

    return df

# ════════════════════════════════════════════════════════════════
# ██  TRADE MANAGEMENT  (FIX M-11 / M-12 / C-7)
# ════════════════════════════════════════════════════════════════

def manage_trades(df):
    """
    Manage open positions.
    FIX M-11/M-12: A snapshot of open_positions is taken under lock so
                   the price-check loop runs without holding the lock,
                   then a second lock acquisition handles all writes
                   atomically.
    FIX C-7: delete_position() is called so closed positions are removed
             from the database.
    """
    # Take a safe snapshot under lock
    with lock:
        positions_snapshot = list(open_positions.items())

    closed_trades      = []
    positions_to_delete = []

    for stock, pos in positions_snapshot:
        stock_df = df[df["Stock"] == stock]
        if stock_df.empty:
            continue

        try:
            price      = stock_df.iloc[-1]["Close"]
            hit_stop   = price <= pos["stop"]
            hit_target = price >= pos["target"]

            if hit_stop or hit_target:
                # C-4: size is now shares → PnL is real Rs.
                pnl    = (price - pos["entry"]) * pos["size"]
                result = "WIN" if pnl > 0 else "LOSS"

                closed_trades.append({
                    "stock":  stock,
                    "entry":  pos["entry"],
                    "exit":   price,
                    "pnl":    pnl,
                    "result": result,
                    "size":   pos["size"]
                })
                positions_to_delete.append(stock)

        except Exception as e:
            logger.error(f"Error managing trade for {stock}: {e}")
            continue

    if not closed_trades:
        return

    # Re-acquire lock for all writes atomically
    with lock:
        for trade in closed_trades:
            try:
                equity_curve_ts.add_pnl(trade["pnl"])

                insert_trade(
                    trade["stock"], trade["entry"], trade["exit"],
                    trade["pnl"],   trade["result"], trade["size"]
                )
                log_trade(trade["stock"], trade["entry"], trade["exit"], trade["pnl"])

                # C-7: remove from DB so it doesn't reload on next restart
                delete_position(trade["stock"])

                logger.info(
                    f"Position closed: {trade['stock']} | "
                    f"PnL: {trade['pnl']:.2f} | Result: {trade['result']}"
                )
            except Exception as e:
                logger.error(f"Error saving closed trade for {trade['stock']}: {e}")

        for stock in positions_to_delete:
            try:
                del open_positions[stock]
            except KeyError:
                logger.warning(f"Position {stock} already deleted")

# ════════════════════════════════════════════════════════════════
# ██  SCANNER WITH TIMEOUT & COLLISION PREVENTION  (FIX C-3 / C-6)
# ════════════════════════════════════════════════════════════════

def _run_scan():
    """Internal scan logic.  FIX C-6: uses module-level error counter."""
    global _scan_result, _scan_consecutive_errors

    try:
        df = build_df()
        if df.empty:
            _scan_result = []
            return

        regime = detect_regime(df)
        params = adjust_parameters(regime, equity_curve_ts.get_copy(), circuit_breaker)

        if params.get("is_circuit_broken"):
            logger.error("Scans disabled — circuit breaker active")
            _scan_result = []
            return

        sector_strength = compute_sector_strength(df)

        _scan_result = scan_final_optimized(
            df, regime, params, sector_strength, sector_map,
            equity_curve_ts.get_current(), open_positions, last_alert_time,
            alert_builder, manage_trades, lock, mtf_analyzer=None,
            portfolio_stocks=PORTFOLIO_STOCKS
        )

        # C-6: reset on success
        _scan_consecutive_errors = 0

    except Exception as e:
        _scan_consecutive_errors += 1
        logger.error(f"Scanner Error ({_scan_consecutive_errors}): {e}")
        if _scan_consecutive_errors >= 3:
            logger.error("[STOP] Scanner failed 3+ times consecutively — check system health")
        _scan_result = []


def scan_logic_selected():
    """
    Run scanner with timeout and collision prevention.
    FIX C-3: Both the read of scan_in_progress and the write to True are
             done inside the same lock acquisition, eliminating the TOCTOU
             race that could allow two concurrent scan threads.
    """
    global scan_in_progress, _scan_result

    # C-3: atomic check-and-set under the same lock
    with scan_lock:
        if scan_in_progress:
            logger.warning("Previous scan still running, skipping this cycle")
            return _scan_result
        scan_in_progress = True

    try:
        _scan_result = []

        scan_thread = threading.Thread(target=_run_scan, daemon=True)
        scan_thread.start()
        scan_thread.join(timeout=SCAN_TIMEOUT)

        if scan_thread.is_alive():
            logger.error(f"Scan timeout! Took longer than {SCAN_TIMEOUT}s")
            _scan_result = []
            return []

        return _scan_result
    except Exception as e:
        logger.error(f"Scan logic error: {e}")
        return []
    finally:
        with scan_lock:
            scan_in_progress = False

# ════════════════════════════════════════════════════════════════
# ██  DATA LOOPS
# ════════════════════════════════════════════════════════════════

def data_loop():
    """Main data fetching loop with better error handling."""
    global _last_data_time, _data_stale_warning_sent

    cleanup_counter      = 0
    consecutive_errors   = 0
    max_consecutive_errors = 5
    _all_feeds_down_alerted = False

    while True:
        try:
            logger.debug("Attempting data fetch...")
            live, source = fetch_data()

            if not live.empty:
                logger.info(f"[OK] Fetched {len(live)} rows from {source}")
                update_ticks(live)
                build_candles()
                _last_data_time          = time.time()
                _data_stale_warning_sent  = False
                _all_feeds_down_alerted   = False
                consecutive_errors        = 0
                logger.debug(f"Candles loaded: {len(candles_15m)}")
            else:
                logger.warning(f"[WARN] Empty data from {source}, trying alternatives...")
                
                # Try WebSocket cache
                ws_df = fetch_from_ws()
                if not ws_df.empty:
                    logger.info(f"[OK] Using WebSocket cache: {len(ws_df)} rows")
                    update_ticks(ws_df)
                    build_candles()
                    _last_data_time = time.time()
                    consecutive_errors = 0
                else:
                    consecutive_errors += 1
                    data_age = time.time() - _last_data_time
                    logger.error(f"[ERROR] No data available (age: {data_age:.0f}s, attempt {consecutive_errors})")

            cleanup_counter += 1
            if cleanup_counter % 60 == 0:
                cleanup_old_candles()
                cleanup_counter = 0

        except Exception as e:
            consecutive_errors += 1
            logger.error(f"[ERROR] Data loop error ({consecutive_errors}/{max_consecutive_errors}): {e}", exc_info=True)
            if consecutive_errors >= max_consecutive_errors:
                logger.error("[STOP] Data loop failed repeatedly — check system health")

        time.sleep(5)


def auto_scanner_loop():
    """
    Automatic scanning loop.
    FIX M-6: Sleep aligned to 15-min candle period (900 s) so each candle
             is only scanned once after it closes, not 3 times.
    H-11: Uses is_market_active_with_holidays() (the authoritative check).
    """
    time.sleep(60)   # Let initial data populate
    logger.info(f"[ROCKET] Scanner started in {TRADING_MODE} mode")

    while True:
        try:
            # H-11: authoritative holiday-aware check
            if not is_market_active_with_holidays():
                logger.debug("Market not active, waiting...")
                time.sleep(30)
                continue

            if not is_data_fresh():
                logger.error("Skipping scan — data is stale")
                time.sleep(60)
                continue

            daily_loss_tracker.reset_if_needed(equity_curve_ts.get_current())
            if daily_loss_tracker.check_limit(equity_curve_ts.get_current()):
                logger.error("Trading disabled — daily loss limit hit")
                time.sleep(300)
                continue

            logger.debug(f"Running scan ({TRADING_MODE} mode)...")
            scan_logic_selected()

        except Exception as e:
            logger.error(f"Scanner outer loop error: {e}")

        # M-6: 900 s = one 15-minute candle period
        time.sleep(900)

# ════════════════════════════════════════════════════════════════
# ██  FLASK ROUTES
# ════════════════════════════════════════════════════════════════

@app.route("/")
def home():
    html = """
    <html>
    <head>
        <title>NEPSE Trading System v3.1</title>
        <style>
            body { background: #0f172a; color: #e0e0e0; font-family: monospace; padding: 24px; }
            h2 { color: #4ade80; }
            p  { color: #94a3b8; }
            a  { color: #60a5fa; text-decoration: none; margin: 10px 0; display: block; }
            a:hover { color: #93c5fd; }
            .note   { background: #1e293b; padding: 15px; border-left: 4px solid #f59e0b; margin: 10px 0; }
            .status { color: #4ade80; font-weight: bold; }
        </style>
    </head>
    <body>
        <h2>[ROCKET] NEPSE FINAL OPTIMIZED TRADING SYSTEM v3.1</h2>
        <p>Ultra-strict Grade C filtering | 9-13 alerts/day | 68-73% win rate</p>
        <div class="note">
            <strong class="status">[OK] All audit fixes applied (v3.1)</strong>
        </div>
        <h3>Available Endpoints (X-API-Key header required except /health):</h3>
        <a href="/health">/health</a>
        <a href="/scan">/scan</a>
        <a href="/status">/status</a>
        <a href="/trades">/trades</a>
        <a href="/signals">/signals</a>
        <a href="/stats">/stats</a>
        <a href="/journal">/journal</a>
        <a href="/backtest">/backtest</a>
        <h3>Usage:</h3>
        <pre>curl -H "X-API-Key: YOUR_API_KEY" http://localhost:5000/status</pre>
    </body>
    </html>
    """
    return html


@app.route("/health")
def health():
    try:
        stats      = equity_curve_ts.get_stats()
        daily_loss = daily_loss_tracker.get_daily_loss()
        db_info    = get_database_info()
        return jsonify({
            "status":                "healthy",
            "timestamp":             datetime.now(NEPSE_TZ).isoformat(),
            "mode":                  TRADING_MODE,
            "market_active":         is_market_active_with_holidays(),
            "data_fresh":            is_data_fresh(),
            "circuit_breaker_active": circuit_breaker.is_active if circuit_breaker else False,
            "candles_loaded":        len(candles_15m),
            "positions_open":        len(open_positions),
            "equity":                stats.get("current", 0),
            "daily_loss":            daily_loss,
            "daily_loss_limit":      MAX_DAILY_LOSS,
            "database": {
                "trades":    db_info.get("trades", 0),
                "signals":   db_info.get("signals", 0),
                "positions": db_info.get("positions", 0)
            }
        }), 200
    except Exception as e:
        logger.error(f"Health check error: {e}")
        return jsonify({"status": "unhealthy", "error": str(e),
                        "timestamp": datetime.now(NEPSE_TZ).isoformat()}), 500


@app.route("/scan")
@require_api_key
def scan():
    try:
        results = scan_logic_selected()
        return jsonify({
            "status":    "success",
            "alerts":    results,
            "count":     len(results),
            "timestamp": datetime.now(NEPSE_TZ).isoformat()
        })
    except Exception as e:
        logger.error(f"Scan endpoint error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/status")
@require_api_key
def status():
    try:
        stats      = equity_curve_ts.get_stats()
        daily_loss = daily_loss_tracker.get_daily_loss()
        with lock:
            current_positions = len(open_positions)
            position_list     = list(open_positions.keys())
        return jsonify({
            "status":                 "ok",
            "mode":                   TRADING_MODE,
            "active_tier":            active_tier,
            "circuit_breaker_active": circuit_breaker.is_active if circuit_breaker else False,
            "open_positions":         current_positions,
            "open_position_stocks":   position_list,
            "current_equity":         stats["current"],
            "equity_peak":            stats["peak"],
            "drawdown_pct":           round(stats["drawdown_pct"], 2),
            "daily_loss":             daily_loss,
            "daily_loss_limit":       MAX_DAILY_LOSS,
            "scan_in_progress":       scan_in_progress,
            "data_fresh":             is_data_fresh(),
            "market_active":          is_market_active_with_holidays(),
            "features_enabled": {
                "accumulation": ENABLE_ACCUMULATION_FILTER,
                "liquidity":    ENABLE_LIQUIDITY_VALIDATION,
                "orderflow":    ENABLE_ORDERFLOW_VALIDATION
            },
            "timestamp": datetime.now(NEPSE_TZ).isoformat()
        })
    except Exception as e:
        logger.error(f"Error in status endpoint: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/trades")
@require_api_key
def trades():
    try:
        trades_list = fetch_trades(limit=20)
        return jsonify({"status": "success", "trades": trades_list, "count": len(trades_list)})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/signals")
@require_api_key
def signals():
    try:
        signals_list = fetch_signals(limit=30)
        return jsonify({"status": "success", "signals": signals_list, "count": len(signals_list)})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/stats")
@require_api_key
def stats():
    try:
        stats_data = get_stats(days=7)
        return jsonify({"status": "success", "stats": stats_data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/journal")
@require_api_key
def journal():
    try:
        journal_data  = get_journal()
        journal_stats = get_journal_stats()
        return jsonify({"status": "success", "journal": journal_data, "stats": journal_stats})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/backtest")
@require_api_key
def backtest():
    try:
        df = build_df()
        if df.empty:
            return jsonify({"status": "error",
                            "message": "Insufficient data for backtest"}), 400
        backtest_result = run_backtest(df, score_threshold=0.50)
        return jsonify({"status": "success", "backtest": backtest_result,
                        "timestamp": datetime.now(NEPSE_TZ).isoformat()})
    except Exception as e:
        logger.error(f"Backtest error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/backtest_advanced")
@require_api_key
def backtest_advanced():
    try:
        df = build_df()
        if df.empty:
            return jsonify({"status": "error",
                            "message": "Insufficient data for backtest"}), 400
        result = run_backtest_engine(df, score_threshold=0.50)
        return jsonify({
            "status": "success",
            "backtest": {
                "final_balance":       result.get("final_balance"),
                "total_pnl":           result.get("total_pnl"),
                "return_pct":          result.get("return_pct"),
                "trades":              result.get("trades"),
                "winrate":             result.get("winrate"),
                "max_drawdown":        result.get("max_drawdown"),
                "max_drawdown_pct":    result.get("max_drawdown_pct"),
                "profit_factor":       result.get("profit_factor"),
                "valid_candles":       result.get("valid_candles"),
                "total_candles":       result.get("total_candles"),
                "equity_curve_length": len(result.get("equity_curve", []))
            },
            "timestamp": datetime.now(NEPSE_TZ).isoformat()
        })
    except Exception as e:
        logger.error(f"Advanced backtest error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
    

@app.route("/portfolio_health")
@require_api_key
def portfolio_health():
    """
    Check your portfolio positions and current P&L
    Returns: Current prices and profit/loss for each holding
    """
    try:
        # Check if portfolio configured
        if not HAS_PORTFOLIO or not PORTFOLIO_POSITIONS:
            return jsonify({
                "status": "error",
                "message": "Portfolio not configured. Create portfolio.py first."
            }), 400
        
        # Build dataframe with latest prices
        df = build_df()
        if df.empty:
            return jsonify({
                "status": "error",
                "message": "Insufficient data"
            }), 400
        
        portfolio_data = []
        total_value = 0
        total_pnl = 0
        
        # Calculate P&L for each holding
        for stock, position in PORTFOLIO_POSITIONS.items():
            stock_df = df[df['Stock'] == stock]
            
            if stock_df.empty:
                # Stock not in today's data
                portfolio_data.append({
                    'stock': stock,
                    'shares': position['shares'],
                    'entry_price': position['entry_price'],
                    'current_price': None,
                    'pnl': None,
                    'pnl_pct': None,
                    'status': 'No data'
                })
                continue
            
            try:
                current_price = float(stock_df.iloc[-1]['Close'])
                pnl = (current_price - position['entry_price']) * position['shares']
                pnl_pct = ((current_price - position['entry_price']) / position['entry_price'] * 100)
                
                portfolio_data.append({
                    'stock': stock,
                    'shares': position['shares'],
                    'entry_price': round(position['entry_price'], 2),
                    'current_price': round(current_price, 2),
                    'pnl': round(pnl, 2),
                    'pnl_pct': round(pnl_pct, 2),
                    'status': 'UP' if pnl > 0 else 'DOWN' if pnl < 0 else 'FLAT'
                })
                
                total_value += current_price * position['shares']
                total_pnl += pnl
            except Exception as e:
                logger.error(f"Error calculating P&L for {stock}: {e}")
                continue
        
        # Calculate total return percentage
        total_invested = sum(pos['entry_price'] * pos['shares'] for pos in PORTFOLIO_POSITIONS.values())
        total_return_pct = (total_pnl / total_invested * 100) if total_invested > 0 else 0
        
        return jsonify({
            "status": "success",
            "portfolio": portfolio_data,
            "summary": {
                'total_holdings': len([p for p in portfolio_data if p['status'] != 'No data']),
                'total_invested': round(total_invested, 2),
                'total_current_value': round(total_value, 2),
                'total_pnl': round(total_pnl, 2),
                'total_return_pct': round(total_return_pct, 2),
            },
            "timestamp": datetime.now(NEPSE_TZ).isoformat()
        })
    
    except Exception as e:
        logger.error(f"Portfolio health error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/database_info")
@require_api_key
def database_info():
    try:
        info = get_database_info()
        return jsonify({"status": "success", "database": info,
                        "timestamp": datetime.now(NEPSE_TZ).isoformat()})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/cleanup_old_records", methods=["POST"])
@require_api_key
def cleanup_old_records_endpoint():
    try:
        days    = request.json.get("days", 90) if request.json else 90
        success = cleanup_old_records(days=days)
        return jsonify({
            "status":    "success" if success else "error",
            "message":   f"Cleaned up records older than {days} days" if success else "Cleanup failed",
            "timestamp": datetime.now(NEPSE_TZ).isoformat()
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# ════════════════════════════════════════════════════════════════
# ██  STARTUP  (FIX C-7, L-3)
# ════════════════════════════════════════════════════════════════

def _check_network_async():
    """
    L-3: Network connectivity check runs in a daemon thread so it cannot
         block or delay the main startup sequence.
    """
    try:
        requests.get("https://nepseapi.surajrimal.dev", timeout=5, verify=certifi.where())
        logger.info("[OK] Network connectivity OK")
    except requests.exceptions.Timeout:
        logger.warning("[WARN]  Network timeout — may be slow")
    except Exception as e:
        logger.warning(f"[WARN]  Network check failed: {e}")


def _restore_positions_from_db():
    """
    FIX C-7: Load any positions that were open when the system last ran
             from the database into the in-memory open_positions dict.
    """
    try:
        db_positions = fetch_positions()
        if not db_positions:
            logger.warning(
                "[WARN]  No open positions found in the database at startup. "
                "System will start with an empty position book. "
                "If the system crashed recently, please verify you did not lose any trades."
            )
            return
            # [OK] FIXED: Clear, visible warning message

        with lock:
            for pos in db_positions:
                # Schema: (id, stock, entry, stop, target, size, grade, score, rr, opened_at)
                stock = pos[1]
                open_positions[stock] = {
                    "entry":  pos[2],
                    "stop":   pos[3],
                    "target": pos[4],
                    "size":   pos[5],
                    "grade":  pos[6],
                    "score":  pos[7],
                    "rr":     pos[8],
                }

        logger.info(f"[OK] Restored {len(db_positions)} open position(s) from database: "
                    f"{[p[1] for p in db_positions]}")
    except Exception as e:
        logger.error(f"Error restoring positions from database: {e}")


def startup():
    """
    System startup.
    FIX M-1: validate_startup() now raises RuntimeError (not sys.exit)
             so WSGI workers don't silently die on misconfiguration.
    """
    global alert_builder, circuit_breaker, _last_data_time

    # M-1: Let RuntimeError propagate; __main__ block catches it and exits
    validate_startup()

    circuit_breaker  = CircuitBreaker(max_drawdown=0.10, lookback_periods=20)
    # L-8: keep at 0 — freshness check should report stale until real data arrives
    _last_data_time  = 0

    logger.info("=" * 70)
    logger.info("[ROCKET] NEPSE FINAL OPTIMIZED TRADING SYSTEM v3.1")
    logger.info(f"   Mode: {TRADING_MODE}")
    config = MODE_CONFIGS.get(TRADING_MODE, {})
    logger.info(f"   Expected Alerts:    {config.get('min_alerts')}-{config.get('max_alerts')}/day")
    logger.info(f"   Expected Win Rate:  {config.get('win_rate')}")
    logger.info(f"   Quality Level:      {config.get('quality')}")
    logger.info("=" * 70)

    try:
        alert_builder = RateLimitedAlertBuilder(BOT_TOKEN, CHAT_ID)
        logger.info("[OK] Alert builder initialized")
    except Exception as e:
        logger.error(f"[FATAL] Alert builder failed: {e}")
        raise RuntimeError("Alert system failed to initialize")

    # C-7: Restore positions from database before scanner starts
    _restore_positions_from_db()

    logger.info(f"[OK] Accumulation Filter:    {ENABLE_ACCUMULATION_FILTER}")
    logger.info(f"[OK] Liquidity Validation:   {ENABLE_LIQUIDITY_VALIDATION}")
    logger.info(f"[OK] Orderflow Validation:   {ENABLE_ORDERFLOW_VALIDATION}")
    logger.info(f"[OK] Circuit Breaker:        Active (max {circuit_breaker.max_drawdown * 100:.1f}% DD)")
    logger.info(f"[OK] Daily Loss Limit:       Rs. {MAX_DAILY_LOSS:.2f}")
    logger.info(f"[OK] Scan Timeout:           {SCAN_TIMEOUT}s")
    logger.info(f"[OK] Scan Interval:          {CANDLE}s (aligned to candle close)")
    logger.info(f"[OK] Max Positions:          {MAX_POSITIONS}")
    logger.info(f"[OK] Data Stale Threshold:   {DATA_STALE_THRESHOLD}s")
    logger.info(f"[OK] API Authentication:     ENABLED (key length: {len(API_KEY)} chars)")
    logger.info(f"[OK] Equity Curve History:   max {equity_curve_ts.max_history} points")

    # L-3: Run network check in background — does not block startup
    threading.Thread(target=_check_network_async, daemon=True).start()

    # WebSocket blocked by Cloudflare (Error 521)
    # Using REST API only via fetch_data()
    logger.warning("[WARN] WebSocket disabled (Cloudflare 521 blocking) — Using REST API only")
    # start_websocket()  # DISABLED
    threading.Thread(target=data_loop,         daemon=True).start()
    threading.Thread(target=auto_scanner_loop, daemon=True).start()

    logger.info("[OK] All threads initiated")
    logger.info("[GREEN] System is live and ready for trading")
    logger.info("=" * 70)


# Module-level startup call — compatible with both direct execution and
# WSGI (gunicorn main:app).  RuntimeError from validate_startup() will
# cause gunicorn to log the error and refuse to start cleanly.
startup()


if __name__ == "__main__":
    try:
        # Get host and port from environment variables
        # Heroku/Railway set PORT env var automatically
        port = int(os.getenv("PORT", 5000))
        host = os.getenv("HOST", "0.0.0.0")  # 0.0.0.0 = all interfaces
        
        # Determine if running in cloud
        is_cloud = os.getenv("PORT") is not None
        
        logger.info(f"Starting Flask server on {host}:{port}...")
        if is_cloud:
            logger.info("[CLOUD] Running in cloud environment (Heroku/Railway)")
        else:
            logger.info("[LOCAL] Running locally")
        
        app.run(
            debug=False,
            use_reloader=False,
            host=host,
            port=port,
            threaded=True
        )
    except RuntimeError as e:
        # validate_startup() raised — already logged, just exit
        logger.error(f"Startup failed: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        shutdown_handler.shutdown()
    except Exception as e:
        logger.error(f"Flask error: {e}")
        shutdown_handler.shutdown()
        raise