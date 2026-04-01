"""
Alert Management and Telegram Notifications
FIXED: Rate limiting, proper error handling, timeout
IMPROVED: Safe data access, stable formatting, no Unicode issues
"""

import requests
import logging
import threading
import time
from collections import deque
from datetime import datetime
import certifi
import pytz

logger = logging.getLogger(__name__)
NEPSE_TZ = pytz.timezone('Asia/Kathmandu')


class RateLimitedAlertBuilder:
    """Alert builder with rate limiting"""
    
    def __init__(self, bot_token, chat_id):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{bot_token}"
        
        # Rate limiting: 25 messages per second
        self.max_messages_per_sec = 25
        self.message_times = deque(maxlen=self.max_messages_per_sec)
        self.rate_limit_lock = threading.Lock()
    
    def _check_rate_limit(self):
        """Check and enforce rate limit"""
        with self.rate_limit_lock:
            now = time.time()
            
            while self.message_times and self.message_times[0] < now - 1:
                self.message_times.popleft()
            
            if len(self.message_times) >= self.max_messages_per_sec:
                sleep_time = 1 - (now - self.message_times[0])
                if sleep_time > 0:
                    logger.debug(f"Rate limit: sleeping {sleep_time:.2f}s")
                    time.sleep(sleep_time)
                    now = time.time()
            
            self.message_times.append(now)
    
    def send_detailed_alert(self, trade_data):
        """Send detailed alert with rate limiting"""
        try:
            self._check_rate_limit()
            
            msg = self._build_message(trade_data)
            self._send_telegram(msg)
        except Exception as e:
            logger.error(f"Error sending alert: {e}")
    
    def _build_message(self, data):
        """Build enhanced alert message with new validations"""
        try:
            # SAFE extraction
            grade = data.get('grade', '?')
            score = data.get('score', 0)

            grade_icon = {
                "A": "[A+]",
                "B": "[B+]",
                "C": "[C]",
                "D": "[D]",
                "F": "[F]"
            }.get(grade, "[?]")

            # Validation indicators
            liq_indicator = "[OK]" if data.get('liquidity_score', 0) > 0.5 else "[WARN]"
            of_indicator = "[OK]" if data.get('orderflow_score', 0) > 0.15 else "[WARN]"

            entry = data.get('entry', 0)
            target = data.get('target', 0)
            stop = data.get('stop', 0)

            # Safe calculations
            try:
                target_pct = ((target - entry) / entry * 100) if entry else 0
                stop_pct = ((entry - stop) / entry * 100) if entry else 0
            except:
                target_pct = 0
                stop_pct = 0

            msg = f"""
{grade_icon} <b>[{grade}] {data.get('stock', 'N/A')}</b>

<b>Entry Setup:</b>
Entry: Rs. {entry:.2f}
Target: Rs. {target:.2f} (+{target_pct:.2f}%)
Stop: Rs. {stop:.2f} (-{stop_pct:.2f}%)
RR: {data.get('rr', 'N/A')}

<b>Signal Quality:</b>
Score: {score}/1.0
Confluence: {data.get('confluence', 'N/A')}
Patterns: {', '.join(data.get('patterns', [])[:4])}

<b>Validations:</b>
{liq_indicator} Liquidity: {data.get('liquidity_score', 'N/A')}
{of_indicator} Orderflow: {data.get('orderflow_score', 'N/A')}
[INFO] Accumulation: {data.get('accumulation', 'N/A')}

<b>Market Context:</b>
Regime: {data.get('regime', 'N/A')}
Sector: {data.get('sector', 'N/A')} ({data.get('sector_strength', 0):.2f})

[WARN] <b>Manual execution required.</b>
Check market depth and spread before entry.
"""
            return msg

        except Exception as e:
            logger.error(f"Error building message: {e}")
            return "[ERROR] Failed to build alert"
    
    def _send_telegram(self, msg):
        """Send telegram message with timeout"""
        try:
            r = requests.post(
                f"{self.base_url}/sendMessage",
                data={
                    "chat_id": self.chat_id,
                    "text": msg,
                    "parse_mode": "HTML"
                },
                timeout=5,
                verify=certifi.where()
            )
            
            if r.status_code != 200:
                logger.error(f"Telegram error: {r.status_code} - {r.text}")
                return False
            
            logger.debug("Telegram alert sent successfully")
            return True

        except requests.exceptions.Timeout:
            logger.error("Telegram request timeout")
            return False
        except Exception as e:
            logger.error(f"Telegram error: {e}")
            return False


def send_system_message(bot_token, chat_id, title, message):
    """Send system message with error handling"""
    try:
        base_url = f"https://api.telegram.org/bot{bot_token}"
        msg = f"<b>{title}</b>\n\n{message}"
        
        r = requests.post(
            f"{base_url}/sendMessage",
            data={
                "chat_id": chat_id,
                "text": msg,
                "parse_mode": "HTML"
            },
            timeout=5,
            verify=certifi.where()
        )
        
        if r.status_code != 200:
            logger.error(f"System message error: {r.status_code}")
            return False
        
        return True

    except Exception as e:
        logger.error(f"Error sending system message: {e}")
        return False