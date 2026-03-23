#!/usr/bin/env python3
"""
token_refresh.py — Daily Upstox token renewal helper
Version: v1.0.0

This script runs as a Render Cron Job every morning at 8:00 AM IST
(2:30 AM UTC). It:
  1. Checks if today's token already exists in DB
  2. If not, sends a Telegram message to the admin with the OAuth URL
  3. After admin completes OAuth, the token is stored automatically

To set up as Render Cron Job:
  Command: python token_refresh.py
  Schedule: 30 2 * * *  (2:30 AM UTC = 8:00 AM IST)

Alternatively, admin opens Admin tab → "Refresh Upstox Token" each morning.
"""

import os, urllib.request, json, logging
from datetime import datetime, timezone, timedelta

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("token_refresh")

IST = timezone(timedelta(hours=5, minutes=30))

def send_telegram(message: str):
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id   = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not bot_token or not chat_id:
        log.warning("Telegram not configured")
        return False
    payload = json.dumps({
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML"
    }).encode()
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{bot_token}/sendMessage",
        data=payload,
        headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status == 200
    except Exception as e:
        log.error("Telegram send failed: %s", e)
        return False

def check_token_exists():
    """Check if today's token is in DB."""
    try:
        import db as _db
        tok = _db.get_token()
        return bool(tok)
    except Exception as e:
        log.error("DB check failed: %s", e)
        return False

def main():
    now_ist = datetime.now(IST)
    log.info("Token refresh check at %s IST", now_ist.strftime("%Y-%m-%d %H:%M"))

    if check_token_exists():
        log.info("Token already set for today. No action needed.")
        return

    # No token — notify admin via Telegram
    base_url = os.environ.get("RENDER_EXTERNAL_URL", "https://nse-proxy-mojx.onrender.com")
    auth_url  = f"{base_url}/auth/login"
    message   = (
        f"🔐 <b>NSE Scanner — Daily Token Renewal</b>\n\n"
        f"No Upstox token found for today ({now_ist.strftime('%d %b %Y')}).\n\n"
        f"Please complete OAuth login to enable live scanning:\n"
        f"<a href=\"{auth_url}\">{auth_url}</a>\n\n"
        f"This takes ~30 seconds. Alerts will not fire until token is set."
    )
    sent = send_telegram(message)
    log.info("Telegram reminder sent: %s", sent)

if __name__ == "__main__":
    main()
