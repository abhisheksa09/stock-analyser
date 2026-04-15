"""
auto_login.py — Daily Telegram login reminder

Sends a Telegram message at 08:30 IST every day with a one-tap Upstox
login link. Tapping it on your phone opens the Upstox OAuth flow and
sets the scanner token automatically — no copy-pasting needed.

No additional env vars required beyond what is already configured:
  TELEGRAM_BOT_TOKEN  — already used by the alert scanner
  TELEGRAM_CHAT_ID    — already used by the alert scanner

Called automatically by APScheduler at 08:30 IST every weekday.
Also callable on-demand via  POST /auth/send-login-reminder  (admin only).
"""

import os
import logging

log = logging.getLogger("auto_login")

LOGIN_URL = "https://nse-proxy-mojx.onrender.com/auth/login"


def send_login_reminder() -> tuple:
    """
    Send a Telegram message with the one-tap Upstox login link.
    Returns (success: bool, message: str).
    Imports scanner.send_telegram to reuse the existing Telegram setup.
    """
    # Import here to avoid circular imports at module load time
    import scanner

    text = (
        "<b>Good morning! Activate the scanner</b>\n\n"
        f'<a href="{LOGIN_URL}">Tap here to login to Upstox</a>\n\n'
        "Market opens in ~45 min. Once you log in, the scanner "
        "starts automatically at 09:15 IST."
    )

    ok = scanner.send_telegram(text, parse_mode="HTML")
    if ok:
        log.info("Login reminder sent via Telegram")
        return True, "Reminder sent successfully"
    else:
        msg = "Failed to send Telegram message — check TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID"
        log.error(msg)
        return False, msg


def is_configured() -> bool:
    """Return True if Telegram is configured (reminder will work)."""
    return bool(
        os.environ.get("TELEGRAM_BOT_TOKEN", "").strip() and
        os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    )
