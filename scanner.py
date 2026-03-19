"""
scanner.py — Scheduled NSE alert scanner with Telegram Bot API
Version : v1.8.0
Runs inside the same Render.com Flask process via APScheduler.

Triggers Telegram alerts when:
  1. Stock hits Green Ready for the first time this session
  2. Confidence crosses 75% (was below, now above)
  3. Signal reversal detected

Environment variables (set in Render dashboard):
  UPSTOX_TOKEN          — set each morning via /set-token-form
  TELEGRAM_BOT_TOKEN    — from @BotFather  e.g. 7123456789:AAF-xxxxx
  TELEGRAM_CHAT_ID      — your personal chat ID  e.g. 123456789
  SCAN_SYMBOLS          — comma-separated symbols (default: all 30)
  ALERT_START_IST       — HH:MM  (default: 09:15)
  ALERT_STOP_IST        — HH:MM  (default: 10:30)
  SCAN_INTERVAL_MINS    — integer (default: 5)
"""

import os
import time
import json
import logging
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timezone, timedelta

from signals import STOCKS, build_setup, get_ltp, get_intraday, get_daily, is_ready, get_market_context
from macro import get_full_macro_context, apply_all_macro_penalties

log = logging.getLogger("scanner")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [scanner] %(message)s")

IST = timezone(timedelta(hours=5, minutes=30))

# ─── Session state ────────────────────────────────────────────────────────────

class SessionState:
    def __init__(self):
        self.date       = None
        self.locked_sig = {}   # sym -> first signal of session
        self.macro_ctx  = None  # refreshed every 30 min
        self.macro_fetched_at = None  # IST datetime of last macro fetch
        self.prev_conf  = {}   # sym -> confidence at previous scan
        self.alerted    = set()
        self.macro_ctx  = None
        self.macro_fetched_at = None
        self._reset()

    def _reset(self):
        self.date       = datetime.now(IST).strftime("%Y-%m-%d")
        self.locked_sig = {}
        self.prev_conf  = {}
        self.alerted    = set()
        log.info("Session state reset for %s", self.date)

    def check_date(self):
        today = datetime.now(IST).strftime("%Y-%m-%d")
        if today != self.date:
            self._reset()

    def already_alerted(self, sym, kind):
        return f"{sym}:{kind}" in self.alerted

    def mark_alerted(self, sym, kind):
        self.alerted.add(f"{sym}:{kind}")

STATE = SessionState()

# ─── Token store ──────────────────────────────────────────────────────────────

_token = {"value": os.environ.get("UPSTOX_TOKEN", "")}

def get_token():    return _token["value"]
def set_token(tok): _token["value"] = tok.strip(); log.info("Upstox token updated")

# ─── Telegram Bot API ─────────────────────────────────────────────────────────

TELEGRAM_API = "https://api.telegram.org"

def send_telegram(text: str, parse_mode: str = "HTML") -> bool:
    """
    Send a message via Telegram Bot API.
    Uses sendMessage endpoint with HTML parse mode for bold/code formatting.

    Requires env vars:
      TELEGRAM_BOT_TOKEN  — from @BotFather
      TELEGRAM_CHAT_ID    — your chat ID (get from /getUpdates after sending /start to bot)
    """
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id   = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

    if not bot_token or not chat_id:
        log.warning("Telegram not configured — set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID")
        return False

    url     = f"{TELEGRAM_API}/bot{bot_token}/sendMessage"
    payload = json.dumps({
        "chat_id":    chat_id,
        "text":       text,
        "parse_mode": parse_mode,
        # Disable link previews so messages look clean
        "link_preview_options": {"is_disabled": True},
    }).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "User-Agent":   "NSEScanner/1.0",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            resp = json.loads(r.read())
            if resp.get("ok"):
                log.info("Telegram sent: %s", text[:60])
                return True
            else:
                log.error("Telegram API error: %s", resp.get("description"))
                return False
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        log.error("Telegram HTTP %d: %s", e.code, body[:200])
        return False
    except Exception as e:
        log.error("Telegram send failed: %s", e)
        return False

def get_telegram_chat_id(bot_token: str) -> dict:
    """
    Helper: fetch recent updates to find the chat ID.
    Returns the parsed JSON from getUpdates.
    """
    url = f"{TELEGRAM_API}/bot{bot_token}/getUpdates"
    req = urllib.request.Request(url, headers={"User-Agent": "NSEScanner/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())
    except Exception as e:
        return {"ok": False, "error": str(e)}

# ─── Message formatter (HTML for Telegram) ────────────────────────────────────

def _pnl_line(s: dict) -> str:
    gain = round(abs(s["tg"] - s["en"]), 2)
    risk = round(abs(s["sl"] - s["en"]), 2)
    return f"+Rs{gain}" if s["sig"] == "BUY" else f"-Rs{gain}", f"-Rs{risk}"

def format_alert(kind: str, s: dict, extra: str = "") -> str:
    ist_time = datetime.now(IST).strftime("%H:%M IST")
    gain     = round(abs(s["tg"] - s["en"]), 2)
    risk     = round(abs(s["sl"] - s["en"]), 2)
    chg_str  = f"{s['chg']:+.2f}%"

    if kind == "green_ready":
        sig_emoji = "🟢" if s["sig"] == "BUY" else "🔴"
        ctx       = s.get("market_ctx", {})
        warnings  = s.get("ctx_warnings", [])
        nifty_str = (f"Nifty: {ctx['nifty_chg']:+.1f}%  "
                     f"Sector: {ctx['sector_chg']:+.1f}%")  if ctx else ""
        warn_str  = ("\n⚠️ " + "  |  ".join(warnings)) if warnings else ""
        return (
            f"{sig_emoji} <b>NSE SCANNER — READY TO TRADE</b>\n"
            f"\n"
            f"<b>{s['sym']}</b>  <code>{s['sec']}</code>\n"
            f"Signal  : <b>{s['sig']}</b>  |  Conf: <b>{s['conf']}%</b>\n"
            f"\n"
            f"Entry   : <code>Rs {s['en']}</code>\n"
            f"Target  : <code>Rs {s['tg']}</code>  (+Rs {gain})\n"
            f"Stop SL : <code>Rs {s['sl']}</code>  (-Rs {risk})\n"
            f"R:R     : <b>{s['rr']}:1</b>\n"
            f"\n"
            f"LTP     : Rs {s['ltp']} ({chg_str})\n"
            f"Market  : {nifty_str}\n"
            f"Setup   : {s['reason']}\n"
            f"{warn_str}\n"
            f"\n"
            f"⏰ {ist_time}"
        )

    elif kind == "conf_crossed":
        return (
            f"📈 <b>NSE SCANNER — CONFIDENCE CROSSED 75%</b>\n"
            f"\n"
            f"<b>{s['sym']}</b>  <code>{s['sec']}</code>\n"
            f"Signal  : <b>{s['sig']}</b>\n"
            f"Conf    : <b>{s['conf']}%</b>  (was {extra}%)\n"
            f"\n"
            f"Entry   : <code>Rs {s['en']}</code>\n"
            f"SL      : <code>Rs {s['sl']}</code>\n"
            f"LTP     : Rs {s['ltp']} ({chg_str})\n"
            f"\n"
            f"⏰ {ist_time}"
        )

    elif kind == "reversal":
        return (
            f"⚡ <b>NSE SCANNER — SIGNAL REVERSAL</b>\n"
            f"\n"
            f"<b>{s['sym']}</b>  <code>{s['sec']}</code>\n"
            f"Was     : <b>{extra}</b>  →  Now: <b>{s['sig']}</b>\n"
            f"LTP     : Rs {s['ltp']} ({chg_str})\n"
            f"\n"
            f"⚠️ Conflicting signals — <b>skip this trade today</b>\n"
            f"\n"
            f"⏰ {ist_time}"
        )

    return f"NSE Scanner: {s['sym']} {kind} @ {ist_time}"

# ─── IST helpers ──────────────────────────────────────────────────────────────

def ist_now_mins():
    n = datetime.now(IST)
    return n.hour * 60 + n.minute

def parse_hhmm(s, default):
    try:
        h, m = s.split(":")
        return int(h) * 60 + int(m)
    except Exception:
        return default

# ─── Core scan ────────────────────────────────────────────────────────────────

def run_scan():
    """Called every N minutes by APScheduler."""
    STATE.check_date()

    token = get_token()
    if not token:
        log.info("No Upstox token set — skipping scan")
        return

    start = parse_hhmm(os.environ.get("ALERT_START_IST", "09:15"), 555)
    stop  = parse_hhmm(os.environ.get("ALERT_STOP_IST",  "10:30"), 630)
    mins  = ist_now_mins()

    if not (start <= mins <= stop):
        log.info("Outside window (%02d:%02d IST) — skip", mins // 60, mins % 60)
        return

    watch = [s.strip().upper() for s in os.environ.get("SCAN_SYMBOLS", "").split(",") if s.strip()]
    syms  = [s for s in STOCKS if (not watch or s["sym"] in watch)]

    log.info("Scanning %d symbols at %02d:%02d IST", len(syms), mins // 60, mins % 60)

    # Refresh macro context every 30 min (expensive: news API + Claude call)
    macro_stale = (
        STATE.macro_fetched_at is None or
        (datetime.now(IST) - STATE.macro_fetched_at).total_seconds() > 1800
    )
    if macro_stale:
        log.info("Refreshing macro context...")
        STATE.macro_ctx = get_full_macro_context()
        STATE.macro_fetched_at = datetime.now(IST)
    macro_ctx = STATE.macro_ctx
    sent = 0

    # Fetch market context ONCE per scan cycle (not per stock — saves API calls)
    # We use HDFCBANK's sector as proxy to get Nifty50; sector context per stock below
    try:
        nifty_ctx = get_market_context("Banking", token)
        log.info("Nifty50: %+.1f%%  (market bias: %s)",
                 nifty_ctx["nifty_chg"], nifty_ctx["market_bias"])
    except Exception as e:
        log.warning("Market context fetch failed: %s — proceeding without filters", e)
        nifty_ctx = None

    for stock in syms:
        sym = stock["sym"]
        try:
            ltp   = get_ltp(stock["ikey"], token)
            intra = get_intraday(stock["ikey"], token)
            daily = get_daily(stock["ikey"], token)

            # Get sector-specific context (reuses nifty_chg, fetches sector index)
            if nifty_ctx is not None:
                try:
                    ctx = get_market_context(stock["sec"], token)
                    # Reuse already-fetched nifty_chg to avoid duplicate call
                    ctx["nifty_chg"]   = nifty_ctx["nifty_chg"]
                    ctx["market_bias"] = nifty_ctx["market_bias"]
                except Exception:
                    ctx = nifty_ctx   # fallback to broad market
            else:
                ctx = None

            s = build_setup(sym, stock["sec"], intra, daily, ltp, market_ctx=ctx)
        except Exception as e:
            log.warning("Fetch error %s: %s", sym, e)
            continue

        verdict, _  = is_ready(s, mins)
        prev_conf   = STATE.prev_conf.get(sym)
        locked      = STATE.locked_sig.get(sym)

        # Lock first signal of session
        if sym not in STATE.locked_sig and s["sig"] != "WATCH":
            STATE.locked_sig[sym] = s["sig"]

        # ── Trigger 1: Green Ready ─────────────────────────────────────────
        if verdict == "green" and not STATE.already_alerted(sym, "green_ready"):
            if send_telegram(format_alert("green_ready", s)):
                STATE.mark_alerted(sym, "green_ready")
                sent += 1
            time.sleep(1)   # avoid Telegram rate limit (30 msg/sec)

        # ── Trigger 2: Confidence crossed 75% ─────────────────────────────
        if (prev_conf is not None
                and prev_conf < 75
                and s["conf"] >= 75
                and s["sig"] != "WATCH"
                and not STATE.already_alerted(sym, "conf_crossed")):
            if send_telegram(format_alert("conf_crossed", s, extra=str(prev_conf))):
                STATE.mark_alerted(sym, "conf_crossed")
                sent += 1
            time.sleep(1)

        # ── Trigger 3: Reversal ────────────────────────────────────────────
        if (locked
                and locked != s["sig"]
                and s["sig"] != "WATCH"
                and not STATE.already_alerted(sym, "reversal")):
            if send_telegram(format_alert("reversal", s, extra=locked)):
                STATE.mark_alerted(sym, "reversal")
                sent += 1
            time.sleep(1)

        STATE.prev_conf[sym] = s["conf"]

    log.info("Scan done — %d alerts sent", sent)
