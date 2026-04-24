"""
scanner.py — Scheduled NSE alert scanner with Telegram Bot API
Runs inside the same Render.com Flask process via APScheduler.

Triggers Telegram alerts when:
  1. Stock hits Green Ready for the first time this session
  2. Confidence crosses 75% (was below, now above)
  3. Signal reversal detected

Automated backtest mode:
  For BACKTEST_SYMBOLS, paper trades are saved automatically at the first scan
  where confidence >= BACKTEST_MIN_CONF (default 55%). This runs every market day
  without any manual action, collecting data to calibrate signal thresholds.

Environment variables (set in Render dashboard):
  UPSTOX_TOKEN          — set each morning via /set-token-form
  TELEGRAM_BOT_TOKEN    — from @BotFather  e.g. 7123456789:AAF-xxxxx
  TELEGRAM_CHAT_ID      — your personal chat ID  e.g. 123456789
  SCAN_SYMBOLS          — comma-separated symbols for Telegram alerts (default: all)
  BACKTEST_SYMBOLS      — comma-separated symbols for auto paper trades
                          (default: 12 hardcoded diverse Nifty50 stocks)
  BACKTEST_MIN_CONF     — min confidence to auto-save a paper trade (default: 55)
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

from signals import STOCKS, build_setup, get_ltp, get_intraday, get_daily, is_ready, get_market_context, get_market_depth, READY_GREEN_MIN
from macro import get_full_macro_context, apply_all_macro_penalties
import db as _db_module

log = logging.getLogger("scanner")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [scanner] %(message)s")

IST = timezone(timedelta(hours=5, minutes=30))

# ─── Backtest config ──────────────────────────────────────────────────────────
# 12 liquid Nifty50 stocks across 8 sectors — good diversity for calibration.
# Override any time via the BACKTEST_SYMBOLS env var (comma-separated).
_DEFAULT_BACKTEST_SYMBOLS = [
    "HDFCBANK",    # Banking (private)
    "ICICIBANK",   # Banking (private)
    "SBIN",        # Banking (PSU — different behaviour)
    "TCS",         # IT
    "INFY",        # IT
    "RELIANCE",    # Energy / Conglomerate
    "TATAMOTORS",  # Auto
    "MARUTI",      # Auto
    "HINDUNILVR",  # FMCG
    "SUNPHARMA",   # Pharma
    "LT",          # Infrastructure
    "BAJFINANCE",  # NBFC
]

def _get_backtest_symbols() -> set:
    """Returns the active backtest symbol set (env override or hardcoded default)."""
    raw = os.environ.get("BACKTEST_SYMBOLS", "").strip()
    if raw:
        return {s.strip().upper() for s in raw.split(",") if s.strip()}
    return set(_DEFAULT_BACKTEST_SYMBOLS)

def _get_backtest_min_conf() -> int:
    try:
        return int(os.environ.get("BACKTEST_MIN_CONF", "55"))
    except ValueError:
        return 55

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
        self.needs_token_refresh = False
        self._reset()

    def _reset(self):
        self.date                = datetime.now(IST).strftime("%Y-%m-%d")
        self.locked_sig          = {}
        self.prev_conf           = {}
        self.prev_sig            = {}
        self.alerted             = set()
        self.bt_saved            = self._load_bt_saved_from_db()
        self.token_expired_alerted = False
        self.needs_token_refresh = True   # signal run_scan to reload token from DB
        log.info("Session state reset for %s (bt_saved from DB: %s)", self.date, sorted(self.bt_saved))

    def _load_bt_saved_from_db(self) -> set:
        """Load today's already-saved paper trade symbols from DB to survive restarts."""
        try:
            today = datetime.now(IST).strftime("%Y-%m-%d")
            trades = _db_module.get_paper_trades(from_date=today, to_date=today)
            return {t["sym"] for t in trades if t.get("sym")}
        except Exception:
            return set()

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
def set_token(tok):
    _token["value"] = tok.strip()
    STATE.token_expired_alerted = False   # reset so next expiry triggers a fresh alert
    log.info("Upstox token updated")

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
            f"📈 <b>NSE SCANNER — CONFIDENCE CROSSED {READY_GREEN_MIN}%</b>\n"
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

def _save_paper_trade(s: dict):
    """
    Persist a simulated paper trade when a green alert fires.
    This creates the 'order placed' record that will be settled at EOD.
    Silently skips on any error so scanner never fails due to DB issues.
    """
    try:
        now = datetime.now(IST)
        trade_id = f"pt_{now.strftime('%Y%m%d')}_{s['sym']}"
        trade = {
            "id":           trade_id,
            "trade_date":   now.strftime("%Y-%m-%d"),
            "signal_time":  now.strftime("%H:%M"),
            "sym":          s["sym"],
            "sec":          s.get("sec", ""),
            "sig":          s["sig"],
            "conf":         int(s["conf"]),
            "signal_price": float(s["ltp"]),
            "entry":        float(s["en"]),
            "target":       float(s["tg"]),
            "stop_loss":    float(s["sl"]),
            "rr":           float(s["rr"]) if s.get("rr") else None,
            "rsi":          float(s["rsi"]) if s.get("rsi") else None,
            "reason":       s.get("reason", ""),
        }
        saved = _db_module.save_paper_trade(trade)
        if saved:
            log.info("Paper trade saved: %s %s @ %.2f (conf %d%%)",
                     s["sig"], s["sym"], s["en"], s["conf"])
    except Exception as e:
        log.warning("_save_paper_trade failed for %s: %s", s.get("sym"), e)


def run_scan(force: bool = False):
    """Called every N minutes by APScheduler. Pass force=True to bypass time/weekend guards."""
    STATE.check_date()

    now_ist = datetime.now(IST)
    if not force and now_ist.weekday() >= 5:   # 5 = Saturday, 6 = Sunday
        log.info("Weekend — skipping scan")
        return

    token = get_token()
    # Refresh from DB when: (a) token is empty, or (b) date just rolled over.
    if not token or STATE.needs_token_refresh:
        db_tok = _db_module.get_token()
        if db_tok:
            set_token(db_tok)
            token = db_tok
            log.info("Upstox token refreshed from DB")
        STATE.needs_token_refresh = False
    if not token:
        log.info("No Upstox token set — skipping scan")
        return

    start = parse_hhmm(os.environ.get("ALERT_START_IST", "09:15"), 555)
    stop  = parse_hhmm(os.environ.get("ALERT_STOP_IST",  "10:30"), 630)
    mins  = ist_now_mins()

    if not force and not (start <= mins <= stop):
        log.info("Outside window (%02d:%02d IST) — skip", mins // 60, mins % 60)
        return

    # Telegram alert symbols (env override or all stocks)
    watch = [s.strip().upper() for s in os.environ.get("SCAN_SYMBOLS", "").split(",") if s.strip()]

    # Backtest symbols always scanned regardless of SCAN_SYMBOLS filter
    bt_syms    = _get_backtest_symbols()
    bt_min_conf = _get_backtest_min_conf()

    # Union: alert symbols + backtest symbols, deduped
    all_syms = [
        s for s in STOCKS
        if (not watch or s["sym"] in watch) or s["sym"] in bt_syms
    ]

    log.info("Scanning %d symbols at %02d:%02d IST  (backtest: %d symbols, min_conf=%d%%)",
             len(all_syms), mins // 60, mins % 60, len(bt_syms), bt_min_conf)

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
    try:
        nifty_ctx = get_market_context("Banking", token)
        log.info("Nifty50: %+.1f%%  (market bias: %s)",
                 nifty_ctx["nifty_chg"], nifty_ctx["market_bias"])
    except Exception as e:
        log.warning("Market context fetch failed: %s — proceeding without filters", e)
        nifty_ctx = None

    for stock in all_syms:
        sym        = stock["sym"]
        in_bt      = sym in bt_syms
        in_watch   = not watch or sym in watch

        time.sleep(0.4)   # avoid Upstox rate-limiting across 30 rapid calls

        try:
            # Retry once on empty-data errors (transient rate-limit vs bad ikey)
            for _attempt in range(2):
                try:
                    ltp   = get_ltp(stock["ikey"], token)
                    intra = get_intraday(stock["ikey"], token)
                    daily = get_daily(stock["ikey"], token)
                    break
                except ValueError as _ve:
                    if _attempt == 0 and "Empty LTP" in str(_ve):
                        log.debug("Retry %s after empty LTP (attempt 1)", sym)
                        time.sleep(1.5)
                    else:
                        raise

            if nifty_ctx is not None:
                try:
                    ctx = get_market_context(stock["sec"], token)
                    ctx["nifty_chg"]   = nifty_ctx["nifty_chg"]
                    ctx["market_bias"] = nifty_ctx["market_bias"]
                except Exception:
                    ctx = nifty_ctx
            else:
                ctx = None

            depth = None
            try:
                depth = get_market_depth(stock["ikey"], token)
            except Exception:
                pass

            s = build_setup(sym, stock["sec"], intra, daily, ltp, market_ctx=ctx, depth=depth)

            if macro_ctx and s["sig"] != "WATCH":
                pen, warns = apply_all_macro_penalties(s["sig"], stock["sec"], macro_ctx)
                if pen != 0:
                    s["conf"] = max(0, s["conf"] - pen)
                    s.setdefault("ctxWarnings", []).extend(warns)
                    log.debug("%s macro adj %+d → conf=%d%%  (%s)", sym, -pen, s["conf"],
                              "; ".join(warns))

        except Exception as e:
            # Detect expired / invalid token (HTTP 401 or 403) and alert once via Telegram
            http_code = e.code if isinstance(e, urllib.error.HTTPError) else None
            is_auth_error = (
                http_code in (401, 403)
                or "401" in str(e)
                or "403" in str(e)
            )
            if is_auth_error and not STATE.token_expired_alerted:
                STATE.token_expired_alerted = True
                log.warning("Token auth error (%s) — attempting auto-auth", http_code)

                # Try fully automated renewal first
                try:
                    import upstox_auto_auth as _auto_auth
                    if _auto_auth.is_configured():
                        ok, result = _auto_auth.run_auto_auth()
                        if ok:
                            set_token(result)
                            try:
                                _db_module.set_token(result, set_by="auto_auth_on_401")
                            except Exception:
                                pass
                            STATE.token_expired_alerted = False  # allow re-alert if it fails again
                            send_telegram(
                                "\u2705 <b>Auto-auth: token renewed automatically</b>\n\n"
                                "The scanner detected an expired token and renewed it "
                                "without any manual action. Resuming next scan cycle."
                            )
                            log.info("Auto-auth on 401 succeeded — token renewed")
                            return
                        else:
                            log.error("Auto-auth on 401 failed: %s", result)
                            # Fall through to send manual login reminder below
                except Exception as _ae:
                    log.error("Auto-auth import/call error: %s", _ae)

                # Fall back: send Telegram reminder with manual login link
                render_base = os.environ.get("RENDER_BASE_URL", "").rstrip("/")
                login_url   = f"{render_base}/auth/login" if render_base else None
                link_line   = (f"\U0001f449 <a href=\"{login_url}\">Tap here to renew</a>\n{login_url}"
                               if login_url else "\U0001f449 Go to your Render app → /auth/login")
                send_telegram(
                    f"\u26a0\ufe0f <b>Upstox token invalid (HTTP {http_code or 'error'})</b>\n\n"
                    "Auto-renewal was not available or failed. "
                    "Please log in to Upstox to get a fresh token.\n\n"
                    f"{link_line}"
                )
                log.warning("Token auth error — Telegram manual-login alert sent, aborting scan")
                return   # no point scanning remaining stocks with a dead token
            log.warning("Fetch error %s: %s", sym, e)
            continue

        verdict, _ = is_ready(s, mins)
        prev_conf  = STATE.prev_conf.get(sym)
        locked     = STATE.locked_sig.get(sym)

        # Lock first signal of session
        if sym not in STATE.locked_sig and s["sig"] != "WATCH":
            STATE.locked_sig[sym] = s["sig"]

        # ── Auto paper trade for backtest symbols ──────────────────────────
        # Saves at the FIRST scan where conf >= bt_min_conf, regardless of
        # Telegram alerts. Captures both amber and green setups for comparison.
        if (in_bt
                and s["sig"] != "WATCH"
                and s["conf"] >= bt_min_conf
                and sym not in STATE.bt_saved):
            _save_paper_trade(s)
            STATE.bt_saved.add(sym)

        # ── Telegram alerts (only for alert-watch symbols) ─────────────────
        if in_watch:
            # Trigger 1: Green Ready
            if verdict == "green" and not STATE.already_alerted(sym, "green_ready"):
                if send_telegram(format_alert("green_ready", s)):
                    STATE.mark_alerted(sym, "green_ready")
                    sent += 1
                    # Also ensure paper trade saved for non-backtest alert symbols
                    if sym not in STATE.bt_saved:
                        _save_paper_trade(s)
                        STATE.bt_saved.add(sym)
                time.sleep(1)

            # Trigger 2: Confidence crossed green threshold
            if (prev_conf is not None
                    and prev_conf < READY_GREEN_MIN
                    and s["conf"] >= READY_GREEN_MIN
                    and s["sig"] != "WATCH"
                    and not STATE.already_alerted(sym, "conf_crossed")):
                if send_telegram(format_alert("conf_crossed", s, extra=str(prev_conf))):
                    STATE.mark_alerted(sym, "conf_crossed")
                    sent += 1
                time.sleep(1)

            # Trigger 3: Reversal
            if (locked
                    and locked != s["sig"]
                    and s["sig"] != "WATCH"
                    and not STATE.already_alerted(sym, "reversal")):
                if send_telegram(format_alert("reversal", s, extra=locked)):
                    STATE.mark_alerted(sym, "reversal")
                    sent += 1
                time.sleep(1)

        STATE.prev_conf[sym] = s["conf"]
        STATE.prev_sig[sym]  = s["sig"]

    # Log per-symbol summary for all backtest symbols (ERR if fetch failed)
    def _bt_label(sym):
        if sym not in STATE.prev_conf:
            return "ERR"
        sig   = STATE.prev_sig.get(sym, "WATCH")
        saved = "✓saved" if sym in STATE.bt_saved else f"UNSAVED(need {bt_min_conf}%+non-WATCH)"
        return f"{sig}/{STATE.prev_conf[sym]}%/{saved}"
    bt_summary = {sym: _bt_label(sym) for sym in bt_syms}
    log.info("Backtest conf snapshot: %s",
             "  ".join(f"{s}={v}" for s, v in sorted(bt_summary.items())))
    log.info("Scan done — %d alerts sent, %d paper trades saved today",
             sent, len(STATE.bt_saved))
