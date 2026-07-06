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
  PAPER_TRADE_EXCLUDE   — comma-separated symbols to never auto-save as paper trades
                          (default: MARUTI — too high-priced for ₹1L capital)
  ALERT_START_IST       — HH:MM  (default: 09:15)
  ALERT_STOP_IST        — HH:MM  (default: 10:30)
  SCAN_INTERVAL_MINS    — integer (default: 5)
  SCAN_TIMEOUT_MINS     — max minutes a single scan may run before skipping remaining
                          stocks (default: 4)
"""

import os
import time
import json
import logging
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

from signals import STOCKS, US_STOCKS, build_setup, get_ltp, get_intraday, get_daily, is_ready, get_market_context, get_market_depth, detect_regime, READY_GREEN_MIN
from data_provider import get_intraday_candles, get_daily_candles, get_ltp_price, get_market_context_us, _alpaca_configured
from macro import get_full_macro_context, apply_all_macro_penalties
import db as _db_module
import email_alerts as _email

log = logging.getLogger("scanner")
logging.basicConfig(level=logging.INFO)  # formatter applied centrally in app.py

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

def _get_paper_trade_excluded() -> set:
    """Symbols that should never be auto-saved as paper trades (e.g. too high-priced for capital)."""
    raw = os.environ.get("PAPER_TRADE_EXCLUDE", "MARUTI").strip()
    return {s.strip().upper() for s in raw.split(",") if s.strip()}

def _get_backtest_min_conf() -> int:
    try:
        return int(os.environ.get("BACKTEST_MIN_CONF", "70"))
    except ValueError:
        return 70

def _get_allow_amber() -> bool:
    """When true, amber setups (all hard gates pass, conf in the 55–74 band) may be
    auto-saved too — not just full-green (conf >= READY_GREEN_MIN). Historically only
    green saved, which buried every borderline setup and made BACKTEST_MIN_CONF a no-op.
    Default on so the Backtest tab fills on marginal days; set BACKTEST_ALLOW_AMBER=0 to
    restore green-only behaviour."""
    return os.environ.get("BACKTEST_ALLOW_AMBER", "1").strip() not in ("0", "false", "False", "")

def _get_confirm_scans() -> int:
    """Consecutive qualifying scans (same direction, in the prime window) required before
    a paper trade is saved. Was hard-coded to 2; default is now 1 (save on first qualifying
    scan). Raise BACKTEST_CONFIRM_SCANS to demand the signal holds across N scans."""
    try:
        return max(1, int(os.environ.get("BACKTEST_CONFIRM_SCANS", "1")))
    except ValueError:
        return 1

def _save_verdict_ok(verdict: str, conf: float, bt_min_conf: int) -> bool:
    """Whether a scan result qualifies for an auto paper-trade save.

    The real bar is now BACKTEST_MIN_CONF (previously dead code — the save gate was an
    undocumented hard-wired 'green' == conf >= 75). 'green' always qualifies; 'amber'
    qualifies when BACKTEST_ALLOW_AMBER is on. In both cases conf must clear bt_min_conf.
    """
    if conf < bt_min_conf:
        return False
    if verdict == "green":
        return True
    if verdict == "amber" and _get_allow_amber():
        return True
    return False

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
        self.locked_sig          = self._load_locked_sig_from_db()
        self.prev_conf           = {}
        self.prev_sig            = {}
        self.alerted             = set()
        self.bt_saved            = self._load_bt_saved_from_db()
        self.bt_first_green      = {}  # sym -> {"sig","count","mins"}: consecutive qualifying-scan streak
        self.bt_last_verdict     = {}  # sym -> last verdict string for diagnostics
        self.token_expired_alerted = False
        self.scan_heartbeat_sent = False  # True after first "scan alive" Telegram sent
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

    def _load_locked_sig_from_db(self) -> dict:
        """Load today's locked signals from DB to survive restarts without re-alerting."""
        try:
            today = datetime.now(IST).strftime("%Y-%m-%d")
            state = _db_module.get_session_state(today)
            return dict(state.get("locked_signals", {})) if state else {}
        except Exception:
            return {}

    def check_date(self):
        today = datetime.now(IST).strftime("%Y-%m-%d")
        if today != self.date:
            self._reset()

    def already_alerted(self, sym, kind):
        return f"{sym}:{kind}" in self.alerted

    def mark_alerted(self, sym, kind):
        self.alerted.add(f"{sym}:{kind}")

STATE    = SessionState()   # NSE session
US_STATE = SessionState()   # US session (independent date/lock tracking)

_us_market_ctx_cache: dict | None = None   # last good sp500_ctx across cycles

# Eastern Time — US market timezone (UTC-5 EST / UTC-4 EDT)
ET = ZoneInfo("America/New_York")

def et_now_mins():
    """Current time as minutes since midnight ET (mirrors ist_now_mins for US market)."""
    n = datetime.now(ET)
    return n.hour * 60 + n.minute


# ─── Token store ──────────────────────────────────────────────────────────────

_initial_token = os.environ.get("UPSTOX_TOKEN", "").strip()
_token = {
    "value": _initial_token,
    # Record the IST date the token was set so stale tokens from previous
    # days are never used (prevents spurious 401 → "token expired" alerts
    # when the server runs continuously overnight).
    "date":  datetime.now(IST).strftime("%Y-%m-%d") if _initial_token else None,
}

def get_token():
    """Return today's in-memory token; returns '' for tokens set on a previous day."""
    if _token["date"] and _token["date"] != datetime.now(IST).strftime("%Y-%m-%d"):
        return ""   # stale — run_scan will reload from DB or skip cleanly
    return _token["value"]

def set_token(tok):
    _token["value"] = tok.strip()
    _token["date"]  = datetime.now(IST).strftime("%Y-%m-%d") if tok.strip() else None
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
    ist_time  = datetime.now(IST).strftime("%H:%M IST")
    gain      = round(abs(s["tg"] - s["en"]), 2)
    risk      = round(abs(s["sl"] - s["en"]), 2)
    chg_str   = f"{s['chg']:+.2f}%"
    is_us     = s.get("market") == "US"
    cur       = "$" if is_us else "Rs"
    mkt_name  = "US STOCK SCANNER" if is_us else "NSE SCANNER"

    if kind == "green_ready":
        sig_emoji = "🟢" if s["sig"] == "BUY" else "🔴"
        ctx        = s.get("market_ctx", {})
        warnings   = s.get("ctx_warnings", [])
        idx_label  = ctx.get("index_name", "Nifty")
        nifty_str  = (f"{idx_label}: {ctx['nifty_chg']:+.1f}%  "
                      f"Sector: {ctx['sector_chg']:+.1f}%") if ctx else ""
        warn_str  = ("\n⚠️ " + "  |  ".join(warnings)) if warnings else ""
        return (
            f"{sig_emoji} <b>{mkt_name} — READY TO TRADE</b>\n"
            f"\n"
            f"<b>{s['sym']}</b>  <code>{s['sec']}</code>\n"
            f"Signal  : <b>{s['sig']}</b>  |  Conf: <b>{s['conf']}%</b>\n"
            f"\n"
            f"Entry   : <code>{cur} {s['en']}</code>\n"
            f"Target  : <code>{cur} {s['tg']}</code>  (+{cur} {gain})\n"
            f"Stop SL : <code>{cur} {s['sl']}</code>  (-{cur} {risk})\n"
            f"R:R     : <b>{s['rr']}:1</b>\n"
            f"\n"
            f"LTP     : {cur} {s['ltp']} ({chg_str})\n"
            f"Market  : {nifty_str}\n"
            f"Setup   : {s['reason']}\n"
            f"{warn_str}\n"
            f"\n"
            f"⏰ {ist_time}"
        )

    elif kind == "conf_crossed":
        return (
            f"📈 <b>{mkt_name} — CONFIDENCE CROSSED {READY_GREEN_MIN}%</b>\n"
            f"\n"
            f"<b>{s['sym']}</b>  <code>{s['sec']}</code>\n"
            f"Signal  : <b>{s['sig']}</b>\n"
            f"Conf    : <b>{s['conf']}%</b>  (was {extra}%)\n"
            f"\n"
            f"Entry   : <code>{cur} {s['en']}</code>\n"
            f"SL      : <code>{cur} {s['sl']}</code>\n"
            f"LTP     : {cur} {s['ltp']} ({chg_str})\n"
            f"\n"
            f"⏰ {ist_time}"
        )

    elif kind == "reversal":
        return (
            f"⚡ <b>{mkt_name} — SIGNAL REVERSAL</b>\n"
            f"\n"
            f"<b>{s['sym']}</b>  <code>{s['sec']}</code>\n"
            f"Was     : <b>{extra}</b>  →  Now: <b>{s['sig']}</b>\n"
            f"LTP     : {cur} {s['ltp']} ({chg_str})\n"
            f"\n"
            f"⚠️ Conflicting signals — <b>skip this trade today</b>\n"
            f"\n"
            f"⏰ {ist_time}"
        )

    return f"{mkt_name}: {s['sym']} {kind} @ {ist_time}"

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

# ─── Scan heartbeat ───────────────────────────────────────────────────────────

def _send_scan_heartbeat(all_syms, market_ctx, mins, state, market: str = "NSE"):
    """Send a one-time 'scanner alive' Telegram when first scan completes with no alerts."""
    ist_time   = datetime.now(IST).strftime("%H:%M IST")
    n          = len(all_syms)
    bias       = market_ctx.get("market_bias", "?") if market_ctx else "?"
    composite  = market_ctx.get("composite_chg", 0.0) if market_ctx else 0.0
    vix        = market_ctx.get("vix", 0.0) if market_ctx else 0.0

    # Top 3 scanned symbols by confidence (gives a quick peek at what's closest)
    scored = sorted(
        [(s["sym"], state.prev_conf.get(s["sym"], 0), state.prev_sig.get(s["sym"], "WATCH"))
         for s in all_syms if s["sym"] in state.prev_conf],
        key=lambda x: x[1], reverse=True,
    )[:3]
    top_str = "  |  ".join(f"{sym} {sig} {conf}%" for sym, conf, sig in scored) or "—"

    mkt_label = "NSE SCANNER" if market == "NSE" else "US SCANNER"
    flag      = "🇮🇳" if market == "NSE" else "🇺🇸"
    send_telegram(
        f"{flag} <b>{mkt_label} — scan complete, no alerts</b>\n"
        f"\n"
        f"{n} stocks scanned — nothing above threshold yet.\n"
        f"Market: <b>{composite:+.2f}%</b>  bias={bias}  VIX={vix:.1f}\n"
        f"\n"
        f"Top conf: {top_str}\n"
        f"\n"
        f"⏰ {ist_time}"
    )


# ─── Core scan ────────────────────────────────────────────────────────────────

def _save_paper_trade(s: dict, market: str = "NSE"):
    """
    Persist a simulated paper trade when a green alert fires.
    This creates the 'order placed' record that will be settled at EOD.
    Silently skips on any error so scanner never fails due to DB issues.
    """
    try:
        now = datetime.now(IST)
        if market == "US":
            # Store signal_time in ET so it aligns with yfinance candle timestamps (also ET)
            now_et = datetime.now(ET)
            sig_time   = now_et.strftime("%H:%M")
            trade_date = now_et.strftime("%Y-%m-%d")
        else:
            sig_time   = now.strftime("%H:%M")
            trade_date = now.strftime("%Y-%m-%d")
        trade_id = f"pt_{now.strftime('%Y%m%d')}_{market}_{s['sym']}"
        trade = {
            "id":           trade_id,
            "trade_date":   trade_date,
            "signal_time":  sig_time,
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
            "market":       market,
        }
        # Attach the market condition this pick was made in, so the backtest
        # can slice win-rate by regime / VIX later (see db.get_paper_trade_stats).
        mctx = s.get("market_ctx") or {}
        trade.update({
            "regime":        s.get("regime"),
            "composite_chg": mctx.get("composite_chg"),
            "vix":           mctx.get("vix"),
            "sector_chg":    mctx.get("sector_chg"),
            "market_bias":   mctx.get("market_bias"),
        })
        saved = _db_module.save_paper_trade(trade)
        if saved:
            log.info("[%s] Paper trade saved: %s %s @ %.2f (conf %d%%)",
                     market, s["sig"], s["sym"], s["en"], s["conf"])
        else:
            log.warning("[%s] Paper trade NOT saved (DB rejected): %s", market, s.get("sym"))
        return bool(saved)
    except Exception as e:
        log.warning("_save_paper_trade failed for %s: %s", s.get("sym"), e)
        return False


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
    # Stop at 11:00 IST to fully cover the 09:45–11:00 "prime" ORB window that green/save
    # requires (was 10:30, which cut the usable window to ~45 min).
    stop  = parse_hhmm(os.environ.get("ALERT_STOP_IST",  "11:00"), 660)
    mins  = ist_now_mins()

    if not force and not (start <= mins <= stop):
        log.debug("Outside window (%02d:%02d IST) — skip", mins // 60, mins % 60)
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

    # Scan-level deadline — skip remaining stocks if scan runs too long
    try:
        _scan_timeout_mins = int(os.environ.get("SCAN_TIMEOUT_MINS", "4"))
    except ValueError:
        _scan_timeout_mins = 4
    scan_deadline = datetime.now(IST) + timedelta(minutes=_scan_timeout_mins)

    # Symbols excluded from paper trade auto-save (e.g. too high-priced for ₹1L capital)
    pt_excluded = _get_paper_trade_excluded()

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
        _b = nifty_ctx.get("broad_chgs", {})
        log.info(
            "Market: N50=%+.2f%% | NXT50=%+.2f%% | MID100=%+.2f%% | SM100=%+.2f%%"
            "  →  composite=%+.2f%%  bias=%s  VIX=%.1f",
            _b.get("NIFTY50", 0), _b.get("NIFTYNEXT50", 0),
            _b.get("NIFTYMIDCAP100", 0), _b.get("NIFTYSMLCAP100", 0),
            nifty_ctx.get("composite_chg", 0), nifty_ctx["market_bias"],
            nifty_ctx.get("vix", 0),
        )
        # Persist the daily market snapshot (one row/day) so the backtest records
        # market condition even on zero-pick days. Market-level regime keys off
        # breadth (chg/gap=0 → detect_regime uses nifty_chg).
        try:
            snap = dict(nifty_ctx)
            snap["regime"] = detect_regime(0.0, 0.0, nifty_ctx)
            _db_module.save_market_snapshot(snap, market="NSE")
        except Exception as _se:
            log.debug("save_market_snapshot skipped: %s", _se)
    except Exception as e:
        log.warning("Market context fetch failed: %s — proceeding without filters", e)
        nifty_ctx = None

    for stock in all_syms:
        if datetime.now(IST) > scan_deadline:
            remaining = [s["sym"] for s in all_syms[all_syms.index(stock):]]
            log.warning("Scan timeout (%d min) reached — skipping %d symbols: %s",
                        _scan_timeout_mins, len(remaining), remaining)
            break

        sym        = stock["sym"]
        in_bt      = sym in bt_syms
        in_watch   = not watch or sym in watch

        time.sleep(0.4)   # avoid Upstox rate-limiting across 30 rapid calls

        try:
            # Retry up to 3 times: backoff on 429, once on empty LTP
            for _attempt in range(3):
                try:
                    ltp   = get_ltp(stock["ikey"], token)
                    intra = get_intraday(stock["ikey"], token)
                    daily = get_daily(stock["ikey"], token)
                    break
                except urllib.error.HTTPError as _he:
                    if _he.code == 429 and _attempt < 2:
                        _wait = 2 ** (_attempt + 1)  # 2s, 4s
                        log.warning("Rate-limited on %s (attempt %d) — backing off %ds",
                                    sym, _attempt + 1, _wait)
                        time.sleep(_wait)
                    else:
                        raise
                except ValueError as _ve:
                    if _attempt == 0 and "Empty LTP" in str(_ve):
                        log.debug("Retry %s after empty LTP (attempt 1)", sym)
                        time.sleep(1.5)
                    else:
                        raise

            if nifty_ctx is not None:
                try:
                    ctx = get_market_context(stock["sec"], token)
                    # Overwrite broad-market fields from the pre-fetched cycle context
                    # (saves 4 extra API calls per stock; only sector_chg is stock-specific)
                    ctx["nifty_chg"]     = nifty_ctx["nifty_chg"]
                    ctx["composite_chg"] = nifty_ctx["composite_chg"]
                    ctx["broad_chgs"]    = nifty_ctx.get("broad_chgs", {})
                    ctx["vix"]           = nifty_ctx.get("vix", 0.0)
                    ctx["market_bias"]   = nifty_ctx["market_bias"]
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
                _db_module.mark_token_invalid(by=f"scanner_http{http_code or 'err'}")   # persist across restarts
                log.warning("Token auth error (%s) — alerting user", http_code)
                render_base = os.environ.get("RENDER_BASE_URL", "").rstrip("/")
                login_url   = f"{render_base}/auth/login" if render_base else None
                link_line   = (f"👉 <a href=\"{login_url}\">Tap here to renew</a>\n{login_url}"
                               if login_url else "👉 Go to your Render app → /auth/login")
                send_telegram(
                    f"⚠️ <b>Upstox token invalid (HTTP {http_code or 'error'})</b>\n\n"
                    "Please log in to Upstox to get a fresh token.\n\n"
                    f"{link_line}"
                )
                _email.send_email(*_email.format_token_expiry(http_code, login_url))
                log.warning("Token auth error — Telegram + email alert sent, aborting scan")
                return   # no point scanning remaining stocks with a dead token
            log.warning("Fetch error %s: %s", sym, e)
            continue

        verdict, _ = is_ready(s, mins)
        prev_conf  = STATE.prev_conf.get(sym)
        locked     = STATE.locked_sig.get(sym)
        if in_bt:
            STATE.bt_last_verdict[sym] = verdict

        # Lock first signal of session
        if sym not in STATE.locked_sig and s["sig"] != "WATCH":
            STATE.locked_sig[sym] = s["sig"]

        # ── Auto paper trade for backtest symbols ──────────────────────────
        # Save when the setup qualifies (green, or amber when BACKTEST_ALLOW_AMBER)
        # for BACKTEST_CONFIRM_SCANS consecutive scans in the same direction.
        # Default confirm=1 → save on the first qualifying scan. The prime-window
        # gate (>= 9:45) still filters first-30-min fakeouts.
        save_ok        = _save_verdict_ok(verdict, s["conf"], bt_min_conf)
        confirm_needed = _get_confirm_scans()
        if in_bt and sym not in STATE.bt_saved and s.get("_time_prime"):
            if save_ok:
                prior  = STATE.bt_first_green.get(sym)
                streak = (prior.get("count", 1) + 1) if (prior and prior["sig"] == s["sig"]) else 1
                if streak >= confirm_needed:
                    # Enough consecutive qualifying scans — fire the trade
                    if sym in pt_excluded:
                        log.info("Paper trade skipped (excluded symbol): %s", sym)
                        STATE.bt_saved.add(sym)  # mark as handled so we don't keep checking
                    else:
                        if _save_paper_trade(s, market="NSE"):
                            _check_real_trade_overlap(s)
                            STATE.bt_saved.add(sym)
                    STATE.bt_first_green.pop(sym, None)
                else:
                    # Not yet confirmed — (re)start or extend the streak
                    if prior and prior["sig"] != s["sig"]:
                        log.info("Paper trade streak reset (reversal %s→%s): %s",
                                 prior["sig"], s["sig"], sym)
                    else:
                        log.info("Paper trade pending confirm %d/%d: %s %s conf=%d%% (verdict=%s)",
                                 streak, confirm_needed, s["sig"], sym, s["conf"], verdict)
                    STATE.bt_first_green[sym] = {"sig": s["sig"], "count": streak, "mins": mins}
            else:
                # Signal no longer qualifies (verdict/conf dropped) — reset the streak
                if sym in STATE.bt_first_green:
                    log.info("Paper trade streak reset (verdict=%s conf=%d%%): %s",
                             verdict, s["conf"], sym)
                    del STATE.bt_first_green[sym]
        elif in_bt and not s.get("_time_prime") and sym in STATE.bt_first_green:
            # Past 11:00 AM — discard any pending confirmations
            log.info("Paper trade streak reset (past 11:00 AM window): %s", sym)
            del STATE.bt_first_green[sym]

        # ── Telegram + email alerts: only when stock is ready to trade ────────
        if in_watch:
            if verdict == "green" and not STATE.already_alerted(sym, "green_ready"):
                _msg = format_alert("green_ready", s)
                if send_telegram(_msg):
                    STATE.mark_alerted(sym, "green_ready")
                    sent += 1
                    # Durably record the alert (independent of the paper-trade write) so a
                    # pick is always recoverable from alert_log even if the trade save fails.
                    try:
                        _db_module.log_alert(sym, "green_ready", int(s["conf"]), s["sig"], _msg, True,
                                             market="NSE")
                    except Exception as _le:
                        log.debug("log_alert failed for %s: %s", sym, _le)
                    # Also ensure paper trade saved for non-backtest alert symbols
                    if sym not in STATE.bt_saved and sym not in pt_excluded:
                        # Only mark saved if the DB write actually succeeded — a failed
                        # save must be retried next cycle, not silently swallowed (else the
                        # alert fires but the trade never reaches the Backtest tab).
                        if _save_paper_trade(s, market="NSE"):
                            STATE.bt_saved.add(sym)
                        _check_real_trade_overlap(s)
                    elif sym in pt_excluded:
                        log.info("Paper trade skipped for alert symbol (excluded): %s", sym)
                _email.send_email(*_email.format_green_ready(s))
                time.sleep(1)

        STATE.prev_conf[sym] = s["conf"]
        STATE.prev_sig[sym]  = s["sig"]

    # Log per-symbol summary for all backtest symbols (ERR if fetch failed)
    def _bt_label(sym):
        if sym not in STATE.prev_conf:
            return "ERR"
        sig     = STATE.prev_sig.get(sym, "WATCH")
        conf    = STATE.prev_conf[sym]
        verdict = STATE.bt_last_verdict.get(sym, "?")
        if sym in STATE.bt_saved:
            status = "✓saved"
        elif sym in STATE.bt_first_green:
            pend = STATE.bt_first_green[sym]
            status = f"pending-confirm({pend.get('count', 1)}/{_get_confirm_scans()})"
        elif not _save_verdict_ok(verdict, conf, _get_backtest_min_conf()):
            status = f"UNSAVED(verdict={verdict},conf<{_get_backtest_min_conf()}%-or-gates)"
        else:
            status = "UNSAVED(outside-9:45-11:00-window)"
        return f"{sig}/{conf}%/{status}"
    bt_summary = {sym: _bt_label(sym) for sym in bt_syms}
    log.info("Backtest conf snapshot: %s",
             "  ".join(f"{s}={v}" for s, v in sorted(bt_summary.items())))
    # Persist session state to DB so locked_sig survives a server restart
    try:
        _db_module.save_session_state({
            "locked_signals":  STATE.locked_sig,
            "alerted":         list(STATE.alerted),
            "prev_confidence": STATE.prev_conf,
        }, date_=STATE.date)
    except Exception as _se:
        log.warning("Failed to persist session state to DB: %s", _se)

    log.info("Scan done — %d alerts sent, %d paper trades saved today",
             sent, len(STATE.bt_saved))

    # Send one heartbeat per session so you know the scan is alive even when nothing fires
    if not STATE.scan_heartbeat_sent:
        STATE.scan_heartbeat_sent = True
        if sent == 0:
            _send_scan_heartbeat(all_syms, nifty_ctx, mins, STATE, market="NSE")


# ─── US market scan ───────────────────────────────────────────────────────────

_DEFAULT_US_BACKTEST_SYMBOLS = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN",
    "TSLA", "JPM", "META", "V", "UNH",
]


def run_us_scan(force: bool = False):
    """
    US market scan — runs every 5 min during 9:30–11:00 AM ET on US trading days.
    Uses yfinance for data (no broker token required).
    Paper trades and alerts follow the same logic as the NSE scan.
    """
    US_STATE.check_date()

    now_et = datetime.now(ET)
    # US markets closed weekends
    if not force and now_et.weekday() >= 5:
        log.info("[US] Weekend — skipping scan")
        return

    mins = et_now_mins()
    start_et = parse_hhmm(os.environ.get("US_ALERT_START_ET", "09:30"), 570)
    stop_et  = parse_hhmm(os.environ.get("US_ALERT_STOP_ET",  "11:00"), 660)

    if not force and not (start_et <= mins <= stop_et):
        log.debug("[US] Outside window (%02d:%02d ET) — skip", mins // 60, mins % 60)
        return

    raw_bt = os.environ.get("US_BACKTEST_SYMBOLS", "").strip()
    bt_syms = {s.strip().upper() for s in raw_bt.split(",") if s.strip()} if raw_bt else set(_DEFAULT_US_BACKTEST_SYMBOLS)

    try:
        bt_min_conf = int(os.environ.get("US_BACKTEST_MIN_CONF", "70"))
    except ValueError:
        bt_min_conf = 70

    pt_excluded = {s.strip().upper() for s in os.environ.get("US_PAPER_TRADE_EXCLUDE", "").split(",") if s.strip()}

    watch = [s.strip().upper() for s in os.environ.get("US_SCAN_SYMBOLS", "").split(",") if s.strip()]
    all_syms = [s for s in US_STOCKS if (not watch or s["sym"] in watch) or s["sym"] in bt_syms]

    data_src = "Alpaca" if _alpaca_configured() else "yfinance"
    log.info("[US] Scanning %d symbols at %02d:%02d ET (data: %s)", len(all_syms), mins // 60, mins % 60, data_src)

    # Fetch S&P 500 context once per cycle; fall back to last good value on rate-limit
    global _us_market_ctx_cache
    try:
        sp500_ctx = get_market_context_us("Technology")
        _us_market_ctx_cache = sp500_ctx
        _ub = sp500_ctx.get("broad_chgs", {})
        log.info(
            "[US] Market: SPX=%+.2f%% | NDX=%+.2f%% | RUT=%+.2f%%"
            "  →  composite=%+.2f%%  bias=%s  VIX=%.1f",
            _ub.get("SP500", 0), _ub.get("NASDAQ", 0), _ub.get("RUSSELL2K", 0),
            sp500_ctx.get("composite_chg", 0), sp500_ctx["market_bias"],
            sp500_ctx.get("vix", 0),
        )
        try:
            snap = dict(sp500_ctx)
            # US ctx has no 'nifty_chg'; feed composite as the regime driver
            _regime_ctx = {**sp500_ctx, "nifty_chg": sp500_ctx.get("composite_chg", 0.0)}
            snap["regime"] = detect_regime(0.0, 0.0, _regime_ctx)
            _db_module.save_market_snapshot(snap, market="US")
        except Exception as _se:
            log.debug("[US] save_market_snapshot skipped: %s", _se)
    except Exception as e:
        if _us_market_ctx_cache is not None:
            log.warning("[US] Market context fetch failed (%s) — using cached values", e)
            sp500_ctx = _us_market_ctx_cache
        else:
            log.warning("[US] Market context fetch failed (%s) — no cache available", e)
            sp500_ctx = None

    try:
        _scan_timeout_mins = int(os.environ.get("SCAN_TIMEOUT_MINS", "4"))
    except ValueError:
        _scan_timeout_mins = 4
    scan_deadline = datetime.now(ET) + timedelta(minutes=_scan_timeout_mins)

    sent = 0
    skipped = 0

    for stock in all_syms:
        if datetime.now(ET) > scan_deadline:
            remaining = [s["sym"] for s in all_syms[all_syms.index(stock):]]
            log.warning("[US] Scan timeout — skipping %d symbols: %s", len(remaining), remaining)
            break

        sym   = stock["sym"]
        in_bt = sym in bt_syms
        in_watch = not watch or sym in watch

        time.sleep(0.2)   # yfinance rate limit is more lenient than Upstox

        try:
            intra = get_intraday_candles(sym, "US")
            daily = get_daily_candles(sym, "US")
            if not intra or not daily:
                log.warning("[US] No data for %s — skipping (possible rate limit)", sym)
                skipped += 1
                continue
            ltp = float(intra[-1][4])

            if sp500_ctx is not None:
                try:
                    ctx = get_market_context_us(stock["sec"])
                    ctx["nifty_chg"]     = sp500_ctx["nifty_chg"]
                    ctx["composite_chg"] = sp500_ctx["composite_chg"]
                    ctx["broad_chgs"]    = sp500_ctx.get("broad_chgs", {})
                    ctx["vix"]           = sp500_ctx.get("vix", 0.0)
                    ctx["market_bias"]   = sp500_ctx["market_bias"]
                except Exception:
                    ctx = sp500_ctx
            else:
                ctx = None

            s = build_setup(sym, stock["sec"], intra, daily, ltp, market_ctx=ctx)
            # Inject market key for frontend/alerts
            if ctx:
                ctx["market"] = "US"
            s["market"] = "US"

        except Exception as e:
            log.warning("[US] Fetch/build error %s: %s", sym, e)
            skipped += 1
            continue

        verdict, _ = is_ready(s, mins, market="US")
        US_STATE.bt_last_verdict[sym] = verdict if in_bt else verdict

        if sym not in US_STATE.locked_sig and s["sig"] != "WATCH":
            US_STATE.locked_sig[sym] = s["sig"]

        # Auto paper trade (same qualifying-verdict + confirm-streak logic as NSE)
        save_ok        = _save_verdict_ok(verdict, s["conf"], bt_min_conf)
        confirm_needed = _get_confirm_scans()
        if in_bt and sym not in US_STATE.bt_saved and s.get("_time_prime"):
            if save_ok:
                prior  = US_STATE.bt_first_green.get(sym)
                streak = (prior.get("count", 1) + 1) if (prior and prior["sig"] == s["sig"]) else 1
                if streak >= confirm_needed:
                    if sym in pt_excluded:
                        US_STATE.bt_saved.add(sym)
                    elif _save_paper_trade(s, market="US"):
                        US_STATE.bt_saved.add(sym)
                    US_STATE.bt_first_green.pop(sym, None)
                else:
                    US_STATE.bt_first_green[sym] = {"sig": s["sig"], "count": streak, "mins": mins}
            else:
                US_STATE.bt_first_green.pop(sym, None)

        # Telegram alerts
        if in_watch and verdict == "green" and not US_STATE.already_alerted(sym, "green_ready"):
            _msg = format_alert("green_ready", s)
            if send_telegram(_msg):
                US_STATE.mark_alerted(sym, "green_ready")
                sent += 1
                # Durably record the alert (independent of the paper-trade write) so a
                # pick is always recoverable from alert_log even if the trade save fails.
                try:
                    _now_et = datetime.now(ET)
                    _db_module.log_alert(sym, "green_ready", int(s["conf"]), s["sig"], _msg, True,
                                         date_=_now_et.strftime("%Y-%m-%d"),
                                         time_=_now_et.strftime("%H:%M"), market="US")
                except Exception as _le:
                    log.debug("[US] log_alert failed for %s: %s", sym, _le)
                if sym not in US_STATE.bt_saved and sym not in pt_excluded:
                    # Only mark saved if the DB write actually succeeded — a failed
                    # save must be retried next cycle, not silently swallowed (else the
                    # alert fires but the trade never reaches the Backtest tab).
                    if _save_paper_trade(s, market="US"):
                        US_STATE.bt_saved.add(sym)

        US_STATE.prev_conf[sym] = s["conf"]
        US_STATE.prev_sig[sym]  = s["sig"]

    log.info("[US] Scan done — %d alerts sent, %d paper trades saved today, %d symbols skipped (no data)", sent, len(US_STATE.bt_saved), skipped)

    if not US_STATE.scan_heartbeat_sent:
        US_STATE.scan_heartbeat_sent = True
        if sent == 0:
            _send_scan_heartbeat(all_syms, _us_market_ctx_cache, mins, US_STATE, market="US")


# ─── Real-trade overlap check ─────────────────────────────────────────────────

def _check_real_trade_overlap(s: dict):
    """
    After a paper trade fires, check if the same stock+direction appeared in
    last night's evening picks. If so, fire a Real Trade Candidate alert.
    """
    try:
        from datetime import date, timedelta
        yesterday = (datetime.now(IST).date() - timedelta(days=1)).isoformat()
        picks = _db_module.get_evening_picks(yesterday)
        match = next((p for p in picks if p["sym"] == s["sym"] and p["sig"] == s["sig"]), None)
        if not match:
            return
        log.info("Real Trade Candidate: %s %s (evening pick conf=%d%%, morning conf=%d%%)",
                 s["sig"], s["sym"], match["conf"], s["conf"])
        tg_msg = _format_real_trade_alert(s, match)
        send_telegram(tg_msg)
        _email.send_email(*_email.format_real_trade_candidate(s, match))
    except Exception as e:
        log.warning("_check_real_trade_overlap failed for %s: %s", s.get("sym"), e)


def _format_real_trade_alert(s: dict, evening_pick: dict) -> str:
    """Telegram HTML message for a Real Trade Candidate."""
    ist_time = datetime.now(IST).strftime("%H:%M IST")
    sig_emoji = "🟢" if s["sig"] == "BUY" else "🔴"
    gain  = round(abs(s["tg"] - s["en"]), 2)
    risk  = round(abs(s["sl"] - s["en"]), 2)
    cur = "$" if s.get("market") == "US" else "Rs"
    return (
        f"⚡ <b>REAL TRADE CANDIDATE</b> {sig_emoji}\n"
        f"\n"
        f"<b>{s['sym']}</b>  <code>{s['sec']}</code>\n"
        f"Signal  : <b>{s['sig']}</b>\n"
        f"Conf    : <b>{s['conf']}%</b>  (evening: {evening_pick['conf']}%)\n"
        f"\n"
        f"Entry   : <code>{cur} {s['en']}</code>\n"
        f"Target  : <code>{cur} {s['tg']}</code>  (+{cur} {gain})\n"
        f"Stop SL : <code>{cur} {s['sl']}</code>  (-{cur} {risk})\n"
        f"R:R     : <b>{s['rr']}:1</b>\n"
        f"\n"
        f"LTP     : {cur} {s['ltp']} ({s['chg']:+.2f}%)\n"
        f"Setup   : {s['reason']}\n"
        f"\n"
        f"✅ Confirmed in both evening watchlist and morning scan\n"
        f"👉 Place limit order near {cur} {s['en']} | Set SL at {cur} {s['sl']}\n"
        f"\n"
        f"⏰ {ist_time}"
    )


# ─── Evening scan ─────────────────────────────────────────────────────────────

def run_evening_scan(force: bool = False):
    """
    Runs at 15:40 IST after market close. Scans all 50 stocks, picks the top 5
    by confidence, saves them as tonight's watchlist, and sends Telegram + email.
    These picks are used the next morning to identify Real Trade Candidates.
    """
    now_ist = datetime.now(IST)
    if not force and now_ist.weekday() == 5:   # 5=Saturday only — Sunday runs for Monday prep
        log.info("Saturday — skipping evening scan")
        return

    token = get_token()
    if not token:
        db_tok = _db_module.get_token()
        if db_tok:
            set_token(db_tok)
            token = db_tok
    if not token:
        log.warning("Evening scan: no Upstox token — skipping")
        send_telegram(
            f"⚠️ <b>Evening Watchlist — {now_ist.strftime('%d %b %Y')}</b>\n\n"
            f"Scan skipped — no Upstox token found.\n"
            f"Please log in to Upstox before market close tomorrow.\n"
            f"⏰ {now_ist.strftime('%H:%M IST')}"
        )
        return

    log.info("Evening scan started — scanning %d stocks", len(STOCKS))
    today = now_ist.strftime("%Y-%m-%d")
    picks = []
    fetch_errors = 0

    near_misses = []   # non-WATCH stocks that didn't reach threshold — for diagnostics
    for stock in STOCKS:
        sym = stock["sym"]
        time.sleep(0.4)
        try:
            # After market close, LTP endpoint may return empty — fall back to daily close
            try:
                ltp = get_ltp(stock["ikey"], token)
            except (ValueError, Exception):
                daily_tmp = get_daily(stock["ikey"], token)
                if not daily_tmp:
                    log.debug("Evening scan: no price data for %s — skipping", sym)
                    continue
                ltp = float(daily_tmp[0][4])   # most recent daily close price
                log.debug("Evening scan: using daily close %.2f for %s (LTP unavailable)", ltp, sym)

            intra = get_intraday(stock["ikey"], token)
            daily = get_daily(stock["ikey"], token)
            s = build_setup(sym, stock["sec"], intra, daily, ltp)
            log.info("Evening scan: %-14s  sig=%-5s  conf=%d%%", sym, s["sig"], s["conf"])
            if s["sig"] != "WATCH" and s["conf"] >= READY_GREEN_MIN:
                picks.append(s)
            elif s["sig"] != "WATCH":
                near_misses.append(s)   # directional but below threshold
        except Exception as e:
            fetch_errors += 1
            log.warning("Evening scan fetch error %s: %s", sym, e)
            continue

    log.info("Evening scan complete — %d candidates from %d stocks (%d errors)",
             len(picks), len(STOCKS), fetch_errors)

    # Sort by confidence descending, take top 5
    picks.sort(key=lambda x: x["conf"], reverse=True)
    top_picks = picks[:5]

    if not top_picks:
        near_misses.sort(key=lambda x: x["conf"], reverse=True)
        top_near = near_misses[:3]
        if top_near:
            miss_lines = "".join(
                f"\n  • {m['sym']} {m['sig']} {m['conf']}%"
                for m in top_near
            )
            near_miss_str = f"\n\nClosest misses (below {READY_GREEN_MIN}% threshold):{miss_lines}"
        else:
            near_miss_str = "\n\nAll 48 stocks closed with no directional signal (WATCH)."
        log.info("Evening scan: no picks met threshold (READY_GREEN_MIN=%d%%). "
                 "Check per-stock lines above for individual conf scores.", READY_GREEN_MIN)
        send_telegram(
            f"📋 <b>Evening Watchlist — {now_ist.strftime('%d %b %Y')}</b>\n\n"
            f"No stocks met the signal threshold today (min {READY_GREEN_MIN}% conf)."
            f"{near_miss_str}\n\n"
            f"⏰ {now_ist.strftime('%H:%M IST')}"
        )
        return

    # Save to DB
    for pick in top_picks:
        pick_id = f"ep_{now_ist.strftime('%Y%m%d')}_{pick['sym']}"
        _db_module.save_evening_pick({
            "id":        pick_id,
            "pick_date": today,
            "sym":       pick["sym"],
            "sec":       pick.get("sec", ""),
            "sig":       pick["sig"],
            "conf":      int(pick["conf"]),
            "entry":     float(pick["en"]),
            "target":    float(pick["tg"]),
            "stop_loss": float(pick["sl"]),
            "rr":        float(pick["rr"]) if pick.get("rr") else None,
            "rsi":       float(pick["rsi"]) if pick.get("rsi") else None,
            "reason":    pick.get("reason", ""),
        })

    log.info("Evening scan saved %d picks: %s", len(top_picks),
             [f"{p['sig']} {p['sym']} {p['conf']}%" for p in top_picks])

    # Send Telegram
    send_telegram(_format_evening_watchlist(top_picks, now_ist))

    # Send email
    _email.send_email(*_email.format_evening_picks(top_picks))


def _format_evening_watchlist(picks: list, now_ist) -> str:
    """Telegram HTML message for tonight's evening watchlist."""
    lines = [
        f"📋 <b>Evening Watchlist — {now_ist.strftime('%d %b %Y')}</b>\n",
        f"Tomorrow's morning scan will confirm these picks.\n",
        f"If any appear in the 9:45 AM scan → Real Trade Candidate alert fires.\n",
    ]
    for i, p in enumerate(picks, 1):
        sig_emoji = "🟢" if p["sig"] == "BUY" else "🔴"
        gain = round(abs(p["tg"] - p["en"]), 2)
        risk = round(abs(p["sl"] - p["en"]), 2)
        lines.append(
            f"\n{i}. {sig_emoji} <b>{p['sym']}</b>  <code>{p['sec']}</code>\n"
            f"   {p['sig']}  |  Conf: <b>{p['conf']}%</b>\n"
            f"   Entry: Rs {p['en']}  |  Target: Rs {p['tg']} (+Rs {gain})\n"
            f"   SL: Rs {p['sl']} (-Rs {risk})  |  R:R: {p['rr']}:1"
        )
    lines.append(f"\n\n⏰ {now_ist.strftime('%H:%M IST')}")
    return "".join(lines)
