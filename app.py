"""
NSE Intraday Scanner — Cloud Proxy + Alert Engine
Version : v2.0.0
Hosted on Render.com at: https://nse-proxy-mojx.onrender.com
Frontend at: https://abhisheksa09.github.io/stock-analyser/nse_scanner.html

New endpoints for alert system:
  POST /set-token          — set today's Upstox token (call once each morning)
  GET  /alert-status       — see scanner state, last scan time, alerts sent
  POST /test-alert         — send a test Telegram message to verify setup
  GET  /set-token-form     — simple HTML form to paste token from phone browser
  GET  /get-chat-id        — find your Telegram chat ID (run once during setup)
"""

import os
import json
import logging
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timezone, timedelta

from flask import Flask, request, Response, jsonify
from apscheduler.schedulers.background import BackgroundScheduler

import scanner
import macro as macro_module

log = logging.getLogger("app")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [app] %(message)s")

app = Flask(__name__)
app.url_map.strict_slashes = False  # prevent /path -> /path/ redirects

UPSTOX_BASE    = "https://api.upstox.com"
ANTHROPIC_BASE = "https://api.anthropic.com"
ALLOWED_ORIGIN = "https://abhisheksa09.github.io"
IST            = timezone(timedelta(hours=5, minutes=30))

CORS_HEADERS = {
    "Access-Control-Allow-Origin":  ALLOWED_ORIGIN,
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS, PATCH, DELETE",
    "Access-Control-Allow-Headers": (
        "Authorization, Content-Type, Accept, "
        "x-api-key, anthropic-version, "
        "anthropic-dangerous-direct-browser-access"
    ),
    "Access-Control-Max-Age": "86400",
}

def cors(r):
    for k, v in CORS_HEADERS.items():
        r.headers[k] = v
    return r

@app.after_request
def add_cors(r): return cors(r)

@app.route("/", defaults={"path": ""}, methods=["OPTIONS"])
@app.route("/<path:path>", methods=["OPTIONS"])
def options_handler(path): return cors(Response(status=204))

# ── Health ────────────────────────────────────────────────────────────────────
@app.route("/ping")
def ping():
    return jsonify({"status": "ok", "proxy": "upstox-render", "alerts": "active", "version": "v2.0.0"})

# ── Admin PIN authentication ──────────────────────────────────────────────────
import hashlib as _hashlib, secrets as _secrets

# Simple in-memory session tokens (cleared on restart — acceptable for personal use)
_admin_sessions = set()

def _check_pin(pin: str) -> bool:
    """Verify PIN against ADMIN_PIN env var. Returns True if correct."""
    correct = os.environ.get("ADMIN_PIN", "").strip()
    if not correct:
        return True   # no PIN set = open access (dev mode)
    return _secrets.compare_digest(pin.strip(), correct)

@app.route("/admin/verify", methods=["POST"])
@app.route("/admin/verify/", methods=["POST"])
def admin_verify():
    """
    Verify admin PIN. Returns a session token on success.
    Body: {"pin": "1234"}
    """
    data = request.get_json(silent=True) or {}
    pin  = data.get("pin", "")
    if not _check_pin(pin):
        return jsonify({"ok": False, "error": "Incorrect PIN"}), 401
    # Generate a session token
    tok = _secrets.token_hex(16)
    _admin_sessions.add(tok)
    return jsonify({"ok": True, "session_token": tok})

@app.route("/admin/check", methods=["GET", "POST"])
@app.route("/admin/check/", methods=["GET", "POST"])
def admin_check():
    """Check if a session token is still valid."""
    tok = (request.headers.get("X-Admin-Token") or
           (request.get_json(silent=True) or {}).get("session_token", ""))
    no_pin = not os.environ.get("ADMIN_PIN", "").strip()
    valid  = no_pin or tok in _admin_sessions
    return jsonify({"ok": valid, "pin_required": not no_pin})

# ── Database endpoints ───────────────────────────────────────────────────────
@app.route("/db/init")
@app.route("/db/init/")
def db_init():
    if not _DB:
        return jsonify({"error": "DATABASE_URL not set in Render env vars"}), 503
    try:
        _db.init_db()
        return jsonify({"status": "ok", "message": "All tables created successfully."})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/db/status")
@app.route("/db/status/")
def db_status():
    if not _DB:
        return jsonify({"database": "not configured"}), 503
    try:
        with _db.cursor() as cur:
            cur.execute(
                "SELECT "
                "(SELECT COUNT(*) FROM token_store) AS tokens,"
                "(SELECT COUNT(*) FROM session_state) AS sessions,"
                "(SELECT COUNT(*) FROM trade_history) AS trades,"
                "(SELECT COUNT(*) FROM alert_log) AS alerts"
            )
            row = cur.fetchone()
        return jsonify({
            "database":   "connected",
            "row_counts": {"tokens": row[0], "sessions": row[1],
                           "trades": row[2], "alerts": row[3]},
            "token_info": _db.get_token_info(),
            "ist_now":    datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST"),
        })
    except Exception as e:
        return jsonify({"database": "error", "detail": str(e)}), 500

@app.route("/history/trades", methods=["GET"])
@app.route("/history/trades/", methods=["GET"])
def get_trades():
    if not _DB:
        return jsonify({"error": "Database not configured"}), 503
    try:
        trades = _db.load_trades(
            from_date=request.args.get("from_date"),
            to_date=request.args.get("to_date"),
            sym=request.args.get("sym"),
            sig=request.args.get("sig"),
            outcome=request.args.get("outcome"),
            limit=int(request.args.get("limit", 500)),
        )
        return jsonify({"trades": trades, "stats": _db.get_trade_stats(), "count": len(trades)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/history/trades", methods=["POST"])
@app.route("/history/trades/", methods=["POST"])
def save_trades():
    if not _DB:
        return jsonify({"error": "Database not configured"}), 503
    try:
        data   = request.get_json(silent=True) or {}
        trades = data.get("trades", [data] if data.get("id") else [])
        saved  = sum(1 for t in trades if t.get("id") and _db.save_trade(t))
        return jsonify({"status": "ok", "saved": saved})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/history/trades/<trade_id>", methods=["PATCH"])
@app.route("/history/trades/<trade_id>/", methods=["PATCH"])
def update_trade_route(trade_id):
    if not _DB:
        return jsonify({"error": "Database not configured"}), 503
    try:
        _db.update_trade(trade_id, request.get_json(silent=True) or {})
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/history/trades/<trade_id>", methods=["DELETE"])
@app.route("/history/trades/<trade_id>/", methods=["DELETE"])
def delete_trade_route(trade_id):
    if not _DB:
        return jsonify({"error": "Database not configured"}), 503
    try:
        _db.delete_trade(trade_id)
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/history/stats")
@app.route("/history/stats/")
def trade_stats():
    if not _DB:
        return jsonify({"error": "Database not configured"}), 503
    try:
        return jsonify(_db.get_trade_stats(from_date=request.args.get("from_date")))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/history/alerts")
@app.route("/history/alerts/")
def alert_history():
    if not _DB:
        return jsonify({"error": "Database not configured"}), 503
    try:
        return jsonify(_db.load_alert_log(
            from_date=request.args.get("from_date"),
            sym=request.args.get("sym"),
            limit=int(request.args.get("limit", 100)),
        ))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── Token management ──────────────────────────────────────────────────────────
@app.route("/set-token", methods=["POST"])
def set_token():
    data  = request.get_json(silent=True) or {}
    token = data.get("token", request.form.get("token", "")).strip()
    if not token:
        return jsonify({"error": "token field required"}), 400
    scanner.set_token(token)
    scanner.STATE.check_date()
    return jsonify({
        "status":  "ok",
        "message": "Token set. Scanner will use it for today's alerts.",
        "scanning_symbols": len(scanner.STOCKS),
        "alert_window": f"{os.environ.get('ALERT_START_IST','09:15')} - {os.environ.get('ALERT_STOP_IST','10:30')} IST",
    })

@app.route("/get-token")
@app.route("/get-token/")
def get_token_for_browser():
    """
    Returns the current server-side Upstox token to the browser.
    Called by the scanner UI after OAuth login to sync the token
    into localStorage so the browser can make direct Upstox API calls.
    Only returns the token if it is set — never returns an empty string.
    """
    tok = scanner.get_token()
    if not tok:
        return jsonify({"status": "not_set",
                        "message": "No token on server. Complete OAuth login first."}), 404
    return jsonify({
        "status": "ok",
        "token":  tok,
    })

@app.route("/set-token-form")
def set_token_form():
    wp_ok      = bool(os.environ.get("TELEGRAM_BOT_TOKEN") and os.environ.get("TELEGRAM_CHAT_ID"))
    news_ok    = bool(os.environ.get("NEWS_API_KEY"))
    sc      = "#27500A" if wp_ok else "#A32D2D"
    st      = "Telegram configured" if wp_ok else "Telegram NOT configured - set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in Render env vars"
    nst     = "NewsAPI configured" if news_ok else "NewsAPI not set (optional) - set NEWS_API_KEY for news sentiment"
    start   = os.environ.get("ALERT_START_IST","09:15")
    stop    = os.environ.get("ALERT_STOP_IST","10:30")
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>NSE Scanner - Set Token</title>
<style>
body{{font-family:-apple-system,sans-serif;background:#f5f5f0;padding:24px;color:#1a1a1a;}}
.card{{background:#fff;border:1px solid #e0e0e0;border-radius:12px;padding:24px;max-width:500px;margin:0 auto;}}
h2{{font-size:18px;margin-bottom:4px;}}
.sub{{font-size:12px;color:#888;margin-bottom:20px;}}
textarea{{width:100%;min-height:100px;padding:10px;border:1px solid #ddd;border-radius:8px;
  font-family:monospace;font-size:12px;resize:vertical;box-sizing:border-box;}}
button{{width:100%;padding:12px;background:#27500A;color:#fff;border:none;border-radius:8px;
  font-size:15px;font-weight:600;cursor:pointer;margin-top:12px;}}
button:hover{{background:#1e3d07;}}
.status{{font-size:12px;color:{sc};margin-top:12px;padding:8px 12px;background:#f5f5f0;border-radius:6px;}}
.window{{font-size:12px;color:#185FA5;margin-top:8px;padding:8px 12px;background:#E6F1FB;border-radius:6px;}}
.result{{display:none;margin-top:12px;padding:12px;border-radius:8px;font-size:13px;}}
.ok{{background:#EAF3DE;color:#27500A;border:1px solid #C0DD97;}}
.err{{background:#FCEBEB;color:#791F1F;border:1px solid #F09595;}}
</style></head>
<body><div class="card">
  <h2>NSE Scanner - Set Token</h2>
  <div class="sub">Paste your Upstox token once each morning to activate Telegram alerts</div>
  <textarea id="tok" placeholder="Paste Upstox access token here..."></textarea>
  <button onclick="submit()">Set token and activate alerts</button>
  <div class="status">{st}</div>
  <div class="status" style="margin-top:4px;font-size:11px;color:#888;">{nst}</div>
  <div class="window">Alert window: {start} - {stop} IST | Every 5 minutes</div>
  <div class="result" id="res"></div>
</div>
<script>
async function submit(){{
  var tok=document.getElementById('tok').value.trim();
  if(!tok){{alert('Paste your token first.');return;}}
  var res=document.getElementById('res');
  try{{
    var r=await fetch('/set-token',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{token:tok}})}});
    var d=await r.json();
    res.className='result '+(r.ok?'ok':'err');
    res.style.display='block';
    res.textContent=r.ok?'OK: '+d.message+' Watching '+d.scanning_symbols+' stocks during '+d.alert_window:'Error: '+(d.error||'unknown');
  }}catch(e){{res.className='result err';res.style.display='block';res.textContent='Error: '+e.message;}}
}}
</script></body></html>"""

# ── Alert status ──────────────────────────────────────────────────────────────
@app.route("/alert-status")
def alert_status():
    return jsonify({
        "session_date":      scanner.STATE.date,
        "token_set":         bool(scanner.get_token()),
        "locked_signals":    scanner.STATE.locked_sig,
        "alerts_sent_today": sorted(scanner.STATE.alerted),
        "prev_confidence":   scanner.STATE.prev_conf,
        "telegram_username": os.environ.get("TELEGRAM_CHAT_ID", "not set"),
        "telegram_bot_set":  bool(os.environ.get("TELEGRAM_BOT_TOKEN")),
        "scan_symbols":      os.environ.get("SCAN_SYMBOLS", "all 30"),
        "alert_window":      f"{os.environ.get('ALERT_START_IST','09:15')} - {os.environ.get('ALERT_STOP_IST','10:30')} IST",
        "ist_now":           datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST"),
    })

# ── Macro status ─────────────────────────────────────────────────────────────
@app.route("/macro-status")
def macro_status():
    """Shows current macro context — economic calendar, proxies, FII/DII, news sentiment."""
    ctx = scanner.STATE.macro_ctx
    if not ctx:
        return jsonify({
            "status": "not fetched yet",
            "note": "Macro context is fetched at first scan. Set token and wait for 09:15 IST.",
        })
    # Simplify proxies for display
    proxies_summary = {}
    for k, v in (ctx.get("proxies") or {}).items():
        if v:
            proxies_summary[k] = {"price": v["price"], "chg_pct": v["chg_pct"]}
    return jsonify({
        "fetched_at":       ctx.get("fetched_at"),
        "in_event_window":  ctx.get("in_event_window"),
        "event_desc":       ctx.get("event_desc"),
        "calendar_events":  len(ctx.get("calendar") or []),
        "proxies":          proxies_summary,
        "fii_dii":          ctx.get("fii_dii"),
        "news_headlines":   len(ctx.get("news_headlines") or []),
        "news_sentiment":   ctx.get("news_sentiment"),
        "ist_now":          datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST"),
    })

# ── Test alert ────────────────────────────────────────────────────────────────
@app.route("/test-alert", methods=["POST", "GET"])
def test_alert():
    ist = datetime.now(IST).strftime("%H:%M IST")
    msg = (f"NSE Scanner - Test Alert\n"
           f"CallMeBot is working.\n"
           f"Alerts fire between "
           f"{os.environ.get('ALERT_START_IST','09:15')} - "
           f"{os.environ.get('ALERT_STOP_IST','10:30')} IST\n"
           f"Sent at {ist}")
    ok = scanner.send_telegram(msg)
    if ok:
        return jsonify({"status": "ok", "message": "Test Telegram message sent"})
    return jsonify({"status": "error", "message": "Failed - check TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID"}), 500

# ── Get Telegram Chat ID (setup helper) ──────────────────────────────────────
@app.route("/get-chat-id")
def get_chat_id():
    """
    Visit this URL after sending /start to your bot.
    Returns your chat ID so you can set TELEGRAM_CHAT_ID env var.
    """
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    if not bot_token:
        return jsonify({"error": "TELEGRAM_BOT_TOKEN not set in Render env vars"}), 400
    result = scanner.get_telegram_chat_id(bot_token)
    if not result.get("ok"):
        return jsonify({"error": "Telegram API error", "detail": result}), 500
    updates = result.get("result", [])
    if not updates:
        return jsonify({
            "error": "No messages found",
            "fix": "Send /start to your bot first, then reload this page"
        }), 404
    # Extract unique chat IDs from recent messages
    chats = []
    seen  = set()
    for u in updates:
        msg  = u.get("message") or u.get("channel_post") or {}
        chat = msg.get("chat", {})
        cid  = chat.get("id")
        if cid and cid not in seen:
            seen.add(cid)
            chats.append({
                "chat_id":   cid,
                "type":      chat.get("type"),
                "username":  chat.get("username"),
                "first_name": chat.get("first_name"),
            })
    return jsonify({
        "found_chats": chats,
        "next_step": "Copy the chat_id value and set it as TELEGRAM_CHAT_ID in Render env vars"
    })

# ── Dry run test scan ────────────────────────────────────────────────────────
@app.route("/dry-scan", methods=["GET", "POST"])
def dry_scan():
    """
    Trigger a one-shot test scan right now, ignoring the time window.
    Uses live Upstox data if token is set, otherwise uses mock data.
    Sends a real Telegram alert for the first stock that qualifies
    (or the best available if none are fully green).

    Query params:
      sym   — specific symbol to test, e.g. /dry-scan?sym=HDFCBANK
      mock  — use mock data instead of live API, e.g. /dry-scan?mock=1
    """
    import time as _time

    # Accept token from: 1) request header (browser passes its fresh token)
    #                    2) server-side scanner token (from DB/OAuth)
    #                    3) fall back to mock if neither available
    header_token = request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    server_token = scanner.get_token()
    token        = header_token or server_token
    sym_req      = request.args.get("sym", "").upper().strip()
    use_mock     = request.args.get("mock", "0") == "1" or not token

    from signals import STOCKS, build_setup, is_ready
    from scanner import send_telegram, format_alert, STATE

    # Pick stock(s) to test
    if sym_req:
        stocks = [s for s in STOCKS if s["sym"] == sym_req]
        if not stocks:
            return jsonify({"error": f"Symbol {sym_req} not found. Try HDFCBANK, INFY etc."}), 400
    else:
        # Default: test first 5 stocks
        stocks = STOCKS[:5]

    results = []
    alert_sent = None

    for stock in stocks:
        sym = stock["sym"]
        try:
            if use_mock:
                # Build a synthetic green setup for testing
                ltp = 1000.0
                s = {
                    "sym": sym, "sec": stock["sec"],
                    "ltp": ltp, "chg": 0.8,
                    "orbH": 1005.0, "orbL": 995.0,
                    "rsi": 52.0, "vwap": 998.0,
                    "atr": 12.0, "tV": 1500000, "aV": 1000000,
                    "sig": "BUY", "en": 1005.05,
                    "tg": round(1005.05 + 12*2, 2),
                    "sl": round(995.0 - 12*0.3, 2),
                    "reason": "DRY RUN — mock data (ORB breakout + above VWAP + oversold RSI)",
                    "rr": 2.1, "av": True, "bo": True, "bd": False,
                    "confirmed": True, "confirmCount": 3,
                    "gapPct": 0.3, "conf": 78,
                    "ctxWarnings": [], "marketBlocked": False,
                    "market_ctx": {}, "cBd": [],
                }
            else:
                from signals import get_ltp, get_intraday, get_daily, get_market_context
                ltp   = get_ltp(stock["ikey"], token)
                intra = get_intraday(stock["ikey"], token)
                daily = get_daily(stock["ikey"], token)
                ctx   = get_market_context(stock["sec"], token)
                s     = build_setup(sym, stock["sec"], intra, daily, ltp, market_ctx=ctx)

            # Force IST minutes to 9:50 (prime window) for readiness check
            verdict, gates_ok = is_ready(s, ist_mins=590)

            results.append({
                "sym":     sym,
                "sig":     s["sig"],
                "conf":    s["conf"],
                "verdict": verdict,
                "ltp":     s["ltp"],
                "entry":   s["en"],
                "target":  s["tg"],
                "sl":      s["sl"],
                "reason":  s["reason"],
                "mock":    use_mock,
            })

            # Send Telegram for first green/amber result
            if alert_sent is None and verdict in ("green", "amber"):
                msg = format_alert("green_ready", s)
                msg = msg.replace(
                    "NSE SCANNER — READY TO TRADE",
                    "DRY RUN — READY TO TRADE (TEST)"
                )
                ok = send_telegram(msg)
                alert_sent = {"sym": sym, "telegram_sent": ok, "verdict": verdict}
                _time.sleep(1)

        except Exception as e:
            err = str(e)
            hint = " — Token may be expired. Login again via OAuth." if "403" in err or "401" in err else ""
            results.append({"sym": sym, "error": err + hint})

    # If nothing was green, force send an alert for the highest confidence result
    if alert_sent is None and results:
        best = max(
            [r for r in results if "conf" in r],
            key=lambda r: r["conf"],
            default=None
        )
        if best:
            # Build minimal message
            msg = (
                "<b>DRY RUN — BEST AVAILABLE (no green signals)</b>\n\n"
                + "<b>" + best["sym"] + "</b>  Conf: <b>" + str(best["conf"]) + "%</b>\n"
                + "Signal: " + best["sig"] + "  |  Verdict: " + best["verdict"] + "\n"
                + "Entry: Rs " + str(best["entry"]) + "  |  SL: Rs " + str(best["sl"]) + "\n"
                + "<i>" + best["reason"][:80] + "</i>"
            )
            ok = send_telegram(msg)
            alert_sent = {"sym": best["sym"], "telegram_sent": ok,
                          "note": "No green signals found — sent best available"}

    return jsonify({
        "mode":        "mock" if use_mock else "live",
        "stocks_tested": len(results),
        "results":     results,
        "alert_sent":  alert_sent,
        "tip": (
            "No token set — used mock data. Call POST /set-token first for live data."
            if use_mock and not token else
            "Live data used. Check your Telegram for the alert."
            if not use_mock else
            "Mock data used. Check your Telegram for the alert."
        ),
    })

# ── Upstox OAuth (mobile-friendly token flow) ────────────────────────────────
#
# How it works:
#   1. User opens /auth/login on phone browser
#   2. Redirected to Upstox login page
#   3. After login, Upstox redirects to /auth/callback?code=xxxxx
#   4. Render exchanges code for token automatically
#   5. Token is set in scanner and user sees success page
#
# ONE-TIME SETUP in Upstox developer portal:
#   developer.upstox.com -> your app -> Redirect URI:
#   https://nse-proxy-mojx.onrender.com/auth/callback
#
# Then set env vars in Render:
#   UPSTOX_API_KEY    = your app's API key
#   UPSTOX_API_SECRET = your app's API secret

UPSTOX_AUTH_BASE  = "https://api.upstox.com/v2/login/authorization"
RENDER_BASE_URL   = "https://nse-proxy-mojx.onrender.com"
OAUTH_REDIRECT    = RENDER_BASE_URL + "/auth/callback"

# Store last OAuth attempt result for debugging
_last_oauth = {"status": "never attempted", "detail": "", "time": ""}

@app.route("/auth/login")
def auth_login():
    """
    Step 1: Open this on your phone every morning.
    Redirects to Upstox login page.
    After login Upstox sends you back to /auth/callback automatically.
    """
    api_key = os.environ.get("UPSTOX_API_KEY", "")
    if not api_key:
        return (
            "<h2 style='font-family:sans-serif;color:#A32D2D;padding:24px'>"
            "UPSTOX_API_KEY not set in Render environment variables.</h2>"
            "<p style='font-family:sans-serif;padding:0 24px'>"
            "Add it in Render dashboard &#8594; Environment &#8594; UPSTOX_API_KEY</p>"
        ), 400

    login_url = (
        f"{UPSTOX_AUTH_BASE}/dialog"
        f"?response_type=code"
        f"&client_id={urllib.parse.quote(api_key)}"
        f"&redirect_uri={urllib.parse.quote(OAUTH_REDIRECT)}"
        f"&state=nse_scanner"
    )
    from flask import redirect as flask_redirect
    return flask_redirect(login_url)


@app.route("/auth/callback")
def auth_callback():
    """
    Step 2: Upstox redirects here after login with ?code=xxxxx
    We exchange the code for a token and set it automatically.
    User sees a success or error page — no copy-pasting needed.
    """
    code  = request.args.get("code", "")
    error = request.args.get("error", "")

    if error or not code:
        return _auth_page(
            success=False,
            title="Login cancelled",
            message=f"Upstox returned an error: {error or 'no code received'}",
        )

    api_key    = os.environ.get("UPSTOX_API_KEY", "")
    api_secret = os.environ.get("UPSTOX_API_SECRET", "")

    if not api_key or not api_secret:
        return _auth_page(
            success=False,
            title="Configuration error",
            message="UPSTOX_API_KEY or UPSTOX_API_SECRET not set in Render env vars.",
        )

    # Exchange auth code for access token
    try:
        payload = urllib.parse.urlencode({
            "code":          code,
            "client_id":     api_key,
            "client_secret": api_secret,
            "redirect_uri":  OAUTH_REDIRECT,
            "grant_type":    "authorization_code",
        }).encode("utf-8")

        req = urllib.request.Request(
            f"{UPSTOX_AUTH_BASE}/token",
            data=payload,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept":       "application/json",
                "User-Agent":   "Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36 Chrome/120.0.0.0 Mobile Safari/537.36",
                "Api-Version":  "2.0",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=15) as r:
            resp = json.loads(r.read())

        token = resp.get("access_token", "")
        if not token:
            _last_oauth["status"] = "failed"
            # Don't store full response — may contain sensitive data
            keys_received = list(resp.keys()) if isinstance(resp, dict) else "non-dict response"
            _last_oauth["detail"] = f"No access_token in response. Keys received: {keys_received}"
            _last_oauth["time"]   = datetime.now(IST).strftime("%H:%M IST")
            return _auth_page(
                success=False,
                title="Token exchange failed",
                message=(
                    f"Upstox did not return an access_token.<br><br>"
                    f"<small style='color:#888'>Response: {json.dumps(resp)[:200]}</small><br><br>"
                    f"Most likely cause: redirect URI in Upstox developer portal "
                    f"does not exactly match:<br>"
                    f"<code>https://nse-proxy-mojx.onrender.com/auth/callback</code>"
                ),
            )

        # Set token in scanner automatically
        scanner.set_token(token)
        scanner.STATE.check_date()

        # Verify it was set
        stored = scanner.get_token()
        if not stored:
            _last_oauth["status"] = "set_failed"
            _last_oauth["detail"] = "set_token called but get_token returned empty"
            _last_oauth["time"]   = datetime.now(IST).strftime("%H:%M IST")
            return _auth_page(
                success=False,
                title="Internal error — token not stored",
                message="Token was received but could not be stored. Please try again.",
            )

        ist_time = datetime.now(IST).strftime("%H:%M IST")
        _last_oauth["status"] = "success"
        _last_oauth["detail"] = f"Token set at {ist_time}, length={len(token)}"
        _last_oauth["time"]   = ist_time
        log.info("OAuth success — token set at %s (length=%d chars)", ist_time, len(token))

        return _auth_page(
            success=True,
            title="Token set successfully",
            message=(
                f"Logged in at {ist_time}.<br>"
                f"Scanner is now active and will run every 5 min "
                f"from {os.environ.get('ALERT_START_IST','09:15')} to "
                f"{os.environ.get('ALERT_STOP_IST','10:30')} IST."
            ),
        )

    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        _last_oauth["status"] = f"http_error_{e.code}"
        # Store error type only, not full body (may contain sensitive fragments)
        import json as _json
        try:
            err_data = _json.loads(body)
            _last_oauth["detail"] = err_data.get("error", body[:100])
        except Exception:
            _last_oauth["detail"] = body[:100]
        _last_oauth["time"]   = datetime.now(IST).strftime("%H:%M IST")
        return _auth_page(
            success=False,
            title=f"Upstox API error (HTTP {e.code})",
            message=(
                f"{body[:200]}<br><br>"
                f"<small>If you see 'invalid_grant' — the auth code was already used "
                f"or expired. Open /auth/login again to get a fresh code.</small>"
            ),
        )
    except Exception as e:
        _last_oauth["status"] = "exception"
        _last_oauth["detail"] = str(e)
        _last_oauth["time"]   = datetime.now(IST).strftime("%H:%M IST")
        return _auth_page(
            success=False,
            title="Unexpected error",
            message=str(e),
        )


def _auth_page(success: bool, title: str, message: str) -> str:
    """Render a clean mobile-friendly result page."""
    color  = "#27500A" if success else "#A32D2D"
    bg     = "#EAF3DE" if success else "#FCEBEB"
    border = "#C0DD97" if success else "#F09595"
    icon   = "&#10003;" if success else "&#10007;"
    status_url = RENDER_BASE_URL + "/alert-status"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>NSE Scanner &#8212; {title}</title>
<style>
  body{{font-family:-apple-system,sans-serif;background:#f5f5f0;
       display:flex;align-items:center;justify-content:center;
       min-height:100vh;margin:0;padding:16px;box-sizing:border-box;}}
  .card{{background:#fff;border:1px solid #e0e0e0;border-radius:16px;
         padding:32px 28px;max-width:420px;width:100%;text-align:center;
         box-shadow:0 4px 24px rgba(0,0,0,.08);}}
  .icon{{font-size:48px;margin-bottom:16px;background:{bg};
         border:2px solid {border};border-radius:50%;width:72px;height:72px;
         display:flex;align-items:center;justify-content:center;
         margin:0 auto 20px;color:{color};font-weight:700;}}
  h2{{font-size:20px;color:{color};margin-bottom:12px;}}
  p{{font-size:14px;color:#555;line-height:1.6;margin-bottom:20px;}}
  .btn{{display:inline-block;padding:12px 24px;border-radius:10px;
        background:#27500A;color:#fff;text-decoration:none;font-size:15px;
        font-weight:600;margin:6px;}}
  .btn-sec{{background:#E6F1FB;color:#185FA5;}}
  .tip{{font-size:12px;color:#aaa;margin-top:16px;}}
</style>
</head>
<body>
<div class="card">
  <div class="icon">{icon}</div>
  <h2>{title}</h2>
  <p>{message}</p>
  {'<a href="/auth/login" class="btn">Try again</a>' if not success else
   '<p style="font-size:13px;color:#888;margin-top:8px;">This tab will close in <span id="cd">3</span>s&#8230;</p>'}
  {'<a href="' + status_url + '" class="btn btn-sec">Check alert status</a>' if not success else ''}
  <p class="tip">NSE Intraday Scanner &#183; Render.com</p>
</div>
{'<script>if(window.opener){window.opener.postMessage({type:"upstox_auth_success"},"*");}let n=3;const i=setInterval(()=>{n--;const el=document.getElementById("cd");if(el)el.textContent=n;if(n<=0){clearInterval(i);window.close();}},1000);</script>' if success else ''}
</body>
</html>"""

# ── OAuth debug status ───────────────────────────────────────────────────────
@app.route("/auth/status")
def auth_status():
    """Shows the result of the last OAuth login attempt. Useful for debugging."""
    api_key    = os.environ.get("UPSTOX_API_KEY", "")
    api_secret = os.environ.get("UPSTOX_API_SECRET", "")
    return jsonify({
        "last_oauth_attempt":  _last_oauth,
        "token_currently_set": bool(scanner.get_token()),
        "token_length":        len(scanner.get_token()) if scanner.get_token() else 0,
        "api_key_set":         bool(api_key),
        "api_secret_set":      bool(api_secret),
        "redirect_uri":        OAUTH_REDIRECT,
        "login_url":           RENDER_BASE_URL + "/auth/login",
        "ist_now":             datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST"),
    })

# ── NSE corporate actions ─────────────────────────────────────────────────────
@app.route("/nse/corporate-actions")
def nse_corporate_actions():
    target  = "https://www.nseindia.com/api/corporates-corporateActions?index=equities"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/market-data/upcoming-corporate-actions",
        "Origin": "https://www.nseindia.com",
    }
    try:
        with urllib.request.urlopen(urllib.request.Request(target, headers=headers), timeout=10) as r:
            return Response(r.read(), status=200, mimetype="application/json")
    except Exception:
        return jsonify([])

# ── Anthropic proxy ───────────────────────────────────────────────────────────
# ── Scanner chatbot endpoint ──────────────────────────────────────────────────
@app.route("/chat", methods=["POST"])
@app.route("/chat/", methods=["POST"])
def scanner_chat():
    """Proxy chat to Anthropic claude-haiku-4-5 with NSE Scanner system prompt embedded."""
    import json as _jsc, urllib.request as _urc

    ak = os.environ.get("ANTHROPIC_API_KEY", "")
    if not ak:
        return jsonify({"error": "ANTHROPIC_API_KEY not configured on server"}), 503

    body     = request.get_json(silent=True) or {}
    messages = body.get("messages", [])
    max_tok  = min(int(body.get("max_tokens", 1024)), 2048)

    SYSTEM = (
        "You are the NSE Intraday Scanner Assistant. Answer questions about this specific app. "
        "App: scanner.html on GitHub Pages + Flask on Render (nse-proxy-mojx.onrender.com) v2.2.0. "
        "SIGNALS: 30 Nifty stocks. BUY=RSI<40+above VWAP+above ORB High. SELL=RSI>60+below VWAP+below ORB Low. "
        "CONFIDENCE: 6 factors: ORB 25%+Volume 20%+VWAP 20%+RSI 15%+RR 15%+ATR 5%. "
        "Green>=75%(full size), Amber 55-74%(half), Red<55%(skip). "
        "MARKET FILTERS: Nifty+-0.5% HARD BLOCK. Sector headwind -15%. Gap -10%. Day trend +-10%. "
        "MACRO LAYERS (30min cache): "
        "1-Calendar: high-impact event+-30min=-50%. "
        "2-Yahoo Finance: crude/gold/USDINR/SPX/VIX/DXY sector-specific penalties. "
        "3-FII/DII: net sellers>500Cr=-10%. "
        "4-NewsAPI+Claude Haiku sentiment: +-5 to +-15%. "
        "ALERTS: APScheduler every 5min 9:15-10:30 IST. 3 triggers: Green Ready/conf crossed 75%/signal reversal. "
        "OAUTH: /auth/login->Upstox->popup auto-closes->dot turns green. "
        "DB: Supabase PostgreSQL. Tables: token_store/session_state/trade_history/alert_log. "
        "ADMIN: PIN-protected (ADMIN_PIN env var). Collapsible cards: System Status/Dry Scan/Database/Quick Links. "
        "COMMON ISSUES: 403=token expired login again. Orange dot=Render sleeping. Version stuck=clear cache ?v=N. "
        "FILES: app.py/scanner.py/signals.py/macro.py/db.py. "
        "Use markdown formatting. Be specific with numbers and thresholds."
    )

    payload = _jsc.dumps({
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": max_tok,
        "system": SYSTEM,
        "messages": messages,
    }).encode("utf-8")

    req = _urc.Request(
        "https://api.anthropic.com/v1/messages",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "x-api-key": ak,
            "anthropic-version": "2023-06-01",
        },
        method="POST",
    )
    try:
        with _urc.urlopen(req, timeout=30) as r:
            return jsonify(_jsc.loads(r.read()))
    except _urc.HTTPError as e:
        return jsonify({"error": e.read().decode(errors="replace")[:200]}), e.code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/ai/<path:subpath>", methods=["GET", "POST"])
def ai_proxy(subpath):
    target = f"{ANTHROPIC_BASE}/{subpath}"
    body   = request.get_data()
    fh = {}
    for h in ["Content-Type","x-api-key","anthropic-version","anthropic-dangerous-direct-browser-access"]:
        v = request.headers.get(h)
        if v: fh[h] = v
    fh.setdefault("anthropic-version","2023-06-01")
    fh.setdefault("anthropic-dangerous-direct-browser-access","true")
    req = urllib.request.Request(target, data=body or None, headers=fh, method=request.method)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return Response(r.read(), status=r.status, mimetype="application/json")
    except urllib.error.HTTPError as e:
        return Response(e.read(), status=e.code, mimetype="application/json")
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── Upstox proxy ──────────────────────────────────────────────────────────────
@app.route("/v2/<path:subpath>", methods=["GET", "POST"])
def upstox_proxy(subpath):
    qs     = request.query_string.decode()
    target = f"{UPSTOX_BASE}/v2/{subpath}" + (f"?{qs}" if qs else "")
    body   = request.get_data() if request.method == "POST" else None
    fh = {}
    for h in ["Authorization","Content-Type","Accept"]:
        v = request.headers.get(h)
        if v: fh[h] = v
    fh["User-Agent"] = "UpstoxProxy/2.0-Render"
    req = urllib.request.Request(target, data=body, headers=fh, method=request.method)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return Response(r.read(), status=r.status,
                            mimetype=r.headers.get("Content-Type","application/json"))
    except urllib.error.HTTPError as e:
        return Response(e.read(), status=e.code, mimetype="application/json")
    except urllib.error.URLError as e:
        return jsonify({"error": str(e.reason)}), 502
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── History disabled ──────────────────────────────────────────────────────────
@app.route("/history/read")
def history_read():
    return jsonify({"error": "File history not available on cloud."}), 410

@app.route("/history/write", methods=["POST"])
def history_write():
    return jsonify({"error": "File history not available on cloud."}), 410

# ── Start background scheduler ────────────────────────────────────────────────
def start_scheduler():
    interval = int(os.environ.get("SCAN_INTERVAL_MINS", "5"))
    sched = BackgroundScheduler(timezone="Asia/Kolkata")
    sched.add_job(scanner.run_scan, trigger="interval", minutes=interval,
                  id="nse_scan", max_instances=1, misfire_grace_time=60)
    sched.start()
    log.info("Scheduler started — scanning every %d min", interval)
    return sched

_scheduler = start_scheduler()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
