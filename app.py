"""
NSE Intraday Scanner — Cloud Proxy + Alert Engine
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
from flask import Flask, request, Response, jsonify, make_response
from apscheduler.schedulers.background import BackgroundScheduler
from ai_insights import get_ai_setup_insight

import scanner
import macro as macro_module
import db as _db_module
import auto_login as _auto_login

log = logging.getLogger("app")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [app] %(message)s")

app = Flask(__name__)
app.url_map.strict_slashes = False  # prevent /path -> /path/ redirects

UPSTOX_BASE    = "https://api.upstox.com"
ANTHROPIC_BASE = "https://api.anthropic.com"
ALLOWED_ORIGIN = "https://abhisheksa09.github.io"
IST            = timezone(timedelta(hours=5, minutes=30))
# ─── Session helpers ──────────────────────────────────────────────────────────
SESSION_COOKIE  = "nse_session"
SESSION_TTL_SEC = 30 * 24 * 3600

def _load_token_from_db():
    try:
        tok = _db_module.get_token()
        if tok:
            scanner.set_token(tok)
            log.info("Loaded Upstox token from DB on startup")
    except Exception as e:
        log.warning("Could not load token from DB: %s", e)

def _get_session(req):
    token = (
        req.headers.get("X-Session-Token", "") or
        req.cookies.get(SESSION_COOKIE, "")
    )
    if not token:
        return None
    return _db_module.validate_app_session(token)

def _require_session(req):
    sess = _get_session(req)
    if not sess:
        return None, (jsonify({"error": "Not authenticated", "code": "AUTH_REQUIRED"}), 401)
    return sess, None

def _has_db():
    return _db_module.get_connection() is not None

CORS_HEADERS = {
    "Access-Control-Allow-Origin":      ALLOWED_ORIGIN,
    "Access-Control-Allow-Methods":     "GET, POST, OPTIONS, PATCH, DELETE",
    "Access-Control-Allow-Headers":     (
        "Authorization, Content-Type, Accept, "
        "X-Session-Token, "
        "x-api-key, anthropic-version, "
        "anthropic-dangerous-direct-browser-access"
    ),
    "Access-Control-Allow-Credentials": "true",
    "Access-Control-Max-Age":           "86400",
}

def cors(r):
    for k, v in CORS_HEADERS.items():
        r.headers[k] = v
    return r

@app.before_request
def handle_preflight():
    if request.method == "OPTIONS":
        return cors(make_response("", 204))

@app.after_request
def add_cors(r): return cors(r)

@app.route("/", defaults={"path": ""}, methods=["OPTIONS"])
@app.route("/<path:path>", methods=["OPTIONS"])
def options_handler(path): return cors(Response(status=204))

# ── Health ────────────────────────────────────────────────────────────────────
@app.route("/ping")
def ping():
    return jsonify({"status": "ok", "proxy": "upstox-render", "alerts": "active"})

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
    Returns today's Upstox token to the browser.
    Uses in-memory token first, then falls back to DB token_store.
    """
    tok = get_effective_token()

    if not tok:
        return jsonify({
            "status": "not_set",
            "message": "No token available for today. Complete Upstox login once."
        }), 404

    return jsonify({
        "status": "ok",
        "token": tok,
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
        "token_set":         bool(get_effective_token()),
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

def get_effective_token():
    """
    Return today's Upstox token.
    First try in-memory scanner token.
    If missing, fall back to DB token_store and reload memory.
    """
    tok = scanner.get_token()

    if not tok:
        try:
            tok = _db_module.get_token()
            if tok:
                scanner.set_token(tok)
                log.info("Loaded Upstox token from DB fallback")
        except Exception as e:
            log.warning("DB token fallback failed: %s", e)
            tok = None

    return tok

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

    token    = get_effective_token()
    sym_req  = request.args.get("sym", "").upper().strip()
    use_mock = request.args.get("mock", "0") == "1" or not token

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
            results.append({"sym": sym, "error": str(e)})

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

# Store last auto-login attempt result
_last_auto_login = {
    "status":    "never attempted",   # never attempted | success | failed | running
    "detail":    "",
    "time":      "",
    "next_run":  "08:30 IST (daily)",
    "configured": False,
}


# ─── Session helpers ──────────────────────────────────────────────────────────

def _get_session(req) -> dict | None:
    token = (
        req.headers.get("X-Session-Token", "") or
        req.headers.get("Authorization", "").replace("Bearer ", "").strip() or
        req.cookies.get(SESSION_COOKIE, "")
    )
    if not token:
        return None
    return _db_module.validate_app_session(token)

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

# ─── App auth ─────────────────────────────────────────────────────────────────
@app.route("/app/login", methods=["POST", "OPTIONS"])
def app_login():
    data     = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    if not username or not password:
        return jsonify({"error": "username and password required"}), 400
    user = _db_module.authenticate_user(username, password)
    if not user:
        return jsonify({"error": "Invalid username or password"}), 401
    token = _db_module.create_app_session(user)
    if not token:
        return jsonify({"error": "Could not create session"}), 500
    resp = jsonify({"status": "ok", "username": user["username"], "role": user["role"], "token": token})
    return _set_session_cookie(resp, token)

@app.route("/app/logout", methods=["POST", "OPTIONS"])
def app_logout():
    token = (request.headers.get("X-Session-Token","") or
             request.cookies.get(SESSION_COOKIE, ""))
    if token:
        _db_module.revoke_app_session(token)
    resp = jsonify({"status": "ok"})
    return _clear_session_cookie(resp)


@app.route("/app/me", methods=["GET", "OPTIONS"])
def app_me():
    sess = _get_session(request)
    if not sess:
        return jsonify({"authenticated": False}), 401
    return jsonify({"authenticated": True, "username": sess["username"], "role": sess["role"]})


@app.route("/app/change-password", methods=["POST", "OPTIONS"])
def app_change_password():
    sess, err = _require_session(request)
    if err: return err
    data    = request.get_json(silent=True) or {}
    current = data.get("current") or ""
    new_pwd = data.get("new") or ""
    if len(new_pwd) < 8:
        return jsonify({"error": "New password must be >= 8 chars"}), 400
    user = _db_module.authenticate_user(sess["username"], current)
    if not user:
        return jsonify({"error": "Current password incorrect"}), 401
    conn = _db_module.db()
    if not conn: return jsonify({"error": "No DB"}), 503
    try:
        pwd_hash = _db_module.hash_password(new_pwd)
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET pwd_hash=%s WHERE username=%s", (pwd_hash, sess["username"]))
        _db_module.revoke_all_sessions(sess["username"])
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE username=%s", (sess["username"],))
            user = dict(cur.fetchone())
        new_token = _db_module.create_app_session(user)
        resp = jsonify({
            "status": "ok",
            "message": "Password changed successfully",
            "token": new_token
        })
        return _set_session_cookie(resp, new_token)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
def _set_session_cookie(response, token: str):
    response.set_cookie(SESSION_COOKIE, token, max_age=SESSION_TTL_SEC,
                        httponly=True, secure=True, samesite="None", path="/")
    return response

def _clear_session_cookie(response):
    response.delete_cookie(SESSION_COOKIE, path="/", samesite="None", secure=True)
    return response

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
        _db_module.set_token(token, set_by="oauth")
        scanner.STATE.check_date()

        # Verify it was set
        stored = get_effective_token()
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


# ─── Morning login reminder (Telegram) ────────────────────────────────────────

def send_login_reminder_job():
    """
    Scheduled job — runs at 08:30 IST every day.
    Sends a Telegram message with a one-tap Upstox login link.
    Tapping it on your phone opens the OAuth flow and sets the token automatically.
    """
    global _last_auto_login
    ist_time = datetime.now(IST).strftime("%H:%M IST")
    log.info("Login reminder job started at %s", ist_time)

    _last_auto_login["status"] = "running"
    _last_auto_login["time"]   = ist_time
    _last_auto_login["detail"] = "Sending reminder…"

    success, msg = _auto_login.send_login_reminder()

    _last_auto_login["status"] = "success" if success else "failed"
    _last_auto_login["detail"] = msg
    _last_auto_login["time"]   = datetime.now(IST).strftime("%H:%M IST")


@app.route("/auth/auto-login-status")
def auto_login_status():
    """Return current reminder state (no auth required)."""
    return jsonify({
        "configured": _auto_login.is_configured(),
        "status":     _last_auto_login["status"],
        "detail":     _last_auto_login["detail"],
        "time":       _last_auto_login["time"],
        "next_run":   _last_auto_login["next_run"],
    })


@app.route("/auth/trigger-auto-login", methods=["POST"])
def trigger_auto_login():
    """Send login reminder now (admin only)."""
    _, err = _require_admin(request)
    if err:
        return err

    send_login_reminder_job()

    if _last_auto_login["status"] == "success":
        return jsonify({
            "status":  "success",
            "message": _last_auto_login["detail"],
            "time":    _last_auto_login["time"],
        })
    return jsonify({
        "status":  "failed",
        "message": _last_auto_login["detail"],
        "time":    _last_auto_login["time"],
    }), 500


# ─── Admin misc ───────────────────────────────────────────────────────────────
@app.route("/admin/check", methods=["POST", "OPTIONS"])
def admin_check():
    """Check if stored admin session token is still valid."""
    data = request.get_json(silent=True) or {}
    stok = data.get("session_token", "")

    if not stok:
        return jsonify({"ok": False, "pin_required": True})

    sess = _db_module.validate_app_session(stok)
    if sess and sess.get("role") == "admin":
        return jsonify({"ok": True, "pin_required": False})

    return jsonify({"ok": False, "pin_required": True})

# ─── Admin user management ────────────────────────────────────────────────────
def _require_admin(req):
    sess, err = _require_session(req)
    if err: return None, err
    if sess.get("role") != "admin":
        return None, (jsonify({"error": "Admin role required"}), 403)
    return sess, None

@app.route("/admin/users", methods=["GET"])
def admin_list_users():
    sess, err = _require_admin(request)
    if err: return err
    return jsonify({"users": _db_module.get_users()})


@app.route("/admin/users/create", methods=["POST"])
def admin_create_user():
    sess, err = _require_admin(request)
    if err: return err
    data     = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    role     = data.get("role", "viewer")
    if not username or len(password) < 8:
        return jsonify({"error": "username required, password >= 8 chars"}), 400
    if role not in ("admin", "viewer"):
        return jsonify({"error": "role must be admin or viewer"}), 400
    result = _db_module.create_user(username, password, role)
    if "error" in result:
        return jsonify(result), 409
    return jsonify(result)


@app.route("/admin/users/<username>/deactivate", methods=["POST"])
def admin_deactivate_user(username):
    sess, err = _require_admin(request)
    if err: return err
    _db_module.set_user_active(username, False)
    _db_module.revoke_all_sessions(username)
    return jsonify({"status": "ok"})


@app.route("/admin/users/<username>/activate", methods=["POST"])
def admin_activate_user(username):
    sess, err = _require_admin(request)
    if err: return err
    _db_module.set_user_active(username, True)
    return jsonify({"status": "ok"})

@app.route("/admin/verify", methods=["POST", "OPTIONS"])
def admin_verify():
    """Verify admin PIN and unlock admin content."""
    data = request.get_json(silent=True) or {}
    pin = data.get("pin", "")

    if not pin or pin != os.environ.get("ADMIN_PIN", ""):
        return jsonify({"ok": False, "error": "Invalid PIN"}), 403

    # If current logged-in app session is admin, return that session token
    sess = _get_session(request)
    if sess and sess.get("role") == "admin":
        current_token = (
            request.headers.get("X-Session-Token", "") or
            request.cookies.get(SESSION_COOKIE, "")
        )
        return jsonify({"ok": True, "session_token": current_token})

    # Fallback: PIN correct but current session not admin
    return jsonify({"ok": True, "session_token": ""})

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
        "token_currently_set": bool(get_effective_token()),
        "token_length":        len(get_effective_token()) if get_effective_token() else 0,
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

# ── History section ──────────────────────────────────────────────────────────
# ─── Trade history ────────────────────────────────────────────────────────────
@app.route("/history/trades", methods=["GET"])
@app.route("/history/trades/", methods=["GET"])
def get_trades():
    sess, err = _require_session(request)
    if err:
        return err

    if not _has_db():
        return jsonify({"trades": [], "note": "No DB"})

    username = sess["username"]
    trades = _db_module.get_trades(
        username=username,
        from_date=request.args.get("from_date"),
        to_date=request.args.get("to_date"),
        sym=request.args.get("sym"),
        limit=int(request.args.get("limit", 500))
    )
    return jsonify({"trades": trades, "count": len(trades)})


@app.route("/history/trades", methods=["POST"])
@app.route("/history/trades/", methods=["POST"])
def save_trade():
    sess, err = _require_session(request)
    if err:
        return err

    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "No data"}), 400

    if not _has_db():
        return jsonify({"status": "ok", "note": "No DB"})

    now = datetime.now(IST)
    username = sess["username"]

    trade = {
        "id":        data.get("id") or f"{int(now.timestamp())}_{data.get('sym','')}_{username[:4]}",
        "username":  username,
        "ist_date":  str(data.get("ist_date") or now.date()),
        "ist_time":  data.get("ist_time") or now.strftime("%H:%M"),
        "sym":       data.get("sym",""),
        "sec":       data.get("sec",""),
        "sig":       data.get("sig",""),
        "conf":      int(data.get("conf",0)),
        "ltp":       data.get("ltp"),
        "en":        data.get("en"),
        "tg":        data.get("tg"),
        "sl":        data.get("sl"),
        "rr":        data.get("rr"),
        "rsi":       data.get("rsi"),
        "reason":    data.get("reason",""),
        "actual_en": data.get("actual_en") or data.get("aEn"),
        "actual_ex": data.get("actual_ex") or data.get("aEx"),
        "outcome":   data.get("outcome","pending"),
        "pnl":       data.get("pnl"),
        "notes":     data.get("notes",""),
    }

    ok = _db_module.upsert_trade(trade)
    return jsonify({"status": "ok" if ok else "error", "id": trade["id"]})


@app.route("/history/trades/<trade_id>", methods=["PATCH"])
@app.route("/history/trades/<trade_id>/", methods=["PATCH"])
def update_trade(trade_id):
    sess, err = _require_session(request)
    if err:
        return err

    if not _has_db():
        return jsonify({"error": "No DB"}), 503

    data = request.get_json(silent=True) or {}
    trades = _db_module.get_trades(username=sess["username"], limit=1000)
    trade = next((t for t in trades if t["id"] == trade_id), None)

    if not trade:
        return jsonify({"error": "Not found"}), 404

    for field in ["actual_en", "actual_ex", "outcome", "pnl", "notes"]:
        if field in data:
            trade[field] = data[field]

    ok = _db_module.upsert_trade(trade)
    return jsonify({"status": "ok" if ok else "error"})


@app.route("/history/trades/<trade_id>", methods=["DELETE"])
@app.route("/history/trades/<trade_id>/", methods=["DELETE"])
def delete_trade(trade_id):
    sess, err = _require_session(request)
    if err:
        return err

    if not _has_db():
        return jsonify({"error": "No DB"}), 503

    trades = _db_module.get_trades(username=sess["username"], limit=1000)
    trade = next((t for t in trades if t["id"] == trade_id), None)
    if not trade:
        return jsonify({"error": "Not found"}), 404

    ok = _db_module.delete_trade(trade_id)
    return jsonify({"status": "ok" if ok else "error"})


@app.route("/history/stats")
@app.route("/history/stats/")
def get_trade_stats():
    sess, err = _require_session(request)
    if err:
        return err

    if not _has_db():
        return jsonify({"stats": {}})

    username = sess["username"]
    trades = _db_module.get_trades(username=username, limit=1000)
    resolved = [t for t in trades if t.get("outcome") != "pending"]
    wins = [t for t in resolved if t.get("pnl") is not None and float(t["pnl"]) > 0]
    pnls = [float(t["pnl"]) for t in resolved if t.get("pnl") is not None]

    return jsonify({"stats": {
        "total":     len(trades),
        "resolved":  len(resolved),
        "wins":      len(wins),
        "losses":    len(resolved) - len(wins),
        "accuracy":  round(len(wins) / len(resolved) * 100) if resolved else 0,
        "avg_pnl":   round(sum(pnls) / len(pnls), 2) if pnls else 0,
        "total_pnl": round(sum(pnls), 2) if pnls else 0,
    }})

@app.route("/history/read")
def history_read():
    return jsonify({"error": "File history not available on cloud."}), 410

@app.route("/history/write", methods=["POST"])
def history_write():
    return jsonify({"error": "File history not available on cloud."}), 410

# ── Paper trades (backtesting) ────────────────────────────────────────────────

@app.route("/paper-trades", methods=["GET"])
def get_paper_trades():
    sess, err = _require_session(request)
    if err:
        return err
    if not _has_db():
        return jsonify({"trades": [], "note": "No DB"})
    trades = _db_module.get_paper_trades(
        from_date=request.args.get("from_date"),
        to_date=request.args.get("to_date"),
        sym=request.args.get("sym"),
        outcome=request.args.get("outcome"),
        limit=int(request.args.get("limit", 500)),
    )
    return jsonify({"trades": trades, "count": len(trades)})


@app.route("/paper-trades/stats", methods=["GET"])
def get_paper_trade_stats():
    sess, err = _require_session(request)
    if err:
        return err
    if not _has_db():
        return jsonify({"stats": {}})
    days = int(request.args.get("days", 30))
    stats = _db_module.get_paper_trade_stats(days=days)
    return jsonify({"stats": stats})


@app.route("/paper-trades/settle", methods=["POST"])
def trigger_paper_trade_settlement():
    """Manually trigger EOD settlement for today's open paper trades (admin only)."""
    sess, err = _require_admin(request)
    if err:
        return err
    settled, skipped, errors = _settle_paper_trades_for_date()
    return jsonify({
        "status":  "ok",
        "settled": settled,
        "skipped": skipped,
        "errors":  errors,
        "ist_now": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST"),
    })


def _compute_outcome(sig: str, entry: float, target: float,
                     stop_loss: float, close: float):
    """
    Given signal direction and actual closing price, return:
      (outcome, pnl_pts, pnl_pct, target_hit, sl_hit)

    Outcomes:
      won          — price reached the full target
      partial_win  — moved in right direction but didn't hit target
      partial_loss — moved against but didn't hit stop loss
      lost         — price hit stop loss
    """
    if sig == "BUY":
        target_hit = close >= target
        sl_hit     = close <= stop_loss
        pnl_pts    = round(close - entry, 2)
    else:  # SELL
        target_hit = close <= target
        sl_hit     = close >= stop_loss
        pnl_pts    = round(entry - close, 2)

    pnl_pct = round(pnl_pts / entry * 100, 3) if entry else 0

    if target_hit:
        outcome = "won"
    elif sl_hit:
        outcome = "lost"
    elif pnl_pts > 0:
        outcome = "partial_win"
    else:
        outcome = "partial_loss"

    return outcome, pnl_pts, pnl_pct, target_hit, sl_hit


def _settle_paper_trades_for_date(date_str: str = None):
    """
    Fetch closing prices for all open paper trades on date_str (default: today IST)
    and mark them as settled.

    Returns: (settled_count, skipped_count, error_list)
    """
    from signals import get_daily, STOCKS

    if date_str is None:
        date_str = datetime.now(IST).strftime("%Y-%m-%d")

    open_trades = _db_module.get_paper_trades(
        from_date=date_str,
        to_date=date_str,
        outcome="open",
        limit=200,
    )

    if not open_trades:
        log.info("EOD settlement: no open paper trades for %s", date_str)
        return 0, 0, []

    token = get_effective_token()
    if not token:
        log.warning("EOD settlement: no Upstox token — cannot fetch closing prices")
        return 0, len(open_trades), ["No Upstox token available"]

    # Build sym → ikey lookup from STOCKS
    sym_to_ikey = {s["sym"]: s["ikey"] for s in STOCKS}

    settled, skipped, errors = 0, 0, []

    for trade in open_trades:
        sym = trade["sym"]
        ikey = sym_to_ikey.get(sym)
        if not ikey:
            log.warning("EOD settlement: unknown symbol %s", sym)
            errors.append(f"Unknown symbol: {sym}")
            skipped += 1
            continue

        try:
            daily = get_daily(ikey, token)
            if not daily:
                raise ValueError("Empty daily candle data")

            # Last candle's close price = today's closing price
            close_price = float(daily[-1]["close"])

            outcome, pnl_pts, pnl_pct, target_hit, sl_hit = _compute_outcome(
                sig       = trade["sig"],
                entry     = float(trade["entry"]),
                target    = float(trade["target"]),
                stop_loss = float(trade["stop_loss"]),
                close     = close_price,
            )

            ok = _db_module.settle_paper_trade(
                trade_id   = trade["id"],
                close_price= close_price,
                outcome    = outcome,
                pnl_pts    = pnl_pts,
                pnl_pct    = pnl_pct,
                target_hit = target_hit,
                sl_hit     = sl_hit,
            )

            if ok:
                settled += 1
                log.info(
                    "Settled %s %s: close=%.2f entry=%.2f → %s (%.2f pts, %.2f%%)",
                    trade["sig"], sym, close_price,
                    float(trade["entry"]), outcome, pnl_pts, pnl_pct
                )
            else:
                skipped += 1
                errors.append(f"{sym}: DB update failed (already settled?)")

        except Exception as e:
            log.warning("EOD settlement error for %s: %s", sym, e)
            errors.append(f"{sym}: {str(e)}")
            skipped += 1

    log.info("EOD settlement done: %d settled, %d skipped", settled, skipped)
    return settled, skipped, errors


def _eod_settlement_job():
    """APScheduler job — runs at 15:35 IST on market days."""
    now_ist = datetime.now(IST)
    # Skip weekends
    if now_ist.weekday() >= 5:
        log.info("EOD settlement: skipping weekend")
        return
    log.info("EOD settlement job triggered at %s IST",
             now_ist.strftime("%H:%M"))
    settled, skipped, errors = _settle_paper_trades_for_date()
    if errors:
        log.warning("EOD settlement errors: %s", errors)

@app.route("/ai/setup-insight", methods=["POST"])
def ai_setup_insight():
    sess, err = _require_session(request)
    if err:
        return err

    data = request.get_json(silent=True) or {}
    setup = data.get("setup") or {}
    macro_ctx = data.get("macro_ctx") or {}

    if not isinstance(setup, dict) or not setup:
        return jsonify({"error": "setup required"}), 400

    result = get_ai_setup_insight(setup, macro_ctx)
    if not result:
        return jsonify({"error": "AI insight unavailable"}), 503

    return jsonify({"status": "ok", "insight": result})

# ── Start background scheduler ────────────────────────────────────────────────
def start_scheduler():
    interval = int(os.environ.get("SCAN_INTERVAL_MINS", "5"))
    sched = BackgroundScheduler(timezone="Asia/Kolkata")

    # Stock scanner — every N minutes
    sched.add_job(scanner.run_scan, trigger="interval", minutes=interval,
                  id="nse_scan", max_instances=1, misfire_grace_time=60)

    # EOD settlement — runs at 15:35 IST every weekday to close paper trades
    sched.add_job(
        _eod_settlement_job,
        trigger="cron",
        hour=15, minute=35,
        id="eod_settlement",
        max_instances=1,
        misfire_grace_time=300,
    )
    log.info("EOD paper-trade settlement job scheduled at 15:35 IST daily")

    # Morning login reminder — every day at 08:30 IST
    # Requires TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID (already used by the scanner)
    if _auto_login.is_configured():
        sched.add_job(
            send_login_reminder_job,
            trigger="cron",
            hour=8, minute=30,
            id="login_reminder",
            max_instances=1,
            misfire_grace_time=300,
        )
        _last_auto_login["next_run"] = "08:30 IST (daily)"
        log.info("Login reminder job scheduled at 08:30 IST daily")
    else:
        _last_auto_login["next_run"] = "not scheduled — TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID missing"
        log.warning("Login reminder not scheduled — Telegram not configured")

    sched.start()
    log.info("Scheduler started — scanning every %d min", interval)
    return sched

_load_token_from_db()
_last_auto_login["configured"] = _auto_login.is_configured()  # Telegram configured?
_scheduler = start_scheduler()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
