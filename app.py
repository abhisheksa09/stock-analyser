"""
NSE Intraday Scanner — Backend API
Version : v3.0.0
"""

import os, json, logging, urllib.request, urllib.error, urllib.parse
from datetime import datetime, timezone, timedelta

from flask import Flask, request, Response, jsonify, redirect
from apscheduler.schedulers.background import BackgroundScheduler

import scanner
import macro as macro_module
import db as _db_module

log = logging.getLogger("app")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")

app = Flask(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
ALLOWED_ORIGIN = "https://abhisheksa09.github.io"
IST            = timezone(timedelta(hours=5, minutes=30))

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

@app.after_request
def add_cors(r): return cors(r)

@app.route("/", defaults={"path": ""}, methods=["OPTIONS"])
@app.route("/<path:path>",             methods=["OPTIONS"])
def options_handler(path=""): return cors(Response("", 204))

# ─── DB helpers ───────────────────────────────────────────────────────────────
def _get_db():
    return _db_module.get_connection()

def _has_db():
    return _get_db() is not None

# ─── Session helpers ──────────────────────────────────────────────────────────
SESSION_COOKIE  = "nse_session"
SESSION_TTL_SEC = 30 * 24 * 3600

def _get_session(req) -> dict | None:
    token = (
        req.headers.get("X-Session-Token", "") or
        req.headers.get("Authorization", "").replace("Bearer ", "").strip() or
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

def _require_admin(req):
    sess, err = _require_session(req)
    if err: return None, err
    if sess.get("role") != "admin":
        return None, (jsonify({"error": "Admin role required"}), 403)
    return sess, None

def _set_session_cookie(response, token: str):
    response.set_cookie(SESSION_COOKIE, token, max_age=SESSION_TTL_SEC,
                        httponly=True, secure=True, samesite="None", path="/")
    return response

def _clear_session_cookie(response):
    response.delete_cookie(SESSION_COOKIE, path="/", samesite="None", secure=True)
    return response

# ─── Startup: load token from DB ──────────────────────────────────────────────
def _load_token_from_db():
    try:
        tok = _db_module.get_token()
        if tok:
            scanner.set_token(tok)
            log.info("Loaded Upstox token from DB on startup")
    except Exception as e:
        log.warning("Could not load token from DB: %s", e)

# ─── Bootstrap ────────────────────────────────────────────────────────────────
@app.route("/bootstrap/create-admin", methods=["POST"])
def bootstrap_create_admin():
    """Create first admin user. Only works when users table is empty."""
    data     = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    if not username or len(password) < 8:
        return jsonify({"error": "username required, password >= 8 chars"}), 400
    existing = _db_module.get_users()
    if existing:
        return jsonify({"error": "Users already exist.", "users": [u["username"] for u in existing]}), 409
    result = _db_module.create_user(username, password, "admin")
    if "error" in result:
        return jsonify(result), 400
    return jsonify({"status": "ok", "message": f"Admin '{username}' created."})


@app.route("/bootstrap/reset-password", methods=["POST"])
def bootstrap_reset_password():
    """Reset any user's password using ADMIN_PIN."""
    data     = request.get_json(silent=True) or {}
    pin      = data.get("pin", "")
    username = (data.get("username") or "").strip().lower()
    password = data.get("password") or ""
    if os.environ.get("ADMIN_PIN", "") != pin or not pin:
        return jsonify({"error": "Invalid PIN"}), 403
    if len(password) < 8:
        return jsonify({"error": "Password >= 8 chars"}), 400
    conn = _db_module.db()
    if not conn:
        return jsonify({"error": "No DB"}), 503
    try:
        pwd_hash = _db_module.hash_password(password)
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET pwd_hash=%s, active=TRUE WHERE username=%s RETURNING id, username, role",
                        (pwd_hash, username))
            row = cur.fetchone()
        if not row:
            return jsonify({"error": f"User '{username}' not found"}), 404
        return jsonify({"status": "ok", "message": f"Password updated for {username}", "user": dict(row)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ─── App auth ─────────────────────────────────────────────────────────────────
@app.route("/app/login", methods=["POST"])
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


@app.route("/app/logout", methods=["POST"])
def app_logout():
    token = (request.headers.get("X-Session-Token","") or
             request.cookies.get(SESSION_COOKIE, ""))
    if token:
        _db_module.revoke_app_session(token)
    resp = jsonify({"status": "ok"})
    return _clear_session_cookie(resp)


@app.route("/app/me")
def app_me():
    sess = _get_session(request)
    if not sess:
        return jsonify({"authenticated": False}), 401
    return jsonify({"authenticated": True, "username": sess["username"], "role": sess["role"]})


@app.route("/app/change-password", methods=["POST"])
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
        resp = jsonify({"status": "ok"})
        return _set_session_cookie(resp, new_token)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/app/request-reset", methods=["POST"])
def app_request_reset():
    data     = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip()
    if not username:
        return jsonify({"error": "username required"}), 400
    users = _db_module.get_users()
    user  = next((u for u in users if u["username"] == username.lower()), None)
    if not user:
        return jsonify({"status": "ok"})   # Don't reveal if user exists
    bot  = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat = os.environ.get("TELEGRAM_CHAT_ID", "")
    if bot and chat:
        msg     = (f"Password reset requested for: <b>{username}</b>\n"
                   f"POST /bootstrap/reset-password with pin+username+password")
        payload = json.dumps({"chat_id": chat, "text": msg, "parse_mode": "HTML"}).encode()
        req     = urllib.request.Request(
            f"https://api.telegram.org/bot{bot}/sendMessage",
            data=payload, headers={"Content-Type": "application/json"})
        try: urllib.request.urlopen(req, timeout=5)
        except Exception: pass
    return jsonify({"status": "ok"})

# ─── Admin user management ────────────────────────────────────────────────────
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

# ─── Ping ─────────────────────────────────────────────────────────────────────
@app.route("/ping")
@app.route("/ping/")
def ping():
    tok_set = bool(scanner.get_token())
    return jsonify({"status": "ok", "version": "v3.0.0", "token_set": tok_set})

# ─── DB ───────────────────────────────────────────────────────────────────────
@app.route("/db/init")
@app.route("/db/init/")
def db_init():
    if not _has_db():
        return jsonify({"error": "DATABASE_URL not set"}), 503
    try:
        result = _db_module.init_db()
        return jsonify({"status": "ok", "detail": result})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/db/status")
@app.route("/db/status/")
def db_status():
    return jsonify(_db_module.db_status())

# ─── Upstox token management ──────────────────────────────────────────────────
@app.route("/get-token")
@app.route("/get-token/")
def get_token():
    """Return today's Upstox token. Requires valid app session."""
    sess = _get_session(request)
    if not sess:
        return jsonify({"error": "Not authenticated"}), 401
    tok = scanner.get_token()
    if not tok and _has_db():
        tok = _db_module.get_token()
        if tok:
            scanner.set_token(tok)
    if not tok:
        return jsonify({"status": "not_set", "message": "No Upstox token. Admin must complete OAuth login."}), 404
    return jsonify({"status": "ok", "token": tok})


@app.route("/set-token", methods=["POST"])
@app.route("/set-token/", methods=["POST"])
def set_token():
    """Set Upstox token. Requires admin session OR ADMIN_PIN in body."""
    # Read body ONCE
    data = request.get_json(silent=True) or {}
    tok  = (data.get("token") or "").strip()
    if not tok:
        return jsonify({"error": "token field required in JSON body"}), 400

    # Auth: admin session OR ADMIN_PIN in body (for set-token-form)
    sess = _get_session(request)
    pin  = data.get("_admin_pin", "")
    admin_pin = os.environ.get("ADMIN_PIN", "")
    is_admin_sess = sess and sess.get("role") == "admin"
    is_pin_auth   = bool(pin and admin_pin and pin == admin_pin)

    if not is_admin_sess and not is_pin_auth:
        return jsonify({"error": "Admin session or valid ADMIN_PIN required"}), 403

    scanner.set_token(tok)
    if _has_db():
        set_by = sess["username"] if sess else "pin_auth"
        _db_module.set_token(tok, set_by=set_by)
    log.info("Upstox token set (len=%d)", len(tok))
    return jsonify({"status": "ok", "message": "Token saved. Scanner will use it immediately."})


@app.route("/get-chat-id")
def get_chat_id():
    """Helper to find your Telegram chat ID."""
    bot = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    if not bot:
        return jsonify({"error": "TELEGRAM_BOT_TOKEN not set"}), 503
    try:
        with urllib.request.urlopen(f"https://api.telegram.org/bot{bot}/getUpdates", timeout=5) as r:
            data = json.loads(r.read())
        chats = list({
            str(m.get("message",{}).get("chat",{}).get("id",""))
            for m in data.get("result",[])
            if m.get("message",{}).get("chat",{}).get("id")
        })
        return jsonify({"updates": len(data.get("result",[])), "chat_ids": chats})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/set-token-form", methods=["GET"])
@app.route("/set-token-form/", methods=["GET"])
def set_token_form():
    """Phone-friendly HTML form to paste the daily Upstox token. Admin only."""
    return """<!DOCTYPE html>
<html><head><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Set Upstox Token</title>
<style>
body{font-family:sans-serif;padding:24px;background:#f5f5f0;max-width:480px;margin:0 auto;}
h2{margin-bottom:16px;}
textarea{width:100%;height:120px;padding:10px;border-radius:8px;border:1px solid #ddd;font-size:12px;font-family:monospace;}
button{width:100%;padding:14px;background:#27500A;color:#fff;border:none;border-radius:10px;font-size:16px;font-weight:600;margin-top:12px;cursor:pointer;}
#msg{margin-top:10px;padding:10px;border-radius:8px;display:none;}
.ok{background:#EAF3DE;color:#27500A;}.err{background:#FCEBEB;color:#A32D2D;}
</style></head><body>
<h2>&#128274; Set Upstox Token</h2>
<p style="color:#666;margin-bottom:12px;">Paste today's Upstox access token below.</p>
<textarea id="tok" placeholder="eyJ0eXAiOiJKV1Q..."></textarea>
<button onclick="setTok()">Save Token</button>
<div id="msg"></div>
<script>
function setTok(){
  var t=(document.getElementById('tok').value||'').trim();
  var msg=document.getElementById('msg');
  if(!t){msg.textContent='Paste token first';msg.className='err';msg.style.display='block';return;}
  fetch('/set-token',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({token:t,_admin_pin:'"""+os.environ.get("ADMIN_PIN","")+ """'})
  }).then(r=>r.json()).then(d=>{
    if(d.status==='ok'){msg.textContent='✓ Token saved!';msg.className='ok';}
    else{msg.textContent='Error: '+(d.error||JSON.stringify(d));msg.className='err';}
    msg.style.display='block';
  }).catch(e=>{msg.textContent='Error: '+e.message;msg.className='err';msg.style.display='block';});
}
</script></body></html>"""


@app.route("/test-alert", methods=["GET","POST"])
def test_alert():
    """Send a test Telegram message."""
    sent = scanner.send_telegram(
        "🔔 <b>NSE Scanner — Test Alert</b>\n\nBot is working correctly!"
    )
    return jsonify({"status":"ok" if sent else "error",
                    "telegram_sent":sent,
                    "message":"Test alert sent" if sent else "Failed — check TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID"})


@app.route("/alert-status")
def alert_status():
    tok = scanner.get_token()
    return jsonify({
        "token_set": bool(tok),
        "token_preview": tok[:12]+"..." if tok else None,
        "session_date": scanner.STATE.date,
        "locked_signals": scanner.STATE.locked_sig,
        "alerted_count": len(scanner.STATE.alerted),
        "db_available": _has_db(),
    })

# ─── Upstox proxy (/v2/*) ─────────────────────────────────────────────────────
@app.route("/v2/<path:subpath>", methods=["GET","POST","OPTIONS"])
def upstox_proxy(subpath):
    """Proxy all /v2/* calls to Upstox with the server-side token."""
    if request.method == "OPTIONS":
        return cors(Response("", 204))
    sess = _get_session(request)
    if not sess:
        return jsonify({"error": "Not authenticated"}), 401
    # Use server-side Upstox token (not the user's session token)
    tok = scanner.get_token()
    if not tok and _has_db():
        tok = _db_module.get_token()
        if tok: scanner.set_token(tok)
    if not tok:
        return jsonify({"errors": [{"message": "No Upstox token — admin must complete daily OAuth"}]}), 403
    url     = f"https://api.upstox.com/v2/{subpath}"
    qs      = request.query_string.decode()
    full_url= f"{url}?{qs}" if qs else url
    headers = {
        "Authorization": f"Bearer {tok}",
        "Accept":        "application/json",
        "Content-Type":  "application/json",
    }
    try:
        req_body = request.get_data() or None
        method   = request.method
        req_obj  = urllib.request.Request(full_url, data=req_body, headers=headers, method=method)
        with urllib.request.urlopen(req_obj, timeout=15) as resp:
            body = resp.read()
            r    = Response(body, status=resp.status, content_type="application/json")
            return cors(r)
    except urllib.error.HTTPError as e:
        body = e.read()
        return cors(Response(body, status=e.code, content_type="application/json"))
    except Exception as e:
        return jsonify({"errors": [{"message": str(e)}]}), 502


# ─── Upstox data endpoints (instrument_key as query param — avoids | in path) ──

@app.route("/upstox/ltp")
def upstox_ltp():
    """GET /upstox/ltp?ikey=NSE_EQ|INE040A01034"""
    sess = _get_session(request)
    if not sess:
        return jsonify({"error": "Not authenticated"}), 401
    ikey = request.args.get("ikey", "")
    if not ikey:
        return jsonify({"error": "ikey required"}), 400
    tok = scanner.get_token() or (_has_db() and _db_module.get_token())
    if not tok:
        return jsonify({"errors": [{"message": "No Upstox token"}]}), 403
    url = f"https://api.upstox.com/v2/market-quote/ltp?instrument_key={urllib.parse.quote(ikey, safe='')}"
    return _upstox_get(url, tok)


@app.route("/upstox/intraday")
def upstox_intraday():
    """GET /upstox/intraday?ikey=NSE_EQ|INE040A01034&interval=1minute"""
    sess = _get_session(request)
    if not sess:
        return jsonify({"error": "Not authenticated"}), 401
    ikey     = request.args.get("ikey", "")
    interval = request.args.get("interval", "1minute")
    if not ikey:
        return jsonify({"error": "ikey required"}), 400
    tok = scanner.get_token() or (_has_db() and _db_module.get_token())
    if not tok:
        return jsonify({"errors": [{"message": "No Upstox token"}]}), 403
    url = (f"https://api.upstox.com/v2/historical-candle/intraday/"
           f"{urllib.parse.quote(ikey, safe='')}/{interval}")
    return _upstox_get(url, tok)


@app.route("/upstox/daily")
def upstox_daily():
    """GET /upstox/daily?ikey=NSE_EQ|INE040A01034&from=2026-01-01&to=2026-03-25"""
    sess = _get_session(request)
    if not sess:
        return jsonify({"error": "Not authenticated"}), 401
    ikey    = request.args.get("ikey", "")
    to_date = request.args.get("to",   "")
    fr_date = request.args.get("from", "")
    if not ikey:
        return jsonify({"error": "ikey required"}), 400
    tok = scanner.get_token() or (_has_db() and _db_module.get_token())
    if not tok:
        return jsonify({"errors": [{"message": "No Upstox token"}]}), 403
    url = (f"https://api.upstox.com/v2/historical-candle/"
           f"{urllib.parse.quote(ikey, safe='')}/day/{to_date}/{fr_date}")
    return _upstox_get(url, tok)


def _upstox_get(url, tok):
    """Make a GET request to Upstox and return the response."""
    headers = {
        "Authorization": f"Bearer {tok}",
        "Accept":        "application/json",
    }
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = resp.read()
            return cors(Response(body, status=resp.status, content_type="application/json"))
    except urllib.error.HTTPError as e:
        body = e.read()
        log.warning("Upstox %s → %d: %s", url, e.code, body[:200])
        return cors(Response(body, status=e.code, content_type="application/json"))
    except Exception as e:
        return jsonify({"errors": [{"message": str(e)}]}), 502

# ─── Trade history ────────────────────────────────────────────────────────────
@app.route("/history/trades", methods=["GET"])
@app.route("/history/trades/", methods=["GET"])
def get_trades():
    if not _has_db():
        return jsonify({"trades": [], "note": "No DB"})
    username    = "default"
    filter_user = username
    trades = _db_module.get_trades(
        username=filter_user,
        from_date=request.args.get("from_date"),
        to_date=request.args.get("to_date"),
        sym=request.args.get("sym"),
        limit=int(request.args.get("limit", 500))
    )
    return jsonify({"trades": trades, "count": len(trades)})


@app.route("/history/trades", methods=["POST"])
@app.route("/history/trades/", methods=["POST"])
def save_trade():
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "No data"}), 400
    if not _has_db():
        return jsonify({"status": "ok", "note": "No DB"})
    now      = datetime.now(IST)
    username = "default"
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
    if not _has_db():
        return jsonify({"error": "No DB"}), 503
    data   = request.get_json(silent=True) or {}
    trades = _db_module.get_trades(username="default", limit=1000)
    trade  = next((t for t in trades if t["id"] == trade_id), None)
    if not trade:
        return jsonify({"error": "Not found"}), 404
    for field in ["actual_en","actual_ex","outcome","pnl","notes"]:
        if field in data:
            trade[field] = data[field]
    ok = _db_module.upsert_trade(trade)
    return jsonify({"status": "ok" if ok else "error"})


@app.route("/history/trades/<trade_id>", methods=["DELETE"])
@app.route("/history/trades/<trade_id>/", methods=["DELETE"])
def delete_trade(trade_id):
    if not _has_db():
        return jsonify({"error": "No DB"}), 503
    ok = _db_module.delete_trade(trade_id)
    return jsonify({"status": "ok" if ok else "error"})


@app.route("/history/stats")
@app.route("/history/stats/")
def get_trade_stats():
    if not _has_db():
        return jsonify({"stats": {}})
    username    = "default"
    filter_user = username
    trades   = _db_module.get_trades(username=filter_user, limit=1000)
    resolved = [t for t in trades if t.get("outcome") != "pending"]
    wins     = [t for t in resolved if t.get("pnl") is not None and float(t["pnl"]) > 0]
    pnls     = [float(t["pnl"]) for t in resolved if t.get("pnl") is not None]
    return jsonify({"stats": {
        "total":    len(trades),
        "resolved": len(resolved),
        "wins":     len(wins),
        "losses":   len(resolved)-len(wins),
        "accuracy": round(len(wins)/len(resolved)*100) if resolved else 0,
        "avg_pnl":  round(sum(pnls)/len(pnls), 2) if pnls else 0,
        "total_pnl":round(sum(pnls), 2) if pnls else 0,
    }})


@app.route("/history/alerts")
@app.route("/history/alerts/")
def get_alert_log():
    if not _has_db():
        return jsonify({"alerts": []})
    alerts = _db_module.get_alerts(date_=request.args.get("date"), limit=50)
    return jsonify({"alerts": alerts, "count": len(alerts)})

# ─── Macro ────────────────────────────────────────────────────────────────────
@app.route("/macro-status")
def macro_status():
    sess = _get_session(request)
    if not sess:
        return jsonify({"error": "Not authenticated"}), 401
    try:
        result = macro_module.get_macro_proxies()
        return jsonify({"status": "ok", "macro": result})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ─── NSE corporate actions ─────────────────────────────────────────────────────
@app.route("/nse/corporate-actions")
def nse_corporate_actions():
    try:
        url = "https://www.nseindia.com/api/corporates-corporateActions?index=equities&from_date=&to_date="
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
            "Referer": "https://www.nseindia.com",
        })
        with urllib.request.urlopen(req, timeout=8) as r:
            data = json.loads(r.read())
        return jsonify(data if isinstance(data, list) else data.get("data", []))
    except Exception as e:
        return jsonify([])

# ─── Admin misc ───────────────────────────────────────────────────────────────
@app.route("/admin/check", methods=["POST"])
def admin_check():
    """Check if stored admin session token is still valid."""
    data  = request.get_json(silent=True) or {}
    stok  = data.get("session_token","")
    if not stok:
        return jsonify({"ok": False, "pin_required": True})
    sess = _db_module.validate_app_session(stok)
    if sess and sess.get("role") == "admin":
        return jsonify({"ok": True, "pin_required": False})
    return jsonify({"ok": False, "pin_required": True})


@app.route("/admin/verify", methods=["POST"])
def admin_verify():
    """Verify admin PIN and return a session token."""
    data = request.get_json(silent=True) or {}
    pin  = data.get("pin","")
    if not pin or pin != os.environ.get("ADMIN_PIN",""):
        return jsonify({"ok": False}), 403
    # Return the existing app session if logged in, else just ok
    sess = _get_session(request)
    if sess and sess.get("role") == "admin":
        return jsonify({"ok": True, "session_token": request.headers.get("X-Session-Token","")})
    return jsonify({"ok": True, "session_token": ""})

# ─── Upstox OAuth ─────────────────────────────────────────────────────────────
@app.route("/auth/login")
@app.route("/auth/login/")
def auth_login():
    """Step 1: Admin-only Upstox OAuth. Opens Upstox login page."""
    api_key = os.environ.get("UPSTOX_API_KEY","")
    if not api_key:
        return "<h2>UPSTOX_API_KEY not set</h2>", 500
    redirect_uri= os.environ.get("UPSTOX_REDIRECT_URI","")

    auth_url = (
        "https://api.upstox.com/v2/login/authorization/dialog"
        f"?response_type=code&client_id={api_key}"
        f"&redirect_uri={urllib.parse.quote(redirect_uri)}"
    )
    return redirect(auth_url)


@app.route("/auth/callback")
def auth_callback():
    """Step 2: Upstox redirects here. Exchanges code for token."""
    code = request.args.get("code","")
    if not code:
        return "<h2>No auth code received from Upstox</h2>", 400
    api_key     = os.environ.get("UPSTOX_API_KEY","")
    api_secret  = os.environ.get("UPSTOX_API_SECRET","")
    redirect_uri= os.environ.get("UPSTOX_REDIRECT_URI","")
    payload = urllib.parse.urlencode({
        "code": code, "client_id": api_key, "client_secret": api_secret,
        "redirect_uri": redirect_uri, "grant_type": "authorization_code"
    }).encode()
    req = urllib.request.Request(
        "https://api.upstox.com/v2/login/authorization/token",
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"},
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
    except Exception as e:
        try:
            error_body = e.read().decode()
        except:
            error_body = str(e)
    
        return f"<h2>Token exchange failed</h2><pre>{error_body}</pre>", 500
    tok = data.get("access_token","")
    if not tok:
        return f"<h2>No access_token in response: {data}</h2>", 500
    scanner.set_token(tok)
    if _has_db():
        _db_module.set_token(tok, set_by="oauth")
    log.info("Upstox OAuth token obtained and stored")
    return """<html><body style="font-family:sans-serif;text-align:center;padding:60px;">
    <h2>&#10003; Upstox Login Successful</h2>
    <p>Token stored on server. You can close this tab.</p>
    <script>if(window.opener){window.opener.postMessage({type:'upstox_auth_success'},'*');setTimeout(()=>window.close(),1500);}</script>
    </body></html>"""


@app.route("/auth/logout", methods=["POST"])
@app.route("/auth/logout/", methods=["POST"])
def auth_logout():
    """Clear the Upstox token from server memory and DB."""
    scanner.set_token("")
    if _has_db():
        _db_module.delete_token()
    return jsonify({"status": "ok"})


@app.route("/auth/status")
def auth_status():
    tok = scanner.get_token()
    return jsonify({"token_set": bool(tok), "token_preview": tok[:12]+"..." if tok else None})

# ─── Dry scan ─────────────────────────────────────────────────────────────────
@app.route("/dry-scan")
def dry_scan():
    sess = _get_session(request)
    if not sess:
        return jsonify({"error": "Not authenticated"}), 401
    sym  = request.args.get("sym","HDFCBANK").upper()
    mock = request.args.get("mock","0") == "1"
    tok  = scanner.get_token()
    if not tok and not mock:
        return jsonify({"error":"No Upstox token — complete OAuth first"}), 403
    # Run a single-stock scan using signals module
    try:
        from signals import build_setup, get_ltp, get_intraday, get_daily, STOCKS as ALL_STOCKS
        stock = next((s for s in ALL_STOCKS if s["sym"]==sym), None)
        if not stock:
            return jsonify({"error": f"Unknown symbol: {sym}"}), 400
        if mock:
            import random
            s = {"sym":sym,"sec":stock["sec"],"ltp":1000.0,"chg":0.5,"sig":"BUY",
                 "conf":72,"en":1002.0,"tg":1025.0,"sl":988.0,"rr":1.77,
                 "rsi":45,"reason":"Mock scan (no token)","market_blocked":False}
        else:
            ltp   = get_ltp(stock["ikey"], tok)
            intra = get_intraday(stock["ikey"], tok)
            daily = get_daily(stock["ikey"], tok)
            s     = build_setup(sym, stock["sec"], intra, daily, ltp)
        return jsonify({"status":"ok","sym":sym,"result":s})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ─── Scheduler ────────────────────────────────────────────────────────────────
def start_scheduler():
    from apscheduler.triggers.cron import CronTrigger
    sched = BackgroundScheduler(timezone=IST)
    start_h = int(os.environ.get("ALERT_START_IST","9").split(":")[0])
    stop_h  = int(os.environ.get("ALERT_STOP_IST","10").split(":")[0])
    interval= int(os.environ.get("SCAN_INTERVAL_MINS","5"))
    sched.add_job(scanner.run_scan, CronTrigger(
        hour=f"{start_h}-{stop_h}", minute=f"*/{interval}", timezone=IST
    ))
    # Daily cleanup of expired sessions at midnight
    sched.add_job(_db_module.cleanup_expired_sessions, CronTrigger(hour=0, minute=5, timezone=IST))
    sched.start()
    log.info("Scheduler started")

# ─── Entry point ──────────────────────────────────────────────────────────────
_load_token_from_db()
start_scheduler()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
