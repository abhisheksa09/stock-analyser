"""
NSE Intraday Scanner — Backend API
Version : v3.1.0
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
        "X-Session-Token, x-api-key"
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
@app.route("/<path:path>", methods=["OPTIONS"])
def options_handler(path=""): return cors(Response("", 204))

# ─── DB helpers ───────────────────────────────────────────────────────────────
def _get_db():
    return _db_module.get_connection()

def _has_db():
    return _get_db() is not None

# ─── Startup: load token from DB ──────────────────────────────────────────────
def _load_token_from_db():
    try:
        tok = _db_module.get_token()
        if tok:
            scanner.set_token(tok)
            log.info("Loaded Upstox token from DB on startup")
    except Exception as e:
        log.warning("Could not load token from DB: %s", e)

# ─── Ping ─────────────────────────────────────────────────────────────────────
@app.route("/ping")
def ping():
    return jsonify({
        "status": "ok",
        "version": "v3.1.0",
        "token_set": bool(scanner.get_token())
    })

# ─────────────────────────────────────────────────────────────────────────────
# 🔥 FIXED: PUBLIC UPSTOX PROXY (NO SESSION REQUIRED)
# ─────────────────────────────────────────────────────────────────────────────
@app.route("/v2/<path:subpath>", methods=["GET","POST","OPTIONS"])
def upstox_proxy(subpath):
    """
    Public proxy for Upstox APIs.
    Uses server-side token. No user session required.
    """

    if request.method == "OPTIONS":
        return cors(Response("", 204))

    # 🔹 Get token
    tok = scanner.get_token()
    if not tok and _has_db():
        tok = _db_module.get_token()
        if tok:
            scanner.set_token(tok)

    if not tok:
        return cors(jsonify({
            "error": "UPSTOX_TOKEN_MISSING",
            "message": "Admin must complete OAuth login"
        })), 503

    url = f"https://api.upstox.com/v2/{subpath}"
    qs = request.query_string.decode()
    full_url = f"{url}?{qs}" if qs else url

    headers = {
        "Authorization": f"Bearer {tok}",
        "Accept": "application/json"
    }

    try:
        req_body = request.get_data() or None

        req_obj = urllib.request.Request(
            full_url,
            data=req_body,
            headers=headers,
            method=request.method
        )

        with urllib.request.urlopen(req_obj, timeout=15) as resp:
            body = resp.read()

            return cors(Response(
                body,
                status=resp.status,
                content_type="application/json"
            ))

    except urllib.error.HTTPError as e:
        body = e.read()

        # ✅ Always return JSON (prevents frontend crash)
        try:
            parsed = json.loads(body)
        except:
            parsed = {
                "error": "UPSTOX_HTTP_ERROR",
                "status": e.code,
                "details": body.decode("utf-8")[:300]
            }

        log.warning("Upstox error %s → %d", full_url, e.code)

        return cors(jsonify(parsed)), e.code

    except Exception as e:
        log.exception("Proxy failure")

        return cors(jsonify({
            "error": "PROXY_FAILURE",
            "message": str(e)
        })), 502


# ─── Upstox OAuth ─────────────────────────────────────────────────────────────
@app.route("/auth/login")
def auth_login():
    api_key = os.environ.get("UPSTOX_API_KEY","")
    if not api_key:
        return "<h2>UPSTOX_API_KEY not set</h2>", 500

    redirect_uri = os.environ.get(
        "UPSTOX_REDIRECT_URI",
        "https://nse-proxy-mojx.onrender.com/auth/callback"
    )

    auth_url = (
        "https://api.upstox.com/v2/login/authorization/dialog"
        f"?response_type=code&client_id={api_key}"
        f"&redirect_uri={urllib.parse.quote(redirect_uri)}"
    )

    return redirect(auth_url)


@app.route("/auth/callback")
def auth_callback():
    code = request.args.get("code","")
    if not code:
        return "<h2>No auth code received</h2>", 400

    payload = urllib.parse.urlencode({
        "code": code,
        "client_id": os.environ.get("UPSTOX_API_KEY",""),
        "client_secret": os.environ.get("UPSTOX_API_SECRET",""),
        "redirect_uri": os.environ.get("UPSTOX_REDIRECT_URI"),
        "grant_type": "authorization_code"
    }).encode()

    req = urllib.request.Request(
        "https://api.upstox.com/v2/login/authorization/token",
        data=payload,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json"
        },
        method="POST"
    )

    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
    except Exception as e:
        return f"<h2>Token exchange failed: {e}</h2>", 500

    tok = data.get("access_token","")
    if not tok:
        return f"<h2>No access_token in response: {data}</h2>", 500

    scanner.set_token(tok)

    if _has_db():
        _db_module.set_token(tok, set_by="oauth")

    log.info("Upstox token stored")

    return """<html><body>
    <h2>Login successful</h2>
    <script>
    if(window.opener){
        window.opener.postMessage({type:'upstox_auth_success'},'*');
        setTimeout(()=>window.close(),1000);
    }
    </script>
    </body></html>"""


@app.route("/auth/status")
def auth_status():
    tok = scanner.get_token()
    return jsonify({
        "token_set": bool(tok),
        "token_preview": tok[:12]+"..." if tok else None
    })


# ─── Scheduler ────────────────────────────────────────────────────────────────
def start_scheduler():
    from apscheduler.triggers.cron import CronTrigger

    sched = BackgroundScheduler(timezone=IST)

    start_h = int(os.environ.get("ALERT_START_IST","9").split(":")[0])
    stop_h  = int(os.environ.get("ALERT_STOP_IST","10").split(":")[0])
    interval= int(os.environ.get("SCAN_INTERVAL_MINS","5"))

    sched.add_job(
        scanner.run_scan,
        CronTrigger(hour=f"{start_h}-{stop_h}", minute=f"*/{interval}")
    )

    sched.start()
    log.info("Scheduler started")


# ─── Init ─────────────────────────────────────────────────────────────────────
_load_token_from_db()
start_scheduler()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
