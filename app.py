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

from flask import Flask, request, Response, jsonify
from apscheduler.schedulers.background import BackgroundScheduler

import scanner
import macro as macro_module

log = logging.getLogger("app")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [app] %(message)s")

app = Flask(__name__)

UPSTOX_BASE    = "https://api.upstox.com"
ANTHROPIC_BASE = "https://api.anthropic.com"
ALLOWED_ORIGIN = "https://abhisheksa09.github.io"
IST            = timezone(timedelta(hours=5, minutes=30))

CORS_HEADERS = {
    "Access-Control-Allow-Origin":  ALLOWED_ORIGIN,
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
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
