"""
NSE Intraday Scanner — Cloud Proxy
Hosted on Render.com at: https://nse-proxy-mojx.onrender.com
Frontend at: https://abhisheksa09.github.io/stock-analyser/nse_scanner.html
"""

import os
import json
import urllib.request
import urllib.error
import urllib.parse

from flask import Flask, request, Response, jsonify

app = Flask(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
UPSTOX_BASE   = "https://api.upstox.com"
ANTHROPIC_BASE = "https://api.anthropic.com"

ALLOWED_ORIGIN = "https://abhisheksa09.github.io"

# ─── CORS helper ──────────────────────────────────────────────────────────────
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

def cors(response):
    """Attach CORS headers to a Flask Response."""
    for k, v in CORS_HEADERS.items():
        response.headers[k] = v
    return response

@app.after_request
def add_cors(response):
    return cors(response)

# ─── OPTIONS pre-flight (all routes) ──────────────────────────────────────────
@app.route("/", defaults={"path": ""}, methods=["OPTIONS"])
@app.route("/<path:path>", methods=["OPTIONS"])
def options_handler(path):
    return cors(Response(status=204))

# ─── Health check ─────────────────────────────────────────────────────────────
@app.route("/ping")
def ping():
    return jsonify({"status": "ok", "proxy": "upstox-render"})

# ─── NSE corporate actions ────────────────────────────────────────────────────
@app.route("/nse/corporate-actions")
def nse_corporate_actions():
    target = (
        "https://www.nseindia.com/api/corporates-corporateActions"
        "?index=equities"
    )
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept":          "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer":  "https://www.nseindia.com/market-data/upcoming-corporate-actions",
        "Origin":   "https://www.nseindia.com",
    }
    try:
        req = urllib.request.Request(target, headers=headers)
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read()
        return Response(raw, status=200, mimetype="application/json")
    except Exception:
        return jsonify([])          # non-critical — return empty array

# ─── Anthropic AI proxy ───────────────────────────────────────────────────────
@app.route("/ai/<path:subpath>", methods=["GET", "POST"])
def ai_proxy(subpath):
    target = f"{ANTHROPIC_BASE}/{subpath}"
    body   = request.get_data()

    forward_headers = {}
    for h in ["Content-Type", "x-api-key", "anthropic-version",
              "anthropic-dangerous-direct-browser-access"]:
        val = request.headers.get(h)
        if val:
            forward_headers[h] = val
    forward_headers.setdefault("anthropic-version", "2023-06-01")
    forward_headers.setdefault("anthropic-dangerous-direct-browser-access", "true")

    req = urllib.request.Request(
        target, data=body if body else None,
        headers=forward_headers, method=request.method
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read()
            return Response(raw, status=resp.status, mimetype="application/json")
    except urllib.error.HTTPError as e:
        raw = e.read()
        return Response(raw, status=e.code, mimetype="application/json")
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ─── Upstox API proxy (everything under /v2/) ─────────────────────────────────
@app.route("/v2/<path:subpath>", methods=["GET", "POST"])
def upstox_proxy(subpath):
    # Rebuild query string
    qs     = request.query_string.decode()
    target = f"{UPSTOX_BASE}/v2/{subpath}" + (f"?{qs}" if qs else "")
    body   = request.get_data() if request.method == "POST" else None

    forward_headers = {}
    for h in ["Authorization", "Content-Type", "Accept"]:
        val = request.headers.get(h)
        if val:
            forward_headers[h] = val
    forward_headers["User-Agent"] = "UpstoxProxy/2.0-Render"

    req = urllib.request.Request(
        target, data=body, headers=forward_headers, method=request.method
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read()
            ct  = resp.headers.get("Content-Type", "application/json")
            return Response(raw, status=resp.status, mimetype=ct)
    except urllib.error.HTTPError as e:
        raw = e.read()
        return Response(raw, status=e.code, mimetype="application/json")
    except urllib.error.URLError as e:
        return jsonify({"error": str(e.reason)}), 502
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ─── History endpoints (disabled on Render — ephemeral filesystem) ────────────
@app.route("/history/read")
def history_read():
    return jsonify({
        "error": "File-based history is not available on the cloud deployment. "
                 "Your history is stored in browser localStorage only."
    }), 410

@app.route("/history/write", methods=["POST"])
def history_write():
    return jsonify({
        "error": "File-based history is not available on the cloud deployment. "
                 "Your history is stored in browser localStorage only."
    }), 410

# ─── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    print(f"NSE Scanner Proxy starting on port {port}")
    app.run(host="0.0.0.0", port=port)
