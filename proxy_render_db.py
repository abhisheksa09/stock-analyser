# file: upstox_proxy_render_db.py
import os
import json
import sqlite3
import urllib.request
import urllib.error

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, FileResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

import json, sqlite3, os

UPSTOX_BASE = "https://api.upstox.com"
ANTHROPIC_BASE = "https://api.anthropic.com"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SCANNER_FILE = os.path.join(SCRIPT_DIR, "nse_scanner.html")
README_FILE = os.path.join(SCRIPT_DIR, "nse_readme.html")
DB_FILE = os.path.join(SCRIPT_DIR, "history.db")  # SQLite database

app = FastAPI()

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# Initialize SQLite DB
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            path TEXT UNIQUE,
            data TEXT
        )
    """)
    conn.commit()
    conn.close()

init_db()

# -----------------------
# Static files & ping
# -----------------------

@app.get("/")
def serve_scanner():
    if not os.path.isfile(SCANNER_FILE):
        return HTMLResponse("<h2>nse_scanner.html not found</h2>", status_code=404)
    return FileResponse(SCANNER_FILE, media_type="text/html")

@app.get("/readme")
def serve_readme():
    if not os.path.isfile(README_FILE):
        return HTMLResponse("<h2>nse_readme.html not found</h2>", status_code=404)
    return FileResponse(README_FILE, media_type="text/html")

@app.get("/ping")
def ping():
    return {"status": "ok", "proxy": "upstox"}

# -----------------------
# History using SQLite
# -----------------------

@app.get("/history/read")
async def read_history(path: str = None):
    if not path:
        return JSONResponse(status_code=400, content={"error": "No file path provided"})
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT data FROM history WHERE path = ?", (path,))
    row = cursor.fetchone()
    conn.close()
    if row:
        return json.loads(row[0])
    return []  # empty if no record

app = FastAPI()
DB_FILE = os.path.join("/tmp", "history.db")

@app.post("/history/write")
async def write_history(request: Request, path: str = None):
    if not path:
        return JSONResponse(status_code=400, content={"error": "No path provided"})
    
    body_bytes = await request.body()
    if not body_bytes:
        return JSONResponse(status_code=400, content={"error": "Empty request body"})
    
    try:
        body_str = body_bytes.decode("utf-8")   # decode bytes → string
        data = json.loads(body_str)             # parse JSON
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {str(e)}"})
    
    # SQLite storage
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS history (
            path TEXT PRIMARY KEY,
            data TEXT
        )
    """)
    cursor.execute("INSERT OR REPLACE INTO history (path, data) VALUES (?, ?)",
                   (path, json.dumps(data)))
    conn.commit()
    conn.close()

    return {"status": "ok", "saved": len(data), "path": path}

# -----------------------
# AI Proxy (Anthropic)
# -----------------------

@app.api_route("/ai/{rest_of_path:path}", methods=["GET", "POST"])
async def ai_proxy(rest_of_path: str, request: Request):
    target_url = ANTHROPIC_BASE + "/" + rest_of_path
    body = await request.body()
    req = urllib.request.Request(target_url, data=body, method=request.method)
    for h in ["Content-Type", "x-api-key", "anthropic-version", "anthropic-dangerous-direct-browser-access"]:
        val = request.headers.get(h)
        if val:
            req.add_header(h, val)
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read()
            return JSONResponse(status_code=resp.status, content=json.loads(raw))
    except urllib.error.HTTPError as e:
        return JSONResponse(status_code=e.code, content=json.loads(e.read()))
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

# -----------------------
# Upstox Proxy
# -----------------------

@app.api_route("/v2/{rest_of_path:path}", methods=["GET", "POST"])
async def upstox_proxy(rest_of_path: str, request: Request):
    target_url = UPSTOX_BASE + "/" + rest_of_path
    body = await request.body() if request.method == "POST" else None
    req = urllib.request.Request(target_url, data=body, method=request.method)
    for h in ["Authorization", "Content-Type", "Accept"]:
        val = request.headers.get(h)
        if val:
            req.add_header(h, val)
    req.add_header("User-Agent", "UpstoxProxy/1.0")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read()
            content_type = resp.headers.get("Content-Type", "application/json")
            return JSONResponse(content=json.loads(raw), status_code=resp.status)
    except urllib.error.HTTPError as e:
        return JSONResponse(content=json.loads(e.read()), status_code=e.code)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
