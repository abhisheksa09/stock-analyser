from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, FileResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import json
import os
import urllib.request
import urllib.error
import urllib.parse

# -------------------------------
# Config
# -------------------------------
PORT = int(os.environ.get("PORT", 10000))
UPSTOX_BASE = "https://api.upstox.com"
ANTHROPIC_BASE = "https://api.anthropic.com"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SCANNER_FILE = os.path.join(SCRIPT_DIR, "nse_scanner.html")
README_FILE = os.path.join(SCRIPT_DIR, "nse_readme.html")

# -------------------------------
# FastAPI app
# -------------------------------
app = FastAPI()

# Enable CORS for all origins (GitHub Pages frontend)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],            # Or restrict to your GitHub Pages URL
    allow_credentials=True,
    allow_methods=["*"],            # GET, POST, OPTIONS, etc.
    allow_headers=["*"],            # Allow all headers
)

# -------------------------------
# Helper functions
# -------------------------------
def expand_filepath(path: str) -> str:
    return os.path.expandvars(os.path.expanduser(path.replace("/", os.sep)))

async def read_json_file(path: str):
    if not os.path.isfile(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

async def write_json_file(path: str, data):
    dirpath = os.path.dirname(path)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    return len(data)

# -------------------------------
# Endpoints
# -------------------------------

@app.get("/ping")
async def ping():
    return {"status": "ok", "proxy": "upstox"}

# Serve scanner HTML
@app.get("/")
@app.get("/index.html")
@app.get("/nse_scanner.html")
async def serve_scanner():
    if not os.path.isfile(SCANNER_FILE):
        return HTMLResponse(
            content="<h2>nse_scanner.html not found in the same folder</h2>",
            status_code=404
        )
    return FileResponse(SCANNER_FILE, media_type="text/html")

# Serve readme
@app.get("/readme")
@app.get("/readme.html")
async def serve_readme():
    if not os.path.isfile(README_FILE):
        return HTMLResponse(
            content="<h2>nse_readme.html not found in the same folder</h2>",
            status_code=404
        )
    return FileResponse(README_FILE, media_type="text/html")

# -------------------------------
# History read/write
# -------------------------------
@app.get("/history/read")
async def read_history(path: str = ""):
    if not path:
        return JSONResponse({"error": "No file path provided"}, status_code=400)
    full_path = expand_filepath(path)
    data = await read_json_file(full_path)
    return JSONResponse(data)

@app.post("/history/write")
async def write_history(request: Request, path: str = ""):
    if not path:
        return JSONResponse({"error": "No file path provided"}, status_code=400)
    try:
        body = await request.body()
        if not body:
            return JSONResponse({"error": "Invalid JSON: empty body"}, status_code=400)
        data = json.loads(body)
        full_path = expand_filepath(path)
        saved_count = await write_json_file(full_path, data)
        return JSONResponse({"status": "ok", "saved": saved_count, "path": path})
    except json.JSONDecodeError as e:
        return JSONResponse({"error": f"Invalid JSON: {str(e)}"}, status_code=400)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

# -------------------------------
# NSE Corporate Actions
# -------------------------------
@app.get("/nse/corporate-actions")
async def nse_corporate_actions():
    target = "https://www.nseindia.com/api/corporates-corporateActions?index=equities"
    headers_to_send = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/market-data/upcoming-corporate-actions",
        "Origin": "https://www.nseindia.com",
    }
    req = urllib.request.Request(target, headers=headers_to_send)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read()
            return JSONResponse(json.loads(raw))
    except Exception:
        return JSONResponse([], status_code=200)

# -------------------------------
# AI Proxy
# -------------------------------
@app.api_route("/ai/{full_path:path}", methods=["GET", "POST", "OPTIONS"])
async def ai_proxy(request: Request, full_path: str):
    target_url = ANTHROPIC_BASE + "/" + full_path
    body = await request.body() if request.method in ["POST", "PUT"] else None

    req = urllib.request.Request(target_url, data=body, method=request.method)
    for h in ["Content-Type", "x-api-key", "anthropic-version", "anthropic-dangerous-direct-browser-access"]:
        val = request.headers.get(h)
        if val:
            req.add_header(h, val)
    req.add_header("anthropic-version", "2023-06-01")
    req.add_header("anthropic-dangerous-direct-browser-access", "true")

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read()
            return JSONResponse(json.loads(raw))
    except urllib.error.HTTPError as e:
        raw = e.read()
        return JSONResponse(json.loads(raw), status_code=e.code)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

# -------------------------------
# Upstox Proxy
# -------------------------------
@app.api_route("/v2/{full_path:path}", methods=["GET", "POST", "OPTIONS"])
async def upstox_proxy(request: Request, full_path: str):
    target_url = f"{UPSTOX_BASE}/v2/{full_path}"
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
            return JSONResponse(json.loads(raw), status_code=resp.status)
    except urllib.error.HTTPError as e:
        raw = e.read()
        return JSONResponse(json.loads(raw), status_code=e.code)
    except urllib.error.URLError as e:
        return JSONResponse({"error": str(e.reason)}, status_code=502)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

# -------------------------------
# Run with Uvicorn (local dev)
# -------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
