from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, FileResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import os, json, sqlite3, urllib.request, urllib.error

UPSTOX_BASE = "https://api.upstox.com"
ANTHROPIC_BASE = "https://api.anthropic.com"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SCANNER_FILE = os.path.join(SCRIPT_DIR, "nse_scanner.html")
README_FILE = os.path.join(SCRIPT_DIR, "nse_readme.html")
DB_FILE = os.path.join("/tmp", "history.db")

app = FastAPI()

# ✅ CORS (important for GitHub pages)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------
# INIT DB
# -----------------------
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS history (
            path TEXT PRIMARY KEY,
            data TEXT
        )
    """)
    conn.commit()
    conn.close()

init_db()

# -----------------------
# BASIC
# -----------------------
@app.get("/ping")
def ping():
    return {"status": "ok"}

# -----------------------
# HISTORY
# -----------------------
@app.get("/history/read")
async def read_history(path: str = None):
    if not path:
        return JSONResponse(status_code=400, content={"error": "No path"})

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT data FROM history WHERE path = ?", (path,))
    row = cursor.fetchone()
    conn.close()

    return json.loads(row[0]) if row else []

@app.post("/history/write")
async def write_history(request: Request, path: str = None):
    if not path:
        return JSONResponse(status_code=400, content={"error": "No path"})

    data = await request.json()

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR REPLACE INTO history (path, data) VALUES (?, ?)",
        (path, json.dumps(data))
    )
    conn.commit()
    conn.close()

    return {"status": "ok", "saved": len(data)}

# -----------------------
# UPSTOX PROXY
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

    try:
        with urllib.request.urlopen(req) as resp:
            return JSONResponse(content=json.loads(resp.read()))
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
