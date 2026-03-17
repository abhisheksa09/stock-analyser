# proxy_render_db.py

import os
import json
import sqlite3
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI(title="NSE Proxy Render DB")

# SQLite DB stored in /tmp (writable on Render)
DB_FILE = os.path.join("/tmp", "history.db")


# -----------------------
# Health check endpoint
# -----------------------
@app.get("/ping")
async def ping():
    return {"status": "ok", "proxy": "upstox"}


# -----------------------
# Write JSON history
# -----------------------
@app.post("/history/write")
async def write_history(request: Request, path: str = None):
    if not path:
        return JSONResponse(status_code=400, content={"error": "No path provided"})
    
    body_bytes = await request.body()
    if not body_bytes:
        return JSONResponse(status_code=400, content={"error": "Empty request body"})
    
    try:
        data = json.loads(body_bytes.decode("utf-8"))
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {str(e)}"})
    
    # Save data to SQLite
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS history (
            path TEXT PRIMARY KEY,
            data TEXT
        )
    """)
    cursor.execute(
        "INSERT OR REPLACE INTO history (path, data) VALUES (?, ?)",
        (path, json.dumps(data))
    )
    conn.commit()
    conn.close()
    
    return {"status": "ok", "saved": len(data), "path": path}


# -----------------------
# Read JSON history
# -----------------------
@app.get("/history/read")
async def read_history(path: str = None):
    if not path:
        return JSONResponse(status_code=400, content={"error": "No path provided"})
    
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS history (
            path TEXT PRIMARY KEY,
            data TEXT
        )
    """)
    cursor.execute("SELECT data FROM history WHERE path=?", (path,))
    row = cursor.fetchone()
    conn.close()
    
    if row:
        return json.loads(row[0])
    else:
        return []  # return empty list if nothing stored


# -----------------------
# Optional: AI / API proxy endpoints
# -----------------------
# You can later add /ai/ and /v2/... proxies here
# Example:
# @app.post("/ai/{rest_of_path:path}")
# async def ai_proxy(rest_of_path: str, request: Request):
#     ...
