"""
db.py — Database layer for NSE Scanner (Supabase / PostgreSQL)
Version : v3.0.0

Tables:
  token_store    — single-row daily Upstox token (set by admin, used by all)
  session_state  — per-day locked signals, alerts fired, prev confidence
  trade_history  — saved predictions with outcomes
  alert_log      — every Telegram message sent
  users          — app users with hashed passwords and roles
  app_sessions   — browser session tokens (replaces Upstox OAuth for login)
"""

import os, json, logging
from datetime import date, datetime, timezone, timedelta

log = logging.getLogger(__name__)

# ── Connection ────────────────────────────────────────────────────────────────
_DB  = None
_db  = None   # psycopg v3 connection

def get_connection():
    global _DB, _db
    import psycopg
    from psycopg.rows import dict_row
    url = os.environ.get("DATABASE_URL", "")
    if not url:
        log.error("DATABASE_URL env var is empty or not set. "
                  "Env keys visible: %s",
                  [k for k in os.environ if "DATA" in k or "POST" in k or "DB" in k])
        return None
    # Neon (and most managed PG poolers) need sslmode=require and no prepared statements
    # Append sslmode if not already in URL
    if "sslmode" not in url:
        url = url + ("&" if "?" in url else "?") + "sslmode=require"
    try:
        if _db is None or _db.closed:
            _db = psycopg.connect(
                url,
                row_factory=dict_row,
                autocommit=True,
                prepare_threshold=None,   # disable prepared statements (pooler requirement)
            )
            _DB = True
    except Exception as e:
        log.error("DB connection failed: %s", e)
        _db = None
        return None
    return _db

def db():
    return get_connection()

# ── IST helper ────────────────────────────────────────────────────────────────
IST = timezone(timedelta(hours=5, minutes=30))
def today_ist():
    return datetime.now(IST).date()

# ── Schema ────────────────────────────────────────────────────────────────────
SCHEMA = """
CREATE TABLE IF NOT EXISTS token_store (
    id          SERIAL PRIMARY KEY,
    token       TEXT        NOT NULL,
    set_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    set_by      TEXT        NOT NULL DEFAULT 'admin',
    ist_date    DATE        NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS session_state (
    id              SERIAL PRIMARY KEY,
    ist_date        DATE        NOT NULL UNIQUE,
    locked_signals  JSONB       NOT NULL DEFAULT '{}',
    alerted         JSONB       NOT NULL DEFAULT '[]',
    prev_confidence JSONB       NOT NULL DEFAULT '{}',
    macro_cache     JSONB
);

CREATE TABLE IF NOT EXISTS trade_history (
    id          TEXT        PRIMARY KEY,
    username    TEXT        NOT NULL DEFAULT 'default',
    ist_date    DATE        NOT NULL,
    ist_time    TEXT        NOT NULL,
    sym         TEXT        NOT NULL,
    sec         TEXT        NOT NULL,
    sig         TEXT        NOT NULL,
    conf        INTEGER     NOT NULL,
    ltp         NUMERIC,
    en          NUMERIC,
    tg          NUMERIC,
    sl          NUMERIC,
    rr          NUMERIC,
    rsi         NUMERIC,
    reason      TEXT,
    actual_en   NUMERIC,
    actual_ex   NUMERIC,
    outcome     TEXT        NOT NULL DEFAULT 'pending',
    pnl         NUMERIC,
    notes       TEXT        NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_trade_history_user ON trade_history(username);

CREATE TABLE IF NOT EXISTS alert_log (
    id          SERIAL      PRIMARY KEY,
    ist_date    DATE        NOT NULL,
    ist_time    TEXT        NOT NULL,
    sym         TEXT        NOT NULL,
    kind        TEXT        NOT NULL,
    conf        INTEGER,
    sig         TEXT,
    message     TEXT,
    sent        BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS users (
    id          SERIAL      PRIMARY KEY,
    username    TEXT        NOT NULL UNIQUE,
    pwd_hash    TEXT        NOT NULL,
    role        TEXT        NOT NULL DEFAULT 'viewer',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_login  TIMESTAMPTZ,
    active      BOOLEAN     NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS app_sessions (
    id          SERIAL      PRIMARY KEY,
    token       TEXT        NOT NULL UNIQUE,
    user_id     INTEGER     NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    username    TEXT        NOT NULL,
    role        TEXT        NOT NULL DEFAULT 'viewer',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at  TIMESTAMPTZ NOT NULL,
    last_seen   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_app_sessions_token    ON app_sessions(token);
CREATE INDEX IF NOT EXISTS idx_app_sessions_expires  ON app_sessions(expires_at);
CREATE INDEX IF NOT EXISTS idx_token_store_date      ON token_store(ist_date);
CREATE INDEX IF NOT EXISTS idx_trade_history_date    ON trade_history(ist_date);

CREATE TABLE IF NOT EXISTS paper_trades (
    id           TEXT        PRIMARY KEY,
    trade_date   DATE        NOT NULL,
    signal_time  TEXT        NOT NULL,
    sym          TEXT        NOT NULL,
    sec          TEXT        NOT NULL DEFAULT '',
    sig          TEXT        NOT NULL,
    conf         INTEGER     NOT NULL,
    signal_price NUMERIC     NOT NULL,
    entry        NUMERIC     NOT NULL,
    target       NUMERIC     NOT NULL,
    stop_loss    NUMERIC     NOT NULL,
    rr           NUMERIC,
    rsi          NUMERIC,
    reason       TEXT        NOT NULL DEFAULT '',
    close_price  NUMERIC,
    settled_at   TIMESTAMPTZ,
    outcome      TEXT        NOT NULL DEFAULT 'open',
    pnl_pct      NUMERIC,
    pnl_pts      NUMERIC,
    target_hit   BOOLEAN,
    sl_hit       BOOLEAN,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_paper_trades_date ON paper_trades(trade_date);
"""

def init_db():
    """Create all tables. Safe to call multiple times (IF NOT EXISTS)."""
    conn = db()
    if not conn:
        return {"status": "skipped", "reason": "DATABASE_URL not set"}
    try:
        with conn.cursor() as cur:
            cur.execute(SCHEMA)
        log.info("DB schema initialised")
        return {"status": "ok"}
    except Exception as e:
        log.error("init_db failed: %s", e)
        return {"status": "error", "error": str(e)}

# ── Token store ───────────────────────────────────────────────────────────────
def get_token(date_=None):
    conn = db()
    if not conn:
        return None
    if date_ is None:
        date_ = today_ist()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT token FROM token_store WHERE ist_date=%s", (date_,))
            row = cur.fetchone()
            return row["token"] if row else None
    except Exception as e:
        log.warning("get_token: %s", e)
        return None

def set_token(token: str, set_by: str = "admin", date_=None):
    conn = db()
    if not conn:
        return False
    if date_ is None:
        date_ = today_ist()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO token_store (token, set_by, ist_date)
                VALUES (%s, %s, %s)
                ON CONFLICT (ist_date) DO UPDATE
                  SET token=%s, set_by=%s, set_at=NOW()
            """, (token, set_by, date_, token, set_by))
        return True
    except Exception as e:
        log.warning("set_token: %s", e)
        return False

def delete_token(date_=None):
    conn = db()
    if not conn:
        return False
    if date_ is None:
        date_ = today_ist()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM token_store WHERE ist_date=%s", (date_,))
        return True
    except Exception as e:
        log.warning("delete_token: %s", e)
        return False

# ── Session state ─────────────────────────────────────────────────────────────
def get_session_state(date_=None):
    conn = db()
    if not conn:
        return None
    if date_ is None:
        date_ = today_ist()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM session_state WHERE ist_date=%s", (date_,))
            row = cur.fetchone()
            if not row:
                return {"ist_date": str(date_), "locked_signals": {},
                        "alerted": [], "prev_confidence": {}, "macro_cache": None}
            return dict(row)
    except Exception as e:
        log.warning("get_session_state: %s", e)
        return None

def save_session_state(state: dict, date_=None):
    conn = db()
    if not conn:
        return False
    if date_ is None:
        date_ = today_ist()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO session_state
                    (ist_date, locked_signals, alerted, prev_confidence, macro_cache)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (ist_date) DO UPDATE SET
                    locked_signals=%s, alerted=%s,
                    prev_confidence=%s, macro_cache=%s
            """, (
                date_,
                json.dumps(state.get("locked_signals", {})),
                json.dumps(state.get("alerted", [])),
                json.dumps(state.get("prev_confidence", {})),
                json.dumps(state.get("macro_cache")) if state.get("macro_cache") else None,
                json.dumps(state.get("locked_signals", {})),
                json.dumps(state.get("alerted", [])),
                json.dumps(state.get("prev_confidence", {})),
                json.dumps(state.get("macro_cache")) if state.get("macro_cache") else None,
            ))
        return True
    except Exception as e:
        log.warning("save_session_state: %s", e)
        return False

# ── Trade history ─────────────────────────────────────────────────────────────
def get_trades(username=None, from_date=None, to_date=None, sym=None, limit=500):
    conn = db()
    if not conn:
        return []
    try:
        where, params = [], []
        if username:
            where.append("username=%s"); params.append(username)
        if from_date:
            where.append("ist_date>=%s"); params.append(from_date)
        if to_date:
            where.append("ist_date<=%s"); params.append(to_date)
        if sym:
            where.append("sym=%s"); params.append(sym.upper())
        sql = "SELECT * FROM trade_history"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY created_at DESC LIMIT %s"
        params.append(limit)
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        log.warning("get_trades: %s", e)
        return []

def upsert_trade(trade: dict):
    conn = db()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO trade_history
                    (id, username, ist_date, ist_time, sym, sec, sig, conf, ltp, en, tg, sl, rr,
                     rsi, reason, actual_en, actual_ex, outcome, pnl, notes)
                VALUES
                    (%(id)s,%(username)s,%(ist_date)s,%(ist_time)s,%(sym)s,%(sec)s,%(sig)s,%(conf)s,
                     %(ltp)s,%(en)s,%(tg)s,%(sl)s,%(rr)s,%(rsi)s,%(reason)s,
                     %(actual_en)s,%(actual_ex)s,%(outcome)s,%(pnl)s,%(notes)s)
                ON CONFLICT (id) DO UPDATE SET
                    actual_en=EXCLUDED.actual_en, actual_ex=EXCLUDED.actual_ex,
                    outcome=EXCLUDED.outcome, pnl=EXCLUDED.pnl, notes=EXCLUDED.notes,
                    username=EXCLUDED.username
            """, trade)
        return True
    except Exception as e:
        log.warning("upsert_trade: %s", e)
        return False

def delete_trade(trade_id: str):
    conn = db()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM trade_history WHERE id=%s", (trade_id,))
        return True
    except Exception as e:
        log.warning("delete_trade: %s", e)
        return False

def delete_all_trades(username: str):
    conn = db()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM trade_history WHERE username=%s", (username,))
        return True
    except Exception as e:
        log.warning("delete_all_trades: %s", e)
        return False

def get_trade_stats(username=None):
    """Quick win/loss stats for a user."""
    trades   = get_trades(username=username, limit=2000)
    resolved = [t for t in trades if t.get("outcome") != "pending"]
    wins     = [t for t in resolved if t.get("pnl") and float(t["pnl"]) > 0]
    pnls     = [float(t["pnl"]) for t in resolved if t.get("pnl") is not None]
    return {
        "total": len(trades),
        "resolved": len(resolved),
        "wins": len(wins),
        "losses": len(resolved) - len(wins),
        "win_rate": round(len(wins)/len(resolved)*100, 1) if resolved else 0,
        "avg_pnl": round(sum(pnls)/len(pnls), 2) if pnls else 0,
        "total_pnl": round(sum(pnls), 2) if pnls else 0,
    }

# ── Alert log ─────────────────────────────────────────────────────────────────
def log_alert(sym, kind, conf, sig, message, sent, date_=None, time_=None):
    conn = db()
    if not conn:
        return False
    if date_ is None:
        date_ = today_ist()
    if time_ is None:
        time_ = datetime.now(IST).strftime("%H:%M")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO alert_log (ist_date, ist_time, sym, kind, conf, sig, message, sent)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """, (date_, time_, sym, kind, conf, sig, message, sent))
        return True
    except Exception as e:
        log.warning("log_alert: %s", e)
        return False

def get_alerts(date_=None, limit=50):
    conn = db()
    if not conn:
        return []
    try:
        with conn.cursor() as cur:
            if date_:
                cur.execute("SELECT * FROM alert_log WHERE ist_date=%s ORDER BY id DESC LIMIT %s",
                            (date_, limit))
            else:
                cur.execute("SELECT * FROM alert_log ORDER BY id DESC LIMIT %s", (limit,))
            return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        log.warning("get_alerts: %s", e)
        return []

# ── Users ─────────────────────────────────────────────────────────────────────
def hash_password(password: str) -> str:
    """Hash password with bcrypt. Falls back to sha256 if bcrypt unavailable."""
    try:
        import bcrypt
        return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    except ImportError:
        import hashlib, secrets
        salt = secrets.token_hex(16)
        h    = hashlib.sha256((salt + password).encode()).hexdigest()
        return f"sha256:{salt}:{h}"

def verify_password(password: str, pwd_hash: str) -> bool:
    """Verify password against stored hash."""
    try:
        import bcrypt
        if pwd_hash.startswith("sha256:"):
            raise ValueError("sha256 hash, use fallback")
        return bcrypt.checkpw(password.encode(), pwd_hash.encode())
    except (ImportError, ValueError):
        import hashlib
        if pwd_hash.startswith("sha256:"):
            _, salt, h = pwd_hash.split(":")
            return hashlib.sha256((salt + password).encode()).hexdigest() == h
        return False

def create_user(username: str, password: str, role: str = "viewer") -> dict:
    conn = db()
    if not conn:
        return {"error": "No database connection"}
    try:
        pwd_hash = hash_password(password)
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO users (username, pwd_hash, role)
                VALUES (%s, %s, %s)
                RETURNING id, username, role, created_at
            """, (username.lower().strip(), pwd_hash, role))
            row = dict(cur.fetchone())
        log.info("User created: %s (%s)", username, role)
        return {"status": "ok", "user": row}
    except Exception as e:
        if "unique" in str(e).lower():
            return {"error": f"Username '{username}' already exists"}
        log.warning("create_user: %s", e)
        return {"error": str(e)}

def authenticate_user(username: str, password: str) -> dict | None:
    """Returns user dict if credentials valid, None otherwise."""
    conn = db()
    if not conn:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE username=%s AND active=TRUE",
                        (username.lower().strip(),))
            row = cur.fetchone()
        if not row:
            return None
        if not verify_password(password, row["pwd_hash"]):
            return None
        # Update last_login
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET last_login=NOW() WHERE id=%s", (row["id"],))
        return dict(row)
    except Exception as e:
        log.warning("authenticate_user: %s", e)
        return None

def get_users():
    conn = db()
    if not conn:
        return []
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id,username,role,created_at,last_login,active FROM users ORDER BY id")
            return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        log.warning("get_users: %s", e)
        return []

def set_user_active(username: str, active: bool):
    conn = db()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET active=%s WHERE username=%s", (active, username.lower()))
        return True
    except Exception as e:
        log.warning("set_user_active: %s", e)
        return False

# ── App sessions ──────────────────────────────────────────────────────────────
SESSION_TTL_DAYS = 30   # sessions last 30 days

def create_app_session(user: dict) -> str:
    """Create a browser session token for an authenticated user."""
    import secrets
    conn = db()
    if not conn:
        return ""
    token = secrets.token_urlsafe(32)
    expires = datetime.now(IST) + timedelta(days=SESSION_TTL_DAYS)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO app_sessions (token, user_id, username, role, expires_at)
                VALUES (%s, %s, %s, %s, %s)
            """, (token, user["id"], user["username"], user["role"], expires))
        return token
    except Exception as e:
        log.warning("create_app_session: %s", e)
        return ""

def validate_app_session(token: str) -> dict | None:
    """Returns session info if token is valid and not expired. Touches last_seen."""
    if not token:
        return None
    conn = db()
    if not conn:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT s.*, u.active as user_active
                FROM app_sessions s
                JOIN users u ON s.user_id = u.id
                WHERE s.token=%s AND s.expires_at > NOW() AND u.active=TRUE
            """, (token,))
            row = cur.fetchone()
        if not row:
            return None
        # Touch last_seen async-style (best effort)
        try:
            with conn.cursor() as cur:
                cur.execute("UPDATE app_sessions SET last_seen=NOW() WHERE token=%s", (token,))
        except Exception:
            pass
        return dict(row)
    except Exception as e:
        log.warning("validate_app_session: %s", e)
        return None

def revoke_app_session(token: str):
    conn = db()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM app_sessions WHERE token=%s", (token,))
        return True
    except Exception as e:
        log.warning("revoke_app_session: %s", e)
        return False

def revoke_all_sessions(username: str):
    """Logout all devices for a user."""
    conn = db()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM app_sessions WHERE username=%s", (username.lower(),))
        return True
    except Exception as e:
        log.warning("revoke_all_sessions: %s", e)
        return False

def cleanup_expired_sessions():
    """Remove expired sessions. Called periodically."""
    conn = db()
    if not conn:
        return 0
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM app_sessions WHERE expires_at < NOW()")
            return cur.rowcount
    except Exception as e:
        log.warning("cleanup_expired_sessions: %s", e)
        return 0

# ── DB status ─────────────────────────────────────────────────────────────────
def db_status():
    conn = db()
    if not conn:
        return {"connected": False, "reason": "DATABASE_URL not set"}
    try:
        counts = {}
        with conn.cursor() as cur:
            for tbl in ["token_store","session_state","trade_history",
                        "alert_log","users","app_sessions","paper_trades"]:
                try:
                    cur.execute(f"SELECT COUNT(*) AS n FROM {tbl}")
                    counts[tbl] = cur.fetchone()["n"]
                except Exception:
                    counts[tbl] = "?"
        return {"connected": True, "tables": counts, "date": str(today_ist())}
    except Exception as e:
        return {"connected": False, "error": str(e)}

# ── Compatibility aliases (used by scanner.py) ────────────────────────────────
def db_available() -> bool:
    """True if DB connection is working."""
    return get_connection() is not None

def load_token(date_=None) -> str | None:
    """Alias for get_token()."""
    return get_token(date_)

def save_token(token: str, set_by: str = "api", date_=None):
    """Alias for set_token()."""
    return set_token(token, set_by=set_by, date_=date_)

def load_session(date_str: str) -> dict:
    """Alias for get_session_state(). Returns dict always (never None)."""
    result = get_session_state(date_str)
    if result is None:
        return {"ist_date": date_str, "locked_signals": {},
                "alerted": [], "prev_confidence": {}, "macro_cache": None}
    return result

def save_session(state: dict):
    """Alias for save_session_state()."""
    return save_session_state(state)

# ── Paper trades ──────────────────────────────────────────────────────────────
def save_paper_trade(trade: dict) -> bool:
    """Insert a new paper trade (simulated order at signal time). Ignores duplicates."""
    conn = db()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO paper_trades
                    (id, trade_date, signal_time, sym, sec, sig, conf,
                     signal_price, entry, target, stop_loss, rr, rsi, reason)
                VALUES
                    (%(id)s, %(trade_date)s, %(signal_time)s, %(sym)s, %(sec)s,
                     %(sig)s, %(conf)s, %(signal_price)s, %(entry)s, %(target)s,
                     %(stop_loss)s, %(rr)s, %(rsi)s, %(reason)s)
                ON CONFLICT (id) DO NOTHING
            """, trade)
        return True
    except Exception as e:
        log.warning("save_paper_trade: %s", e)
        return False

def get_paper_trades(from_date=None, to_date=None, sym=None,
                     outcome=None, limit=500) -> list:
    conn = db()
    if not conn:
        return []
    try:
        where, params = [], []
        if from_date:
            where.append("trade_date >= %s"); params.append(from_date)
        if to_date:
            where.append("trade_date <= %s"); params.append(to_date)
        if sym:
            where.append("sym = %s"); params.append(sym.upper())
        if outcome:
            where.append("outcome = %s"); params.append(outcome)
        sql = "SELECT * FROM paper_trades"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY created_at DESC LIMIT %s"
        params.append(limit)
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        # Convert Decimal/date to JSON-safe types
        result = []
        for r in rows:
            row = dict(r)
            for k, v in row.items():
                if hasattr(v, "isoformat"):        # date / datetime
                    row[k] = v.isoformat()
                elif hasattr(v, "__float__"):       # Decimal
                    row[k] = float(v)
            result.append(row)
        return result
    except Exception as e:
        log.warning("get_paper_trades: %s", e)
        return []

def settle_paper_trade(trade_id: str, close_price: float,
                       outcome: str, pnl_pts: float, pnl_pct: float,
                       target_hit: bool, sl_hit: bool) -> bool:
    """Update a paper trade with end-of-day settlement data."""
    conn = db()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE paper_trades
                SET close_price=%s, settled_at=NOW(), outcome=%s,
                    pnl_pts=%s, pnl_pct=%s, target_hit=%s, sl_hit=%s
                WHERE id=%s AND outcome='open'
            """, (close_price, outcome, pnl_pts, pnl_pct,
                  target_hit, sl_hit, trade_id))
        return True
    except Exception as e:
        log.warning("settle_paper_trade: %s", e)
        return False

def get_paper_trade_stats(days: int = 30) -> dict:
    """Accuracy metrics over the last N calendar days."""
    conn = db()
    if not conn:
        return {}
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT outcome, sig, conf,
                       pnl_pts, pnl_pct, target_hit, sl_hit
                FROM paper_trades
                WHERE trade_date >= CURRENT_DATE - %s
                  AND outcome <> 'open'
            """, (days,))
            rows = [dict(r) for r in cur.fetchall()]
    except Exception as e:
        log.warning("get_paper_trade_stats: %s", e)
        return {}

    if not rows:
        return {"total": 0, "settled": 0}

    settled = rows
    won     = [r for r in settled if r["outcome"] in ("won", "partial_win")]
    lost    = [r for r in settled if r["outcome"] in ("lost", "partial_loss")]
    tgt_hit = [r for r in settled if r.get("target_hit")]
    sl_hit  = [r for r in settled if r.get("sl_hit")]
    pnl_pts = [float(r["pnl_pts"]) for r in settled if r.get("pnl_pts") is not None]
    pnl_pct = [float(r["pnl_pct"]) for r in settled if r.get("pnl_pct") is not None]

    # Break down by signal direction
    by_sig = {}
    for sig in ("BUY", "SELL"):
        sub = [r for r in settled if r["sig"] == sig]
        sub_won = [r for r in sub if r["outcome"] in ("won", "partial_win")]
        by_sig[sig] = {
            "total": len(sub),
            "won":   len(sub_won),
            "win_rate": round(len(sub_won) / len(sub) * 100, 1) if sub else 0,
        }

    # Confidence bucket breakdown
    buckets = {}
    for lo, hi, label in [(75, 101, "75-100%"), (55, 75, "55-74%"), (0, 55, "<55%")]:
        sub = [r for r in settled if lo <= r["conf"] < hi]
        sub_won = [r for r in sub if r["outcome"] in ("won", "partial_win")]
        buckets[label] = {
            "total": len(sub),
            "won":   len(sub_won),
            "win_rate": round(len(sub_won) / len(sub) * 100, 1) if sub else 0,
        }

    return {
        "total":       len(settled),
        "settled":     len(settled),
        "won":         len(won),
        "lost":        len(lost),
        "win_rate":    round(len(won) / len(settled) * 100, 1) if settled else 0,
        "target_hit_rate": round(len(tgt_hit) / len(settled) * 100, 1) if settled else 0,
        "sl_hit_rate": round(len(sl_hit) / len(settled) * 100, 1) if settled else 0,
        "avg_pnl_pts": round(sum(pnl_pts) / len(pnl_pts), 2) if pnl_pts else 0,
        "avg_pnl_pct": round(sum(pnl_pct) / len(pnl_pct), 2) if pnl_pct else 0,
        "total_pnl_pts": round(sum(pnl_pts), 2) if pnl_pts else 0,
        "by_signal":   by_sig,
        "by_conf":     buckets,
        "days":        days,
    }
