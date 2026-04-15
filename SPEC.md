# NSE Intraday Scanner — Project Spec

## What it is
A real-time NSE intraday trading alert system. Monitors 30 Nifty50 stocks every 5 minutes, computes technical signals (RSI, VWAP, ATR, ORB), applies macro context penalties, and fires Telegram alerts when a trade reaches ≥75% confidence.

## Deployment
- **Backend:** Render.com — `https://nse-proxy-mojx.onrender.com`
- **Frontend:** GitHub Pages — `https://abhisheksa09.github.io/stock-analyser/`
- **Database:** Supabase (PostgreSQL)
- **Process model:** Single Gunicorn worker; APScheduler runs inside the same process

## Tech stack
| Layer | Library/Service |
|-------|----------------|
| Web server | Flask 3.0.3 + Gunicorn 22.0.0 |
| Scheduler | APScheduler 3.10.4 |
| Database | psycopg 3.2.10 (Supabase PostgreSQL) |
| Auth | bcrypt 4.1.2 (passwords), random 32-byte session tokens |
| HTTP client | requests 2.31.0 + urllib (stdlib) |
| TOTP | pyotp 2.9.0 |
| Market data | Upstox API (primary), Yahoo Finance (macro), NSE India API (FII/DII) |
| AI | Anthropic Claude Haiku (news sentiment), OpenAI gpt-4-mini (ai_insights.py) |
| Alerts | Telegram Bot API |

## File map
```
app.py          — Flask server, all routes, APScheduler setup (~1700 lines)
scanner.py      — Scan loop, Telegram formatting, alert trigger logic, backtest auto-save
signals.py      — RSI14, VWAP, ATR14, ORB breakout, confidence scoring
macro.py        — Economic calendar, crude/gold/VIX/FII-DII/news context
auto_login.py   — Headless Upstox TOTP login (no browser needed)
db.py           — PostgreSQL schema + CRUD (users, sessions, trades, alerts, paper_trades)
ai_insights.py  — OpenAI setup quality review (rarely called)
requirements.txt

scanner.html    — Main SPA (Scanner / History / Backtest / Admin tabs)
changelog.html  — Auth-gated changelog page
login.html      — Login form
password-reset.html
readme.html
```

## Database schema (Supabase PostgreSQL)
| Table | Purpose | Key columns |
|-------|---------|-------------|
| `token_store` | Daily Upstox token | `token`, `ist_date`, `set_by` |
| `session_state` | Daily scan state | `locked_signals` (JSON), `alerted` (JSON), `prev_confidence` |
| `trade_history` | Saved trades + P&L | `id` (TEXT PK), `sym`, `sig`, `conf`, `en`, `tg`, `sl`, `outcome`, `pnl` |
| `alert_log` | Telegram messages sent | `sym`, `kind`, `message`, `sent`, `created_at` |
| `users` | App users | `username`, `pwd_hash`, `role` (viewer/admin), `active` |
| `app_sessions` | Browser sessions | `token`, `user_id`, `expires_at`, `last_seen` |
| `paper_trades` | Simulated orders for backtesting | `id`, `trade_date`, `signal_time`, `sym`, `sec`, `sig`, `conf`, `signal_price`, `entry`, `target`, `stop_loss`, `rr`, `rsi`, `reason`, `close_price`, `settled_at`, `outcome`, `pnl_pct`, `pnl_pts`, `target_hit`, `sl_hit` |

## Signal logic (signals.py)
**Trigger conditions:**
- BUY: RSI14 < 40 AND price > VWAP AND price > ORB_HIGH
- SELL: RSI14 > 60 AND price < VWAP AND price < ORB_LOW
- WATCH: everything else

**Confidence score (0–100) — 6 weighted factors:**
| Factor | Weight |
|--------|--------|
| ORB breakout | 25% |
| Volume vs 20-day avg | 20% |
| VWAP position | 20% |
| RSI extreme | 15% |
| Risk:Reward ≥ 2 | 15% |
| ATR volatility | 5% |

**Risk penalties applied after scoring:**
- Market context (Nifty down >1%): −30%
- Sector headwind: −10 to −15%
- Gap opening: −10%
- Day trend conflict: −10%
- Candle confirmation: −10%
- Confidence capped at 45% for WATCH signals
- Final = signal_conf − risk_penalty, clamped 5–100

## Macro context layers (macro.py)
1. **Economic calendar** — hardcoded RBI/FOMC/Budget dates; suppresses signals 30 min around high-impact events
2. **Yahoo Finance proxies** — Crude, Gold, USD/INR, S&P500 futures, VIX, DXY
3. **FII/DII flows** — NSE India API; ±5–10% confidence on trend/counter-trend trades
4. **News sentiment** — NewsAPI (last 6h) → Claude Haiku classification → confidence multiplier (0.3–1.0×)

## Scanner schedule (scanner.py)
- Runs every 5 min (configurable via `SCAN_INTERVAL_MINS`)
- Active window: `ALERT_START_IST` to `ALERT_STOP_IST` (default 09:15–10:30 IST)
- Alert triggers: (1) confidence ≥ 75% first time, (2) confidence crossed 75% from below
- Session state resets each calendar day; locked signals prevent re-alerting same trade

## Auto-login (auto_login.py)
Headless Upstox OAuth using TOTP — no browser needed.

**Flow:** auth dialog (cookies) → POST mobile → POST PIN → POST TOTP → redirect code → exchange for token

**Scheduled:** APScheduler cron at **08:30 IST daily** (registered only if all 3 env vars present)

**Manual trigger:** `POST /auth/trigger-auto-login` (admin only)

**Required env vars:**
```
UPSTOX_MOBILE       — registered mobile, digits only
UPSTOX_PIN          — 6-digit login PIN
UPSTOX_TOTP_SECRET  — base-32 key (Upstox → Settings → Enable TOTP → "Can't scan?")
```

## Paper trading / backtest system

Simulates trade entries at signal time and settles them at end of day using intraday 1-minute candle replay.

### How it works
1. **Auto-save**: When a backtest symbol scores ≥ `BACKTEST_MIN_CONF` (default 55%), a paper trade row is inserted into `paper_trades` with `outcome='open'`.
2. **EOD settlement** (15:35 IST): For each open trade, fetches 1-min candles from signal time, walks candle by candle checking SL first then target (conservative). If neither is hit by session close, outcome is `partial_win` / `partial_loss` based on exit vs entry.
3. **Outcomes**: `won`, `lost`, `partial_win`, `partial_loss`
4. **Backtest symbols**: 12 hardcoded Nifty50 stocks across 8 sectors (overridable via `BACKTEST_SYMBOLS` env var). Scanned every 5 min alongside alert watchlist. Deduped via in-memory `bt_saved` set + DB `ON CONFLICT DO NOTHING`.
5. **Dry test**: `POST /paper-trades/dry-test` creates 4 synthetic trades and settles them immediately — returns a full audit report without needing real market data.

### Default backtest symbols
`HDFCBANK`, `ICICIBANK`, `SBIN`, `TCS`, `INFY`, `RELIANCE`, `TATAMOTORS`, `MARUTI`, `HINDUNILVR`, `SUNPHARMA`, `LT`, `BAJFINANCE`

### Settlement logic (intraday candle replay)
- BUY: check `low ≤ stop_loss` (SL hit) first, then `high ≥ target` (target hit)
- SELL: check `high ≥ stop_loss` (SL hit) first, then `low ≤ target` (target hit)
- Gap open past a level: exit at candle open immediately
- Both hit same candle → SL assumed first (conservative)
- Neither hit → exit at last candle close

## APScheduler jobs
| Job ID | Schedule | Purpose |
|--------|----------|---------|
| `nse_scan` | Every N min (default 5) | Run stock scanner |
| `eod_settlement` | 15:35 IST weekdays | Settle all open paper trades |
| `token_reminder` | 00:00 IST Sun–Thu | Telegram reminder to set next day's Upstox token |
| `login_reminder` | 08:30 IST daily | Telegram reminder + auto-login (if configured) |

**Token reminder detail:** Fires at midnight IST (~6:30–7:30 PM Netherlands time). Skips Friday nights (no Saturday trading). Sends a Telegram message with a direct link to `/auth/login` so the token can be set from the Netherlands the evening before.

## All environment variables
```
# Supabase
DATABASE_URL            postgresql://...?sslmode=require

# Upstox
UPSTOX_API_KEY          OAuth app client ID
UPSTOX_API_SECRET       OAuth app client secret
UPSTOX_MOBILE           (auto-login) registered mobile
UPSTOX_PIN              (auto-login) 6-digit PIN
UPSTOX_TOTP_SECRET      (auto-login) base-32 TOTP key

# Telegram
TELEGRAM_BOT_TOKEN      123456789:AAF-xxxxx
TELEGRAM_CHAT_ID        your chat ID

# Scanner tuning
ALERT_START_IST         HH:MM  default 09:15
ALERT_STOP_IST          HH:MM  default 10:30
SCAN_INTERVAL_MINS      integer default 5

# Backtest / paper trading
BACKTEST_SYMBOLS        comma-separated symbols (default: 12 hardcoded Nifty50 stocks)
BACKTEST_MIN_CONF       integer 0–100, min confidence to auto-save paper trade (default 55)
RENDER_BASE_URL         full Render URL e.g. https://nse-proxy-mojx.onrender.com
                        (used in midnight token reminder Telegram link)
TOKEN_REMINDER_TIME     HH:MM IST for the nightly token reminder (default 00:00)
                        Change to e.g. 14:30 on Render to test without waiting for midnight

# Optional AI/news
NEWS_API_KEY            NewsAPI key
ANTHROPIC_KEY           Claude API key
OPENAI_API_KEY          OpenAI key (ai_insights.py only)

# App
ADMIN_PIN               Admin panel PIN
PORT                    default 10000
```

## Key API routes
```
# Auth
GET  /auth/login                    → redirect to Upstox OAuth (manual flow)
GET  /auth/callback                 → Upstox OAuth callback, sets token
GET  /auth/auto-login-status        → auto-login state (no auth)
POST /auth/trigger-auto-login       → manual trigger (admin only)
POST /app/login                     → username/password login
POST /app/logout
GET  /app/me

# Token
POST /set-token                     → paste token manually
GET  /set-token-form                → HTML form for manual paste
GET  /get-token                     → return current token (debug)

# Scanner
GET  /alert-status                  → scanner state + last scan
POST /test-alert                    → send test Telegram message
POST /dry-scan                      → run scan now, ignore time window

# Trade history
GET    /history/trades
POST   /history/trades
PATCH  /history/trades/<id>
DELETE /history/trades/<id>
GET    /history/stats

# Paper trades (backtest)
GET    /paper-trades                → list paper trades (filters: from, to, sym, outcome, limit)
GET    /paper-trades/stats          → win rate, breakdown by signal type and confidence bucket
POST   /paper-trades/settle         → manually trigger EOD settlement for a date (admin)
POST   /paper-trades/dry-test       → create + settle 4 synthetic trades, returns audit report (admin)
GET    /paper-trades/config         → show backtest symbols and min-confidence setting

# Admin
GET  /admin/users
POST /admin/users/create
POST /admin/users/<username>/activate
POST /admin/users/<username>/deactivate

# Proxies
GET  /v2/<path>     → Upstox API proxy
POST /ai/<path>     → Anthropic API proxy
GET  /nse/*         → NSE corporate actions

# Health
GET  /ping
GET  /auth/status
GET  /macro-status
GET  /db/status
```

## Frontend (scanner.html) structure
Single HTML file. Four tabs: **Scanner**, **History**, **Backtest**, **Admin**.

- **Scanner tab:** live scan results table, manual scan trigger, macro context panel
- **History tab:** trade log with outcome/PnL editing, stats (win rate, avg PnL)
- **Backtest tab:** paper trade performance — stats cards (total trades, win rate, avg P&L), accuracy bars per signal type, filterable trade table (date range, signal, outcome). Exit price column shows actual intraday exit price from candle replay.
- **Admin tab** (PIN-gated): User Management, Cloud Proxy status, Scanner Token (OAuth + manual paste + Auto-Login), System Status, Dry Scan, Database controls, **Backtest Config** (symbol pills with saved-today indicator), **Backtest Dry Test** button

Key JS globals: `BACKEND` (Render URL), `token` (Upstox bearer token), `sessionToken` (app session)

## Known issues / tech debt
1. **Duplicate `build_setup()` in signals.py** — defined twice (~line 301 and ~line 464); second definition silently overwrites the first.
2. **OpenAI model typo in ai_insights.py** — `"gpt-4.1-mini"` should likely be `"gpt-4o-mini"`.
3. **Narrow alert window** — only 09:15–10:30 IST (75 min); afternoon setups are missed by design.
4. **State sync gap** — in-memory `STATE.locked_sig` resets on crash; DB has the correct state but brief window exists for duplicate alerts after restart.
5. **No Upstox rate-limit handling** — ~150 API calls/hour at 5-min intervals across 30 stocks; no retry/backoff.
6. **Trade history TEXT PK** — no server-side collision prevention.
