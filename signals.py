"""
signals.py — NSE signal computation with market context filters
Shared between app.py (proxy) and scanner.py (alert engine)

Filters added:
  1. Market filter  — Nifty50 down >1% hard-blocks BUY; up >1% hard-blocks SELL
  2. Sector filter  — Bank Nifty / Nifty IT direction penalises counter-trend trades
  3. Gap filter     — gap-down open on BUY (or gap-up on SELL) reduces confidence
  4. Day trend      — stock net negative on day reduces BUY confidence (and vice versa)
"""

import math
import os
import urllib.request
import urllib.error
import urllib.parse
import json

UPSTOX_BASE = "https://api.upstox.com"

# ─── Tunable constants ────────────────────────────────────────────────────────
MARKET_HARD_BLOCK_PCT   = 1.0
SECTOR_HEADWIND_PENALTY = 15
SECTOR_TAILWIND_BONUS   = 5
GAP_PENALTY             = 10
DAY_TREND_PENALTY       = 10
DAY_TREND_BONUS         = 5
CANDLE_CONFIRM_PENALTY  = 20

# ALERT_GREEN_THRESHOLD env var lets you lower the green bar for testing
# e.g. set to 50 on Render to fire alerts at 50%+ confidence
READY_GREEN_MIN = int(os.environ.get("ALERT_GREEN_THRESHOLD", "75"))
READY_AMBER_MIN = 55
MIN_RVOL_GREEN  = 100   # percent of avg daily volume proxy


# ─── Nifty 50 stocks ──────────────────────────────────────────────────────────
STOCKS = [
    {"sym": "HDFCBANK",   "ikey": "NSE_EQ|INE040A01034", "sec": "Banking"},
    {"sym": "RELIANCE",   "ikey": "NSE_EQ|INE002A01018", "sec": "Energy"},
    {"sym": "TCS",        "ikey": "NSE_EQ|INE467B01029", "sec": "IT"},
    {"sym": "INFY",       "ikey": "NSE_EQ|INE009A01021", "sec": "IT"},
    {"sym": "ICICIBANK",  "ikey": "NSE_EQ|INE090A01021", "sec": "Banking"},
    {"sym": "SBIN",       "ikey": "NSE_EQ|INE062A01020", "sec": "Banking"},
    {"sym": "BHARTIARTL", "ikey": "NSE_EQ|INE397D01024", "sec": "Telecom"},
    {"sym": "KOTAKBANK",  "ikey": "NSE_EQ|INE237A01028", "sec": "Banking"},
    {"sym": "HINDUNILVR", "ikey": "NSE_EQ|INE030A01027", "sec": "FMCG"},
    {"sym": "BAJFINANCE", "ikey": "NSE_EQ|INE296A01024", "sec": "NBFC"},
    {"sym": "LT",         "ikey": "NSE_EQ|INE018A01030", "sec": "Infra"},
    {"sym": "AXISBANK",   "ikey": "NSE_EQ|INE238A01034", "sec": "Banking"},
    {"sym": "MARUTI",     "ikey": "NSE_EQ|INE585B01010", "sec": "Auto"},
    {"sym": "SUNPHARMA",  "ikey": "NSE_EQ|INE044A01036", "sec": "Pharma"},
    {"sym": "TITAN",      "ikey": "NSE_EQ|INE280A01028", "sec": "Consumer"},
    {"sym": "WIPRO",      "ikey": "NSE_EQ|INE075A01022", "sec": "IT"},
    {"sym": "HCLTECH",    "ikey": "NSE_EQ|INE860A01027", "sec": "IT"},
    {"sym": "ITC",        "ikey": "NSE_EQ|INE154A01025", "sec": "FMCG"},
    {"sym": "TATAMOTORS", "ikey": "NSE_EQ|INE155A01022", "sec": "Auto"},
    {"sym": "TATASTEEL",  "ikey": "NSE_EQ|INE081A01020", "sec": "Metals"},
    {"sym": "DRREDDY",    "ikey": "NSE_EQ|INE089A01023", "sec": "Pharma"},
    {"sym": "CIPLA",      "ikey": "NSE_EQ|INE059A01026", "sec": "Pharma"},
    {"sym": "TECHM",      "ikey": "NSE_EQ|INE669C01036", "sec": "IT"},
    {"sym": "INDUSINDBK", "ikey": "NSE_EQ|INE095A01012", "sec": "Banking"},
    {"sym": "NTPC",       "ikey": "NSE_EQ|INE733E01010", "sec": "Utilities"},
    {"sym": "ONGC",       "ikey": "NSE_EQ|INE213A01029", "sec": "Energy"},
    {"sym": "COALINDIA",  "ikey": "NSE_EQ|INE522F01014", "sec": "Energy"},
    {"sym": "ASIANPAINT", "ikey": "NSE_EQ|INE021A01026", "sec": "Consumer"},
    {"sym": "ULTRACEMCO", "ikey": "NSE_EQ|INE481G01011", "sec": "Cement"},
    {"sym": "BAJAJAUTO",  "ikey": "NSE_EQ|INE917I01026", "sec": "Auto"},
]

# ─── Index instrument keys (for market/sector context) ───────────────────────
INDEX_KEYS = {
    "NIFTY50":   "NSE_INDEX|Nifty 50",
    "BANKNIFTY": "NSE_INDEX|Nifty Bank",
    "NIFTYIT":   "NSE_INDEX|Nifty IT",
    "NIFTYAUTO": "NSE_INDEX|Nifty Auto",
    "NIFTYPHRM": "NSE_INDEX|Nifty Pharma",
}

# Map stock sectors to their relevant index
SECTOR_INDEX = {
    "Banking": "BANKNIFTY",
    "NBFC":    "BANKNIFTY",
    "IT":      "NIFTYIT",
    "Auto":    "NIFTYAUTO",
    "Pharma":  "NIFTYPHRM",
}

CONFIRM_CANDLES = 3

# ─── Upstox API helpers ───────────────────────────────────────────────────────

def _upstox_get(path, token, timeout=15):
    url = UPSTOX_BASE + path
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())

def get_ltp(ikey, token):
    d = _upstox_get(
        f"/v2/market-quote/ltp?instrument_key={urllib.parse.quote(ikey)}", token
    )
    k   = list((d.get("data") or {}).keys())[0]
    ltp = d["data"][k]["last_price"]
    if not ltp:
        raise ValueError("No LTP")
    return float(ltp)

def get_intraday(ikey, token):
    d = _upstox_get(
        f"/v2/historical-candle/intraday/{urllib.parse.quote(ikey)}/1minute", token
    )
    return d.get("data", {}).get("candles", [])

def get_daily(ikey, token):
    from datetime import datetime, timedelta
    to  = datetime.utcnow().strftime("%Y-%m-%d")
    frm = (datetime.utcnow() - timedelta(days=35)).strftime("%Y-%m-%d")
    d = _upstox_get(
        f"/v2/historical-candle/{urllib.parse.quote(ikey)}/day/{to}/{frm}", token
    )
    return d.get("data", {}).get("candles", [])

# ─── Market context (Nifty + sector index) ───────────────────────────────────

def get_index_change(index_name, token):
    """
    Returns % change of an index vs previous close.
    Uses intraday candles: first candle open = today's open, last candle close = LTP.
    Falls back to 0.0 on any error so a missing index never blocks the scan.
    """
    ikey = INDEX_KEYS.get(index_name)
    if not ikey:
        return 0.0
    try:
        # Get daily to find previous close
        daily  = get_daily(ikey, token)
        prev_c = daily[0][4] if daily else None
        if not prev_c:
            return 0.0
        # Get current LTP
        ltp = get_ltp(ikey, token)
        return round((ltp - prev_c) / prev_c * 100, 2)
    except Exception:
        return 0.0   # never crash the scan due to index fetch failure

def get_market_context(sec, token):
    """
    Returns a dict with:
      nifty_chg    — Nifty 50 % change today
      sector_chg   — sector index % change (0.0 if no specific index)
      market_bias  — 'bullish' | 'bearish' | 'neutral'
      sector_bias  — 'bullish' | 'bearish' | 'neutral'
    """
    nifty_chg  = get_index_change("NIFTY50", token)
    sector_idx = SECTOR_INDEX.get(sec)
    sector_chg = get_index_change(sector_idx, token) if sector_idx else 0.0

    def bias(chg):
        if chg <= -0.5: return "bearish"
        if chg >= +0.5: return "bullish"
        return "neutral"

    return {
        "nifty_chg":   nifty_chg,
        "sector_chg":  sector_chg,
        "market_bias": bias(nifty_chg),
        "sector_bias": bias(sector_chg),
    }

# ─── Indicators ───────────────────────────────────────────────────────────────

def rsi14(closes):
    if len(closes) < 15:
        return 50.0
    g = l = 0.0
    for i in range(len(closes) - 14, len(closes)):
        d = closes[i] - closes[i - 1]
        if d > 0: g += d
        else:     l += abs(d)
    avg_g = g / 14
    avg_l = l / 14 or 0.001
    return round(100 - 100 / (1 + avg_g / avg_l), 1)

def vwap(candles):
    tv = vol = 0.0
    for c in candles:
        tp  = (c[2] + c[3] + c[4]) / 3
        tv  += tp * c[5]
        vol += c[5]
    return round(tv / vol, 2) if vol else 0.0

def atr14(candles):
    if len(candles) < 2:
        return 0.0
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i][2], candles[i][3], candles[i - 1][4]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    s = trs[-14:]
    return round(sum(s) / len(s), 2)

# ─── Confidence scoring ───────────────────────────────────────────────────────

def _clamp(v, lo, hi):
    return max(lo, min(hi, v))

def _score_orb(s):
    if s["sig"] == "WATCH":
        return 0.30
    if s["sig"] == "BUY":
        return 1.0 if s["bo"] else 0.35
    if s["sig"] == "SELL":
        return 1.0 if s["bd"] else 0.35
    return 0.30

def _score_volume(s):
    rv = s["tV"] / (s["aV"] or 1)
    if rv >= 1.5: return 1.0
    if rv >= 1.2: return 0.85
    if rv >= 1.0: return 0.70
    if rv >= 0.7: return 0.45
    return 0.15

def _score_vwap(s):
    if s["sig"] == "WATCH":
        return 0.35
    if s["sig"] == "BUY":
        return 1.0 if s["av"] else 0.10
    if s["sig"] == "SELL":
        return 1.0 if not s["av"] else 0.10
    return 0.35

def _score_rsi(s):
    rsi = s["rsi"]
    if s["sig"] == "BUY":
        if rsi <= 25: return 0.55
        if rsi <= 35: return 1.00
        if rsi <= 40: return 0.85
        if rsi <= 50: return 0.45
        return 0.10
    if s["sig"] == "SELL":
        if rsi >= 75: return 0.55
        if rsi >= 65: return 1.00
        if rsi >= 60: return 0.85
        if rsi >= 50: return 0.45
        return 0.10
    return 0.35

def _score_rr(s):
    rr = s["rr"]
    if rr >= 3.0: return 1.0
    if rr >= 2.0: return 0.85
    if rr >= 1.5: return 0.60
    if rr >= 1.0: return 0.30
    return 0.10

def _score_atr(s):
    p = (s["atr"] / s["ltp"] * 100) if s["ltp"] else 0
    if 0.8 <= p <= 2.5: return 1.0
    if 0.5 <= p < 0.8: return 0.6
    if 2.5 < p <= 4.0: return 0.7
    return 0.3

CF = [
    ("ORB breakout",        25, _score_orb),
    ("Volume confirmation", 20, _score_volume),
    ("VWAP alignment",      20, _score_vwap),
    ("RSI alignment",       15, _score_rsi),
    ("Risk:Reward",         15, _score_rr),
    ("ATR/volatility",       5, _score_atr),
]

def conf_score(s):
    tot = max_w = 0.0
    feature_scores = {}

    for name, w, fn in CF:
        sc = _clamp(fn(s), 0.0, 1.0)
        feature_scores[name] = round(sc * 100)
        tot += sc * w
        max_w += w

    pct = round(tot / max_w * 100) if max_w else 0
    pct = min(pct, 45) if s["sig"] == "WATCH" else pct
    return pct, feature_scores


def detect_regime(chg, gap_pct, market_ctx=None):
    nifty = ((market_ctx or {}).get("nifty_chg", 0.0) or 0.0)

    if abs(gap_pct) >= 1.0 and abs(chg) < 0.4:
        return "gap_stall"
    if nifty >= 0.8:
        return "bull_trend"
    if nifty <= -0.8:
        return "bear_trend"
    return "mixed"


def build_setup(sym, sec, intra, daily, ltp, market_ctx=None):
    """
    Build a complete trade setup.
    market_ctx — dict from get_market_context(). If None, filters are skipped
                 (backwards compatible with existing calls that don't pass context).
    """
    orb   = intra[:15]
    orb_h = round(max((c[2] for c in orb), default=ltp * 1.005), 2)
    orb_l = round(min((c[3] for c in orb), default=ltp * 0.995), 2)
    vw    = vwap(intra) if intra else ltp
    rs    = rsi14([c[4] for c in reversed(daily)])
    at    = atr14(list(reversed(daily[:20]))) or round(ltp * 0.015, 2)
    t_vol = sum(c[5] for c in intra)
    a_vol = sum(c[5] for c in daily[:20]) / max(len(daily[:20]), 1)
    pc    = daily[0][4] if daily else ltp
    chg   = round((ltp - pc) / pc * 100, 2)

    today_open = intra[0][1] if intra else ltp
    gap_pct    = round((today_open - pc) / pc * 100, 2)

    av = ltp > vw
    bo = ltp > orb_h
    bd = ltp < orb_l

    # ── Base signal logic ────────────────────────────────────────────────────
    if rs < 40 and av and bo:
        sig    = "BUY"
        en     = round(orb_h + 0.05, 2)
        sl     = round(min(orb_l - 0.3 * at, en - 0.5 * at), 2)
        tg     = round(en + 2.0 * at, 2)
        reason = "Above VWAP with bullish momentum"
    elif rs > 60 and (not av) and bd:
        sig    = "SELL"
        en     = round(orb_l - 0.05, 2)
        sl     = round(max(orb_h + 0.3 * at, en + 0.5 * at), 2)
        tg     = round(en - 2.0 * at, 2)
        reason = "Below VWAP with bearish momentum"
    else:
        sig    = "WATCH"
        en     = round(ltp, 2)
        sl     = round(ltp - at, 2)
        tg     = round(ltp + at, 2)
        reason = "Mixed signals — wait for clear breakout or VWAP test"

    rr = round(abs(tg - en) / max(abs(en - sl), 0.01), 2)

    # ── Market / sector / gap / day-trend penalties ─────────────────────────
    conf_penalties = 0
    ctx_warnings   = []
    market_blocked = False

    if market_ctx:
        nifty_chg = market_ctx.get("nifty_chg", 0.0) or 0.0
        sector_chg = market_ctx.get("sector_chg", 0.0) or 0.0

        # Market hard block
        if sig == "BUY" and nifty_chg <= -MARKET_HARD_BLOCK_PCT:
            sig = "WATCH"
            market_blocked = True
            ctx_warnings.append(f"Nifty {nifty_chg:+.1f}% — BUY blocked")
            reason = "Blocked by broad market weakness"
        elif sig == "SELL" and nifty_chg >= MARKET_HARD_BLOCK_PCT:
            sig = "WATCH"
            market_blocked = True
            ctx_warnings.append(f"Nifty {nifty_chg:+.1f}% — SELL blocked")
            reason = "Blocked by broad market strength"

        if not market_blocked and sig != "WATCH":
            # Sector headwind / tailwind
            if sig == "BUY" and sector_chg <= -0.5:
                conf_penalties += SECTOR_HEADWIND_PENALTY
                ctx_warnings.append(f"{sec} sector weak ({sector_chg:+.1f}%) (-{SECTOR_HEADWIND_PENALTY}% conf)")
            elif sig == "SELL" and sector_chg >= 0.5:
                conf_penalties += SECTOR_HEADWIND_PENALTY
                ctx_warnings.append(f"{sec} sector strong ({sector_chg:+.1f}%) (-{SECTOR_HEADWIND_PENALTY}% conf)")
            elif sig == "BUY" and sector_chg >= 0.5:
                conf_penalties -= SECTOR_TAILWIND_BONUS
                ctx_warnings.append(f"{sec} sector supportive ({sector_chg:+.1f}%) (+{SECTOR_TAILWIND_BONUS}% conf)")
            elif sig == "SELL" and sector_chg <= -0.5:
                conf_penalties -= SECTOR_TAILWIND_BONUS
                ctx_warnings.append(f"{sec} sector supportive ({sector_chg:+.1f}%) (+{SECTOR_TAILWIND_BONUS}% conf)")

            # Gap filter
            if sig == "BUY" and gap_pct <= -0.5:
                conf_penalties += GAP_PENALTY
                ctx_warnings.append(f"Gap-down open ({gap_pct:+.1f}%) reduces BUY confidence (-{GAP_PENALTY}% conf)")
            elif sig == "SELL" and gap_pct >= 0.5:
                conf_penalties += GAP_PENALTY
                ctx_warnings.append(f"Gap-up open ({gap_pct:+.1f}%) reduces SELL confidence (-{GAP_PENALTY}% conf)")

            # Day trend filter
            if sig == "BUY" and chg <= -0.5:
                conf_penalties += DAY_TREND_PENALTY
                ctx_warnings.append(
                    f"Stock {chg:+.1f}% on day — net negative reduces BUY confidence (-{DAY_TREND_PENALTY}% conf)"
                )
            elif sig == "SELL" and chg >= 0.5:
                conf_penalties += DAY_TREND_PENALTY
                ctx_warnings.append(
                    f"Stock {chg:+.1f}% on day — net positive reduces SELL confidence (-{DAY_TREND_PENALTY}% conf)"
                )
            elif sig == "BUY" and chg >= +0.5:
                conf_penalties -= DAY_TREND_BONUS
                ctx_warnings.append(
                    f"Stock {chg:+.1f}% on day — momentum supports BUY (+{DAY_TREND_BONUS}% conf)"
                )
            elif sig == "SELL" and chg <= -0.5:
                conf_penalties -= DAY_TREND_BONUS
                ctx_warnings.append(
                    f"Stock {chg:+.1f}% on day — momentum supports SELL (+{DAY_TREND_BONUS}% conf)"
                )

    # ── Candle confirmation ───────────────────────────────────────────────────
    confirmed     = True
    confirm_count = 0
    recent        = intra[-CONFIRM_CANDLES:]

    if len(recent) >= CONFIRM_CANDLES and sig != "WATCH":
        checks = []
        for cn in recent:
            o, h, l, c, v = cn[1], cn[2], cn[3], cn[4], cn[5]
            side_ok = c > vw if sig == "BUY" else c < vw
            body_ok = c > o if sig == "BUY" else c < o
            checks.append(side_ok and body_ok)
        confirm_count = sum(checks)
        confirmed     = confirm_count >= CONFIRM_CANDLES
    else:
        confirm_count = len(recent)

    regime = detect_regime(chg, gap_pct, market_ctx)

    s = dict(
        sym=sym, sec=sec, ltp=round(ltp, 2), chg=chg,
        orb_h=orb_h, orb_l=orb_l, rsi=rs,
        vwap=round(vw, 2), atr=at,
        tV=round(t_vol), aV=round(a_vol),
        sig=sig, en=en, tg=tg, sl=sl,
        reason=reason, rr=rr, av=av, bo=bo, bd=bd,
        confirmed=confirmed, confirm_count=confirm_count,
        gap_pct=gap_pct,
        market_ctx=market_ctx or {},
        ctx_warnings=ctx_warnings,
        market_blocked=market_blocked,
        regime=regime,
    )

    # Base setup quality only
    signal_conf, feature_scores = conf_score(s)
    risk_penalty = conf_penalties

    # Candle confirmation penalty
    if not confirmed and sig != "WATCH":
        risk_penalty += CANDLE_CONFIRM_PENALTY
        s["reason"] += f" ⚠ ({confirm_count}/{CONFIRM_CANDLES} candles confirm)"

    s["signal_conf"]   = signal_conf
    s["risk_penalty"]  = round(risk_penalty)
    s["feature_scores"] = feature_scores
    s["conf"]          = max(5, min(100, signal_conf - risk_penalty))

    return s
# ─── Build setup with market context ─────────────────────────────────────────

def build_setup(sym, sec, intra, daily, ltp, market_ctx=None):
    """
    Build a complete trade setup.
    market_ctx — dict from get_market_context(). If None, filters are skipped
                 (backwards compatible with existing calls that don't pass context).
    """
    orb   = intra[:15]
    orb_h = round(max((c[2] for c in orb), default=ltp * 1.005), 2)
    orb_l = round(min((c[3] for c in orb), default=ltp * 0.995), 2)
    vw    = vwap(intra) if intra else ltp
    rs    = rsi14([c[4] for c in reversed(daily)])
    at    = atr14(list(reversed(daily[:20]))) or round(ltp * 0.015, 2)
    t_vol = sum(c[5] for c in intra)
    a_vol = sum(c[5] for c in daily[:20]) / max(len(daily[:20]), 1)
    pc    = daily[0][4] if daily else ltp
    chg   = round((ltp - pc) / pc * 100, 2)

    today_open = intra[0][1] if intra else ltp
    gap_pct    = round((today_open - pc) / pc * 100, 2)

    av = ltp > vw
    bo = ltp > orb_h
    bd = ltp < orb_l

    # ── Base signal logic ────────────────────────────────────────────────────
    if rs < 40 and av and bo:
        sig    = "BUY"
        en     = round(orb_h + 0.05, 2)
        sl     = round(min(orb_l - 0.3 * at, en - 0.5 * at), 2)
        tg     = round(en + 2.0 * at, 2)
        reason = "Above VWAP with bullish momentum"
    elif rs > 60 and (not av) and bd:
        sig    = "SELL"
        en     = round(orb_l - 0.05, 2)
        sl     = round(max(orb_h + 0.3 * at, en + 0.5 * at), 2)
        tg     = round(en - 2.0 * at, 2)
        reason = "Below VWAP with bearish momentum"
    else:
        sig    = "WATCH"
        en     = round(ltp, 2)
        sl     = round(ltp - at, 2)
        tg     = round(ltp + at, 2)
        reason = "Mixed signals — wait for clear breakout or VWAP test"

    rr = round(abs(tg - en) / max(abs(en - sl), 0.01), 2)

    # ── Market / sector / gap / day-trend penalties ─────────────────────────
    conf_penalties = 0
    ctx_warnings   = []
    market_blocked = False

    if market_ctx:
        nifty_chg = market_ctx.get("nifty_chg", 0.0) or 0.0
        sector_chg = market_ctx.get("sector_chg", 0.0) or 0.0

        # Market hard block
        if sig == "BUY" and nifty_chg <= -MARKET_HARD_BLOCK_PCT:
            sig = "WATCH"
            market_blocked = True
            ctx_warnings.append(f"Nifty {nifty_chg:+.1f}% — BUY blocked")
            reason = "Blocked by broad market weakness"
        elif sig == "SELL" and nifty_chg >= MARKET_HARD_BLOCK_PCT:
            sig = "WATCH"
            market_blocked = True
            ctx_warnings.append(f"Nifty {nifty_chg:+.1f}% — SELL blocked")
            reason = "Blocked by broad market strength"

        if not market_blocked and sig != "WATCH":
            # Sector headwind / tailwind
            if sig == "BUY" and sector_chg <= -0.5:
                conf_penalties += SECTOR_HEADWIND_PENALTY
                ctx_warnings.append(f"{sec} sector weak ({sector_chg:+.1f}%) (-{SECTOR_HEADWIND_PENALTY}% conf)")
            elif sig == "SELL" and sector_chg >= 0.5:
                conf_penalties += SECTOR_HEADWIND_PENALTY
                ctx_warnings.append(f"{sec} sector strong ({sector_chg:+.1f}%) (-{SECTOR_HEADWIND_PENALTY}% conf)")
            elif sig == "BUY" and sector_chg >= 0.5:
                conf_penalties -= SECTOR_TAILWIND_BONUS
                ctx_warnings.append(f"{sec} sector supportive ({sector_chg:+.1f}%) (+{SECTOR_TAILWIND_BONUS}% conf)")
            elif sig == "SELL" and sector_chg <= -0.5:
                conf_penalties -= SECTOR_TAILWIND_BONUS
                ctx_warnings.append(f"{sec} sector supportive ({sector_chg:+.1f}%) (+{SECTOR_TAILWIND_BONUS}% conf)")

            # Gap filter
            if sig == "BUY" and gap_pct <= -0.5:
                conf_penalties += GAP_PENALTY
                ctx_warnings.append(f"Gap-down open ({gap_pct:+.1f}%) reduces BUY confidence (-{GAP_PENALTY}% conf)")
            elif sig == "SELL" and gap_pct >= 0.5:
                conf_penalties += GAP_PENALTY
                ctx_warnings.append(f"Gap-up open ({gap_pct:+.1f}%) reduces SELL confidence (-{GAP_PENALTY}% conf)")

            # Day trend filter
            if sig == "BUY" and chg <= -0.5:
                conf_penalties += DAY_TREND_PENALTY
                ctx_warnings.append(
                    f"Stock {chg:+.1f}% on day — net negative reduces BUY confidence (-{DAY_TREND_PENALTY}% conf)"
                )
            elif sig == "SELL" and chg >= 0.5:
                conf_penalties += DAY_TREND_PENALTY
                ctx_warnings.append(
                    f"Stock {chg:+.1f}% on day — net positive reduces SELL confidence (-{DAY_TREND_PENALTY}% conf)"
                )
            elif sig == "BUY" and chg >= +0.5:
                conf_penalties -= DAY_TREND_BONUS
                ctx_warnings.append(
                    f"Stock {chg:+.1f}% on day — momentum supports BUY (+{DAY_TREND_BONUS}% conf)"
                )
            elif sig == "SELL" and chg <= -0.5:
                conf_penalties -= DAY_TREND_BONUS
                ctx_warnings.append(
                    f"Stock {chg:+.1f}% on day — momentum supports SELL (+{DAY_TREND_BONUS}% conf)"
                )

    # ── Candle confirmation ───────────────────────────────────────────────────
    confirmed     = True
    confirm_count = 0
    recent        = intra[-CONFIRM_CANDLES:]

    if len(recent) >= CONFIRM_CANDLES and sig != "WATCH":
        checks = []
        for cn in recent:
            o, h, l, c, v = cn[1], cn[2], cn[3], cn[4], cn[5]
            side_ok = c > vw if sig == "BUY" else c < vw
            body_ok = c > o if sig == "BUY" else c < o
            checks.append(side_ok and body_ok)
        confirm_count = sum(checks)
        confirmed     = confirm_count >= CONFIRM_CANDLES
    else:
        confirm_count = len(recent)

    regime = detect_regime(chg, gap_pct, market_ctx)

    s = dict(
        sym=sym, sec=sec, ltp=round(ltp, 2), chg=chg,
        orb_h=orb_h, orb_l=orb_l, rsi=rs,
        vwap=round(vw, 2), atr=at,
        tV=round(t_vol), aV=round(a_vol),
        sig=sig, en=en, tg=tg, sl=sl,
        reason=reason, rr=rr, av=av, bo=bo, bd=bd,
        confirmed=confirmed, confirm_count=confirm_count,
        gap_pct=gap_pct,
        market_ctx=market_ctx or {},
        ctx_warnings=ctx_warnings,
        market_blocked=market_blocked,
        regime=regime,
    )

    # Base setup quality only
    signal_conf, feature_scores = conf_score(s)
    risk_penalty = conf_penalties

    # Candle confirmation penalty
    if not confirmed and sig != "WATCH":
        risk_penalty += CANDLE_CONFIRM_PENALTY
        s["reason"] += f" ⚠ ({confirm_count}/{CONFIRM_CANDLES} candles confirm)"

    s["signal_conf"]   = signal_conf
    s["risk_penalty"]  = round(risk_penalty)
    s["feature_scores"] = feature_scores
    s["conf"]          = max(5, min(100, signal_conf - risk_penalty))

    return s

# ─── Readiness check ──────────────────────────────────────────────────────────
def is_ready(s, ist_mins):
    """Returns (verdict, gates_pass_count)"""
    vp = round(s["tV"] / (s["aV"] or 1) * 100)

    time_ok    = 585 <= ist_mins <= 900
    time_prime = 585 <= ist_mins <= 630
    is_actual  = s["sig"] != "WATCH"
    orb_ok     = (
        (s["sig"] == "BUY"  and s["bo"]) or
        (s["sig"] == "SELL" and s["bd"]) or
        not is_actual
    )
    vol_ok     = vp >= MIN_RVOL_GREEN
    conf_ok    = s["conf"] >= READY_GREEN_MIN
    conf_warn  = READY_AMBER_MIN <= s["conf"] < READY_GREEN_MIN
    candle_ok  = s["confirmed"]

    if s.get("market_blocked"):
        return "red", 0

    hard_fails = sum([
        not time_ok,
        not is_actual,
        not candle_ok,
        not conf_ok and not conf_warn,
        not vol_ok,
        not orb_ok,
    ])
    warnings = sum([not time_prime and time_ok, conf_warn])

    if not is_actual:
        return "watch", 0
    if hard_fails > 0:
        return "red", 6 - hard_fails
    if warnings > 0:
        return "amber", 6
    return "green", 6
