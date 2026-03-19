"""
signals.py — NSE signal computation
Shared between app.py (proxy) and scanner.py (alert engine)
All indicator logic matches nse_scanner.html exactly.
"""

import math
import urllib.request
import urllib.error
import urllib.parse
import json

UPSTOX_BASE = "https://api.upstox.com"

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
    d = _upstox_get(f"/v2/market-quote/ltp?instrument_key={urllib.parse.quote(ikey)}", token)
    k = list((d.get("data") or {}).keys())[0]
    ltp = d["data"][k]["last_price"]
    if not ltp:
        raise ValueError("No LTP")
    return float(ltp)

def get_intraday(ikey, token):
    d = _upstox_get(f"/v2/historical-candle/intraday/{urllib.parse.quote(ikey)}/1minute", token)
    return d.get("data", {}).get("candles", [])

def get_daily(ikey, token):
    from datetime import datetime, timedelta
    to   = datetime.utcnow().strftime("%Y-%m-%d")
    frm  = (datetime.utcnow() - timedelta(days=35)).strftime("%Y-%m-%d")
    d = _upstox_get(
        f"/v2/historical-candle/{urllib.parse.quote(ikey)}/day/{to}/{frm}", token
    )
    return d.get("data", {}).get("candles", [])

# ─── Indicators ───────────────────────────────────────────────────────────────

def rsi14(closes):
    if len(closes) < 15:
        return 50.0
    g = l = 0.0
    for i in range(len(closes) - 14, len(closes)):
        d = closes[i] - closes[i - 1]
        if d > 0:
            g += d
        else:
            l += abs(d)
    avg_g = g / 14
    avg_l = l / 14 or 0.001
    return round(100 - 100 / (1 + avg_g / avg_l), 1)

def vwap(candles):
    tv = vol = 0.0
    for c in candles:
        tp = (c[2] + c[3] + c[4]) / 3
        tv += tp * c[5]
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

CF = [
    ("ORB breakout",        25, lambda s: 0.3 if s["sig"] == "WATCH" else (
        1.0 if (s["sig"] == "BUY" and s["bo"]) or (s["sig"] == "SELL" and s["bd"]) else 0.4
    )),
    ("Volume confirmation", 20, lambda s: (
        1.0 if s["tV"] / (s["aV"] or 1) >= 1.5 else
        0.8 if s["tV"] / (s["aV"] or 1) >= 1.0 else
        0.5 if s["tV"] / (s["aV"] or 1) >= 0.7 else 0.1
    )),
    ("VWAP alignment",      20, lambda s: 0.4 if s["sig"] == "WATCH" else (
        1.0 if (s["sig"] == "BUY" and s["av"]) or (s["sig"] == "SELL" and not s["av"]) else 0.1
    )),
    ("RSI alignment",       15, lambda s: (
        (1.0 if 40 <= s["rsi"] <= 60 else 0.8 if s["rsi"] < 40 else 0.6 if s["rsi"] <= 70 else 0.2)
        if s["sig"] == "BUY" else
        (1.0 if 40 <= s["rsi"] <= 60 else 0.8 if s["rsi"] > 60 else 0.6 if s["rsi"] >= 30 else 0.2)
        if s["sig"] == "SELL" else 0.4
    )),
    ("Risk:Reward",         15, lambda s: (
        1.0 if s["rr"] >= 3 else 0.85 if s["rr"] >= 2 else 0.6 if s["rr"] >= 1.5 else 0.3 if s["rr"] >= 1 else 0.1
    )),
    ("ATR/volatility",       5, lambda s: (
        lambda p: 1.0 if 0.8 <= p <= 2.5 else 0.6 if 0.5 <= p < 0.8 else 0.7 if 2.5 < p <= 4 else 0.3
    )(s["atr"] / s["ltp"] * 100)),
]

def conf_score(s):
    tot = max_w = 0.0
    for _, w, fn in CF:
        sc = fn(s)
        tot += sc * w
        max_w += w
    pct = round(tot / max_w * 100)
    return min(pct, 45) if s["sig"] == "WATCH" else pct

# ─── Build setup (mirrors buildSetup in JS) ───────────────────────────────────

def build_setup(sym, sec, intra, daily, ltp):
    orb = intra[:15]
    orb_h = round(max((c[2] for c in orb), default=ltp * 1.005), 2)
    orb_l = round(min((c[3] for c in orb), default=ltp * 0.995), 2)
    vw    = vwap(intra) if intra else ltp
    rs    = rsi14([c[4] for c in reversed(daily)])
    at    = atr14(list(reversed(daily[:20]))) or round(ltp * 0.015, 2)
    t_vol = sum(c[5] for c in intra)
    a_vol = sum(c[5] for c in daily[:20]) / max(len(daily[:20]), 1)
    pc    = daily[0][4] if daily else ltp
    chg   = round((ltp - pc) / pc * 100, 2)
    av    = ltp > vw
    bo    = ltp > orb_h
    bd    = ltp < orb_l

    if rs < 40 and av and bo:
        sig = "BUY";  en = round(orb_h + 0.05, 2); tg = round(en + at * 2, 2);   sl = round(orb_l - at * 0.3, 2)
        reason = "ORB breakout + above VWAP + oversold RSI"
    elif rs > 65 and not av and bd:
        sig = "SELL"; en = round(orb_l - 0.05, 2); tg = round(en - at * 2, 2);   sl = round(orb_h + at * 0.3, 2)
        reason = "ORB breakdown + below VWAP + overbought RSI"
    elif rs > 55 and av:
        sig = "BUY";  en = round(vw + at * 0.15, 2); tg = round(en + at * 1.5, 2); sl = round(vw - at * 0.4, 2)
        reason = "Above VWAP with bullish momentum"
    elif rs < 45 and not av:
        sig = "SELL"; en = round(vw - at * 0.15, 2); tg = round(en - at * 1.5, 2); sl = round(vw + at * 0.4, 2)
        reason = "Below VWAP with bearish momentum"
    else:
        sig = "WATCH"; en = round(ltp, 2); tg = round(ltp + at, 2); sl = round(ltp - at * 0.7, 2)
        reason = "Mixed signals — wait for clear breakout"

    rr = round(abs(tg - en) / (abs(sl - en) or 0.01), 2)

    # Candle confirmation
    confirmed = True
    confirm_count = 0
    recent = intra[-CONFIRM_CANDLES:]
    if len(recent) >= CONFIRM_CANDLES and sig != "WATCH":
        cum_tpv = cum_vol = 0.0
        for c in intra:
            tp = (c[2] + c[3] + c[4]) / 3
            cum_tpv += tp * c[5]
            cum_vol += c[5]
        running_vwap = cum_tpv / cum_vol if cum_vol else ltp
        checks = [c[4] > running_vwap if sig == "BUY" else c[4] < running_vwap for c in recent]
        confirm_count = sum(checks)
        confirmed = confirm_count >= CONFIRM_CANDLES
    else:
        confirm_count = len(recent)

    s = dict(
        sym=sym, sec=sec, ltp=round(ltp, 2), chg=chg,
        orb_h=orb_h, orb_l=orb_l, rsi=rs, vwap=round(vw, 2),
        atr=at, tV=round(t_vol), aV=round(a_vol),
        sig=sig, en=en, tg=tg, sl=sl, reason=reason,
        rr=rr, av=av, bo=bo, bd=bd,
        confirmed=confirmed, confirm_count=confirm_count,
    )
    conf = conf_score(s)
    if not confirmed and sig != "WATCH":
        conf = max(10, conf - 20)
    s["conf"] = conf
    return s

# ─── Readiness check (mirrors buildReadinessCheck in JS) ─────────────────────

def is_ready(s, ist_mins):
    """Returns (verdict, gates_pass_count) where verdict is 'green'/'amber'/'red'"""
    vp = round(s["tV"] / (s["aV"] or 1) * 100)

    time_ok    = 585 <= ist_mins <= 900
    time_prime = 585 <= ist_mins <= 630
    is_actual  = s["sig"] != "WATCH"
    orb_ok     = (s["sig"] == "BUY" and s["bo"]) or (s["sig"] == "SELL" and s["bd"]) or not is_actual
    vol_ok     = vp >= 100
    conf_ok    = s["conf"] >= 75
    conf_warn  = 55 <= s["conf"] < 75
    candle_ok  = s["confirmed"]

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
