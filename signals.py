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
    import logging as _logging
    _log = _logging.getLogger("scanner")
    url = UPSTOX_BASE + path
    req = urllib.request.Request(url, headers={
        "Authorization":  f"Bearer {token}",
        "Accept":         "application/json",
        "Api-Version":    "2.0",
        "User-Agent":     "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin":         "https://upstox.com",
        "Referer":        "https://upstox.com/",
    })
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="replace")[:300]
        except Exception:
            pass
        _log.warning("Upstox HTTP %s on %s — %s", e.code, path, body)
        raise

def get_ltp(ikey, token):
    d = _upstox_get(
        f"/v2/market-quote/ltp?instrument_key={urllib.parse.quote(ikey)}", token
    )
    data = d.get("data") or {}
    if not data:
        raise ValueError(f"Empty LTP data for {ikey} (rate-limited or market closed?)")
    k   = list(data.keys())[0]
    ltp = data[k]["last_price"]
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

def _ema(values, period):
    """Exponential moving average — returns list of same length as values."""
    if not values or period <= 0:
        return []
    k = 2 / (period + 1)
    emas = [values[0]]
    for v in values[1:]:
        emas.append(v * k + emas[-1] * (1 - k))
    return emas

def rvol_spike(intra, lookback=10):
    """
    Compares the most recent candle's volume against the rolling average of the
    previous `lookback` candles — detects big-money entry in real time.
    Returns ratio: 2.0 means last candle had 2× the recent average volume.
    """
    if len(intra) < lookback + 1:
        return 1.0
    recent_vol = intra[-1][5]
    avg_vol    = sum(c[5] for c in intra[-(lookback + 1):-1]) / lookback
    return round(recent_vol / (avg_vol or 1), 2)

def macd_signal(closes):
    """
    Standard MACD (12, 26, 9) on chronological closes (oldest → newest).
    Returns (macd_val, signal_val, histogram).
    """
    if len(closes) < 35:
        return 0.0, 0.0, 0.0
    ema12 = _ema(closes, 12)
    ema26 = _ema(closes, 26)
    n     = min(len(ema12), len(ema26))
    macd_line   = [ema12[i] - ema26[i] for i in range(n)]
    signal_line = _ema(macd_line, 9)
    hist = macd_line[-1] - (signal_line[-1] if signal_line else 0)
    return (
        round(macd_line[-1],   4),
        round(signal_line[-1] if signal_line else 0, 4),
        round(hist, 4),
    )

def bollinger_bands(closes, period=20, std_mult=2.0):
    """
    Bollinger Bands on chronological closes.
    Returns (upper, lower, bandwidth_pct, squeeze_score).
    squeeze_score 1.0 = very tight (coiled spring); 0.0 = fully expanded.
    """
    if len(closes) < period + 5:
        return 0.0, 0.0, 0.0, 0.5
    recent  = closes[-period:]
    mid     = sum(recent) / period
    std     = (sum((c - mid) ** 2 for c in recent) / period) ** 0.5
    upper   = mid + std_mult * std
    lower   = mid - std_mult * std
    bw      = (upper - lower) / mid if mid else 0

    # Squeeze: compare current bandwidth to the last 10 historical values
    hist_bws = []
    for i in range(1, 11):
        if len(closes) >= period + i:
            chunk = closes[-(period + i):-i]
            m     = sum(chunk) / period
            s     = (sum((c - m) ** 2 for c in chunk) / period) ** 0.5
            hist_bws.append((2 * s) / m if m else 0)

    squeeze_score = 0.5
    if hist_bws:
        mn, mx = min(hist_bws), max(hist_bws)
        if mx > mn:
            squeeze_score = round(1.0 - (bw - mn) / (mx - mn), 2)
            squeeze_score = max(0.0, min(1.0, squeeze_score))

    return round(upper, 2), round(lower, 2), round(bw * 100, 2), round(squeeze_score, 2)

def get_market_depth(ikey, token):
    """
    Fetch L1 market depth (top-5 bid/ask) from Upstox /v2/market-quote/quotes.
    Returns {buy_qty, sell_qty, ratio} where ratio > 1 means more buyers than sellers.
    No additional subscription required — available on standard Upstox API.
    """
    d    = _upstox_get(
        f"/v2/market-quote/quotes?instrument_key={urllib.parse.quote(ikey)}", token
    )
    data = d.get("data") or {}
    if not data:
        return None
    k     = list(data.keys())[0]
    depth = data[k].get("depth", {})
    buy_q  = sum(level.get("quantity", 0) for level in depth.get("buy",  []))
    sell_q = sum(level.get("quantity", 0) for level in depth.get("sell", []))
    return {
        "buy_qty":  buy_q,
        "sell_qty": sell_q,
        "ratio":    round(buy_q / (sell_q or 1), 2),
    }

def rsi14(closes):
    if len(closes) < 15:
        return 50.0
    # Seed with simple average of first 14 changes
    avg_g = avg_l = 0.0
    for i in range(1, 15):
        d = closes[i] - closes[i - 1]
        if d > 0: avg_g += d
        else:     avg_l += abs(d)
    avg_g /= 14
    avg_l  = avg_l / 14 or 0.001
    # Wilder smoothing for all remaining bars
    for i in range(15, len(closes)):
        d      = closes[i] - closes[i - 1]
        avg_g  = (avg_g * 13 + max(d,  0)) / 14
        avg_l  = (avg_l * 13 + max(-d, 0)) / 14 or 0.001
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
    if not trs:
        return 0.0
    # Seed with simple average of the first (up to) 14 true ranges
    atr = sum(trs[:14]) / min(len(trs), 14)
    # Wilder smoothing for all remaining bars
    for tr in trs[14:]:
        atr = (atr * 13 + tr) / 14
    return round(atr, 2)

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
    # Use per-minute rates so early-morning volume is fairly compared
    rv = s["tVpm"] / (s["aVpm"] or 1)
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

def _score_rvol_spike(s):
    """Recent candle volume spike vs 10-candle rolling average."""
    spike = s.get("rvol_spike", 1.0)
    if spike >= 3.0: return 1.0
    if spike >= 2.0: return 0.85
    if spike >= 1.5: return 0.70
    if spike >= 1.0: return 0.50
    return 0.20

def _score_macd(s):
    """MACD (12,26,9) alignment with signal direction."""
    mv   = s.get("macd",      0.0)
    ms_v = s.get("macd_sig",  0.0)
    mh   = s.get("macd_hist", 0.0)
    sig  = s["sig"]
    if sig == "BUY":
        if mv > ms_v and mh > 0:  return 1.0
        if mv > ms_v:             return 0.65
        if mh > 0:                return 0.45
        return 0.15
    if sig == "SELL":
        if mv < ms_v and mh < 0:  return 1.0
        if mv < ms_v:             return 0.65
        if mh < 0:                return 0.45
        return 0.15
    return 0.35

def _score_bollinger(s):
    """Bollinger Band position + squeeze confirmation."""
    upper   = s.get("bb_upper",   0.0)
    lower   = s.get("bb_lower",   0.0)
    squeeze = s.get("bb_squeeze", 0.5)
    ltp     = s["ltp"]
    sig     = s["sig"]
    if not upper or not lower:
        return 0.5
    mid = (upper + lower) / 2
    if sig == "BUY":
        if ltp > upper:                    return 1.0   # breakout above upper band
        if ltp > mid and squeeze >= 0.75:  return 0.85  # above mid + tight squeeze
        if ltp > mid:                      return 0.60
        return 0.20
    if sig == "SELL":
        if ltp < lower:                    return 1.0
        if ltp < mid and squeeze >= 0.75:  return 0.85
        if ltp < mid:                      return 0.60
        return 0.20
    return squeeze * 0.5   # WATCH: squeeze alone is interesting

def _score_depth(s):
    """L1 market depth: bid/ask quantity ratio — 0.5 neutral when unavailable."""
    depth = s.get("depth")
    if not depth:
        return 0.5
    ratio = depth.get("ratio", 1.0)
    sig   = s["sig"]
    if sig == "BUY":
        if ratio >= 2.5: return 1.0
        if ratio >= 1.8: return 0.85
        if ratio >= 1.2: return 0.65
        if ratio >= 0.8: return 0.45
        return 0.20
    if sig == "SELL":
        inv = 1 / (ratio or 1)
        if inv >= 2.5: return 1.0
        if inv >= 1.8: return 0.85
        if inv >= 1.2: return 0.65
        if inv >= 0.8: return 0.45
        return 0.20
    return 0.5

# Weights are relative — conf_score() normalises by sum(weights).
# New total = 128; effective % shown alongside each entry.
CF = [
    ("ORB breakout",        25, _score_orb),        # ~19.5%
    ("VWAP alignment",      20, _score_vwap),       # ~15.6%
    ("RSI alignment",       15, _score_rsi),        # ~11.7%
    ("Volume confirmation", 15, _score_volume),     # ~11.7%
    ("Risk:Reward",         12, _score_rr),         # ~9.4%
    ("MACD momentum",       10, _score_macd),       # ~7.8%  ← new
    ("RVOL spike",          10, _score_rvol_spike), # ~7.8%  ← new
    ("Bollinger squeeze",    8, _score_bollinger),  # ~6.3%  ← new
    ("L1 depth",             8, _score_depth),      # ~6.3%  ← new
    ("ATR/volatility",       5, _score_atr),        # ~3.9%
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


# ─── Build setup with market context ─────────────────────────────────────────

def build_setup(sym, sec, intra, daily, ltp, market_ctx=None, depth=None):
    """
    Build a complete trade setup.
    market_ctx — dict from get_market_context(). If None, filters are skipped
                 (backwards compatible with existing calls that don't pass context).
    """
    orb   = intra[:15]
    orb_h = round(max((c[2] for c in orb), default=ltp * 1.005), 2)
    orb_l = round(min((c[3] for c in orb), default=ltp * 0.995), 2)
    vw    = vwap(intra) if intra else ltp
    closes_chron = [c[4] for c in reversed(daily)]   # oldest → newest
    rs    = rsi14(closes_chron)
    at    = atr14(list(reversed(daily[:20]))) or round(ltp * 0.015, 2)

    # ── New indicators ────────────────────────────────────────────────────────
    rvol_ratio              = rvol_spike(intra) if intra else 1.0
    mc, ms_v, mh            = macd_signal(closes_chron)
    bb_upper, bb_lower, bb_bw, bb_sq = bollinger_bands(closes_chron)
    t_vol = sum(c[5] for c in intra)
    a_vol = sum(c[5] for c in daily[:20]) / max(len(daily[:20]), 1)
    # Per-minute volume rates: today vs historical average
    # Full session = 375 minutes (09:15–15:30); compare rate, not total
    DAY_MINS = 375
    t_vol_pm = t_vol / max(len(intra), 1)          # avg vol per candle today
    a_vol_pm = a_vol / DAY_MINS                     # avg vol per minute historically
    pc    = daily[0][4] if daily else ltp
    chg   = round((ltp - pc) / pc * 100, 2)

    today_open = intra[0][1] if intra else ltp
    gap_pct    = round((today_open - pc) / pc * 100, 2)

    av = ltp > vw
    bo = ltp > orb_h
    bd = ltp < orb_l

    # ── Base signal logic ────────────────────────────────────────────────────
    # RSI < 55: not overbought — valid entry for an ORB breakout upward
    # RSI > 45: not oversold — valid entry for an ORB breakdown downward
    if rs < 55 and av and bo:
        sig    = "BUY"
        en     = round(orb_h + 0.05, 2)
        sl     = round(min(orb_l - 0.3 * at, en - 0.5 * at), 2)
        tg     = round(en + 2.0 * at, 2)
        reason = "Above VWAP with bullish momentum"
    elif rs > 45 and (not av) and bd:
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

    # ── Regime bonus/penalty ──────────────────────────────────────────────────
    # detect_regime needs chg + gap_pct + market_ctx so compute it here early
    regime = detect_regime(chg, gap_pct, market_ctx)
    if sig != "WATCH":
        if regime == "bull_trend" and sig == "BUY":
            conf_penalties -= 8
            ctx_warnings.append("Bull trend regime — tailwind for BUY (+8% conf)")
        elif regime == "bear_trend" and sig == "SELL":
            conf_penalties -= 8
            ctx_warnings.append("Bear trend regime — tailwind for SELL (+8% conf)")
        elif regime == "gap_stall":
            conf_penalties += 10
            ctx_warnings.append("Gap-stall regime — price stalled after gap, caution (-10% conf)")

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

    s = dict(
        sym=sym, sec=sec, ltp=round(ltp, 2), chg=chg,
        orb_h=orb_h, orb_l=orb_l, rsi=rs,
        vwap=round(vw, 2), atr=at,
        tV=round(t_vol), aV=round(a_vol),
        tVpm=round(t_vol_pm, 2), aVpm=round(a_vol_pm, 2),
        sig=sig, en=en, tg=tg, sl=sl,
        reason=reason, rr=rr, av=av, bo=bo, bd=bd,
        confirmed=confirmed, confirm_count=confirm_count,
        gap_pct=gap_pct,
        market_ctx=market_ctx or {},
        ctx_warnings=ctx_warnings,
        market_blocked=market_blocked,
        regime=regime,
        # ── New indicators ─────────────────────────────────────────────────
        rvol_spike=rvol_ratio,
        macd=mc, macd_sig=ms_v, macd_hist=mh,
        bb_upper=bb_upper, bb_lower=bb_lower, bb_bw=bb_bw, bb_squeeze=bb_sq,
        depth=depth,
    )

    # Base setup quality only
    signal_conf, feature_scores = conf_score(s)
    risk_penalty = conf_penalties

    # Candle confirmation — graduated penalty (not all-or-nothing)
    # 3/3 = no penalty, 2/3 = -10, 1/3 or 0/3 = -20
    if sig != "WATCH":
        if confirm_count == 0:
            risk_penalty += CANDLE_CONFIRM_PENALTY          # full 20-pt penalty
            s["reason"] += f" ⚠ ({confirm_count}/{CONFIRM_CANDLES} candles confirm)"
        elif not confirmed:
            risk_penalty += CANDLE_CONFIRM_PENALTY // 2    # partial 10-pt penalty
            s["reason"] += f" ⚠ ({confirm_count}/{CONFIRM_CANDLES} candles confirm)"

    s["signal_conf"]   = signal_conf
    s["risk_penalty"]  = round(risk_penalty)
    s["feature_scores"] = feature_scores
    s["conf"]          = max(5, min(100, signal_conf - risk_penalty))

    return s

# ─── Readiness check ──────────────────────────────────────────────────────────
def is_ready(s, ist_mins):
    """Returns (verdict, gates_pass_count)"""
    vp = round(s["tVpm"] / (s["aVpm"] or 1) * 100)

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
