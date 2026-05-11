"""
fundamentals.py — Long-Term Stock Scorer
Segment-aware (Large / Mid / Small Cap) fundamental + technical scoring.
Data sources: yfinance (primary), Screener.in (promoter holding), NSE India (events).
Runs as a weekly batch job (Sunday 8pm IST) via APScheduler in app.py.
Does NOT touch any intraday scanner state.
"""

import os
import json
import logging
import time
import warnings
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timezone, timedelta, date

# yfinance/pandas trigger deprecation warnings about Timestamp.utcnow internally.
# Suppress the entire yfinance + pandas_datareader warning namespace.
warnings.filterwarnings("ignore", module=r"yfinance\..*")
warnings.filterwarnings("ignore", module=r"pandas\..*", message=".*utcnow.*")
warnings.filterwarnings("ignore", message=".*utcnow.*")

log = logging.getLogger("fundamentals")

IST = timezone(timedelta(hours=5, minutes=30))

# ── Index constituent lists (Nifty 100 / Midcap 150 / Smallcap 250) ──────────
# Symbols as used by yfinance (.NS suffix added at fetch time)
LARGE_CAP = [
    "RELIANCE","TCS","HDFCBANK","BHARTIARTL","ICICIBANK","INFOSYS","SBIN","HINDUNILVR",
    "ITC","BAJFINANCE","KOTAKBANK","LT","HCLTECH","MARUTI","ASIANPAINT","AXISBANK",
    "TITAN","SUNPHARMA","ULTRACEMCO","BAJAJFINSV","NESTLEIND","WIPRO","POWERGRID",
    "NTPC","ADANIENT","ADANIPORTS","TECHM","TATAMOTORS","DRREDDY","DIVISLAB",
    "CIPLA","JSWSTEEL","TATASTEEL","COALINDIA","ONGC","BPCL","HEROMOTOCO",
    "EICHERMOT","GRASIM","SHREECEM","APOLLOHOSP","BRITANNIA","TATACONSUM","PIDILITIND",
    "DABUR","HAVELLS","GODREJCP","BOSCHLTD","MUTHOOTFIN","SIEMENS","INDIGO",
    "DLF","VEDL","HINDALCO","NMDC","SAIL","JINDALSTEL","LUPIN","AUROPHARMA",
    "TORNTPHARM","LICHSGFIN","CHOLAFIN","MFSL","SBILIFE","HDFCLIFE","ICICIPRULI",
    "ICICIGI","BAJAJ-AUTO","TVSMOTORS","M&M","TVSMOTOR","ESCORTS","ASHOKLEY",
    "BERGEPAINT","KANSAINER","MARICO","COLPAL","EMAMILTD","VBL","TRENT","NYKAA",
    "DMART","ZOMATO","PAYTM","POLICYBZR","NAUKRI","INDIAMART","IRCTC","ZEEL",
    "SUNTV","PVRINOX","JUBLFOOD","DEVYANI","WESTLIFE","MCDOWELL-N","UNITEDSPIRITS",
    "GMRAIRPORT","AIAENG","CUMMINSIND","THERMAX","ABB","BHEL","BEL","HAL",
]

MIDCAP = [
    "PERSISTENT","MPHASIS","COFORGE","LTTS","KPITTECH","TATAELXSI","HEXAWARE",
    "OFSS","CYIENT","ZENSAR","NIITTECH","MASTEK","RATEGAIN","TANLA",
    "IDFCFIRSTB","FEDERALBNK","KARURVYSYA","CSBBANK","DCBBANK","RBLBANK",
    "BANDHANBNK","UJJIVANSFB","EQUITASBNK","SURYODAY","JKCEMENT","RAMCOCEM",
    "HEIDELBERG","BIRLACORPN","PRSMJOHNSN","ORIENTCEM","STARCEMENT",
    "APLAPOLLO","RATNAMANI","WELSPUNIND","TRIDENT","VARDHACRLC","ALOKTEXT",
    "PAGEIND","RAYMOND","SPENCERS","VMART","SHOPERSTOP","BATA","RELAXO",
    "CAMPUS","METROBRAND","KPRMILL","GOCOLORS","SUNDRMFAST","MOTHERSON",
    "BALKRISIND","APOLLOTYRE","CEATLTD","MRFLTD","JKTYRE","GOODYEAR",
    "CONCOR","BLUEDART","MAHINDCIE","ENDURANCE","SUPRAJIT","FIEM",
    "LALPATHLAB","METROPOLIS","KRSNAA","VIJAYA","SUVENPHAR","AJANTPHARM",
    "ALKEM","GRANULES","LAURUSLABS","SOLARA","NATCOPHARM","GLAND",
    "SUDARSCHEM","AAVAS","HOMEFIRST","APTUS","CREDITACC","SPANDANA",
    "MUTHOOTMF","MANAPPURAM","IIFL","FIVE-STAR","UGROCAP","PAISALO",
    "CAMS","CDSL","BSE","MCX","ISEC","ANGELONE",
    "IRFC","RECLTD","PFCLTD","HUDCO","NABARD","RVNL",
    "TTKPRESTIG","HAWKINCOOK","VSTIND","RADICO","GLOBUSSPR","KSCL",
]

SMALLCAP = [
    "ROUTE","RPGLIFE","SEQUENT","LXCHEM","VALIANTORG","STARHEALTH","ACCELYA",
    "INTELLECT","NEWGEN","KFINTECH","DATAMATICS","BIRLASOFT","INFOBEAN","GREENPANEL",
    "CENTUM","RPTECH","QUICKHEAL","NUCLEUS","SAKSOFT","MSTCLTD","RAILTEL",
    "IRCON","TITAGARH","TEXRAIL","NDTVMEDIA","HATHWAY","GTLINFRA","TATACOMM",
    "STLTECH","VINDHYATEL","TEJASNET","HFCL","ITI","TANGT","SPICEJET",
    "GLOBUSMED","CONTROLPRINT","PONDY","ANDHRAPET","LGBBROSEXP","SAFARI",
    "VIPIND","SKFINDIA","GRINDWELL","SCHAEFFLER","ELGIEQUIP","KIRLOSENG",
    "THERMAX","INGERSRAND","KENNAMET","JYOTHYLAB","BAJAJCON","ZYDUSWELL",
    "HONASA","VLCC","ARCHIES","NYKAA","SAPPHIRE","BIKAJI",
    "POKARNA","ASAHIINDIA","POLYPLEX","UFLEX","GPPL","SHREEPIPE",
    "PRINCEPIPE","ASTRAL","SUPREMEIND","NILKAMAL","PLASSON","SKIPPER",
    "KERNEX","TEXINFRA","HGINFRA","DBREALTY","ANANTRAJ","KOLTEPATIL",
    "SUNTECK","GODREJPROP","MAHLIFE","ARVIND","KIRIINDS","PNBHOUSING",
    "CANFINHOME","GRUH","REPCO","AROGRANITE","ORIENTBELL","SOMANYCER",
    "REGENCYCER","ASIANSTAR","THEJEWEL","TITAN","PCJEWELLER","SENCO",
]

# ── Segment scoring weights ────────────────────────────────────────────────────
WEIGHTS = {
    "large": {
        "eps_growth":     0.15,
        "rev_growth":     0.10,
        "roe":            0.15,
        "debt_equity":    0.15,
        "pe_vs_sector":   0.15,
        "above_200dma":   0.10,
        "rel_strength":   0.10,
        "promoter":       0.10,
    },
    "mid": {
        "eps_growth":     0.20,
        "rev_growth":     0.15,
        "roe":            0.15,
        "debt_equity":    0.10,
        "pe_vs_sector":   0.10,
        "above_200dma":   0.10,
        "rel_strength":   0.10,
        "promoter":       0.10,
    },
    "small": {
        "eps_growth":     0.25,
        "rev_growth":     0.20,
        "roe":            0.10,
        "debt_equity":    0.05,
        "pe_vs_sector":   0.05,
        "above_200dma":   0.10,
        "rel_strength":   0.10,
        "promoter":       0.15,
    },
}

# ── NSE session (needed for corporate events API) ─────────────────────────────
_nse_session_cookie = None
_nse_session_ts     = None
NSE_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://www.nseindia.com/",
}

def _get_nse_cookies() -> dict:
    """Fetch a fresh NSE India session cookie (valid ~5 min)."""
    global _nse_session_cookie, _nse_session_ts
    now = time.time()
    if _nse_session_cookie and _nse_session_ts and (now - _nse_session_ts < 240):
        return _nse_session_cookie
    try:
        req = urllib.request.Request(
            "https://www.nseindia.com/",
            headers={**NSE_HEADERS, "Accept": "text/html"},
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            cookies = {}
            for hdr in r.headers.get_all("Set-Cookie") or []:
                name, _, rest = hdr.partition("=")
                val, _, _     = rest.partition(";")
                cookies[name.strip()] = val.strip()
            _nse_session_cookie = cookies
            _nse_session_ts     = now
            return cookies
    except Exception as e:
        log.warning("NSE session fetch failed: %s", e)
        return {}

def _nse_get(url: str) -> dict | None:
    """GET an NSE India API endpoint with session cookie. Returns parsed JSON or None."""
    cookies = _get_nse_cookies()
    cookie_str = "; ".join(f"{k}={v}" for k, v in cookies.items())
    try:
        req = urllib.request.Request(url, headers={**NSE_HEADERS, "Cookie": cookie_str})
        with urllib.request.urlopen(req, timeout=4) as r:
            return json.loads(r.read())
    except Exception as e:
        log.debug("NSE API %s failed: %s", url, e)
        return None

# ── Screener.in promoter holding ──────────────────────────────────────────────
def _get_promoter_holding(symbol: str) -> float | None:
    """Fetch promoter holding % from Screener.in. Returns float or None."""
    url = f"https://www.screener.in/company/{symbol}/consolidated/"
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0", "Accept": "text/html"},
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            html = r.read().decode("utf-8", errors="replace")
        # Find promoter holding in the shareholding table
        # Screener renders: "Promoters\n...XX.XX%"
        import re
        m = re.search(r'Promoters[^%]{0,200}?(\d{1,2}\.\d{1,2})%', html, re.DOTALL)
        if m:
            return float(m.group(1))
    except Exception as e:
        log.debug("Screener.in %s failed: %s", symbol, e)
    return None

# ── Corporate events (NSE India) ──────────────────────────────────────────────
def _get_corporate_events(symbol: str) -> dict:
    """
    Fetch upcoming results + recent dividend from NSE India.
    Returns dict with keys: results_due (date str or None), dividend_yield (float or None),
    dividend_consistent (bool), last_pat_growth (float or None), event_risk (bool).
    """
    out = {
        "results_due":       None,
        "dividend_yield":    None,
        "dividend_consistent": False,
        "last_pat_growth":   None,
        "event_risk":        False,
    }
    today     = date.today()
    in_7_days = (today + timedelta(days=7)).isoformat()
    today_str = today.isoformat()

    # Upcoming quarterly results
    url = (
        f"https://www.nseindia.com/api/corporateEvents"
        f"?index=equities&from_date={today_str}&to_date={in_7_days}"
        f"&type=Quarterly%20Results&symbol={symbol}"
    )
    data = _nse_get(url)
    if data and isinstance(data, list) and data:
        out["results_due"] = data[0].get("exDate") or data[0].get("date")
        out["event_risk"]  = True

    # Recent dividends (last 3 years)
    three_yr = (today - timedelta(days=1095)).isoformat()
    url2 = (
        f"https://www.nseindia.com/api/corporateEvents"
        f"?index=equities&from_date={three_yr}&to_date={today_str}"
        f"&type=Dividend&symbol={symbol}"
    )
    data2 = _nse_get(url2)
    if data2 and isinstance(data2, list):
        years = set()
        for ev in data2:
            d = ev.get("exDate") or ev.get("date") or ""
            if len(d) >= 4:
                years.add(d[:4])
        out["dividend_consistent"] = len(years) >= 3  # paid in all 3 of last 3 years

    return out

# ── yfinance fundamentals ─────────────────────────────────────────────────────
def _fetch_yf(symbol: str) -> dict | None:
    """Fetch fundamentals + price history for one NSE stock via yfinance."""
    try:
        import yfinance as yf
    except ImportError:
        log.error("yfinance not installed — run: pip install yfinance")
        return None

    # Suppress all warnings inside yfinance/pandas calls — Pandas4Warning about
    # Timestamp.utcnow is raised by pandas internals and not actionable from here.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return _fetch_yf_inner(symbol, yf)


def _fetch_yf_inner(symbol: str, yf) -> dict | None:
    """Inner implementation — called inside warnings.catch_warnings() block."""
    ticker = yf.Ticker(f"{symbol}.NS")
    try:
        info = ticker.info or {}
    except Exception as e:
        log.debug("yfinance info %s: %s", symbol, e)
        info = {}

    # Price history for 200 DMA + 6-month relative strength
    hist = None
    try:
        hist = ticker.history(period="1y", interval="1d", auto_adjust=True)
    except Exception as e:
        log.debug("yfinance history %s: %s", symbol, e)

    cmp = info.get("currentPrice") or info.get("regularMarketPrice")
    if not cmp and hist is not None and not hist.empty:
        cmp = float(hist["Close"].iloc[-1])

    dma_200 = None
    above_200dma = None
    if hist is not None and len(hist) >= 200:
        dma_200 = float(hist["Close"].tail(200).mean())
        above_200dma = cmp > dma_200 if cmp else None
    elif hist is not None and len(hist) >= 50:
        dma_200 = float(hist["Close"].mean())
        above_200dma = cmp > dma_200 if cmp else None

    # 6-month relative strength vs Nifty 50
    rel_strength_6m = None
    try:
        nifty = yf.Ticker("^NSEI")
        nh = nifty.history(period="6mo", interval="1d", auto_adjust=True)
        if hist is not None and not hist.empty and not nh.empty:
            stock_ret = (hist["Close"].iloc[-1] - hist["Close"].iloc[-126]) / hist["Close"].iloc[-126] if len(hist) >= 126 else None
            nifty_ret = (nh["Close"].iloc[-1] - nh["Close"].iloc[-126]) / nh["Close"].iloc[-126] if len(nh) >= 126 else None
            if stock_ret is not None and nifty_ret is not None:
                rel_strength_6m = float(stock_ret - nifty_ret)  # positive = outperforming
    except Exception as e:
        log.debug("rel_strength %s: %s", symbol, e)

    # Financials for EPS/revenue growth
    eps_growth = None
    rev_growth = None
    try:
        fin = ticker.financials  # annual, most recent first
        if fin is not None and not fin.empty and fin.shape[1] >= 2:
            if "Net Income" in fin.index:
                ni = fin.loc["Net Income"]
                if ni.iloc[0] and ni.iloc[1] and ni.iloc[1] != 0:
                    eps_growth = float((ni.iloc[0] - ni.iloc[1]) / abs(ni.iloc[1]))
            if "Total Revenue" in fin.index:
                rev = fin.loc["Total Revenue"]
                if rev.iloc[0] and rev.iloc[1] and rev.iloc[1] != 0:
                    rev_growth = float((rev.iloc[0] - rev.iloc[1]) / abs(rev.iloc[1]))
    except Exception as e:
        log.debug("financials %s: %s", symbol, e)

    # EPS (trailing)
    eps = info.get("trailingEps") or info.get("forwardEps")
    pe  = info.get("trailingPE")  or info.get("forwardPE")

    return {
        "symbol":        symbol,
        "cmp":           cmp,
        "pe":            float(pe)     if pe else None,
        "eps":           float(eps)    if eps else None,
        "roe":           float(info.get("returnOnEquity", 0) or 0) * 100,  # yf gives 0-1 scale
        "debt_equity":   float(info.get("debtToEquity",  0) or 0) / 100,  # yf gives % form
        "eps_growth":    eps_growth,
        "rev_growth":    rev_growth,
        "sector":        info.get("sector")  or info.get("industry") or "",
        "above_200dma":  above_200dma,
        "rel_strength_6m": rel_strength_6m,
        "analyst_target":  float(info.get("targetMeanPrice")) if info.get("targetMeanPrice") else None,
        "book_value":    float(info.get("bookValue")) if info.get("bookValue") else None,
        "dividend_yield":float(info.get("dividendYield", 0) or 0) * 100,
        "52w_high":      float(info.get("fiftyTwoWeekHigh")) if info.get("fiftyTwoWeekHigh") else None,
        "52w_low":       float(info.get("fiftyTwoWeekLow"))  if info.get("fiftyTwoWeekLow")  else None,
    }

# ── Sector P/E median (computed from batch results) ──────────────────────────
def _compute_sector_medians(stock_data_list: list) -> dict:
    """Given a list of fetched stock dicts, return {sector: median_pe}."""
    from collections import defaultdict
    import statistics
    sector_pes = defaultdict(list)
    for d in stock_data_list:
        if d and d.get("sector") and d.get("pe") and 0 < d["pe"] < 200:
            sector_pes[d["sector"]].append(d["pe"])
    return {
        sec: statistics.median(pes)
        for sec, pes in sector_pes.items()
        if len(pes) >= 3
    }

# ── Scoring ───────────────────────────────────────────────────────────────────
def _score_factor(value, low_bad, low_ok, high_ok, high_great) -> float:
    """Linear interpolation: returns 0-100 for a value on a scale."""
    if value is None:
        return 50.0  # neutral when data missing
    if value <= low_bad:
        return 0.0
    if value <= low_ok:
        return 50.0 * (value - low_bad) / (low_ok - low_bad)
    if value <= high_ok:
        return 50.0 + 50.0 * (value - low_ok) / (high_ok - low_ok)
    if value <= high_great:
        return 100.0
    return 100.0

def score_stock(data: dict, segment: str, sector_medians: dict) -> dict:
    """
    Score a stock 0-100 using segment-specific weights.
    Returns dict with total score + per-factor breakdown.
    """
    w       = WEIGHTS[segment]
    factors = {}

    # EPS growth: <0% = bad, 0-10% = ok, 10-25% = good, >25% = great
    factors["eps_growth"]  = _score_factor(data.get("eps_growth"),  -0.10,  0.00,  0.15,  0.25)
    # Revenue growth
    factors["rev_growth"]  = _score_factor(data.get("rev_growth"),  -0.05,  0.05,  0.15,  0.25)
    # ROE: <8% = bad, 8-15% = ok, 15-25% = good, >25% = great
    factors["roe"]         = _score_factor(data.get("roe"),           5.0,  10.0,  18.0,  25.0)
    # Debt/Equity: 0 = best, 0.5 = ok, 1.0 = limit, >2 = bad (inverted)
    de = data.get("debt_equity", 0)
    factors["debt_equity"] = _score_factor(-de,                      -2.0,  -1.0,  -0.5,   0.0)
    # P/E vs sector: <0.8x median = great, 0.8-1.2x = ok, >1.5x = bad
    sector_median_pe = sector_medians.get(data.get("sector", ""), None)
    pe               = data.get("pe")
    if pe and sector_median_pe and sector_median_pe > 0:
        pe_ratio = pe / sector_median_pe
        factors["pe_vs_sector"] = _score_factor(-pe_ratio, -2.0, -1.5, -1.0, -0.8)
    else:
        factors["pe_vs_sector"] = 50.0
    # 200 DMA
    factors["above_200dma"] = 100.0 if data.get("above_200dma") else (0.0 if data.get("above_200dma") is False else 50.0)
    # Relative strength vs Nifty 6M
    factors["rel_strength"] = _score_factor(data.get("rel_strength_6m"), -0.20, -0.05, 0.05, 0.20)
    # Promoter holding
    promo = data.get("promoter_holding")
    factors["promoter"]     = _score_factor(promo, 20.0, 35.0, 50.0, 65.0)

    # Weighted total
    total = sum(factors[k] * w[k] for k in w)

    # Bonuses (not part of main score — applied after)
    bonus = 0.0
    if data.get("events", {}).get("dividend_consistent"):
        bonus += 3.0
    if data.get("events", {}).get("last_pat_growth") and data["events"]["last_pat_growth"] > 0.15:
        bonus += 3.0

    # Penalties
    penalty = 0.0
    if data.get("events", {}).get("event_risk"):
        penalty += 5.0   # results due this week — uncertainty

    final = min(100.0, max(0.0, total + bonus - penalty))

    return {
        "score":   round(final, 1),
        "factors": {k: round(v, 1) for k, v in factors.items()},
        "bonus":   bonus,
        "penalty": penalty,
        "data_gaps": [k for k, v in factors.items() if v == 50.0 and
                      data.get(k.replace("_", "_")) is None],
    }

# ── Target range computation ──────────────────────────────────────────────────
def compute_targets(data: dict, sector_medians: dict) -> dict:
    """
    Returns price target range:
      low  = P/E reversion to sector median × trailing EPS  (conservative)
      high = PEG-based: EPS × (1 + growth) × growth_pe      (growth scenario)
    Upside % computed from CMP.
    """
    cmp = data.get("cmp")
    eps = data.get("eps")
    pe  = data.get("pe")
    sector_median_pe = sector_medians.get(data.get("sector", ""), pe)
    eps_growth = data.get("eps_growth") or 0.10  # default 10% if missing

    target_low  = None
    target_high = None

    if eps and sector_median_pe:
        # Low: sector median P/E reversion
        target_low = round(sector_median_pe * eps, 2)

    if eps and eps_growth > 0:
        # High: PEG-based — fair P/E = EPS growth rate (as %)
        peg_pe     = min(max(eps_growth * 100, 10), 50)  # clamp 10–50
        fwd_eps    = eps * (1 + eps_growth)
        target_high = round(peg_pe * fwd_eps, 2)

    # Fallback to analyst target or 52W high
    if not target_high:
        target_high = data.get("analyst_target") or data.get("52w_high")
    if not target_low and target_high:
        target_low = round(target_high * 0.85, 2)

    # Clamp: targets must be above CMP to be a buy pick
    if cmp and target_low and target_low < cmp:
        target_low = None
    if cmp and target_high and target_high < cmp:
        target_high = None

    upside_low  = round((target_low  / cmp - 1) * 100, 1) if (cmp and target_low)  else None
    upside_high = round((target_high / cmp - 1) * 100, 1) if (cmp and target_high) else None

    return {
        "target_low":   target_low,
        "target_high":  target_high,
        "upside_low":   upside_low,
        "upside_high":  upside_high,
        "analyst_target": data.get("analyst_target"),
    }

# ── Per-stock news sentiment (reuses NewsAPI + Claude from macro.py) ──────────
def _get_stock_news_sentiment(symbol: str) -> float:
    """
    Fetch last 24h news for a stock, classify via Claude Haiku.
    Returns sentiment multiplier 0.7–1.1 (same scale as macro.py).
    """
    news_key = os.environ.get("NEWS_API_KEY", "")
    if not news_key:
        return 1.0
    try:
        query    = urllib.parse.quote(f"{symbol} NSE stock")
        url      = (
            f"https://newsapi.org/v2/everything?q={query}"
            f"&language=en&sortBy=publishedAt&pageSize=5"
            f"&apiKey={news_key}"
        )
        req  = urllib.request.Request(url, headers={"User-Agent": "NSEScanner/1.0"})
        with urllib.request.urlopen(req, timeout=8) as r:
            articles = json.loads(r.read()).get("articles", [])
        if not articles:
            return 1.0
        headlines = " | ".join(a.get("title", "") for a in articles[:5])
        return _classify_sentiment_claude(headlines, symbol)
    except Exception as e:
        log.debug("news sentiment %s: %s", symbol, e)
        return 1.0

def _classify_sentiment_claude(headlines: str, symbol: str) -> float:
    """Send headlines to Claude Haiku for sentiment classification. Returns 0.7-1.1."""
    api_key = os.environ.get("ANTHROPIC_KEY", os.environ.get("ANTHROPIC_API_KEY", ""))
    if not api_key:
        return 1.0
    try:
        payload = json.dumps({
            "model":      "claude-haiku-4-5-20251001",
            "max_tokens": 20,
            "messages": [{
                "role": "user",
                "content": (
                    f"Stock: {symbol}\nHeadlines: {headlines[:500]}\n"
                    "Rate overall sentiment for long-term investors: "
                    "VERY_POSITIVE, POSITIVE, NEUTRAL, NEGATIVE, VERY_NEGATIVE. "
                    "Reply with only one word."
                ),
            }],
        }).encode()
        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=payload,
            headers={
                "x-api-key":         api_key,
                "anthropic-version": "2023-06-01",
                "Content-Type":      "application/json",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            resp = json.loads(r.read())
        word = resp["content"][0]["text"].strip().upper()
        return {"VERY_POSITIVE": 1.10, "POSITIVE": 1.05, "NEUTRAL": 1.0,
                "NEGATIVE": 0.90, "VERY_NEGATIVE": 0.75}.get(word, 1.0)
    except Exception:
        return 1.0

# ── Dedicated DB connection for LT scan (avoids sharing with intraday thread) ─
def _lt_save_pick(pick: dict, conn) -> bool:
    """Save one pick using an already-open dedicated connection."""
    try:
        import json as _json
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO long_term_picks
                    (scan_date, symbol, segment, score, signal, cmp, pe, roe,
                     eps_growth, rev_growth, debt_equity, promoter_pct, sector,
                     above_200dma, rel_strength_6m, target_low, target_high,
                     upside_low, upside_high, analyst_target, results_due,
                     dividend_yield, dividend_consistent, event_risk,
                     factors, sentiment)
                VALUES
                    (%(scan_date)s, %(symbol)s, %(segment)s, %(score)s, %(signal)s,
                     %(cmp)s, %(pe)s, %(roe)s, %(eps_growth)s, %(rev_growth)s,
                     %(debt_equity)s, %(promoter_pct)s, %(sector)s,
                     %(above_200dma)s, %(rel_strength_6m)s,
                     %(target_low)s, %(target_high)s,
                     %(upside_low)s, %(upside_high)s, %(analyst_target)s,
                     %(results_due)s, %(dividend_yield)s,
                     %(dividend_consistent)s, %(event_risk)s,
                     %(factors)s, %(sentiment)s)
                ON CONFLICT (scan_date, symbol, segment) DO UPDATE SET
                    score=EXCLUDED.score, signal=EXCLUDED.signal,
                    cmp=EXCLUDED.cmp, target_low=EXCLUDED.target_low,
                    target_high=EXCLUDED.target_high,
                    upside_low=EXCLUDED.upside_low, upside_high=EXCLUDED.upside_high,
                    factors=EXCLUDED.factors, sentiment=EXCLUDED.sentiment,
                    created_at=NOW()
            """, {**pick, "factors": _json.dumps(pick.get("factors", {}))})
        return True
    except Exception as e:
        log.warning("_lt_save_pick %s: %s", pick.get("symbol"), e)
        return False

def _open_lt_db_conn():
    """Open a fresh, dedicated psycopg connection for the LT scan thread."""
    import psycopg
    from psycopg.rows import dict_row
    url = os.environ.get("DATABASE_URL", "")
    if not url:
        return None
    if "sslmode" not in url:
        url = url + ("&" if "?" in url else "?") + "sslmode=require"
    try:
        return psycopg.connect(
            url,
            row_factory=dict_row,
            autocommit=True,
            prepare_threshold=None,
        )
    except Exception as e:
        log.error("LT DB connect failed: %s", e)
        return None

# ── Main scan function ────────────────────────────────────────────────────────
def run_lt_scan(segment: str = None) -> dict:
    """
    Run the full long-term scan for one or all segments.
    Saves results to DB via a dedicated connection (not shared with intraday thread).
    Returns summary dict.
    """
    segments = [segment] if segment else ["large", "mid", "small"]
    universe = {"large": LARGE_CAP, "mid": MIDCAP, "small": SMALLCAP}
    all_picks = []
    summary   = {}

    # Open a dedicated DB connection for this scan run — never shares with intraday thread
    lt_conn = _open_lt_db_conn()
    if not lt_conn:
        log.warning("LT scan: no DB connection — picks will not be saved")

    for seg in segments:
        log.info("LT scan: starting %s cap (%d stocks)", seg, len(universe[seg]))
        stocks = universe[seg]

        # Step 1: Fetch yfinance data for all stocks in segment
        stock_data = []
        for i, sym in enumerate(stocks, 1):
            try:
                d = _fetch_yf(sym)
                if d:
                    stock_data.append(d)
                time.sleep(0.3)  # be gentle to yfinance
            except Exception as e:
                log.debug("fetch_yf %s: %s", sym, e)
            if i % 10 == 0:
                log.info("LT scan %s: fetched %d/%d stocks (%d ok)", seg, i, len(stocks), len(stock_data))

        # Step 2: Compute sector medians from this segment's data
        sector_medians = _compute_sector_medians(stock_data)

        # Step 3: Enrich with NSE events
        # Note: Screener.in is skipped — Cloudflare blocks server/cloud IPs reliably.
        # Promoter holding falls back to None (scored as neutral 50 pts).
        enriched = []
        for i, d in enumerate(stock_data, 1):
            sym = d["symbol"]
            d["promoter_holding"] = None  # Screener.in not reachable from Render
            try:
                d["events"] = _get_corporate_events(sym)
            except Exception:
                d["events"] = {}
            enriched.append(d)
            if i % 10 == 0:
                log.info("LT scan %s: enriched %d/%d stocks", seg, i, len(stock_data))

        # Step 4: Score + compute targets
        picks = []
        for d in enriched:
            try:
                scored  = score_stock(d, seg, sector_medians)
                targets = compute_targets(d, sector_medians)
                sentiment = 1.0
                if scored["score"] >= 55:
                    # Only fetch news for plausible picks (save API calls)
                    sentiment = _get_stock_news_sentiment(d["symbol"])

                final_score = round(min(100.0, scored["score"] * sentiment), 1)

                picks.append({
                    "symbol":          d["symbol"],
                    "segment":         seg,
                    "score":           final_score,
                    "signal":          "STRONG_BUY" if final_score >= 70 else ("WATCH" if final_score >= 55 else "SKIP"),
                    "cmp":             d.get("cmp"),
                    "pe":              d.get("pe"),
                    "roe":             round(d.get("roe") or 0, 1),
                    "eps_growth":      round((d.get("eps_growth") or 0) * 100, 1),
                    "rev_growth":      round((d.get("rev_growth") or 0) * 100, 1),
                    "debt_equity":     round(d.get("debt_equity") or 0, 2),
                    "promoter_pct":    d.get("promoter_holding"),
                    "sector":          d.get("sector", ""),
                    "above_200dma":    d.get("above_200dma"),
                    "rel_strength_6m": round((d.get("rel_strength_6m") or 0) * 100, 1),
                    "target_low":      targets.get("target_low"),
                    "target_high":     targets.get("target_high"),
                    "upside_low":      targets.get("upside_low"),
                    "upside_high":     targets.get("upside_high"),
                    "analyst_target":  targets.get("analyst_target"),
                    "results_due":     d.get("events", {}).get("results_due"),
                    "dividend_yield":  d.get("dividend_yield"),
                    "dividend_consistent": d.get("events", {}).get("dividend_consistent"),
                    "event_risk":      d.get("events", {}).get("event_risk", False),
                    "factors":         scored.get("factors", {}),
                    "sentiment":       round(sentiment, 2),
                    "scan_date":       datetime.now(IST).date().isoformat(),
                })
            except Exception as e:
                log.warning("score %s: %s", d.get("symbol"), e)

        # Sort by score desc, keep top 10 per segment
        picks.sort(key=lambda x: x["score"], reverse=True)
        top_picks = [p for p in picks if p["signal"] != "SKIP"][:10]

        log.info("LT scan %s: %d stocks scored, %d picks (≥55)", seg, len(picks), len(top_picks))

        # Save to DB via dedicated connection
        if lt_conn:
            for p in top_picks:
                _lt_save_pick(p, lt_conn)

        all_picks.extend(top_picks)
        summary[seg] = {"scanned": len(picks), "picks": len(top_picks)}

    if lt_conn:
        try:
            lt_conn.close()
        except Exception:
            pass

    return {"summary": summary, "picks": all_picks, "run_at": datetime.now(IST).isoformat()}
