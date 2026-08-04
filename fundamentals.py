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
import threading
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

# ── yfinance log-noise cap ────────────────────────────────────────────────────
class _YFNoiseFilter(logging.Filter):
    """
    yfinance logs one WARNING/ERROR line per failed symbol through its own
    logger, which propagates to our root handler. When Yahoo rate-limits the
    scan that is hundreds of near-identical '401 Unauthorized' lines that bury
    every other scan message.

    Let the first `cap` through (they carry the actual diagnosis), then swallow
    the rest and report a single count via report_and_reset() at end of run.
    Installed on the root handlers, not the yfinance logger, so it also catches
    records from child loggers ('yfinance.data' etc.).
    """
    def __init__(self, cap: int = 12):
        super().__init__()
        self.cap        = cap
        self.seen       = 0
        self.suppressed = 0

    def filter(self, record):
        if not record.name.startswith("yfinance"):
            return True
        self.seen += 1
        if self.seen <= self.cap:
            return True
        self.suppressed += 1
        return False

    def report_and_reset(self):
        if self.suppressed:
            log.warning(
                "yfinance emitted %d error lines this run (%d suppressed after the first %d)",
                self.seen, self.suppressed, self.cap,
            )
        self.seen = self.suppressed = 0


_yf_noise = _YFNoiseFilter()


def _install_yf_noise_filter():
    """
    Attach the filter to every root handler. Idempotent, and re-run at the top of
    each scan because app.py installs its own handler after importing this module.
    """
    for h in logging.root.handlers:
        if _yf_noise not in h.filters:
            h.addFilter(_yf_noise)


_install_yf_noise_filter()

# ── yfinance rate-limit / crumb state ─────────────────────────────────────────
# Yahoo's quoteSummary endpoint (ticker.info) and fundamentals-timeseries
# endpoint (ticker.financials) both require a "crumb" token that yfinance
# fetches once and caches in a process-global singleton. When the crumb endpoint
# is itself rate-limited it returns the body "Too Many Requests\r\n" with HTTP
# 200, and yfinance caches that string AS the crumb. Every subsequent call then
# sends `crumb=Too+Many+Requests%0D%0A` and gets 401 Unauthorized — for the rest
# of the process lifetime. It never self-heals. That is the 401 flood in the log.
_YF_BAD_CRUMB_MARKERS = ("too many requests", "unauthorized", "edge:", "error")

_yf_rl = {
    "delay":          0.3,    # adaptive inter-symbol sleep (seconds)
    "resets":         0,      # crumb resets attempted this run
    "consec_fails":   0,      # consecutive rate-limited info/financials calls
    "info_disabled":  False,  # circuit breaker — stop calling crumb endpoints
}

_YF_MAX_RESETS      = 3       # give up on the crumb after this many resets
_YF_FAILS_TO_RESET  = 5       # consecutive failures before attempting a reset
_YF_MAX_DELAY       = 4.0


def _yf_reset_run_state():
    """Reset per-run yfinance pacing state. Called at the top of run_lt_scan()."""
    _yf_rl.update({"delay": 0.3, "resets": 0, "consec_fails": 0, "info_disabled": False})


def _is_rate_limited(err) -> bool:
    """True if an exception/message looks like Yahoo throttling or a bad crumb."""
    msg = str(err).lower()
    return any(t in msg for t in ("401", "429", "too many requests", "unauthorized"))


def _yf_crumb_is_poisoned(yf) -> bool:
    """True if yfinance's cached crumb is a rate-limit error body, not a token."""
    try:
        crumb = getattr(yf.data.YfData(session=None), "_crumb", None)
    except Exception:
        return False
    if not crumb or not isinstance(crumb, str):
        return False
    low = crumb.lower()
    # A real crumb is a short opaque token with no whitespace.
    return any(m in low for m in _YF_BAD_CRUMB_MARKERS) or any(c.isspace() for c in crumb)


def _yf_reset_crumb(yf, reason: str) -> bool:
    """
    Drop yfinance's cached cookie + crumb so the next call re-negotiates, and
    back off before that happens. Returns False once we've given up.
    """
    if _yf_rl["resets"] >= _YF_MAX_RESETS:
        return False
    _yf_rl["resets"] += 1
    backoff = 15 * _yf_rl["resets"]      # 15s, 30s, 45s
    try:
        yd = yf.data.YfData(session=None)
        yd._crumb  = None
        yd._cookie = None
        # Toggle the strategy so the retry takes the other negotiation path.
        strategy = getattr(yd, "_cookie_strategy", None)
        if strategy in ("basic", "csrf") and hasattr(yd, "_set_cookie_strategy"):
            yd._set_cookie_strategy("csrf" if strategy == "basic" else "basic")
    except Exception as e:
        log.warning("LT scan: could not reset yfinance crumb (%s) — %s", reason, e)
        return False
    log.warning(
        "LT scan: yfinance crumb reset %d/%d (%s) — backing off %ds",
        _yf_rl["resets"], _YF_MAX_RESETS, reason, backoff,
    )
    time.sleep(backoff)
    return True


def _yf_note_failure(yf, symbol: str, what: str, err) -> None:
    """Record a rate-limited crumb-endpoint call; reset or trip the breaker."""
    if _yf_rl["info_disabled"]:
        return
    _yf_rl["consec_fails"] += 1
    _yf_rl["delay"] = min(_yf_rl["delay"] * 1.5, _YF_MAX_DELAY)
    log.debug("yfinance %s %s rate-limited: %s", what, symbol, err)
    if _yf_rl["consec_fails"] < _YF_FAILS_TO_RESET:
        return
    if not _yf_reset_crumb(yf, f"{_yf_rl['consec_fails']} consecutive {what} failures"):
        _yf_rl["info_disabled"] = True
        log.error(
            "LT scan: Yahoo fundamentals endpoints unreachable after %d crumb resets — "
            "continuing with price/technical data only (P/E, ROE, growth will be blank). "
            "Set YF_PROXY to a working HTTP proxy to restore fundamentals.",
            _YF_MAX_RESETS,
        )
        return
    _yf_rl["consec_fails"] = 0


def _yf_note_success() -> None:
    """A crumb-endpoint call worked — decay the backoff back toward baseline."""
    _yf_rl["consec_fails"] = 0
    _yf_rl["delay"] = max(0.3, _yf_rl["delay"] * 0.8)

# ── Index constituent lists (Nifty 100 / Midcap 150 / Smallcap 250) ──────────
# Symbols as used by yfinance (.NS suffix added at fetch time)
LARGE_CAP = [
    "RELIANCE","TCS","HDFCBANK","BHARTIARTL","ICICIBANK","INFY","SBIN","HINDUNILVR",
    "ITC","BAJFINANCE","KOTAKBANK","LT","HCLTECH","MARUTI","ASIANPAINT","AXISBANK",
    "TITAN","SUNPHARMA","ULTRACEMCO","BAJAJFINSV","NESTLEIND","WIPRO","POWERGRID",
    "NTPC","ADANIENT","ADANIPORTS","TECHM","DRREDDY","DIVISLAB",
    "CIPLA","JSWSTEEL","TATASTEEL","COALINDIA","ONGC","BPCL","HEROMOTOCO",
    "EICHERMOT","GRASIM","SHREECEM","APOLLOHOSP","BRITANNIA","TATACONSUM","PIDILITIND",
    "DABUR","HAVELLS","GODREJCP","BOSCHLTD","MUTHOOTFIN","SIEMENS","INDIGO",
    "DLF","VEDL","HINDALCO","NMDC","SAIL","JINDALSTEL","LUPIN","AUROPHARMA",
    "TORNTPHARM","LICHSGFIN","CHOLAFIN","MFSL","SBILIFE","HDFCLIFE","ICICIPRULI",
    "ICICIGI","BAJAJ-AUTO","M&M","TVSMOTOR","ESCORTS","ASHOKLEY",
    "BERGEPAINT","KANSAINER","MARICO","COLPAL","EMAMILTD","VBL","TRENT","NYKAA",
    "DMART","ZOMATO","PAYTM","POLICYBZR","NAUKRI","INDIAMART","IRCTC","ZEEL",
    "SUNTV","PVRINOX","JUBLFOOD","DEVYANI","WESTLIFE","UNITDSPR",
    "GMRAIRPORT","AIAENG","CUMMINSIND","THERMAX","ABB","BHEL","BEL","HAL",
]
# Removed / corrected (each was a guaranteed Yahoo error + 3 wasted requests):
#   INFOSYS      → INFY       (NSE ticker)
#   TVSMOTORS    → dropped    (duplicate of TVSMOTOR, which is the NSE ticker)
#   MCDOWELL-N   → UNITDSPR   (NSE renamed the symbol)
#   UNITEDSPIRITS→ dropped    (same company as above, never a valid ticker)
#   TATAMOTORS   → dropped    (the "possibly delisted, no price data" line in the
#                              log — the entity demerged and the old ticker is
#                              retired. Add the successor ticker(s) once confirmed
#                              against the NSE symbol list.)

MIDCAP = [
    "PERSISTENT","MPHASIS","COFORGE","LTTS","KPITTECH","TATAELXSI","HEXAWARE",
    "OFSS","CYIENT","ZENSAR","MASTEK","RATEGAIN","TANLA",
    "IDFCFIRSTB","FEDERALBNK","KARURVYSYA","CSBBANK","DCBBANK","RBLBANK",
    "BANDHANBNK","UJJIVANSFB","EQUITASBNK","SURYODAY","JKCEMENT","RAMCOCEM",
    "HEIDELBERG","BIRLACORPN","PRSMJOHNSN","ORIENTCEM","STARCEMENT",
    "APLAPOLLO","RATNAMANI","WELSPUNIND","TRIDENT","VARDHACRLC","ALOKTEXT",
    "PAGEIND","RAYMOND","SPENCERS","VMART","SHOPERSTOP","BATA","RELAXO",
    "CAMPUS","METROBRAND","KPRMILL","GOCOLORS","SUNDRMFAST","MOTHERSON",
    "BALKRISIND","APOLLOTYRE","CEATLTD","MRF","JKTYRE","GOODYEAR",
    "CONCOR","BLUEDART","CIEINDIA","ENDURANCE","SUPRAJIT","FIEM",
    "LALPATHLAB","METROPOLIS","KRSNAA","VIJAYA","SUVENPHAR","AJANTPHARM",
    "ALKEM","GRANULES","LAURUSLABS","SOLARA","NATCOPHARM","GLAND",
    "SUDARSCHEM","AAVAS","HOMEFIRST","APTUS","CREDITACC","SPANDANA",
    "MUTHOOTMF","MANAPPURAM","IIFL","FIVE-STAR","UGROCAP","PAISALO",
    "CAMS","CDSL","BSE","MCX","ISEC","ANGELONE",
    "IRFC","RECLTD","PFC","HUDCO","RVNL",
    "TTKPRESTIG","HAWKINCOOK","VSTIND","RADICO","GLOBUSSPR","KSCL",
]
# Removed / corrected:
#   NIITTECH  → dropped   (renamed COFORGE in 2020; COFORGE already in this list)
#   MRFLTD    → MRF       (NSE ticker)
#   MAHINDCIE → CIEINDIA  (NSE renamed the symbol)
#   PFCLTD    → PFC       (NSE ticker)
#   NABARD    → dropped   (a development bank — no NSE-listed equity)

SMALLCAP = [
    "ROUTE","RPGLIFE","SEQUENT","LXCHEM","VALIANTORG","STARHEALTH","ACCELYA",
    "INTELLECT","NEWGEN","KFINTECH","DATAMATICS","BIRLASOFT","INFOBEAN","GREENPANEL",
    "CENTUM","RPTECH","QUICKHEAL","NUCLEUS","SAKSOFT","MSTCLTD","RAILTEL",
    "IRCON","TITAGARH","TEXRAIL","NDTVMEDIA","HATHWAY","GTLINFRA","TATACOMM",
    "STLTECH","VINDHYATEL","TEJASNET","HFCL","ITI","TANGT","SPICEJET",
    "GLOBUSMED","CONTROLPRINT","PONDY","ANDHRAPET","LGBBROSEXP","SAFARI",
    "VIPIND","SKFINDIA","GRINDWELL","SCHAEFFLER","ELGIEQUIP","KIRLOSENG",
    "INGERSRAND","KENNAMET","JYOTHYLAB","BAJAJCON","ZYDUSWELL",
    "HONASA","VLCC","ARCHIES","SAPPHIRE","BIKAJI",
    "POKARNA","ASAHIINDIA","POLYPLEX","UFLEX","GPPL","SHREEPIPE",
    "PRINCEPIPE","ASTRAL","SUPREMEIND","NILKAMAL","PLASSON","SKIPPER",
    "KERNEX","TEXINFRA","HGINFRA","DBREALTY","ANANTRAJ","KOLTEPATIL",
    "SUNTECK","GODREJPROP","MAHLIFE","ARVIND","KIRIINDS","PNBHOUSING",
    "CANFINHOME","REPCO","AROGRANITE","ORIENTBELL","SOMANYCER",
    "REGENCYCER","ASIANSTAR","THEJEWEL","PCJEWELLER","SENCO",
]
# Removed:
#   THERMAX, NYKAA, TITAN → duplicates of LARGE_CAP entries (they were scanned
#                           twice per run: double the Yahoo requests, and the
#                           same stock could surface in two segments' picks)
#   GRUH                  → merged into Bandhan Bank in 2019, ticker retired

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
_nse_session_cookie  = None
_nse_session_ts      = None
_nse_blocked         = False   # set True on first 403 — skip all further NSE calls this run
_nse_lock            = threading.Lock()
NSE_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://www.nseindia.com/",
}

def _get_nse_cookies() -> dict:
    """Fetch a fresh NSE India session cookie (valid ~5 min)."""
    global _nse_session_cookie, _nse_session_ts, _nse_blocked
    # Fast path: check outside lock to avoid contention on every call
    if _nse_blocked:
        return {}
    with _nse_lock:
        # Re-check inside lock — another thread may have just set _nse_blocked
        if _nse_blocked:
            return {}
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
        except urllib.error.HTTPError as e:
            if e.code == 403:
                _nse_blocked = True
                log.warning("NSE India blocked this server IP (403) — skipping corporate events for this scan")
            else:
                log.debug("NSE session fetch failed: %s", e)
            return {}
        except Exception as e:
            log.debug("NSE session fetch failed: %s", e)
            return {}

def _nse_get(url: str) -> dict | None:
    """GET an NSE India API endpoint with session cookie. Returns parsed JSON or None."""
    cookies = _get_nse_cookies()
    if not cookies:
        return None
    cookie_str = "; ".join(f"{k}={v}" for k, v in cookies.items())
    try:
        req = urllib.request.Request(url, headers={**NSE_HEADERS, "Cookie": cookie_str})
        with urllib.request.urlopen(req, timeout=4) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        if e.code == 403:
            global _nse_blocked
            _nse_blocked = True
            log.warning("NSE India API blocked (403) — skipping remaining corporate events")
        else:
            log.debug("NSE API %s failed: %s", url, e)
        return None
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
def _get_yf_proxy() -> str | None:
    """Return the YF_PROXY env var if set, else None."""
    return os.environ.get("YF_PROXY") or None


def _fetch_nifty_history(yf):
    """Fetch Nifty 50 1-year history once per scan. Returns DataFrame or None."""
    proxy = _get_yf_proxy()
    try:
        nh = yf.Ticker("^NSEI").history(
            period="1y", interval="1d", auto_adjust=True,
            **({"proxy": proxy} if proxy else {}),
        )
        if nh is not None and not nh.empty:
            log.info("LT scan: Nifty 50 history fetched (%d days)", len(nh))
            return nh
    except Exception as e:
        log.warning("LT scan: Nifty 50 history failed: %s", e)
    return None


def _fetch_yf(symbol: str, nifty_hist=None) -> dict | None:
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
        return _fetch_yf_inner(symbol, yf, nifty_hist)


def _fetch_yf_inner(symbol: str, yf, nifty_hist=None) -> dict | None:
    """Inner implementation — called inside warnings.catch_warnings() block."""
    proxy = _get_yf_proxy()
    ticker = yf.Ticker(f"{symbol}.NS", **({"proxy": proxy} if proxy else {}))

    # Price history FIRST. The chart endpoint needs no crumb, so it keeps working
    # when quoteSummary is 401-ing — this guarantees we still get CMP, 200 DMA
    # and relative strength for every symbol even in a fully throttled run.
    hist = None
    try:
        hist = ticker.history(
            period="1y", interval="1d", auto_adjust=True,
            **({"proxy": proxy} if proxy else {}),
        )
    except Exception as e:
        log.debug("yfinance history %s: %s", symbol, e)

    # Fundamentals (quoteSummary — crumb-gated). Skipped entirely once the
    # circuit breaker has tripped, which drops the per-symbol cost from 3 Yahoo
    # requests to 1 and stops the 401 flood.
    info = {}
    if not _yf_rl["info_disabled"]:
        if _yf_crumb_is_poisoned(yf):
            _yf_reset_crumb(yf, "cached crumb is a rate-limit error body")
        err = None
        try:
            info = ticker.info or {}
        except Exception as e:
            err = e
            if not _is_rate_limited(e):
                log.debug("yfinance info %s: %s", symbol, e)
        # ticker.info swallows HTTP errors internally in some yfinance versions
        # and returns a near-empty dict instead of raising, so treat "no usable
        # keys at all" as a failure signal too — otherwise the breaker never trips.
        if info.get("regularMarketPrice") or info.get("currentPrice") or info.get("trailingPE"):
            _yf_note_success()
        else:
            _yf_note_failure(yf, symbol, "info", err or "empty quoteSummary response")

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

    # 6-month relative strength vs Nifty 50 — uses pre-fetched history (not per-stock)
    rel_strength_6m = None
    try:
        nh = nifty_hist
        if hist is not None and not hist.empty and nh is not None and not nh.empty:
            stock_ret = (hist["Close"].iloc[-1] - hist["Close"].iloc[-126]) / hist["Close"].iloc[-126] if len(hist) >= 126 else None
            nifty_ret = (nh["Close"].iloc[-1] - nh["Close"].iloc[-126]) / nh["Close"].iloc[-126] if len(nh) >= 126 else None
            if stock_ret is not None and nifty_ret is not None:
                rel_strength_6m = float(stock_ret - nifty_ret)
    except Exception as e:
        log.debug("rel_strength %s: %s", symbol, e)

    # Financials for EPS/revenue growth
    # yfinance renames rows occasionally — try multiple known labels in priority order
    _NI_KEYS  = ["Net Income", "Net Income Common Stockholders",
                 "Net Income From Continuing Operations", "NetIncome"]
    _REV_KEYS = ["Total Revenue", "Revenue", "TotalRevenue", "Operating Revenue"]
    eps_growth = None
    rev_growth = None
    # Also crumb-gated (fundamentals-timeseries endpoint) — skip when the breaker
    # is open rather than spend a request that can only 401.
    try:
        fin = None
        if not _yf_rl["info_disabled"]:
            fin = ticker.get_financials(proxy=proxy) if proxy else ticker.financials  # annual, most recent first
        if fin is not None and not fin.empty and fin.shape[1] >= 2:
            for key in _NI_KEYS:
                if key in fin.index:
                    ni = fin.loc[key]
                    if ni.iloc[0] and ni.iloc[1] and ni.iloc[1] != 0:
                        eps_growth = float((ni.iloc[0] - ni.iloc[1]) / abs(ni.iloc[1]))
                    break
            for key in _REV_KEYS:
                if key in fin.index:
                    rev = fin.loc[key]
                    if rev.iloc[0] and rev.iloc[1] and rev.iloc[1] != 0:
                        rev_growth = float((rev.iloc[0] - rev.iloc[1]) / abs(rev.iloc[1]))
                    break
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
        # Cap growth at 50% — one-off recoveries (e.g. 500% from near-zero base) produce nonsensical targets
        peg_growth  = min(eps_growth, 0.50)
        peg_pe      = min(max(peg_growth * 100, 10), 50)  # clamp 10–50
        fwd_eps     = eps * (1 + peg_growth)
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

    # Hard cap: >150% upside in 6-12 months is not actionable for established stocks
    if upside_high and upside_high > 150:
        upside_high = None
        target_high = None
    if upside_low and upside_low > 150:
        upside_low = None
        target_low = None

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
    global _nse_blocked
    _nse_blocked = False  # reset per scan run — give NSE a fresh attempt each time
    _yf_reset_run_state()          # fresh crumb/backoff budget each run
    _install_yf_noise_filter()     # app.py may have replaced the root handler since import

    segments = [segment] if segment else ["large", "mid", "small"]
    universe = {"large": LARGE_CAP, "mid": MIDCAP, "small": SMALLCAP}
    all_picks = []
    summary   = {}

    proxy = _get_yf_proxy()
    log.info("LT scan: proxy=%s", proxy if proxy else "none (direct connection)")

    # Open a dedicated DB connection for this scan run — never shares with intraday thread
    lt_conn = _open_lt_db_conn()
    if not lt_conn:
        log.warning("LT scan: no DB connection — picks will not be saved")

    for seg in segments:
        log.info("LT scan: starting %s cap (%d stocks)", seg, len(universe[seg]))
        stocks = universe[seg]

        # Step 1a: Fetch Nifty 50 history once for the whole segment
        try:
            import yfinance as _yf
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                nifty_hist = _fetch_nifty_history(_yf)
        except Exception:
            nifty_hist = None

        # Step 1b: Fetch yfinance data for all stocks in segment.
        # Pace adaptively: _yf_rl["delay"] grows on rate-limit signals and decays
        # on success, so a throttled run slows down instead of hammering Yahoo.
        stock_data = []
        for i, sym in enumerate(stocks, 1):
            try:
                d = _fetch_yf(sym, nifty_hist=nifty_hist)
                if d:
                    stock_data.append(d)
                time.sleep(_yf_rl["delay"])  # be gentle to yfinance
            except Exception as e:
                log.debug("fetch_yf %s: %s", sym, e)
            if i % 10 == 0:
                # "priced" = has a CMP; "fund" = has fundamentals (P/E or ROE).
                # A dict is returned even when every field is null, so a bare
                # count of dicts says nothing about whether the fetch worked.
                priced = sum(1 for d in stock_data if d.get("cmp"))
                fund   = sum(1 for d in stock_data if d.get("pe") or d.get("roe"))
                log.info(
                    "LT scan %s: fetched %d/%d stocks (%d priced, %d with fundamentals, delay=%.1fs)",
                    seg, i, len(stocks), priced, fund, _yf_rl["delay"],
                )

        # Step 1c: Sanity-check — abort segment if >80% of stocks have no CMP.
        # This means yfinance is being blocked (Render IP rate-limited by Yahoo Finance).
        # Saving all-null picks would produce meaningless 50-score WATCH rows.
        if stock_data:
            missing_cmp = sum(1 for d in stock_data if not d.get("cmp"))
            pct_missing = missing_cmp / len(stock_data)
            if pct_missing > 0.80:
                log.error(
                    "LT scan %s: %.0f%% of stocks have no CMP — yfinance fetch failed "
                    "(likely Render IP blocked by Yahoo Finance). "
                    "Set YF_PROXY env var to a working HTTP proxy. Skipping segment.",
                    seg, pct_missing * 100,
                )
                summary[seg] = {"scanned": 0, "picks": 0, "error": "yfinance_blocked"}
                continue
            elif pct_missing > 0.30:
                log.warning(
                    "LT scan %s: %.0f%% of stocks have no CMP — partial yfinance failure, "
                    "proxy may be rate-limited.", seg, pct_missing * 100,
                )

            # Step 1d: Same guard for fundamentals. Prices can come from the
            # crumb-free chart endpoint while quoteSummary is 401-ing, so a
            # segment can be fully priced and still have no P/E, ROE or growth
            # for anything. Six of eight scoring factors would then be the
            # neutral 50 default and every stock lands at ~50 = a WATCH row that
            # means nothing. Skip rather than persist that.
            missing_fund = sum(1 for d in stock_data if not (d.get("pe") or d.get("roe")))
            pct_no_fund  = missing_fund / len(stock_data)
            if pct_no_fund > 0.80:
                log.error(
                    "LT scan %s: %.0f%% of stocks have no P/E or ROE — Yahoo quoteSummary "
                    "is rate-limiting this IP (crumb resets tried: %d). Prices were fetched "
                    "but scoring would be meaningless. Skipping segment. "
                    "Set YF_PROXY env var to a working HTTP proxy.",
                    seg, pct_no_fund * 100, _yf_rl["resets"],
                )
                summary[seg] = {"scanned": 0, "picks": 0, "error": "yf_fundamentals_blocked"}
                continue
            elif pct_no_fund > 0.30:
                log.warning(
                    "LT scan %s: %.0f%% of stocks have no P/E or ROE — partial quoteSummary "
                    "failure, scores for those stocks lean on the neutral default.",
                    seg, pct_no_fund * 100,
                )

            # Step 1e: Name the symbols that came back with nothing at all. When
            # only a handful do so while the rest of the segment is fine, they are
            # dead tickers (renamed, merged, delisted) rather than throttling —
            # each one costs 3 wasted Yahoo requests and one "possibly delisted"
            # error line per run. Listing them makes the universe self-auditing
            # instead of needing a manual pass over 300 symbols.
            dead = [d["symbol"] for d in stock_data
                    if not d.get("cmp") and not d.get("pe") and not d.get("roe")]
            if dead and pct_missing <= 0.30:
                log.warning(
                    "LT scan %s: %d symbols returned no data at all — likely renamed or "
                    "delisted, consider pruning: %s",
                    seg, len(dead), ", ".join(dead),
                )

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
                if scored["score"] >= 50:
                    # Only fetch news for plausible picks (save API calls)
                    sentiment = _get_stock_news_sentiment(d["symbol"])

                final_score = round(min(100.0, scored["score"] * sentiment), 1)

                picks.append({
                    "symbol":          d["symbol"],
                    "segment":         seg,
                    "score":           final_score,
                    "signal":          "STRONG_BUY" if final_score >= 70 else ("WATCH" if final_score >= 50 else "SKIP"),
                    "cmp":             d.get("cmp"),
                    "pe":              d.get("pe"),
                    "roe":             round(d.get("roe") or 0, 1),
                    "eps_growth":      round((d.get("eps_growth") or 0) * 100, 1),
                    "rev_growth":      round((d.get("rev_growth") or 0) * 100, 1),
                    "debt_equity":     round(d.get("debt_equity") or 0, 2),
                    "promoter_pct":    d.get("promoter_holding"),
                    "sector":          d.get("sector", ""),
                    "above_200dma":    d.get("above_200dma"),
                    "rel_strength_6m": None if (_rs := d.get("rel_strength_6m")) != _rs else round((_rs or 0) * 100, 1),
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

        log.info("LT scan %s: %d stocks scored, %d picks (≥50)", seg, len(picks), len(top_picks))
        if picks:
            top5 = picks[:5]
            log.info(
                "LT scan %s top-5 scores: %s",
                seg,
                ", ".join(
                    f"{p['symbol']}={p['score']} "
                    f"(eps_g={p['eps_growth']}% rev_g={p['rev_growth']}% "
                    f"roe={p['roe']} 200dma={'Y' if p['above_200dma'] else 'N' if p['above_200dma'] is False else '?'} "
                    f"rs={p['rel_strength_6m']}%)"
                    for p in top5
                ),
            )

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

    _yf_noise.report_and_reset()
    return {"summary": summary, "picks": all_picks, "run_at": datetime.now(IST).isoformat()}
