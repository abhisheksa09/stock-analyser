"""
us_fundamentals.py — inputs for the US Top 50 long-term scan (segment "us").

Screener.in only covers Indian stocks, and Yahoo's quoteSummary rate-limits both Render
and GitHub runners, so the US scan uses:
  - fundamentals: SEC EDGAR XBRL company facts (free, no key; needs a User-Agent with a
    contact email: set SEC_USER_AGENT, e.g. "StockAnalyser you@example.com")
  - prices:       Alpaca daily bars when ALPACA_API_KEY is set, else Yahoo's chart
                  endpoint (crumb-free)
  - benchmark:    SPY, for relative strength and the track record

fetch_us(symbol) returns the same dict shape fundamentals._fetch_stock does, so the
scoring and target code is shared with the Indian scan. There's no promoter holding in
the US, and the "us" weights in fundamentals.WEIGHTS leave it out.
"""

import gzip
import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone

log = logging.getLogger("us_fundamentals")

# ── Universe: top 50 US stocks by market cap (Nov 2026), with GICS sector ────
# Sector is fixed here rather than read from SEC (SIC codes don't map cleanly to GICS).
# P/E is compared within a sector, which needs at least 3 stocks; sectors with fewer
# (Energy, Industrials, Materials) score the neutral 50 on that factor.
US_TOP50 = {
    "AAPL": "Technology",  "MSFT": "Technology",  "NVDA": "Technology",  "AVGO": "Technology",
    "ORCL": "Technology",  "CRM":  "Technology",  "AMD":  "Technology",  "CSCO": "Technology",
    "ACN":  "Technology",  "ADBE": "Technology",  "IBM":  "Technology",  "NOW":  "Technology",
    "INTU": "Technology",  "QCOM": "Technology",  "TXN":  "Technology",
    "GOOGL": "Communication", "META": "Communication", "NFLX": "Communication",
    "DIS":  "Communication",  "VZ":   "Communication",
    "AMZN": "Consumer Discretionary", "TSLA": "Consumer Discretionary",
    "HD":   "Consumer Discretionary", "MCD":  "Consumer Discretionary",
    "WMT":  "Consumer Staples", "COST": "Consumer Staples", "PG":  "Consumer Staples",
    "KO":   "Consumer Staples", "PEP":  "Consumer Staples", "PM":  "Consumer Staples",
    "BRK-B": "Financials", "JPM": "Financials", "V":   "Financials", "MA": "Financials",
    "BAC":  "Financials",  "WFC": "Financials", "GS":  "Financials",
    "LLY":  "Health Care", "UNH": "Health Care", "JNJ": "Health Care", "ABBV": "Health Care",
    "MRK":  "Health Care", "TMO": "Health Care", "ABT": "Health Care", "AMGN": "Health Care",
    "XOM":  "Energy",      "CVX": "Energy",
    "GE":   "Industrials", "CAT": "Industrials",
    "LIN":  "Materials",
}
US_SYMBOLS = list(US_TOP50)
BENCHMARK  = "SPY"

SEC_USER_AGENT = os.environ.get("SEC_USER_AGENT", "").strip() or "StockAnalyser lt-scan@example.com"
SEC_DELAY_S    = 0.15          # SEC allows 10 requests/s
_PRICE_DAYS    = 800           # 2+ years: 200 DMA, 6m return, and picks up to 12 months old


# ── HTTP ──────────────────────────────────────────────────────────────────────
def _get(url: str, headers: dict, timeout: int = 30) -> bytes | None:
    """GET with one backoff on 429/5xx. None on failure."""
    for attempt in range(2):
        try:
            req = urllib.request.Request(url, headers={"Accept-Encoding": "gzip", **headers})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                body = r.read()
                return gzip.decompress(body) if r.headers.get("Content-Encoding") == "gzip" else body
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503) and attempt == 0:
                time.sleep(10)
                continue
            log.debug("GET %s: HTTP %s", url, e.code)
            return None
        except Exception as e:
            log.debug("GET %s: %s", url, e)
            return None
    return None


def _sec_json(url: str):
    body = _get(url, {"User-Agent": SEC_USER_AGENT})
    time.sleep(SEC_DELAY_S)
    try:
        return json.loads(body) if body else None
    except ValueError:
        return None


# ── SEC company facts ─────────────────────────────────────────────────────────
_cik_map: dict = {}


def _cik(symbol: str) -> str | None:
    """Ticker → 10-digit CIK, from SEC's ticker file (fetched once per process)."""
    if not _cik_map:
        data = _sec_json("https://www.sec.gov/files/company_tickers.json") or {}
        for row in data.values():
            _cik_map[str(row["ticker"]).upper()] = f"{int(row['cik_str']):010d}"
        if not _cik_map:
            log.error("SEC ticker map unavailable — set SEC_USER_AGENT to 'Name email@domain' "
                      "(SEC refuses requests without a contact email)")
    return _cik_map.get(symbol.upper())


def _days(a: str, b: str) -> int:
    return (date.fromisoformat(b) - date.fromisoformat(a)).days


def _dedupe(rows: list) -> dict:
    """{(start, end): val}, keeping the latest-filed value for each period (later filings
    restate earlier comparatives)."""
    out = {}
    for r in sorted(rows, key=lambda r: r.get("filed", "")):
        out[(r.get("start"), r["end"])] = r["val"]
    return out


def _concept_rows(facts: dict, concepts: list, unit: str) -> list:
    """Rows of the first concept, plus earlier-ending rows of later concepts — companies
    switch concepts over time (AAPL: Revenues until 2018, then RevenueFromContract...)."""
    merged, ends = [], set()
    for c in concepts:
        rows = facts.get(c, {}).get("units", {}).get(unit, [])
        new = [r for r in rows if r["end"] not in ends]
        merged += new
        ends |= {r["end"] for r in new}
    return merged


def _annual(rows: list) -> list:
    """[(fy_end, val)] for full fiscal years, oldest first."""
    per = _dedupe(r for r in rows if r.get("start") and 350 <= _days(r["start"], r["end"]) <= 380)
    by_end = {}
    for (start, end), v in per.items():
        by_end[end] = v
    return sorted(by_end.items())


def _ttm(rows: list) -> tuple[str | None, float | None]:
    """(as_of, trailing-twelve-month value): latest fiscal year, rolled forward by any
    later year-to-date figure: FY + YTD(now) - YTD(same period last year)."""
    ann = _annual(rows)
    if not ann:
        return None, None
    fy_end, fy_val = ann[-1]
    per = _dedupe(r for r in rows if r.get("start"))
    ytd = [(s, e, v) for (s, e), v in per.items()
           if e > fy_end and 80 <= _days(s, e) <= 300 and _days(fy_end, s) <= 7]
    if not ytd:
        return fy_end, fy_val
    s, e, v = max(ytd, key=lambda t: t[1])
    span = _days(s, e)
    prior = [pv for (ps, pe), pv in per.items()
             if ps and abs(_days(ps, s) - 365) <= 10 and abs(_days(ps, pe) - span) <= 10]
    if not prior:
        return fy_end, fy_val
    return e, fy_val + v - prior[0]


def _instant(facts: dict, concept: str) -> dict:
    """{end: val} for a balance-sheet concept."""
    rows = facts.get(concept, {}).get("units", {}).get("USD", [])
    return {e: v for (_, e), v in _dedupe(r for r in rows if not r.get("start")).items()}


def _cagr(series: list, years: int = 3) -> float | None:
    """Compound annual growth over `years` from an annual series; 1-year growth if the
    series is shorter. None when the base isn't positive (a loss year can't compound)."""
    if len(series) < 2:
        return None
    n = min(years, len(series) - 1)
    a, b = series[-1 - n][1], series[-1][1]
    if not a or a <= 0 or b <= 0:
        return None
    return (b / a) ** (1 / n) - 1


_NI      = ["NetIncomeLoss", "NetIncomeLossAvailableToCommonStockholdersBasic", "ProfitLoss"]
_REV     = ["RevenueFromContractWithCustomerExcludingAssessedTax", "Revenues",
            "RevenueFromContractWithCustomerIncludingAssessedTax", "SalesRevenueNet"]
_EPS     = ["EarningsPerShareDiluted", "EarningsPerShareBasic"]
_DPS     = ["CommonStockDividendsPerShareDeclared", "CommonStockDividendsPerShareCashPaid"]
_EQUITY  = ["StockholdersEquity", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"]


# Registrants that reorganised under a new holding company: SEC's ticker file points at
# the new CIK, whose facts start at the reorganisation, so the history comes from the old
# one. Rows are merged (latest filing wins per period).
_PREDECESSOR_CIK = {
    "XOM": ("0000034088",),   # Exxon Mobil Corp → new holding-company registrant, 2026
}


def sec_fundamentals(symbol: str) -> dict | None:
    """Scoring fundamentals for one US stock from SEC XBRL. None if SEC has nothing."""
    cik = _cik(symbol)
    if not cik:
        log.debug("SEC: no CIK for %s", symbol)
        return None
    facts = {}
    for c in (cik, *_PREDECESSOR_CIK.get(symbol, ())):
        data = _sec_json(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{c}.json")
        for concept, body in (data or {}).get("facts", {}).get("us-gaap", {}).items():
            units = facts.setdefault(concept, {"units": {}})["units"]
            for unit, rows in body.get("units", {}).items():
                units.setdefault(unit, []).extend(rows)
    if not facts:
        return None

    stale = (date.today() - timedelta(days=550)).isoformat()   # ignore discontinued concepts
    ni_rows, rev_rows = _concept_rows(facts, _NI, "USD"), _concept_rows(facts, _REV, "USD")
    eps_rows = _concept_rows(facts, _EPS, "USD/shares")
    ni_ann, rev_ann = _annual(ni_rows), _annual(rev_rows)
    if rev_ann and rev_ann[-1][0] < stale:
        rev_ann = []
    _, ni_ttm  = _ttm(ni_rows)
    _, eps_ttm = _ttm(eps_rows)

    # Balance sheet at the latest reported date
    equity = {}
    for c in _EQUITY:
        for e, v in _instant(facts, c).items():
            equity.setdefault(e, v)
    eq_end = max(equity) if equity else None
    eq = equity.get(eq_end) if eq_end else None

    debt = None
    if eq_end:
        total = _instant(facts, "LongTermDebt").get(eq_end)
        nonc  = _instant(facts, "LongTermDebtNoncurrent").get(eq_end)
        curr  = (_instant(facts, "LongTermDebtCurrent").get(eq_end)
                 or _instant(facts, "DebtCurrent").get(eq_end) or 0)
        debt = total if total is not None else (nonc + curr if nonc is not None else None)

    # Negative equity (buyback-heavy MCD, HD, ABBV...) makes ROE and D/E meaningless:
    # leave them None so they score the neutral 50 rather than a nonsense extreme.
    roe = round(ni_ttm / eq * 100, 1) if (ni_ttm is not None and eq and eq > 0) else None
    de  = round(debt / eq, 2) if (debt is not None and eq and eq > 0) else None

    dps = _annual(_concept_rows(facts, _DPS, "USD/shares"))
    recent_dps = [v for e, v in dps if e >= (date.today() - timedelta(days=4 * 365)).isoformat()]

    return {
        "eps":         round(eps_ttm, 2) if eps_ttm else None,
        "roe":         roe,
        "debt_equity": de,
        "eps_growth":  _cagr(ni_ann) if ni_ann and ni_ann[-1][0] >= stale else None,
        "rev_growth":  _cagr(rev_ann),
        "dps":         recent_dps[-1] if recent_dps else None,
        "dividend_consistent": len(recent_dps) >= 3 and all(v > 0 for v in recent_dps[-3:]),
    }


# ── Prices ────────────────────────────────────────────────────────────────────
def _alpaca_closes(symbol: str) -> tuple[list, list]:
    key = os.environ.get("ALPACA_API_KEY", "")
    if not key:
        return [], []
    start = (date.today() - timedelta(days=_PRICE_DAYS)).isoformat()
    # Free plans may read SIP (consolidated) data older than 15 minutes
    end = (datetime.now(timezone.utc) - timedelta(minutes=20)).strftime("%Y-%m-%dT%H:%M:%SZ")
    headers = {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": os.environ.get("ALPACA_API_SECRET", "")}
    for feed in ("sip", "iex"):
        q = urllib.parse.urlencode({"timeframe": "1Day", "start": start, "end": end,
                                    "limit": 10000, "adjustment": "all", "feed": feed})
        body = _get(f"https://data.alpaca.markets/v2/stocks/{symbol.replace('-', '.')}/bars?{q}", headers)
        try:
            bars = json.loads(body).get("bars") or [] if body else []
        except ValueError:
            bars = []
        if bars:
            return [b["t"][:10] for b in bars], [float(b["c"]) for b in bars]
    return [], []


def _yahoo_closes(symbol: str) -> tuple[list, list]:
    body = _get(f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=2y&interval=1d",
                {"User-Agent": "Mozilla/5.0"})
    try:
        res = json.loads(body)["chart"]["result"][0]
        ts  = res["timestamp"]
        adj = (res["indicators"].get("adjclose") or [{}])[0].get("adjclose") \
              or res["indicators"]["quote"][0]["close"]
    except Exception:
        return [], []
    pts = [(datetime.fromtimestamp(t, timezone.utc).date().isoformat(), float(c))
           for t, c in zip(ts, adj) if c]
    return [d for d, _ in pts], [c for _, c in pts]


_series: dict = {}


def price_series(symbol: str) -> tuple[list, list]:
    """(dates 'YYYY-MM-DD', closes) oldest first, cached for the run."""
    if symbol not in _series:
        dates, closes = _alpaca_closes(symbol)
        src = "alpaca"
        if len(closes) < 200:
            dates, closes = _yahoo_closes(symbol)
            src = "yahoo"
            time.sleep(0.5)
        _series[symbol] = (dates, closes)
        if symbol == BENCHMARK:
            log.info("US LT: %s history from %s (%d days)", BENCHMARK, src, len(closes))
    return _series[symbol]


def reset_run_state():
    _series.clear()


def _ret_6m(closes: list) -> float | None:
    return closes[-1] / closes[-127] - 1 if len(closes) >= 127 and closes[-127] else None


# ── One stock ─────────────────────────────────────────────────────────────────
def fetch_us(symbol: str) -> dict:
    """Scoring inputs for one US stock, same keys as fundamentals._fetch_stock."""
    d = {"symbol": symbol, "sector": US_TOP50.get(symbol, ""), "above_200dma": None,
         "rel_strength_6m": None, "analyst_target": None, "promoter_holding": None,
         "events": {}}
    dates, closes = price_series(symbol)
    if closes:
        cmp = round(closes[-1], 2)
        d["cmp"] = cmp
        year = closes[-252:]
        d["52w_high"], d["52w_low"] = max(year), min(year)
        if len(closes) >= 200:
            d["above_200dma"] = cmp > sum(closes[-200:]) / 200
        r, b = _ret_6m(closes), _ret_6m(price_series(BENCHMARK)[1])
        if r is not None and b is not None:
            d["rel_strength_6m"] = r - b

    f = sec_fundamentals(symbol)
    if f:
        cmp = d.get("cmp")
        d.update({k: f[k] for k in ("eps", "roe", "debt_equity", "eps_growth", "rev_growth")})
        if cmp and f["eps"] and f["eps"] > 0:
            d["pe"] = round(cmp / f["eps"], 1)
        d["dividend_yield"] = round(f["dps"] / cmp * 100, 2) if (cmp and f["dps"]) else 0.0
        # SEC has no forward results calendar, so no results_due / event_risk for the US
        d["events"] = {"dividend_consistent": f["dividend_consistent"], "results_due": None,
                       "event_risk": False}
    return d
