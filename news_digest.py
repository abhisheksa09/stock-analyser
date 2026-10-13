"""
news_digest.py — Daily financial-news headline digest (email)

Pulls public RSS feeds from major financial news sites, keeps the last
NEWS_LOOKBACK_HOURS of headlines, de-duplicates them, groups them into
India / Global sections and returns them for email_alerts.format_news_digest().

Plain headlines only (no AI layer yet). RSS rather than scraping: free,
sanctioned by the publishers and won't get the Render IP blocked.

Scheduled from app.py at 08:45 IST on weekdays (inside the keepalive window
that starts 08:30 IST — an earlier slot would fire while Render is asleep).

Optional env vars:
  NEWS_LOOKBACK_HOURS   — how far back to include headlines (default 24)
  NEWS_MAX_PER_SOURCE   — headline cap per feed (default 6)
"""

import os
import re
import html
import logging
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime

log = logging.getLogger("news_digest")

IST = timezone(timedelta(hours=5, minutes=30))

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)

# (section, source label, url)
FEEDS = [
    ("India",  "Economic Times",    "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms"),
    ("India",  "ET Stocks",         "https://economictimes.indiatimes.com/markets/stocks/rssfeeds/2146842.cms"),
    ("India",  "ET Economy",        "https://economictimes.indiatimes.com/news/economy/rssfeeds/1373380680.cms"),
    ("India",  "Mint",              "https://www.livemint.com/rss/markets"),
    ("India",  "Business Standard", "https://www.business-standard.com/rss/markets-106.rss"),
    ("Global", "Financial Times",   "https://www.ft.com/markets?format=rss"),
    ("Global", "Yahoo Finance",     "https://feeds.finance.yahoo.com/rss/2.0/headline?s=^NSEI,^GSPC&region=US&lang=en-US"),
    ("Global", "CNBC Markets",      "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=20910258"),
    ("Global", "CNBC Top News",     "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114"),
]

SECTIONS = ["India", "Global"]

_ATOM = "{http://www.w3.org/2005/Atom}"
# A bare '&' that isn't the start of an entity — Business Standard ships these
_BARE_AMP = re.compile(r"&(?!#\d+;|#x[0-9a-fA-F]+;|[A-Za-z][A-Za-z0-9]*;)")
_TAGS     = re.compile(r"<[^>]+>")


# ── Fetch + parse ─────────────────────────────────────────────────────────────

def _fetch(url: str) -> bytes:
    req = urllib.request.Request(url, headers={
        "User-Agent": _UA,
        "Accept":     "application/rss+xml, application/xml, text/xml, */*",
    })
    with urllib.request.urlopen(req, timeout=15) as r:
        return r.read()


def _sanitize(raw: bytes) -> str:
    """Repair the common feed defects that break a strict XML parser:
    invalid UTF-8 bytes and unescaped '&' (Business Standard)."""
    text = raw.decode("utf-8", errors="replace")
    text = re.sub(r"^\s*<\?xml[^>]*\?>", "", text)   # str input can't carry an encoding decl
    return _BARE_AMP.sub("&amp;", text)


def _parse_date(s: str):
    if not s:
        return None
    s = s.strip()
    try:
        dt = parsedate_to_datetime(s)           # RSS: RFC 822
    except (TypeError, ValueError):
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))   # Atom: ISO 8601
        except ValueError:
            return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _clean(s: str) -> str:
    return " ".join(html.unescape(_TAGS.sub(" ", s or "")).split())


def _parse(raw: bytes) -> list:
    root  = ET.fromstring(_sanitize(raw))
    items = []
    for it in root.iter("item"):
        items.append({
            "title":     _clean(it.findtext("title")),
            "link":      (it.findtext("link") or "").strip(),
            "summary":   _clean(it.findtext("description")),
            "published": _parse_date(it.findtext("pubDate")),
        })
    for it in root.iter(f"{_ATOM}entry"):
        link_el = it.find(f"{_ATOM}link")
        items.append({
            "title":     _clean(it.findtext(f"{_ATOM}title")),
            "link":      (link_el.get("href") if link_el is not None else "") or "",
            "summary":   _clean(it.findtext(f"{_ATOM}summary")),
            "published": _parse_date(it.findtext(f"{_ATOM}updated") or it.findtext(f"{_ATOM}published")),
        })
    return [i for i in items if i["title"] and i["link"]]


def _fetch_feed(feed: tuple) -> tuple:
    section, source, url = feed
    try:
        return feed, _parse(_fetch(url)), ""
    except Exception as e:
        log.warning("News feed failed — %s: %s", source, e)
        return feed, [], f"{type(e).__name__}: {str(e)[:120]}"


# ── Sector tagging ────────────────────────────────────────────────────────────
# Keyword match (case-insensitive, whole words) on title + summary. A title hit
# scores 2, a summary hit 1; a sector needs a score of 2 to be tagged, so one
# stray word in the summary isn't enough. Up to 2 tags per headline.

SECTOR_KEYWORDS = {
    "Banking & Finance": [
        "bank", "banks", "banking", "lender", "lenders", "nbfc", "loan", "loans", "credit",
        "deposit", "deposits", "insurance", "insurer", "fintech", "mutual fund", "mutual funds",
        "hdfc", "icici", "sbi", "kotak", "axis bank", "bajaj finance", "shriram finance",
        "upi", "npci", "payments", "paytm", "mobikwik", "pine labs", "broking", "brokerage",
        "gilts", "amc", "sebi", "mf",
        "jpmorgan", "goldman", "morgan stanley", "citigroup", "wells fargo", "bank of america",
    ],
    "IT & Tech": [
        "tech", "technology", "software", "it services", "ai", "artificial intelligence",
        "semiconductor", "semiconductors", "chip", "chips", "chipmaker", "cloud", "data centre",
        "data center", "infosys", "tcs", "wipro", "hcl", "tech mahindra", "ltimindtree",
        "nvidia", "microsoft", "apple", "google", "alphabet", "meta", "amazon", "openai", "tsmc",
    ],
    "Energy & Oil": [
        "energy", "oil", "crude", "brent", "opec", "gas", "lng", "petrol", "diesel", "fuel", "refinery",
        "refiner", "power", "electricity", "renewable", "solar", "coal", "ongc", "reliance",
        "adani green", "ntpc", "tata power", "exxon", "chevron", "bp", "shell",
    ],
    "Industrials & Cap Goods": [
        "capital goods", "industrial", "industrials", "engineering", "engines", "machinery",
        "cable", "cables", "wire", "polycab", "kei", "siemens", "abb", "bhel", "cummins",
        "transmission", "electricals", "manufacturing", "boeing",
    ],
    "Agri & Chemicals": [
        "agri", "agriculture", "agrochemical", "farm", "farmers", "fertiliser", "fertilizer",
        "crop", "crops", "monsoon", "sugar", "kharif", "rabi", "chemical", "chemicals",
        "upl", "bayer", "pi industries",
    ],
    "Auto": [
        "auto", "autos", "automaker", "carmaker", "car", "cars", "ev", "evs", "electric vehicle",
        "two-wheeler", "tata motors", "maruti", "mahindra", "hero motocorp", "bajaj auto",
        "tesla", "toyota", "ford", "general motors",
    ],
    "Pharma & Healthcare": [
        "pharma", "pharmaceutical", "drug", "drugs", "drugmaker", "healthcare", "hospital",
        "hospitals", "vaccine", "medical", "biotech", "fda", "sun pharma", "cipla",
        "dr reddy", "lupin", "j&j", "johnson & johnson", "pfizer", "eli lilly",
    ],
    "Metals & Mining": [
        "metal", "metals", "steel", "aluminium", "aluminum", "copper", "zinc", "iron ore",
        "mining", "miner", "tata steel", "jsw steel", "hindalco", "vedanta",
    ],
    "Commodities & Gold": [
        "gold", "silver", "commodity", "commodities", "bullion", "precious metals",
    ],
    "FMCG & Consumer": [
        "fmcg", "consumer", "retail", "retailer", "e-commerce", "ecommerce", "food",
        "beverage", "hindustan unilever", "hul", "itc", "nestle", "dabur", "titan",
        "dmart", "walmart", "flipkart",
    ],
    "Telecom & Media": [
        "telecom", "5g", "airtel", "bharti", "jio", "vodafone idea", "media", "streaming",
        "netflix",
    ],
    "Real Estate & Infra": [
        "real estate", "realty", "housing", "property", "infrastructure", "infra",
        "construction", "cement", "railway", "railways", "dlf", "larsen", "l&t",
        "ultratech", "adani ports", "logistics", "gatishakti",
    ],
    "Defence": ["defence", "defense", "hal", "bel", "bharat electronics", "missile"],
    "Crypto": ["crypto", "bitcoin", "ethereum", "cryptocurrency", "stablecoin", "blockchain"],
    "Economy & Policy": [
        "rbi", "fed", "federal reserve", "ecb", "rate cut", "rate hike", "interest rate",
        "interest rates", "inflation", "cpi", "gdp", "growth", "recession", "rupee", "dollar",
        "yield", "yields", "bond", "bonds", "tariff", "tariffs", "trade deal", "budget",
        "fiscal", "imf", "debt", "economy", "economic", "jobs", "unemployment", "fx", "forex",
    ],
}

_SECTOR_RES = {
    sector: re.compile(r"\b(?:" + "|".join(re.escape(k) for k in kws) + r")\b", re.I)
    for sector, kws in SECTOR_KEYWORDS.items()
}


# Fallback tag for headlines about the market as a whole (no specific sector)
BROAD_MARKET = "Broad Market"
_BROAD_RE = re.compile(
    r"\b(?:sensex|nifty|dalal street|stock market|stock markets|wall street|s&p 500|"
    r"nasdaq|dow jones|ipo|listing|lists|smallcap|midcap|bull market|bear market)\b", re.I)


def tag_sectors(title: str, summary: str = "", max_tags: int = 2) -> list:
    """Return up to max_tags sector names for a headline, strongest first.
    Falls back to BROAD_MARKET when only index/market-wide terms match."""
    scores = {}
    for sector, rx in _SECTOR_RES.items():
        score = 2 * len(rx.findall(title)) + len(rx.findall(summary or ""))
        if score >= 2:
            scores[sector] = score
    if not scores and _BROAD_RE.search(f"{title} {summary or ''}"):
        return [BROAD_MARKET]
    return sorted(scores, key=scores.get, reverse=True)[:max_tags]


# ── Digest ────────────────────────────────────────────────────────────────────

def _norm_title(t: str) -> str:
    return re.sub(r"[^a-z0-9 ]", "", t.lower())[:80]


def build_digest() -> dict:
    """Fetch every feed and return
    {"sections": {section: [item, ...]}, "total": int, "failed": [source, ...],
     "sector_counts": {sector: n, ...}}  (most-covered sector first).
    Each item: title, link, summary, published (aware datetime or None), source,
    sectors (list of sector names, may be empty)."""
    lookback = int(os.environ.get("NEWS_LOOKBACK_HOURS", "24"))
    per_src  = int(os.environ.get("NEWS_MAX_PER_SOURCE", "6"))
    cutoff   = datetime.now(timezone.utc) - timedelta(hours=lookback)

    with ThreadPoolExecutor(max_workers=len(FEEDS)) as pool:
        results = list(pool.map(_fetch_feed, FEEDS))

    sections   = {s: [] for s in SECTIONS}
    seen_links = set()
    seen_title = set()
    failed     = []

    for (section, source, _url), items, err in results:
        if err:
            failed.append(source)
            continue
        fresh = [i for i in items if i["published"] is None or i["published"] >= cutoff]
        fresh.sort(key=lambda i: i["published"] or cutoff, reverse=True)
        kept = 0
        for i in fresh:
            key = _norm_title(i["title"])
            if i["link"] in seen_links or key in seen_title:
                continue
            seen_links.add(i["link"])
            seen_title.add(key)
            sections[section].append({
                **i, "source": source,
                "sectors": tag_sectors(i["title"], i["summary"]),
            })
            kept += 1
            if kept >= per_src:
                break

    for items in sections.values():
        items.sort(key=lambda i: i["published"] or cutoff, reverse=True)

    sector_counts = {}
    for items in sections.values():
        for i in items:
            for sec in i["sectors"]:
                sector_counts[sec] = sector_counts.get(sec, 0) + 1
    sector_counts = dict(sorted(sector_counts.items(), key=lambda kv: kv[1], reverse=True))

    total = sum(len(v) for v in sections.values())
    log.info("News digest built — %d headlines, %d feed(s) failed%s",
             total, len(failed), f" ({', '.join(failed)})" if failed else "")
    return {"sections": sections, "total": total, "failed": failed,
            "sector_counts": sector_counts}
