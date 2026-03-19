"""
macro.py — Macro intelligence layers for NSE Scanner
Fetches economic calendar, macro proxies, FII/DII flows, and news sentiment.
All functions are safe to call — they return neutral/empty data on failure
so a broken data source never blocks the scan.

Environment variables:
  NEWS_API_KEY   — from newsapi.org (free tier: 100 req/day)
  ANTHROPIC_KEY  — reuse existing key for news classification
"""

import os
import json
import logging
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timezone, timedelta

log = logging.getLogger("macro")

IST = timezone(timedelta(hours=5, minutes=30))

# ─── Helpers ──────────────────────────────────────────────────────────────────

def _get(url, headers=None, timeout=10):
    """Simple GET → parsed JSON. Returns None on any error."""
    try:
        req = urllib.request.Request(
            url,
            headers=headers or {"User-Agent": "NSEScanner/1.0 (macro data)"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception as e:
        log.warning("GET %s failed: %s", url[:80], e)
        return None

def _ist_now():
    return datetime.now(IST)

# ══════════════════════════════════════════════════════════════════════════════
# LAYER 1 — Economic Calendar
# ══════════════════════════════════════════════════════════════════════════════

# Hard-coded high-impact recurring events (dates updated manually or via API)
# Format: (month, day, description, affected_sectors)
HARDCODED_EVENTS = [
    # RBI MPC dates 2025 (approximate — update each year)
    # These are typically 1st Wednesday of every other month
    {"type": "RBI_MPC",    "desc": "RBI Monetary Policy Committee Decision",
     "impact": "high",     "sectors": ["Banking", "NBFC", "Infra"]},
    {"type": "US_FOMC",    "desc": "US Federal Reserve FOMC Decision",
     "impact": "high",     "sectors": ["all"]},
    {"type": "INDIA_GDP",  "desc": "India GDP Data Release",
     "impact": "medium",   "sectors": ["all"]},
    {"type": "INDIA_CPI",  "desc": "India CPI Inflation Data",
     "impact": "medium",   "sectors": ["all"]},
    {"type": "INDIA_BUDGET","desc": "Union Budget",
     "impact": "critical", "sectors": ["all"]},
    {"type": "NIFTY_EXPIRY","desc": "Nifty Monthly Expiry",
     "impact": "medium",   "sectors": ["all"]},
]

def get_economic_calendar():
    """
    Fetch today's economic events from Investing.com calendar API.
    Returns list of high-impact events happening today/tomorrow.
    Falls back to empty list on failure.

    Structure returned:
    [{"time": "14:30", "event": "RBI Policy Rate", "impact": "high",
      "actual": "6.50%", "forecast": "6.25%", "previous": "6.50%"}]
    """
    today = _ist_now().strftime("%Y-%m-%d")
    tomorrow = (_ist_now() + timedelta(days=1)).strftime("%Y-%m-%d")

    # Investing.com economic calendar (public endpoint)
    url = (
        "https://economic-calendar.tradingeconomics.com/calendar?c=india&d1="
        + today + "&d2=" + tomorrow
    )
    data = _get(url)

    events = []
    if data and isinstance(data, list):
        for item in data:
            importance = item.get("importance", 0)
            if importance >= 2:   # 1=low, 2=medium, 3=high
                events.append({
                    "time":     item.get("date", "")[-5:],
                    "event":    item.get("event", "Unknown"),
                    "impact":   "high" if importance == 3 else "medium",
                    "actual":   item.get("actual", ""),
                    "forecast": item.get("forecast", ""),
                    "previous": item.get("previous", ""),
                    "country":  item.get("country", ""),
                })
        log.info("Economic calendar: %d medium/high events today", len(events))
    else:
        log.info("Economic calendar: no data or empty response")

    return events

def is_high_impact_window(events, buffer_mins=30):
    """
    Returns (True, event_desc) if we are within buffer_mins of a high-impact event.
    Used to suppress signals around volatile announcement times.
    """
    now_ist = _ist_now()
    for ev in events:
        if ev.get("impact") != "high":
            continue
        try:
            # Parse event time (HH:MM) as today's IST datetime
            h, m = ev["time"].split(":")
            ev_dt = now_ist.replace(hour=int(h), minute=int(m), second=0, microsecond=0)
            diff_mins = abs((now_ist - ev_dt).total_seconds() / 60)
            if diff_mins <= buffer_mins:
                return True, ev["event"]
        except Exception:
            continue
    return False, ""


# ══════════════════════════════════════════════════════════════════════════════
# LAYER 2 — Macro Proxy Fetchers (Yahoo Finance)
# ══════════════════════════════════════════════════════════════════════════════

# Yahoo Finance symbols
MACRO_SYMBOLS = {
    "crude":     "BZ=F",        # Brent crude futures
    "gold":      "GC=F",        # Gold futures
    "usdinr":    "USDINR=X",    # USD/INR exchange rate
    "spx":       "ES=F",        # S&P 500 futures (pre-market indicator)
    "vix":       "^VIX",        # CBOE Volatility Index
    "dxy":       "DX-Y.NYB",    # US Dollar Index
}

def _yahoo_quote(symbol):
    """Fetch current price + prev close from Yahoo Finance v8 API."""
    url = (
        "https://query1.finance.yahoo.com/v8/finance/chart/"
        + urllib.parse.quote(symbol)
        + "?interval=1d&range=2d"
    )
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept":     "application/json",
    }
    data = _get(url, headers=headers, timeout=8)
    if not data:
        return None
    try:
        meta   = data["chart"]["result"][0]["meta"]
        price  = meta.get("regularMarketPrice") or meta.get("previousClose")
        prev_c = meta.get("chartPreviousClose") or meta.get("previousClose")
        if not price or not prev_c:
            return None
        chg_pct = round((price - prev_c) / prev_c * 100, 2)
        return {
            "price":    round(price, 2),
            "prev_c":   round(prev_c, 2),
            "chg_pct":  chg_pct,
            "symbol":   symbol,
        }
    except (KeyError, IndexError, TypeError) as e:
        log.warning("Yahoo parse error for %s: %s", symbol, e)
        return None

def get_macro_proxies():
    """
    Fetch all macro proxy instruments.
    Returns dict with keys: crude, gold, usdinr, spx, vix, dxy
    Each value: {"price": float, "chg_pct": float} or None on failure.

    Impact rules applied by caller:
      crude  > +2%   → bearish for auto, FMCG, chemicals (input cost spike)
      crude  < -2%   → bullish for above sectors
      gold   > +1%   → risk-off, bearish for growth stocks
      usdinr > +0.5% → INR weakening, bearish for importers (crude, electronics)
      spx    < -1%   → global risk-off, Indian markets likely to follow
      vix    > 25    → high fear, reduce all confidence
      dxy    > +0.5% → dollar strength, FII outflows from India likely
    """
    results = {}
    for name, symbol in MACRO_SYMBOLS.items():
        q = _yahoo_quote(symbol)
        if q:
            results[name] = q
            log.info("%-8s  price=%-10.2f  chg=%+.2f%%", name, q["price"], q["chg_pct"])
        else:
            results[name] = None
            log.warning("%-8s  fetch failed", name)
    return results

def apply_macro_penalties(sig, sec, proxies):
    """
    Given a signal direction, sector, and macro proxies,
    returns (penalty_pct, list_of_warnings).
    Positive penalty = reduce confidence. Negative = bonus.
    """
    if not proxies:
        return 0, []

    penalty  = 0
    warnings = []
    crude    = proxies.get("crude")
    gold     = proxies.get("gold")
    usdinr   = proxies.get("usdinr")
    spx      = proxies.get("spx")
    vix      = proxies.get("vix")

    # ── S&P 500 futures: global risk-off ──────────────────────────────────────
    if spx:
        if spx["chg_pct"] <= -1.5 and sig == "BUY":
            penalty += 15
            warnings.append(f"S&P500 futures {spx['chg_pct']:+.1f}% — global risk-off, BUY risky (-15%)")
        elif spx["chg_pct"] >= +1.5 and sig == "SELL":
            penalty += 15
            warnings.append(f"S&P500 futures {spx['chg_pct']:+.1f}% — global rally, SELL risky (-15%)")
        elif spx["chg_pct"] >= +1.0 and sig == "BUY":
            penalty -= 5
            warnings.append(f"S&P500 futures {spx['chg_pct']:+.1f}% — global tailwind (+5%)")

    # ── VIX: fear index ───────────────────────────────────────────────────────
    if vix:
        if vix["price"] >= 30:
            penalty += 20
            warnings.append(f"VIX {vix['price']:.0f} — extreme fear, avoid directional trades (-20%)")
        elif vix["price"] >= 25:
            penalty += 10
            warnings.append(f"VIX {vix['price']:.0f} — elevated fear (-10%)")

    # ── Brent crude: sector-specific impact ───────────────────────────────────
    if crude:
        crude_chg = crude["chg_pct"]
        # Sectors hurt by rising oil
        oil_sensitive_bearish = {"Auto", "FMCG", "Consumer", "Pharma", "Cement"}
        # Sectors helped by rising oil
        oil_sensitive_bullish = {"Energy"}

        if crude_chg >= +2.0:
            if sec in oil_sensitive_bearish and sig == "BUY":
                penalty += 12
                warnings.append(f"Crude {crude_chg:+.1f}% — cost pressure on {sec} sector (-12%)")
            elif sec in oil_sensitive_bullish and sig == "BUY":
                penalty -= 8
                warnings.append(f"Crude {crude_chg:+.1f}% — tailwind for {sec} sector (+8%)")
        elif crude_chg <= -2.0:
            if sec in oil_sensitive_bearish and sig == "BUY":
                penalty -= 8
                warnings.append(f"Crude {crude_chg:+.1f}% — input cost relief for {sec} (+8%)")
            elif sec in oil_sensitive_bullish and sig == "BUY":
                penalty += 12
                warnings.append(f"Crude {crude_chg:+.1f}% — headwind for {sec} sector (-12%)")

    # ── USD/INR: currency impact ───────────────────────────────────────────────
    if usdinr:
        inr_chg = usdinr["chg_pct"]  # positive = INR weakening (USD stronger)
        # Sectors hurt by weak INR (import-heavy)
        import_heavy = {"Auto", "Consumer", "Pharma"}
        # Sectors helped by weak INR (export earners)
        export_earners = {"IT"}

        if inr_chg >= +0.5:
            if sec in import_heavy and sig == "BUY":
                penalty += 10
                warnings.append(f"INR weakening {inr_chg:+.2f}% vs USD — import cost pressure on {sec} (-10%)")
            elif sec in export_earners and sig == "BUY":
                penalty -= 8
                warnings.append(f"INR weakening {inr_chg:+.2f}% — export earnings boost for {sec} (+8%)")
        elif inr_chg <= -0.5:
            if sec in export_earners and sig == "BUY":
                penalty += 8
                warnings.append(f"INR strengthening {inr_chg:+.2f}% — headwind for IT exports (-8%)")

    # ── Gold: risk-off signal ──────────────────────────────────────────────────
    if gold:
        if gold["chg_pct"] >= +1.5 and sig == "BUY":
            # Strong gold rally = investors fleeing to safety = bad for equities
            penalty += 10
            warnings.append(f"Gold {gold['chg_pct']:+.1f}% — risk-off signal, BUY equity risky (-10%)")

    return penalty, warnings


# ══════════════════════════════════════════════════════════════════════════════
# LAYER 3 — FII/DII Flow Data (NSE India)
# ══════════════════════════════════════════════════════════════════════════════

def get_fii_dii_flows():
    """
    Fetch latest FII (Foreign Institutional Investor) and
    DII (Domestic Institutional Investor) cash market flows from NSE.

    Returns:
    {
      "date": "19-Mar-2026",
      "fii_net": -1234.56,   # crores, negative = net sellers
      "dii_net":  2345.67,   # crores, positive = net buyers
      "fii_bias": "selling" | "buying" | "neutral",
      "dii_bias": "selling" | "buying" | "neutral",
      "combined_bias": "bearish" | "bullish" | "neutral",
      "note": "FII sold Rs 1234 Cr, DII bought Rs 2345 Cr"
    }
    """
    url = "https://www.nseindia.com/api/fiidiiTradeReact"
    headers = {
        "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
        "Accept":          "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer":         "https://www.nseindia.com/market-data/fii-dii-activity",
    }
    data = _get(url, headers=headers, timeout=10)

    if not data or not isinstance(data, list) or not data:
        log.warning("FII/DII: no data returned")
        return None

    # NSE returns array, most recent first
    latest = data[0]
    try:
        fii_net = float(latest.get("fiiBuy", 0)) - float(latest.get("fiiSell", 0))
        dii_net = float(latest.get("diiBuy", 0)) - float(latest.get("diiSell", 0))
        date    = latest.get("date", "")

        def flow_bias(net):
            if net <= -500:   return "selling"
            if net >= +500:   return "buying"
            return "neutral"

        fii_bias = flow_bias(fii_net)
        dii_bias = flow_bias(dii_net)

        # Combined: FII dominates because they move markets more
        if fii_bias == "selling" and dii_bias != "buying":
            combined = "bearish"
        elif fii_bias == "buying":
            combined = "bullish"
        elif fii_bias == "neutral" and dii_bias == "buying":
            combined = "bullish"
        else:
            combined = "neutral"

        note = (
            f"FII {'sold' if fii_net < 0 else 'bought'} "
            f"Rs {abs(fii_net):,.0f} Cr  |  "
            f"DII {'sold' if dii_net < 0 else 'bought'} "
            f"Rs {abs(dii_net):,.0f} Cr"
        )
        log.info("FII/DII (%s): FII %+.0f Cr, DII %+.0f Cr → %s",
                 date, fii_net, dii_net, combined)

        return {
            "date":          date,
            "fii_net":       round(fii_net, 2),
            "dii_net":       round(dii_net, 2),
            "fii_bias":      fii_bias,
            "dii_bias":      dii_bias,
            "combined_bias": combined,
            "note":          note,
        }
    except (KeyError, ValueError, TypeError) as e:
        log.warning("FII/DII parse error: %s", e)
        return None

def apply_fii_penalty(sig, fii_data):
    """Returns (penalty_pct, warning_str) based on FII/DII flows."""
    if not fii_data:
        return 0, ""
    bias = fii_data.get("combined_bias", "neutral")
    note = fii_data.get("note", "")
    if bias == "bearish" and sig == "BUY":
        return 10, f"Institutional flows bearish — {note} (-10%)"
    if bias == "bullish" and sig == "SELL":
        return 10, f"Institutional flows bullish — {note} (-10%)"
    if bias == "bullish" and sig == "BUY":
        return -5, f"Institutional flows bullish — {note} (+5%)"
    if bias == "bearish" and sig == "SELL":
        return -5, f"Institutional flows bearish — {note} (+5%)"
    return 0, ""


# ══════════════════════════════════════════════════════════════════════════════
# LAYER 4 — News Sentiment via NewsAPI + Claude
# ══════════════════════════════════════════════════════════════════════════════

NEWSAPI_BASE = "https://newsapi.org/v2"
ANTHROPIC_BASE = "https://api.anthropic.com"

def fetch_market_headlines(max_articles=15):
    """
    Fetch recent Indian financial market headlines via NewsAPI.
    Returns list of headline strings. Empty list on failure.
    """
    api_key = os.environ.get("NEWS_API_KEY", "")
    if not api_key:
        log.info("NEWS_API_KEY not set — skipping news sentiment")
        return []

    # Search for India market news from last 6 hours
    from_time = (datetime.utcnow() - timedelta(hours=6)).strftime("%Y-%m-%dT%H:%M:%S")
    params = urllib.parse.urlencode({
        "q":        "India stock market OR NSE OR Nifty OR BSE OR RBI OR SEBI",
        "language": "en",
        "sortBy":   "publishedAt",
        "from":     from_time,
        "pageSize": max_articles,
        "apiKey":   api_key,
    })
    url  = f"{NEWSAPI_BASE}/everything?{params}"
    data = _get(url)

    if not data or data.get("status") != "ok":
        log.warning("NewsAPI error: %s", (data or {}).get("message", "no response"))
        return []

    headlines = []
    for art in data.get("articles", []):
        title = art.get("title", "").strip()
        desc  = art.get("description", "").strip()
        if title and "[Removed]" not in title:
            headlines.append(title + (f" — {desc[:100]}" if desc else ""))

    log.info("NewsAPI: fetched %d headlines", len(headlines))
    return headlines

def classify_news_with_claude(headlines):
    """
    Send headlines to Claude (claude-haiku for speed/cost) and get back
    a structured market sentiment classification.

    Returns:
    {
      "overall_bias":   "bullish" | "bearish" | "neutral",
      "confidence":     "high" | "medium" | "low",
      "key_themes":     ["RBI rate hold", "FII outflows", ...],
      "sector_impacts": {"Banking": "bearish", "IT": "neutral", ...},
      "alert_events":   ["RBI press conference at 2PM", ...],
      "summary":        "2-sentence plain English summary"
    }
    Returns None on failure.
    """
    if not headlines:
        return None

    api_key = os.environ.get("ANTHROPIC_KEY", os.environ.get("ANTHROPIC_API_KEY", ""))
    if not api_key:
        log.info("No Anthropic key — skipping news classification")
        return None

    headlines_text = "\n".join(f"- {h}" for h in headlines[:15])
    prompt = f"""You are a market analyst for Indian equities (NSE/BSE).
Analyse these recent headlines and return ONLY a JSON object with this exact structure:

{{
  "overall_bias": "bullish" or "bearish" or "neutral",
  "confidence": "high" or "medium" or "low",
  "key_themes": ["theme1", "theme2"],
  "sector_impacts": {{
    "Banking": "bullish" or "bearish" or "neutral",
    "IT": "bullish" or "bearish" or "neutral",
    "Auto": "bullish" or "bearish" or "neutral",
    "Energy": "bullish" or "bearish" or "neutral",
    "Pharma": "bullish" or "bearish" or "neutral",
    "FMCG": "bullish" or "bearish" or "neutral"
  }},
  "alert_events": ["any scheduled events mentioned, e.g. RBI policy at 2PM"],
  "summary": "2 sentences max plain English"
}}

Headlines:
{headlines_text}

Return ONLY the JSON. No explanation."""

    payload = json.dumps({
        "model":      "claude-haiku-4-5-20251001",
        "max_tokens": 600,
        "messages":   [{"role": "user", "content": prompt}],
    }).encode("utf-8")

    req = urllib.request.Request(
        f"{ANTHROPIC_BASE}/v1/messages",
        data=payload,
        headers={
            "Content-Type":    "application/json",
            "x-api-key":       api_key,
            "anthropic-version": "2023-06-01",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            resp = json.loads(r.read())
        raw_text = resp["content"][0]["text"].strip()
        # Strip markdown fences if present
        if raw_text.startswith("```"):
            raw_text = raw_text.split("```")[1]
            if raw_text.startswith("json"):
                raw_text = raw_text[4:]
        result = json.loads(raw_text)
        log.info("News sentiment: %s (confidence: %s) — %s",
                 result.get("overall_bias"), result.get("confidence"),
                 result.get("summary", "")[:80])
        return result
    except Exception as e:
        log.warning("Claude news classification failed: %s", e)
        return None

def apply_news_penalty(sig, sec, sentiment):
    """
    Returns (penalty_pct, warning_str) based on Claude news sentiment.
    """
    if not sentiment:
        return 0, ""

    overall = sentiment.get("overall_bias", "neutral")
    conf    = sentiment.get("confidence",   "low")
    sectors = sentiment.get("sector_impacts", {})
    summary = sentiment.get("summary", "")

    # Only apply if Claude is reasonably confident
    conf_mult = {"high": 1.0, "medium": 0.6, "low": 0.3}.get(conf, 0.3)
    penalty   = 0
    warnings  = []

    # Overall market sentiment
    if overall == "bearish" and sig == "BUY":
        base = 15 * conf_mult
        penalty += base
        warnings.append(f"News sentiment bearish ({conf} conf): {summary[:60]} (-{base:.0f}%)")
    elif overall == "bullish" and sig == "SELL":
        base = 15 * conf_mult
        penalty += base
        warnings.append(f"News sentiment bullish ({conf} conf): {summary[:60]} (-{base:.0f}%)")
    elif overall == "bullish" and sig == "BUY":
        base = 5 * conf_mult
        penalty -= base
        warnings.append(f"News sentiment bullish — tailwind (+{base:.0f}%)")
    elif overall == "bearish" and sig == "SELL":
        base = 5 * conf_mult
        penalty -= base
        warnings.append(f"News sentiment bearish — tailwind for short (+{base:.0f}%)")

    # Sector-specific override
    sec_bias = sectors.get(sec, "neutral")
    if sec_bias == "bearish" and sig == "BUY":
        base = 10 * conf_mult
        penalty += base
        warnings.append(f"News bearish for {sec} sector (-{base:.0f}%)")
    elif sec_bias == "bullish" and sig == "SELL":
        base = 10 * conf_mult
        penalty += base
        warnings.append(f"News bullish for {sec} sector (-{base:.0f}%)")

    return round(penalty), [w for w in warnings if w]


# ══════════════════════════════════════════════════════════════════════════════
# COMBINED MACRO CONTEXT — called once per scan
# ══════════════════════════════════════════════════════════════════════════════

def get_full_macro_context():
    """
    Fetch all macro data in one call. Returns a dict with all layers.
    Safe to call — returns partial data if some sources fail.
    """
    log.info("Fetching full macro context...")

    # Fetch all layers (headlines + Claude classification can run together)
    cal      = get_economic_calendar()
    proxies  = get_macro_proxies()
    fii      = get_fii_dii_flows()
    headlines = fetch_market_headlines()
    sentiment = classify_news_with_claude(headlines) if headlines else None

    in_event_window, event_desc = is_high_impact_window(cal)

    ctx = {
        "calendar":          cal,
        "in_event_window":   in_event_window,
        "event_desc":        event_desc,
        "proxies":           proxies,
        "fii_dii":           fii,
        "news_headlines":    headlines,
        "news_sentiment":    sentiment,
        "fetched_at":        _ist_now().strftime("%H:%M IST"),
    }

    log.info(
        "Macro context ready: calendar=%d events, proxies=%d/%d, fii=%s, news=%d headlines",
        len(cal),
        sum(1 for v in proxies.values() if v),
        len(proxies),
        "ok" if fii else "fail",
        len(headlines),
    )
    return ctx

def apply_all_macro_penalties(sig, sec, macro_ctx):
    """
    Apply all macro layers to a signal. Returns (total_penalty, all_warnings).
    Called by build_setup in signals.py for each stock.
    """
    if not macro_ctx or sig == "WATCH":
        return 0, []

    total_pen = 0
    all_warns = []

    # Layer 1: Economic calendar — hard block during event window
    if macro_ctx.get("in_event_window"):
        ev = macro_ctx.get("event_desc", "high-impact event")
        all_warns.append(f"HIGH-IMPACT EVENT in progress: {ev} — signals suppressed")
        # Return a very high penalty that will push confidence below any threshold
        return 50, all_warns

    # Layer 2: Macro proxies
    proxies = macro_ctx.get("proxies")
    if proxies:
        pen, warns = apply_macro_penalties(sig, sec, proxies)
        total_pen += pen
        all_warns.extend(warns)

    # Layer 3: FII/DII flows
    fii = macro_ctx.get("fii_dii")
    if fii:
        pen, warn = apply_fii_penalty(sig, fii)
        if pen != 0:
            total_pen += pen
            if warn:
                all_warns.append(warn)

    # Layer 4: News sentiment
    sentiment = macro_ctx.get("news_sentiment")
    if sentiment:
        pen, warns = apply_news_penalty(sig, sec, sentiment)
        total_pen += pen
        all_warns.extend(warns)

    return round(total_pen), all_warns
