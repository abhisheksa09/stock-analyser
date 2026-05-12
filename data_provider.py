"""
data_provider.py — Market-agnostic data adapter.

NSE: delegates to Upstox API (existing signals.py functions, token required)
US:  Alpaca IEX feed (real-time) when ALPACA_API_KEY is set,
     falls back to yfinance (15-min delayed) otherwise.

Candle format (both markets): [timestamp_str, open, high, low, close, volume]
  Index: 0=timestamp, 1=open, 2=high, 3=low, 4=close, 5=volume
  This matches the Upstox candle format used throughout signals.py / scanner.py.
"""

import os
import logging
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import requests

log = logging.getLogger("data_provider")


class YFRateLimitError(Exception):
    pass


_YF_SESSION = requests.Session()
_YF_SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
})


def _yf_ticker(sym):
    """Return a yf.Ticker with the shared session."""
    import yfinance as yf
    return yf.Ticker(sym, session=_YF_SESSION)


def _yf_fetch(sym, period, interval, retries=2):
    """Fetch history; raises YFRateLimitError on HTTP 429, returns None on other errors."""
    last_exc = None
    for attempt in range(retries):
        try:
            df = _yf_ticker(sym).history(period=period, interval=interval)
            return df
        except Exception as e:
            msg = str(e)
            if "429" in msg or "too many requests" in msg.lower():
                raise YFRateLimitError(f"{sym}: Too Many Requests. Rate limited. Try after a while.")
            last_exc = e
            if attempt < retries - 1:
                time.sleep(1)
    log.warning("yf_fetch %s %s/%s failed: %s", sym, period, interval, last_exc)
    return None

MARKET_NSE = "NSE"
MARKET_US  = "US"
ET         = ZoneInfo("America/New_York")

ALPACA_DATA_URL = "https://data.alpaca.markets/v2"


def _alpaca_configured():
    return bool(os.environ.get("ALPACA_API_KEY"))


def _alpaca_headers():
    return {
        "APCA-API-KEY-ID":     os.environ.get("ALPACA_API_KEY", ""),
        "APCA-API-SECRET-KEY": os.environ.get("ALPACA_API_SECRET", ""),
    }


def _alpaca_bars(sym, timeframe, start, end, limit=500):
    """Fetch bars from Alpaca. Returns chronological list of [ts_str, o, h, l, c, v]."""
    params = {"timeframe": timeframe, "start": start, "end": end,
              "limit": limit, "feed": "iex", "sort": "asc"}
    resp = _YF_SESSION.get(
        f"{ALPACA_DATA_URL}/stocks/{sym}/bars",
        headers=_alpaca_headers(), params=params, timeout=10,
    )
    resp.raise_for_status()
    candles = []
    for bar in resp.json().get("bars") or []:
        ts_et = datetime.fromisoformat(bar["t"].replace("Z", "+00:00")).astimezone(ET)
        # str(ts_et) → "2024-01-15 09:30:00-05:00"  ts[11:16] = "09:30" ✓
        candles.append([str(ts_et), float(bar["o"]), float(bar["h"]),
                        float(bar["l"]), float(bar["c"]), int(bar["v"])])
    return candles


def _alpaca_intraday(sym):
    """Real-time 1-min candles for today's ET session via Alpaca IEX feed."""
    today = datetime.now(ET).date()
    start = datetime(today.year, today.month, today.day, 9, 30, tzinfo=ET).isoformat()
    end   = datetime(today.year, today.month, today.day, 16, 0, tzinfo=ET).isoformat()
    return _alpaca_bars(sym, "1Min", start, end, limit=400)


def _alpaca_daily(sym):
    """Daily candles for last 60 days, newest-first."""
    end   = datetime.now(ET).date().isoformat()
    start = (datetime.now(ET) - timedelta(days=60)).date().isoformat()
    return list(reversed(_alpaca_bars(sym, "1Day", start, end, limit=60)))


def _alpaca_ltp(sym):
    """Latest trade price via Alpaca."""
    resp = _YF_SESSION.get(
        f"{ALPACA_DATA_URL}/stocks/{sym}/trades/latest",
        headers=_alpaca_headers(), params={"feed": "iex"}, timeout=5,
    )
    resp.raise_for_status()
    price = (resp.json().get("trade") or {}).get("p")
    if price:
        return float(price)
    intra = _alpaca_intraday(sym)
    if intra:
        return float(intra[-1][4])
    raise ValueError(f"No price data for {sym}")


# ─── Unified interface ────────────────────────────────────────────────────────

def get_intraday_candles(sym, market, token=None, ikey=None):
    """1-min intraday candles. Returns chronological list (oldest first)."""
    if market == MARKET_US:
        return _alpaca_intraday(sym) if _alpaca_configured() else _yf_intraday(sym)
    from signals import get_intraday
    return get_intraday(ikey, token)


def get_daily_candles(sym, market, token=None, ikey=None):
    """Daily candles. Returns newest-first list (matches Upstox format)."""
    if market == MARKET_US:
        return _alpaca_daily(sym) if _alpaca_configured() else _yf_daily(sym)
    from signals import get_daily
    return get_daily(ikey, token)


def get_ltp_price(sym, market, token=None, ikey=None):
    """Last traded / most recent close price."""
    if market == MARKET_US:
        return _alpaca_ltp(sym) if _alpaca_configured() else _yf_ltp(sym)
    from signals import get_ltp
    return get_ltp(ikey, token)


def get_market_context_us(sector):
    """
    US market context using yfinance ETFs. Returns same dict shape as
    signals.get_market_context() so build_setup() works without changes.
    """
    sp500_chg  = _yf_index_change("SP500")
    sector_idx = US_SECTOR_INDEX.get(sector)
    sector_chg = _yf_index_change(sector_idx) if sector_idx else 0.0

    def bias(chg):
        if chg <= -0.5: return "bearish"
        if chg >= +0.5: return "bullish"
        return "neutral"

    return {
        "nifty_chg":   sp500_chg,   # reuses same key — build_setup reads this for hard-block logic
        "sector_chg":  sector_chg,
        "market_bias": bias(sp500_chg),
        "sector_bias": bias(sector_chg),
        "index_name":  "S&P 500",   # for display labels in Telegram/frontend
        "market":      MARKET_US,
    }


# ─── US stock universe ────────────────────────────────────────────────────────

US_STOCKS = [
    # Technology
    {"sym": "AAPL",  "sec": "Technology"},
    {"sym": "MSFT",  "sec": "Technology"},
    {"sym": "NVDA",  "sec": "Technology"},
    {"sym": "GOOGL", "sec": "Technology"},
    {"sym": "META",  "sec": "Technology"},
    {"sym": "AMD",   "sec": "Technology"},
    {"sym": "NFLX",  "sec": "Technology"},
    {"sym": "CRM",   "sec": "Technology"},
    # Consumer / E-commerce
    {"sym": "AMZN",  "sec": "Consumer"},
    {"sym": "TSLA",  "sec": "Consumer"},
    {"sym": "WMT",   "sec": "Consumer"},
    {"sym": "HD",    "sec": "Consumer"},
    {"sym": "MCD",   "sec": "Consumer"},
    {"sym": "COST",  "sec": "Consumer"},
    {"sym": "DIS",   "sec": "Consumer"},
    # Financials
    {"sym": "JPM",   "sec": "Financials"},
    {"sym": "BAC",   "sec": "Financials"},
    {"sym": "GS",    "sec": "Financials"},
    {"sym": "V",     "sec": "Financials"},
    {"sym": "MA",    "sec": "Financials"},
    # Healthcare
    {"sym": "JNJ",   "sec": "Healthcare"},
    {"sym": "UNH",   "sec": "Healthcare"},
    {"sym": "PFE",   "sec": "Healthcare"},
    {"sym": "ABBV",  "sec": "Healthcare"},
    # Energy
    {"sym": "XOM",   "sec": "Energy"},
    {"sym": "CVX",   "sec": "Energy"},
    # Industrial
    {"sym": "BA",    "sec": "Industrial"},
    {"sym": "CAT",   "sec": "Industrial"},
    {"sym": "GE",    "sec": "Industrial"},
    # Broad market ETFs (useful for context + easy signals)
    {"sym": "SPY",   "sec": "ETF"},
    {"sym": "QQQ",   "sec": "ETF"},
]

# Maps US stock sectors to the ETF used for sector context
US_SECTOR_INDEX = {
    "Technology": "XLK",
    "Financials": "XLF",
    "Healthcare": "XLV",
    "Energy":     "XLE",
    "Consumer":   "XLY",
    "Industrial": "XLI",
    "Utilities":  "XLU",
    "Materials":  "XLB",
    "ETF":        "SP500",
}

_YF_INDEX_MAP = {
    "SP500":  "^GSPC",
    "NASDAQ": "^IXIC",
    "VIX":    "^VIX",
    "XLK": "XLK", "XLF": "XLF", "XLV": "XLV", "XLE": "XLE",
    "XLY": "XLY", "XLI": "XLI", "XLU": "XLU", "XLB": "XLB",
}


# ─── yfinance helpers ─────────────────────────────────────────────────────────

def _df_to_candles(df):
    """Convert a yfinance history DataFrame to [[ts,o,h,l,c,v], ...]."""
    result = []
    for ts, row in df.iterrows():
        result.append([
            str(ts),
            float(row["Open"]),
            float(row["High"]),
            float(row["Low"]),
            float(row["Close"]),
            int(row["Volume"]),
        ])
    return result


def _yf_intraday(sym):
    """1-min candles for the most recent session (chronological, oldest first).
    Uses period='5d' so pre-market runs still return the previous session."""
    try:
        df = _yf_fetch(sym, period="5d", interval="1m")
    except YFRateLimitError as e:
        log.warning("yf_intraday %s: %s", sym, e)
        raise
    if df is None or df.empty:
        return []
    candles = _df_to_candles(df)
    if not candles:
        return []
    # Keep only the most recent trading session (same date as last candle)
    last_date = candles[-1][0][:10]
    return [c for c in candles if c[0][:10] == last_date]


def _yf_daily(sym):
    """Daily candles for last 60 days, newest-first (matches Upstox format)."""
    try:
        df = _yf_fetch(sym, period="60d", interval="1d")
    except YFRateLimitError as e:
        log.warning("yf_daily %s: %s", sym, e)
        raise
    if df is None or df.empty:
        return []
    return list(reversed(_df_to_candles(df)))


def _yf_ltp(sym):
    """Most recent price: tries fast_info first, falls back to last candle.
    Returns None if no data available (non-rate-limit case)."""
    try:
        fi = _yf_ticker(sym).fast_info
        price = getattr(fi, "last_price", None) or getattr(fi, "regularMarketPrice", None)
        if price:
            return float(price)
    except YFRateLimitError:
        raise
    except Exception:
        pass
    # Fallback: last close from intraday, then daily (YFRateLimitError propagates)
    intra = _yf_intraday(sym)
    if intra:
        return float(intra[-1][4])
    daily = _yf_daily(sym)
    if daily:
        return float(daily[0][4])
    return None


def _yf_index_change(index_name):
    """% change of an index vs its previous session close."""
    ticker = _YF_INDEX_MAP.get(index_name)
    if not ticker:
        return 0.0
    df = _yf_fetch(ticker, period="5d", interval="1d")
    if df is None or len(df) < 2:
        return 0.0
    try:
        prev_close = float(df["Close"].iloc[-2])
        curr_close = float(df["Close"].iloc[-1])
        return round((curr_close - prev_close) / prev_close * 100, 2)
    except Exception:
        return 0.0
