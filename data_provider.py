"""
data_provider.py — Market-agnostic data adapter.

NSE: delegates to Upstox API (existing signals.py functions, token required)
US:  uses yfinance for real-time delayed intraday + daily candles (no token needed)

Candle format (both markets): [timestamp_str, open, high, low, close, volume]
  Index: 0=timestamp, 1=open, 2=high, 3=low, 4=close, 5=volume
  This matches the Upstox candle format used throughout signals.py / scanner.py.
"""

import logging
import requests

log = logging.getLogger("data_provider")

_YF_SESSION = requests.Session()
_YF_SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
})

MARKET_NSE = "NSE"
MARKET_US  = "US"


# ─── Unified interface ────────────────────────────────────────────────────────

def get_intraday_candles(sym, market, token=None, ikey=None):
    """1-min intraday candles. Returns chronological list (oldest first)."""
    if market == MARKET_US:
        return _yf_intraday(sym)
    from signals import get_intraday
    return get_intraday(ikey, token)


def get_daily_candles(sym, market, token=None, ikey=None):
    """Daily candles. Returns newest-first list (matches Upstox format)."""
    if market == MARKET_US:
        return _yf_daily(sym)
    from signals import get_daily
    return get_daily(ikey, token)


def get_ltp_price(sym, market, token=None, ikey=None):
    """Last traded / most recent close price."""
    if market == MARKET_US:
        candles = _yf_intraday(sym)
        if candles:
            return float(candles[-1][4])
        raise ValueError(f"No US intraday data for {sym}")
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

def _yf_intraday(sym):
    """1-min candles for today (chronological, oldest first)."""
    try:
        import yfinance as yf
        df = yf.Ticker(sym, session=_YF_SESSION).history(period="1d", interval="1m")
        if df.empty:
            return []
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
    except Exception as e:
        log.warning("yf_intraday %s: %s", sym, e)
        return []


def _yf_daily(sym):
    """Daily candles for last 60 days, newest-first (matches Upstox format)."""
    try:
        import yfinance as yf
        df = yf.Ticker(sym, session=_YF_SESSION).history(period="60d", interval="1d")
        if df.empty:
            return []
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
        return list(reversed(result))  # newest first to match Upstox
    except Exception as e:
        log.warning("yf_daily %s: %s", sym, e)
        return []


def _yf_index_change(index_name):
    """% change of an index vs its previous session close."""
    ticker = _YF_INDEX_MAP.get(index_name)
    if not ticker:
        return 0.0
    try:
        import yfinance as yf
        df = yf.Ticker(ticker, session=_YF_SESSION).history(period="5d", interval="1d")
        if len(df) < 2:
            return 0.0
        prev_close = float(df["Close"].iloc[-2])
        curr_close = float(df["Close"].iloc[-1])
        return round((curr_close - prev_close) / prev_close * 100, 2)
    except Exception:
        return 0.0
