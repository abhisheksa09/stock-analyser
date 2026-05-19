"""
One-shot utility: download Upstox instrument master and search for NSE index keys.

Usage:
    python lookup_instrument_key.py [search_term ...]

Example:
    python lookup_instrument_key.py infrastructure healthcare digital

Without args, prints all NSE_INDEX rows.
"""
import csv
import gzip
import io
import sys
import urllib.request

MASTER_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"


def fetch_master():
    print(f"Downloading {MASTER_URL} ...", flush=True)
    with urllib.request.urlopen(MASTER_URL, timeout=30) as r:
        raw = r.read()
    with gzip.open(io.BytesIO(raw), "rt", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def main():
    terms = [t.lower() for t in sys.argv[1:]]
    rows = fetch_master()

    index_rows = [r for r in rows if r.get("instrument_key", "").startswith("NSE_INDEX|")]

    if terms:
        index_rows = [
            r for r in index_rows
            if any(t in r.get("name", "").lower() or t in r.get("instrument_key", "").lower() for t in terms)
        ]

    if not index_rows:
        print("No matches found.")
        return

    col_w = 55
    print(f"\n{'instrument_key':<{col_w}} name")
    print("-" * (col_w + 40))
    for r in sorted(index_rows, key=lambda x: x.get("name", "")):
        print(f"{r['instrument_key']:<{col_w}} {r.get('name', '')}")


if __name__ == "__main__":
    main()
