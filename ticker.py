import json
import csv
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

PAGE_URL = "https://www.tradingview.com/markets/stocks-usa/market-movers-all-stocks/"
API_URL  = "https://scanner.tradingview.com/america/scan"   
OUT_CSV  = "tradingview_all_stocks.csv"


DEFAULT_COLUMNS = [
    "name",                 # Company name
    "close",                # Last price
    "change",               # % change
    "change_abs",           # Abs change
    "volume",               # Volume
    "market_cap_basic",     # Market cap
    "price_earnings_ttm",   # P/E (TTM)
    "sector",               # Sector
    "industry"              # Industry
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.tradingview.com",
    "Referer": PAGE_URL
}

def discover_columns_with_bs(session) -> list:
    """
    Read initial columns from the page's embedded Next.js data.
    Falls back to DEFAULT_COLUMNS if not found.
    """
    try:
        r = session.get(PAGE_URL, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        script = soup.find("script", id="__NEXT_DATA__")
        if not script or not script.string:
            return DEFAULT_COLUMNS

        data = json.loads(script.string)
        
        pp = data.get("props", {}).get("pageProps", {})
        
        for key in ("screener", "screenerStore", "screenerProps", "table"):
            if key in pp and isinstance(pp[key], dict):
                for k2 in ("columns", "tableColumns", "cols"):
                    cols = pp[key].get(k2)
                    if isinstance(cols, list) and cols:
                      
                        # Normalize to field keys.
                        normalized = []
                        for c in cols:
                            if isinstance(c, str):
                                normalized.append(c)
                            elif isinstance(c, dict):
                                #common keys: "name", "key", "code"
                                normalized.append(c.get("name") or c.get("key") or c.get("code"))
                        normalized = [c for c in normalized if c]
                        if normalized:
                            return normalized
        return DEFAULT_COLUMNS
    except Exception:
        return DEFAULT_COLUMNS

def fetch_batch(session, columns, start, batch):
    """
    Fetch a batch [start, start+batch) from the screener.
    """
    payload = {
        "filter": [
            {"left": "type", "operation": "in_range", "right": ["stock"]},
        ],
        "options": {"lang": "en"},
        "symbols": {"query": {"types": []}, "tickers": []},
        "columns": columns,
        "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"},
        "range": [start, start + batch]
    }
    r = session.post(API_URL, headers=HEADERS, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

def to_rows(api_item, columns):
    """
    Transform one item from the API to a dict row for CSV
    api_item looks like: {"s": "NASDAQ:AAPL", "d": [vals...]}
    """
    ticker = api_item.get("s")
    values = api_item.get("d", [])
    row = {"ticker": ticker}
    for col, val in zip(columns, values):
        row[col] = val
    return row

def main():
    session = requests.Session()
    session.headers.update(HEADERS)

    columns = discover_columns_with_bs(session)
    base_cols = []
    for c in DEFAULT_COLUMNS:
        if c not in columns:
            base_cols.append(c)
    columns = columns + base_cols  #add any missing defaults to the end

    #pull everything in pages
    start = 0
    batch = 150
    all_rows = []
    total_count = None

    while True:
        data = fetch_batch(session, columns, start, batch)
        items = data.get("data", [])
        if total_count is None:
            total_count = data.get("totalCount", None)

        if not items:
            break

        for it in items:
            all_rows.append(to_rows(it, columns))

        start += batch
        time.sleep(0.2)

        if total_count is not None and start >= total_count:
            break

    if not all_rows:
        raise SystemExit("No rows fetched. The endpoint/fields may have changed.")

   
    out_path = Path(OUT_CSV)
    fieldnames = ["ticker"] + columns
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"Saved {len(all_rows)} rows to {out_path.resolve()}")

if __name__ == "__main__":
    main()
