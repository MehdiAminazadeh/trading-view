import csv
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

PAGE_URL = "https://www.tradingview.com/markets/stocks-usa/market-movers-all-stocks/"
API_URL  = "https://scanner.tradingview.com/america/scan"
OUT_CSV  = "tradingview_performance.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.tradingview.com",
    "Referer": PAGE_URL
}

PERF_COLUMNS = [
    "name",           # company name
    "close",          # last price
    "change",         # % change (1D)
    "change_abs",     # absolute change
    "Perf.W",         # 1W %
    "Perf.1M",        # 1M %
    "Perf.3M",        # 3M %
    "Perf.6M",        # 6M %
    "Perf.YTD",       # YTD %
    "Perf.Y",         # 1Y %
    "Perf.5Y",        # 5Y %
    "Perf.All",       # All-time %
    "volume",         # volume
    "market_cap_basic",
    "sector",
    "industry"
]

def fetch_batch(session, start, batch, columns):
    payload = {
        "filter": [{"left": "type", "operation": "in_range", "right": ["stock"]}],
        "options": {"lang": "en"},
        "symbols": {"query": {"types": []}, "tickers": []},
        "columns": columns,
        "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"},
        "range": [start, start + batch]
    }
    r = session.post(API_URL, headers=HEADERS, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

def format_percent(x):
    try:
        return f"{float(x):.2f}%"
    except (TypeError, ValueError):
        return ""

def format_money(x):
    try:
        # market cap in dollars with $ and separators; keep no decimals for big numbers
        return f"${int(float(x)):,}"
    except (TypeError, ValueError):
        return ""

def format_int(x):
    try:
        return f"{int(float(x)):,}"
    except (TypeError, ValueError):
        return ""

def main():
    session = requests.Session()
    session.headers.update(HEADERS)

    start, batch = 0, 150
    rows = []
    total = None

    while True:
        data = fetch_batch(session, start, batch, PERF_COLUMNS)
        items = data.get("data", [])
        if total is None:
            total = data.get("totalCount")
        if not items:
            break

        for it in items:
            ticker = it.get("s", "")
            vals = it.get("d", [])
            rec = {"ticker": ticker}

            # map values to column names
            for col, val in zip(PERF_COLUMNS, vals):
                rec[col] = val

            
            if rec.get("close") not in (None, ""):
                rec["close"] = f"{float(rec['close']):.4f}"

            
            rec["change"] = format_percent(rec.get("change"))

            try:
                rec["change_abs"] = f"{float(rec['change_abs']):.4f}"
            except Exception:
                rec["change_abs"] = ""

            for pcol in ["Perf.W","Perf.1M","Perf.3M","Perf.6M","Perf.YTD","Perf.Y","Perf.5Y","Perf.All"]:
                rec[pcol] = format_percent(rec.get(pcol))

            # volume, market cap
            rec["volume"] = format_int(rec.get("volume"))
            rec["market_cap_basic"] = format_money(rec.get("market_cap_basic"))

            rows.append(rec)

        start += batch
        time.sleep(0.2)
        if total is not None and start >= total:
            break

    if not rows:
        raise SystemExit("No rows fetched. TradingView may have changed the fields.")

    fieldnames = ["ticker"] + PERF_COLUMNS
    with Path(OUT_CSV).open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Saved {len(rows)} rows to {OUT_CSV}")

if __name__ == "__main__":
    main()
