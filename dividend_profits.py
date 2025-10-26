import csv
import time
from pathlib import Path
import requests

PAGE_URL = "https://www.tradingview.com/markets/stocks-usa/market-movers-all-stocks/"
API_SCAN = "https://scanner.tradingview.com/america/scan"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.tradingview.com",
    "Referer": PAGE_URL,
}

PROFITABILITY_TARGETS = [
    "name", "close",
    "gross_margin_ttm",        # Gross margin (TTM %)
    "operating_margin_ttm",    # Operating margin (TTM %)
    "net_margin_ttm",          # Net margin (TTM %)
    "ebitda_margin_ttm",       # EBITDA margin (TTM %)
    "ebit_margin_ttm",         # EBIT margin (TTM %)
    "return_on_assets_ttm",    # ROA (TTM %)
    "return_on_equity_ttm",    # ROE (TTM %)
    "return_on_invested_capital_ttm",  # ROIC (TTM %)
]

DIVIDENDS_TARGETS = [
    "name", "close",
    "dividends_yield",         # Dividend yield (TTM %)
    "dividends_yield_fwd",     # Forward yield (%)
    "dividend_payout_ratio_ttm",   # Payout ratio (TTM %)
    "dividends_per_share",     # Dividends per share (TTM)
    "dividends_paid_ttm",      # Dividends paid (TTM)
    "dividend_growth_rate_5y", # 5-year CAGR
    "ex_dividend_date",        # Ex-dividend date
]

def try_scan(session, columns):
    payload = {
        "filter": [{"left": "type", "operation": "in_range", "right": ["stock"]}],
        "options": {"lang": "en"},
        "symbols": {"query": {"types": []}, "tickers": []},
        "columns": columns,
        "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"},
        "range": [0, 1],
    }
    r = session.post(API_SCAN, headers=HEADERS, json=payload, timeout=30)
    return r.status_code < 400

def validate_columns(session, desired_cols):
    valid = []
    for c in desired_cols:
        if try_scan(session, valid + [c]):
            valid.append(c)
        else:
            print(f"[-] Dropping unsupported column: {c}")
            time.sleep(0.05)
    return valid

def fetch_batch(session, start, batch, columns):
    payload = {
        "filter": [{"left": "type", "operation": "in_range", "right": ["stock"]}],
        "options": {"lang": "en"},
        "symbols": {"query": {"types": []}, "tickers": []},
        "columns": columns,
        "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"},
        "range": [start, start + batch],
    }
    r = session.post(API_SCAN, headers=HEADERS, json=payload, timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(f"Scan HTTP {r.status_code}: {r.text[:300]}")
    return r.json()

def dollar(x):
    try: return f"${int(float(x)):,}"
    except: return ""

def num(x):
    try: return f"{float(x):.4f}"
    except: return ""

def pct(x):
    try: return f"{float(x):.2f}%"
    except: return ""

def pretty_record(rec):
    out = {}
    for k, v in rec.items():
        if v in (None, ""): out[k] = ""; continue
        if k == "close":
            out[k] = num(v)
        elif "margin" in k or "yield" in k or "growth" in k or "return_on_" in k:
            out[k] = pct(v)
        elif any(tok in k for tok in ["dividends_paid"]):
            out[k] = dollar(v)
        else:
            out[k] = str(v)
    return out

def collect_to_csv(session, targets, out_csv):
    columns = validate_columns(session, targets)
    if not columns:
        raise SystemExit(f"No supported columns for {out_csv}.")
    print(f"[{out_csv}] Using columns: {columns}")

    start, batch, rows, total = 0, 150, [], None
    while True:
        data = fetch_batch(session, start, batch, columns)
        items = data.get("data", [])
        if total is None: total = data.get("totalCount")
        if not items: break

        for it in items:
            vals = it.get("d", [])
            rec = {"ticker": it.get("s", "")}
            for col, val in zip(columns, vals):
                rec[col] = val
            rows.append(pretty_record(rec))

        start += batch
        time.sleep(0.2)
        if total is not None and start >= total: break

    fieldnames = ["ticker"] + columns
    with Path(out_csv).open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader(); w.writerows(rows)
    print(f"Saved {len(rows)} rows to {out_csv}")
    
    

def main():
    s = requests.Session(); s.headers.update(HEADERS)
    collect_to_csv(s, PROFITABILITY_TARGETS, "profitability.csv")
    collect_to_csv(s, DIVIDENDS_TARGETS, "dividends.csv")

if __name__ == "__main__":
    main()
