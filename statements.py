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

BALANCE_SHEET_TARGETS = [
    "name", "close",
    "total_assets_fq",
    "current_assets_fq",
    "cash_n_short_term_invest_fq",  # Cash on hand (FQ)
    "total_liabilities_fq",
    "total_debt_fq",
    "net_debt_fq",
    "total_equity_fq",
    "current_ratio_fq",
    "quick_ratio_fq",
    "debt_to_equity_fq",
    "cash_to_debt_fq",
]

INCOME_STATEMENT_TARGETS = [
    "name", "close",
    "total_revenue_ttm",          # Revenue (TTM)
    "revenue_growth_yoy",         # Revenue growth (TTM YoY %)
    "gross_profit_ttm",           # Gross profit (TTM)
    "operating_income_ttm",       # Operating income (TTM)
    "net_income_ttm",             # Net income (TTM)
    "ebitda_ttm",                 # EBITDA (TTM)
    "eps_diluted_ttm",            # EPS dil (TTM)
    "eps_diluted_yoy_growth_ttm", # EPS dil growth (TTM YoY %)
]

CASHFLOW_TARGETS = [
    "name", "close",
    "cash_flow_from_operating_activities_ttm",  # Operating CF (TTM)
    "cash_flow_from_investing_activities_ttm",  # Investing CF (TTM)
    "cash_flow_from_financing_activities_ttm",  # Financing CF (TTM)
    "free_cash_flow_ttm",                       # FCF (TTM)
    "capital_expenditure_ttm",                  # CAPEX (TTM)
]

def try_scan(session, columns):
    """Return True if columns are accepted by the scan API, else False."""
    payload = {
        "filter": [{"left": "type", "operation": "in_range", "right": ["stock"]}],
        "options": {"lang": "en"},
        "symbols": {"query": {"types": []}, "tickers": []},
        "columns": columns,
        "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"},
        "range": [0, 1],  # probe with tiny range
    }
    r = session.post(API_SCAN, headers=HEADERS, json=payload, timeout=30)
    return r.status_code < 400

def validate_columns(session, desired_cols):
    """Keep only columns that the API currently accepts."""
    valid = []
    for c in desired_cols:
        if try_scan(session, valid + [c]):
            valid.append(c)
        else:
            print(f"[-] Dropping unsupported column: {c}")
            time.sleep(0.1)
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
        if k in {"close"} or k.startswith("eps"):
            out[k] = num(v)
        elif k.endswith("_margin") or "growth" in k:
            out[k] = pct(v)
        elif any(tok in k for tok in
                 ["cap", "cash", "debt", "assets", "liabilities", "equity", "expenditure", "profit", "income", "revenue"]):
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
    collect_to_csv(s, BALANCE_SHEET_TARGETS, "balance_sheet.csv")
    collect_to_csv(s, INCOME_STATEMENT_TARGETS, "income_statement.csv")
    collect_to_csv(s, CASHFLOW_TARGETS, "cashflow.csv")

if __name__ == "__main__":
    main()
