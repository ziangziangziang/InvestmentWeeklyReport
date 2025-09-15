"""
Weekly SP sector analyser.

Computes for a list of sector ETFs:
- YTD change (compare latest close to earliest stored close in the same calendar year)
- Weekly change (latest week's pct_change from summary)
- Week-to-week growth change (difference between latest week's pct_change and previous week's pct_change)

Requires MongoDB populated by av_fetch_and_store.py (collections: alpha_weekly_raw, alpha_weekly_summary)

Usage:
    python toolbox/sp/weekly_sp_analyse.py

Output: prints a table and dumps JSON to stdout
"""
from __future__ import annotations
import os
import json
import datetime
from typing import List, Dict, Any, Optional
from pymongo import MongoClient
# reuse alphaventage helpers for fetching and saving
try:
    # when run as package
    from toolbox.sp.alphaventage import AlphaMongoClient, fetch_weekly, load_api_key, save_weekly_to_mongo
except Exception:
    # fallback when running script from the same directory
    from alphaventage import AlphaMongoClient, fetch_weekly, load_api_key, save_weekly_to_mongo
import argparse

SECTORS = [
    "SPY",
    "XLY",  # Consumer Discretionary
    "XLP",  # Consumer Staples
    "XLE",  # Energy
    "XLF",  # Financials
    "XLV",  # Health Care
    "XLI",  # Industrials
    "XLB",  # Materials
    "XLK",  # Technology
    "XLC",  # Communication Services
    "XLRE", # Real Estate
    "XLU",  # Utilities
]

DB_NAME = os.environ.get("SP_DB", "FIN")
MONGO_URI = os.environ.get("MONGO_URI") or "mongodb://localhost:27017"

# map ticker symbols to human-readable sector names
TICKER_TO_SECTOR = {
    "SPY": "S&P 500",
    "XLY": "Consumer Discretionary",
    "XLP": "Consumer Staples",
    "XLE": "Energy",
    "XLF": "Financials",
    "XLV": "Health Care",
    "XLI": "Industrials",
    "XLB": "Materials",
    "XLK": "Technology",
    "XLC": "Communication Services",
    "XLRE": "Real Estate",
    "XLU": "Utilities",
}


class WeeklyAnalyser:
    def __init__(self, mongo_uri: str = MONGO_URI, db_name: str = DB_NAME):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.summary = self.db["alpha_weekly_summary"]
        self.raw = self.db["alpha_weekly_raw"]

    def latest_summaries(self, symbol: str, limit: int = 4) -> List[Dict[str, Any]]:
        # Return latest `limit` summary docs for the symbol ordered by week_end desc
        docs = list(self.summary.find({"symbol": symbol}).sort("week_end", -1).limit(limit))
        return docs

    def year_start_close(self, symbol: str, year: int) -> Optional[float]:
        # Attempt to find the earliest stored week_end in the given year and return its last_close
        cursor = self.summary.find({"symbol": symbol, "week_end": {"$regex": f"^{year}-"}}).sort("week_end", 1).limit(1)
        first = list(cursor)
        if first:
            return first[0].get("last_close")
        # Fallback: scan raw data in that year for earliest week
        # Prefer the latest raw doc (it usually contains the full historical series)
        latest_raw = self.raw.find_one({"symbol": symbol, "raw": {"$exists": True}}, sort=[("fetched_at", -1)])
        if latest_raw:
            raw = latest_raw.get("raw", {})
            key = next((k for k in raw.keys() if k.lower().startswith("weekly")), None)
            if key:
                series = raw.get(key, {})
                dates = sorted([d for d in series.keys() if d.startswith(str(year) + "-")])
                if dates:
                    return float(series[dates[0]].get("4. close"))
        # If latest doesn't have it, scan older raw docs (earliest fetched first)
        cursor2 = self.raw.find({"symbol": symbol, "raw": {"$exists": True}}).sort("fetched_at", 1)
        for doc in cursor2:
            raw = doc.get("raw", {})
            key = next((k for k in raw.keys() if k.lower().startswith("weekly")), None)
            if not key:
                continue
            series = raw.get(key, {})
            dates = sorted([d for d in series.keys() if d.startswith(str(year) + "-")])
            if dates:
                return float(series[dates[0]].get("4. close"))
        return None

    def weekly_pct_from_raw(self, symbol: str, index: int = 0) -> Optional[float]:
        """Return the weekly pct_change for the week at position `index` in the series (0=latest,1=previous)."""
        cursor = self.raw.find({"symbol": symbol, "raw": {"$exists": True}}).sort("fetched_at", -1).limit(1)
        docs = list(cursor)
        if not docs:
            return None
        raw = docs[0].get("raw", {})
        key = next((k for k in raw.keys() if k.lower().startswith("weekly")), None)
        if not key:
            return None
        series = raw.get(key, {})
        dates = sorted(series.keys(), reverse=True)
        # need at least index+2 dates to compute pct for that week (close_i and close_{i+1})
        if len(dates) <= index + 1:
            return None
        try:
            close_i = float(series[dates[index]].get("4. close"))
            close_next = float(series[dates[index + 1]].get("4. close"))
            pct = (close_i - close_next) / close_next * 100.0
            return pct
        except Exception:
            return None

    def analyse_symbol(self, symbol: str) -> Dict[str, Any]:
        docs = self.latest_summaries(symbol, limit=4)
        latest = docs[0] if docs else None
        prev = docs[1] if len(docs) > 1 else None
        prev2 = docs[2] if len(docs) > 2 else None

        result = {"symbol": symbol}
        if latest:
            result.update({
                "week_end": latest.get("week_end"),
                "last_close": latest.get("last_close"),
                "weekly_change_pct": latest.get("pct_change"),
            })
        else:
            result.update({"note": "no summary data"})

        # weekly change vs previous week change
        if latest:
            latest_pct = latest.get("pct_change") or 0.0
            # prefer previous summary pct, fallback to raw series previous week
            if prev:
                prev_pct = prev.get("pct_change") or 0.0
            else:
                prev_pct = self.weekly_pct_from_raw(symbol, index=1)
            result["week_to_week_growth"] = (latest_pct - prev_pct) if prev_pct is not None else None
        else:
            result["week_to_week_growth"] = None

        # YTD change
        year = datetime.datetime.utcnow().year
        ystart = self.year_start_close(symbol, year)
        # If year_start_close is missing or equals latest (meaning no earlier data), try extracting from raw series
        if (not ystart or (latest and latest.get("last_close") == ystart)):
            # attempt to get earliest in-year close from raw series
            raw_ystart = None
            latest_raw = self.raw.find_one({"symbol": symbol, "raw": {"$exists": True}}, sort=[("fetched_at", -1)])
            if latest_raw:
                raw = latest_raw.get("raw", {})
                key = next((k for k in raw.keys() if k.lower().startswith("weekly")), None)
                if key:
                    series = raw.get(key, {})
                    dates = sorted([d for d in series.keys() if d.startswith(str(year) + "-")])
                    if dates:
                        raw_ystart = float(series[dates[0]].get("4. close"))
            if raw_ystart:
                ystart = raw_ystart

        if ystart and latest:
            try:
                ytd_pct = (latest.get("last_close") - ystart) / ystart * 100.0
            except Exception:
                ytd_pct = None
            result["ytd_pct"] = ytd_pct
            result["year_start_close"] = ystart
        else:
            result["ytd_pct"] = None
            result["year_start_close"] = ystart

        # include previous week's pct for context
        result["previous_week_pct"] = prev.get("pct_change") if prev else None
        result["previous2_week_pct"] = prev2.get("pct_change") if prev2 else None

        return result

    def run_all(self, symbols: List[str]) -> List[Dict[str, Any]]:
        out = []
        for s in symbols:
            out.append(self.analyse_symbol(s))
        return out

    def has_summary_for(self, symbol: str) -> bool:
        return self.summary.count_documents({"symbol": symbol}) > 0


def pretty_print(results: List[Dict[str, Any]]):
    # simple aligned table
    headers = ["symbol", "week_end", "last_close", "weekly_change%", "week_to_week_g%", "ytd%"]
    print("\t".join(headers))
    for r in results:
        row = [
            r.get("symbol", ""),
            r.get("week_end", ""),
            f"{r.get('last_close', ''):.2f}" if r.get('last_close') is not None else "",
            f"{r.get('weekly_change_pct', 0.0):.2f}" if r.get('weekly_change_pct') is not None else "",
            f"{r.get('week_to_week_growth', 0.0):.2f}" if r.get('week_to_week_growth') is not None else "",
            f"{r.get('ytd_pct', 0.0):.2f}" if r.get('ytd_pct') is not None else "",
        ]
        print("\t".join(row))


def print_sp500_table(results: List[Dict[str, Any]]):
    """
    Print a markdown table with the header:
    | Sector              | Weekly Return (%) | WeekLy Change| YTD Return (%) |

    Rows will be ordered as provided in results.
    """
    header = "| Sector | Weekly Return (%) | WeekLy Change | YTD Return (%) |"
    sep = "|---|---:|---:|---:|"
    print(header)
    print(sep)
    for r in results:
        sym = r.get("symbol", "")
        sector = TICKER_TO_SECTOR.get(sym, sym)
        weekly = f"{r.get('weekly_change_pct', 0.0):.2f}" if r.get('weekly_change_pct') is not None else ""
        week_change = f"{r.get('week_to_week_growth', 0.0):.2f}" if r.get('week_to_week_growth') is not None else ""
        ytd = f"{r.get('ytd_pct', 0.0):.2f}" if r.get('ytd_pct') is not None else ""
        print(f"| {sector:<6} | {weekly:>16} | {week_change:>12} | {ytd:>14} |")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Weekly SP sector analyser")
    parser.add_argument("--symbols", help="Comma-separated list of symbols to analyse (default: SECTORS)")
    parser.add_argument("--sp500", action="store_true", help="Print SP500-style markdown table")
    parser.add_argument("--mock", action="store_true", help="Use mock/sample data instead of querying MongoDB (for testing)")
    parser.add_argument("--fetch-missing", action="store_true", help="Fetch missing symbols from AlphaVantage and store to DB before analysis")
    parser.add_argument("--secure", action="store_true", help="Enable SSL verification when fetching from AlphaVantage (default is insecure)")
    parser.add_argument("--recompute-ytd", action="store_true", help="Recompute YTD for symbols from stored raw series and update summary documents")
    args = parser.parse_args()

    analyser = WeeklyAnalyser()
    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    else:
        symbols = SECTORS

    if args.mock:
        # create simple mock results with plausible numbers for testing
        import random
        results = []
        for s in symbols:
            weekly = round(random.uniform(-3.0, 3.0), 2)
            week_to_week = round(random.uniform(-1.5, 1.5), 2)
            ytd = round(random.uniform(-10.0, 40.0), 2)
            results.append({
                "symbol": s,
                "week_end": "2025-09-12",
                "last_close": round(random.uniform(50, 500), 2),
                "weekly_change_pct": weekly,
                "week_to_week_growth": week_to_week,
                "ytd_pct": ytd,
            })
    else:
        # optionally fetch missing symbols from AlphaVantage before running
        if args.fetch_missing:
            api_key = load_api_key()
            if not api_key:
                print("ALPHAVANTAGE_KEY not found; cannot fetch missing data")
            else:
                amin = AlphaMongoClient(mongo_uri=os.environ.get('MONGO_URI'), db_name=os.environ.get('SP_DB', 'FIN'))
                for s in symbols:
                    if not analyser.has_summary_for(s):
                        try:
                            fetched = fetch_weekly(s, api_key, verify=args.secure)
                            res = save_weekly_to_mongo(s, fetched, amin, avoid_duplicates=True)
                            print(f"Fetched and saved data for {s}: {res}")
                        except Exception as e:
                            print(f"Error fetching {s}: {e}")
        results = analyser.run_all(symbols)

    # optionally recompute YTD in DB (updates latest summary doc per symbol)
    if args.recompute_ytd and not args.mock:
        year = datetime.datetime.utcnow().year
        for s in symbols:
            ystart = analyser.year_start_close(s, year)
            # find latest summary doc
            latest = analyser.summary.find_one({"symbol": s}, sort=[("week_end", -1)])
            if latest and ystart:
                try:
                    ytd_pct = (latest.get("last_close") - ystart) / ystart * 100.0
                except Exception:
                    ytd_pct = None
                update = {"ytd_pct": ytd_pct, "year_start_close": ystart}
                analyser.summary.update_one({"_id": latest.get("_id")}, {"$set": update})
                print(f"Updated YTD for {s}: {update}")
    if args.sp500:
        print_sp500_table(results)
    else:
        pretty_print(results)

    # also print machine-readable json
    print(json.dumps(results, indent=2, default=str))
