"""
AlphaVantage weekly fetcher and MongoDB persister.

Usage:
  - set ALPHAVANTAGE_KEY in environment (or create toolbox/sp/alphaventage.api.local with the key)
  - set MONGO_URI in environment (defaults to mongodb://localhost:27017)

Run example:
    python toolbox/sp/av_fetch_and_store.py --symbol SPY

This script will:
  - fetch TIME_SERIES_WEEKLY_ADJUSTED for the provided symbol
  - compute the most recent week's pct change (based on last two week-close values)
  - save the raw JSON into collection `alpha_weekly_raw`
  - save a derived summary into `alpha_weekly_summary` referencing the raw doc

Dependencies: requests, pymongo
"""

from __future__ import annotations
import os
import json
import argparse
import datetime
import time
from typing import Optional, Dict, Any

import requests
from pymongo import MongoClient
from pymongo.collection import Collection
from bson.objectid import ObjectId
import urllib3

# suppress insecure request warnings when verify is disabled by default
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ALPHA_URL = "https://www.alphavantage.co/query"
RAW_COLL = "alpha_weekly_raw"
SUMMARY_COLL = "alpha_weekly_summary"


def load_api_key(local_path: str = "toolbox/sp/alphaventage.api.local") -> Optional[str]:
    # Try env first
    api_key = os.environ.get("ALPHAVANTAGE_KEY")
    if api_key:
        return api_key.strip()
    # Then try local file
    try:
        if os.path.exists(local_path):
            with open(local_path, "r") as f:
                content = f.read().strip()
                if content:
                    return content.splitlines()[0].strip()
    except Exception:
        pass
    return None


class AlphaMongoClient:
    def __init__(self, mongo_uri: Optional[str] = None, db_name: str = "investment_weekly"):
        self.mongo_uri = mongo_uri or os.environ.get("MONGO_URI") or "mongodb://localhost:27017"
        self.client = MongoClient(self.mongo_uri)
        self.db = self.client[db_name]
        self.raw: Collection = self.db[RAW_COLL]
        self.summary: Collection = self.db[SUMMARY_COLL]

    def insert_raw(self, symbol: str, params: Dict[str, Any], raw_json: Dict[str, Any]) -> ObjectId:
        doc = {
            "symbol": symbol,
            "fetched_at": datetime.datetime.utcnow(),
            "query": params,
            "raw": raw_json,
        }
        res = self.raw.insert_one(doc)
        return res.inserted_id

    def insert_summary(self, summary_doc: Dict[str, Any]) -> ObjectId:
        res = self.summary.insert_one(summary_doc)
        return res.inserted_id

    def latest_raw_for_symbol(self, symbol: str) -> Optional[Dict[str, Any]]:
        return self.raw.find_one({"symbol": symbol}, sort=[("fetched_at", -1)])


def fetch_weekly(symbol: str, api_key: str, retry: int = 3, pause: float = 12.0, verify: bool = False) -> Dict[str, Any]:
    params = {
        "function": "TIME_SERIES_WEEKLY_ADJUSTED",
        "symbol": symbol,
        "apikey": api_key,
    }
    for attempt in range(1, retry + 1):
        resp = requests.get(ALPHA_URL, params=params, timeout=30, verify=verify)
        if resp.status_code == 200:
            data = resp.json()
            if "Note" in data or "Error Message" in data:
                # API limit or invalid request
                raise RuntimeError(f"AlphaVantage API error: {data.get('Note') or data.get('Error Message')}" )
            return {"params": params, "data": data}
        else:
            if attempt < retry:
                time.sleep(pause)
            else:
                resp.raise_for_status()
    raise RuntimeError("Failed to fetch weekly data")


def compute_last_week_change(weekly_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # weekly_data expected to be the 'Weekly Adjusted Time Series' or 'Weekly Time Series' key
    key_candidates = [k for k in weekly_data.keys() if k.lower().startswith('weekly')]
    if not key_candidates:
        return None
    series_key = key_candidates[0]
    series = weekly_data.get(series_key, {})
    # series keys are dates in YYYY-MM-DD order (descending if returned by API)
    dates = sorted(series.keys(), reverse=True)
    if len(dates) < 2:
        return None
    last = series[dates[0]]
    prev = series[dates[1]]
    try:
        last_close = float(last.get('4. close') or last.get('4. close'))
        prev_close = float(prev.get('4. close') or prev.get('4. close'))
    except Exception:
        return None
    pct_change = (last_close - prev_close) / prev_close * 100.0
    return {
        "week_end": dates[0],
        "last_close": last_close,
        "prev_close": prev_close,
        "pct_change": pct_change,
    }


def save_weekly_to_mongo(symbol: str, fetched: Dict[str, Any], mongo: AlphaMongoClient, avoid_duplicates: bool = True) -> Dict[str, Any]:
    params = fetched["params"]
    raw = fetched["data"]
    # check duplicate: if same series date matches latest raw, skip inserting duplicate raw
    last_summary = compute_last_week_change(raw)
    raw_id = None
    if avoid_duplicates:
        latest = mongo.latest_raw_for_symbol(symbol)
        if latest:
            # compare weekly latest date
            latest_raw_series = latest.get('raw', {})
            latest_summary = compute_last_week_change(latest_raw_series) if latest_raw_series else None
            if latest_summary and last_summary and latest_summary['week_end'] == last_summary['week_end']:
                # duplicate for same week end; still return existing summary info
                return {"inserted_raw_id": None, "existing_week": last_summary}
    # insert raw
    raw_id = mongo.insert_raw(symbol, params, raw)
    summary = last_summary or {}
    summary_doc = {
        "symbol": symbol,
        "week_end": summary.get('week_end'),
        "last_close": summary.get('last_close'),
        "prev_close": summary.get('prev_close'),
        "pct_change": summary.get('pct_change'),
        "fetched_at": datetime.datetime.utcnow(),
        "raw_id": raw_id,
    }
    summary_id = mongo.insert_summary(summary_doc)
    return {"inserted_raw_id": raw_id, "inserted_summary_id": summary_id, "summary": summary_doc}


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="SPY", help="Ticker symbol to fetch (default SPY)")
    parser.add_argument("--mongo-uri", default=None, help="MongoDB URI (overrides MONGO_URI env)")
    parser.add_argument("--db", default="investment_weekly", help="Mongo DB name")
    parser.add_argument("--no-dup-check", dest="dup_check", action="store_false", help="Disable duplicate-week check")
    args = parser.parse_args(argv)

    api_key = load_api_key()
    if not api_key:
        raise RuntimeError("ALPHAVANTAGE_KEY not found in env or toolbox/sp/alphaventage.api.local")

    mongo = AlphaMongoClient(mongo_uri=args.mongo_uri, db_name=args.db)

    fetched = fetch_weekly(args.symbol, api_key)
    res = save_weekly_to_mongo(args.symbol, fetched, mongo, avoid_duplicates=args.dup_check)
    print(json.dumps({"result": res}, default=str, indent=2))


if __name__ == "__main__":
    main()
