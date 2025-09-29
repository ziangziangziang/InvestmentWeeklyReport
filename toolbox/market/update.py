#!/usr/bin/env python3
# To update the stock data to the mongodb database
# 1. check the mongodb latest date for the symbol in collection "stock_daily"
# 2. fetch stock data from polygon.io from the next date to today using get_grouped_daily_aggs and insert to collection "stock_daily"
#   T string The exchange symbol that this item is traded under. -> ticker
#   c number The close price for the symbol in the given time period. -> close
#   h number The highest price for the symbol in the given time period. -> high
#   l number The lowest price for the symbol in the given time period. -> low
#   n integer optional The number of transactions in the aggregate window. -> transactions
#   o number The open price for the symbol in the given time period. -> open
#   otc boolean optional Whether or not this aggregate is for an OTC ticker. This field will be left off if false. -> otc
#   t integer The Unix millisecond timestamp for the end of the aggregate window. -> timestamp
#   v number The trading volume of the symbol in the given time period. -> volume
#   vw number optional The volume weighted average price. -> weighted_average_price
# 3. check the mongodb latest date for the symbol in collection "index_daily" for each index in INDEX_LIST
# 4. fetch new index data from yfinance from the next date to today and insert to collection "index_daily"
# 
# usage: python3 update.py

from polygon import RESTClient
from dotenv import load_dotenv
import yfinance as yf
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta, date, timezone
from typing import List, Dict, Any, Set
from pymongo import MongoClient
import yaml

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "FIN")
polygon_api_key = os.getenv("API_KEY")
assert polygon_api_key is not None, "API_KEY environment variable is required for polygon.io"

with open('directory.yml') as f:
	directory = yaml.safe_load(f)

INDEX_DICT = directory['indexes'] if 'indexes' in directory else {}
INDEX_LIST = list(INDEX_DICT.keys())

STOCK_COLLECTION = "stock_daily"
INDEX_COLLECTION = "index_daily"

DEFAULT_START_DATE = datetime.strptime(os.getenv("DEFAULT_START_DATE", "2025-01-01"), "%Y-%m-%d").date()
MAX_TICKERS = int(os.getenv("MAX_TICKERS", "0"))  # 0 = unlimited (dev helper)
SLEEP_SECONDS = float(os.getenv("API_SLEEP_SECONDS", "1.5"))  # simple pacing
MAX_INITIAL_DAYS = int(os.getenv("MAX_INITIAL_DAYS", "90"))  # limit initial catch-up span to reduce 429s
MAX_POLYGON_RETRIES = int(os.getenv("MAX_POLYGON_RETRIES", "10"))
BACKOFF_BASE = float(os.getenv("BACKOFF_BASE", "1.5"))
DRY_RUN = os.getenv("DRY_RUN", "0") == "1"
DEBUG_POLYGON = os.getenv("DEBUG_POLYGON", "0") == "1"

import time
try:
	import yaml  # type: ignore
except Exception:
	yaml = None  # fallback if PyYAML not installed; interest tickers feature will be skipped
try:
	from tqdm import tqdm  # type: ignore
except Exception:  # fallback noop if tqdm not installed
	def tqdm(iterable=None, **kwargs):
		return iterable
from pymongo import ASCENDING
from pymongo.errors import PyMongoError

# --- Helpers -----------------------------------------------------------------

def _ensure_indexes(db):
	db[STOCK_COLLECTION].create_index([("ticker", ASCENDING), ("date", ASCENDING)], unique=True)
	db[INDEX_COLLECTION].create_index([("ticker", ASCENDING), ("date", ASCENDING)], unique=True)


def _as_date(ts_ms: int) -> date:
	# Polygon gives unix ms timestamp (UTC)
	return datetime.fromtimestamp(ts_ms / 1000, timezone.utc).date()


def _daterange(start_date: date, end_date: date):
	cur = start_date
	while cur <= end_date:
		yield cur
		cur += timedelta(days=1)


def get_last_stock_date(db) -> date | None:
	doc = db[STOCK_COLLECTION].find_one({}, sort=[("date", -1)])
	return doc["date"].date() if doc and isinstance(doc.get("date"), datetime) else doc.get("date") if doc else None


def get_last_index_date(db, ticker: str) -> date | None:
	doc = db[INDEX_COLLECTION].find_one({"ticker": ticker}, sort=[("date", -1)])
	if not doc:
		return None
	d = doc.get("date")
	if isinstance(d, datetime):
		return d.date()
	return d


def fetch_and_upsert_stocks(db):
	tqdm.write("[stocks] starting grouped daily aggregation ingestion via polygon")
	last_date = get_last_stock_date(db)
	today = datetime.now(timezone.utc).date()
	if last_date is None:
		start_date = DEFAULT_START_DATE
		qdm = tqdm.write
		qdm(f"[stocks] no existing data; defaulting start_date={start_date} (configured DEFAULT_START_DATE)")
	else:
		start_date = last_date + timedelta(days=1)
		qdm = tqdm.write
		qdm(f"[stocks] last stored date={last_date}; start_date={start_date}")

	if start_date > today:
		tqdm.write("[stocks] up to date; nothing to do")
		return

	client = RESTClient(polygon_api_key)
	coll = db[STOCK_COLLECTION]
	total_docs = 0
	# Limit initial catch-up window if environment variable requests it
	if last_date is None and MAX_INITIAL_DAYS > 0:
		max_start = today - timedelta(days=MAX_INITIAL_DAYS)
		if start_date < max_start:
			tqdm.write(f"[stocks] trimming initial span to last {MAX_INITIAL_DAYS} days (from {start_date} -> {max_start})")
			start_date = max_start

	days = list(_daterange(start_date, today))

	def request_with_retry(date_str: str):
		"""Fetch grouped daily aggregates with resilience and flexible response handling.

		Polygon python client versions may either:
		- return a list[GroupedDailyAgg]
		- return an object with a .results attribute (legacy style)
		We normalize to a list of aggregate objects.
		"""
		if DRY_RUN:
			tqdm.write(f"[stocks][dry-run] would fetch grouped daily {date_str}")
			return []
		attempt = 0
		while True:
			try:
				try:
					resp_local = client.get_grouped_daily_aggs(date=date_str, adjusted=True, locale="us")
				except TypeError:
					# Fallback for older SDK signature (date, adjusted) only
					resp_local = client.get_grouped_daily_aggs(date=date_str, adjusted=True)
				# Normalize response to list
				if isinstance(resp_local, (list, tuple)):
					return list(resp_local)
				results_attr = getattr(resp_local, "results", None)
				if results_attr is not None:
					if isinstance(results_attr, (list, tuple)):
						return list(results_attr)
					# Some SDK variants may expose an iterator
					try:
						return list(results_attr)
					except TypeError:
						return []
				# Fallback: if the response itself is iterable
				try:
					return list(iter(resp_local))
				except TypeError:
					return []
			except Exception as e:
				msg = str(e)
				rate_limited = ("429" in msg) or ("too many 429" in msg.lower()) or ("rate" in msg.lower() and "limit" in msg.lower())
				attempt += 1
				if rate_limited:
					if attempt > MAX_POLYGON_RETRIES:
						tqdm.write(f"[stocks] rate limit persists after {MAX_POLYGON_RETRIES} retries for {date_str}; skipping day")
						return []
					backoff = BACKOFF_BASE ** attempt
					if backoff > 20:
						sec_to_next_min = 60 - (time.time() % 60)
						backoff = max(backoff, sec_to_next_min + 1)
					tqdm.write(f"[stocks] 429 rate limit on {date_str}; retry {attempt}/{MAX_POLYGON_RETRIES} after {backoff:.1f}s")
					time.sleep(backoff)
					continue
				else:
					tqdm.write(f"[stocks] ERROR fetching {date_str}: {e}")
					return []
	for day in tqdm(days, desc="stocks-days", disable=not days):
		# Skip weekends (Polygon grouped daily will return results but normally only trading days matter)
		if day.weekday() >= 5:  # 5=Sat,6=Sun
			continue
		date_str = day.strftime("%Y-%m-%d")
		# minimal per-day logging; progress bar is primary indicator
		# tqdm.write(f"[stocks] fetching grouped daily aggs for {date_str}")
		results = request_with_retry(date_str)
		if not results:
			if DEBUG_POLYGON:
				# Run a probe for a well-known liquid ticker to see if per-ticker endpoint works
				try:
					probe_ticker = "AAPL"
					oc = client.get_daily_open_close_agg(ticker=probe_ticker, date=date_str, adjusted=True)
					open_price = getattr(oc, "open", None) or getattr(oc, "o", None)
					close_price = getattr(oc, "close", None) or getattr(oc, "c", None)
					if open_price is not None and close_price is not None:
						tqdm.write(f"[stocks][debug] grouped empty {date_str} BUT open/close probe {probe_ticker} open={open_price} close={close_price}")
					else:
						tqdm.write(f"[stocks][debug] open/close probe returned no prices for {probe_ticker} {date_str}")
				except Exception as e:
					tqdm.write(f"[stocks][debug] open/close probe failed {date_str}: {e}")
			# Avoid spamming for weekends already skipped; log once per missing trading day.
			tqdm.write(f"[stocks] no results for {date_str}")
			continue
		batch = 0
		iterable_aggs = results if not MAX_TICKERS else results[:MAX_TICKERS]
		for agg in tqdm(iterable_aggs, desc=f"{date_str} aggs", leave=False):
			try:
				# Support both modern attribute names and legacy single-letter keys
				ticker = getattr(agg, "ticker", None) or getattr(agg, "T", None)
				if not ticker:
					continue
				if MAX_TICKERS and batch >= MAX_TICKERS:
					break
				ts_ms = getattr(agg, "timestamp", None) or getattr(agg, "t", None)
				if not ts_ms:
					continue
				d = _as_date(ts_ms)
				doc = {
					"ticker": ticker,
					"date": datetime(d.year, d.month, d.day),
					"open": getattr(agg, "open", None) or getattr(agg, "o", None),
					"high": getattr(agg, "high", None) or getattr(agg, "h", None),
					"low": getattr(agg, "low", None) or getattr(agg, "l", None),
					"close": getattr(agg, "close", None) or getattr(agg, "c", None),
					"volume": getattr(agg, "volume", None) or getattr(agg, "v", None),
					"transactions": getattr(agg, "transactions", None) or getattr(agg, "n", None),
					"otc": getattr(agg, "otc", None),
					"weighted_average_price": getattr(agg, "weighted_average_price", None) or getattr(agg, "vw", None),
					"source": "polygon",
					"ingested_at": datetime.now(timezone.utc),
				}
				coll.update_one({"ticker": ticker, "date": doc["date"]}, {"$set": doc}, upsert=True)
				batch += 1
			except PyMongoError as me:
				qdm = tqdm.write
				qdm(f"[stocks] mongo error for ticker {ticker}: {me}")
			except Exception as ex:
				qdm = tqdm.write
				qdm(f"[stocks] generic error for ticker {ticker}: {ex}")
		total_docs += batch
		if batch:
			tqdm.write(f"[stocks] {date_str} upserted {batch} docs")
		time.sleep(SLEEP_SECONDS)
	tqdm.write(f"[stocks] done. total new/updated docs approx={total_docs}")


def _load_interest_tickers() -> Set[str]:
	"""Parse the multi-document YAML file stock.yml and collect all tickers.

	Expected structure per document:
	category_name:
	  - description: "..."
		tickers: [TICK1, TICK2, ...]
	"""
	tickers: Set[str] = set()
	stock_yml_path = Path(__file__).resolve().parent / "stock.yml"
	if not stock_yml_path.exists():
		return tickers
	if yaml is None:
		tqdm.write("[interest] PyYAML not installed; skipping interest tickers load")
		return tickers
	try:
		with stock_yml_path.open("r", encoding="utf-8") as f:
			for doc in yaml.safe_load_all(f):  # type: ignore[attr-defined]
				if not isinstance(doc, dict):
					continue
				for _category, items in doc.items():
					if not isinstance(items, list):
						continue
					for block in items:
						if not isinstance(block, dict):
							continue
						arr = block.get("tickers") or []
						if isinstance(arr, list):
							for t in arr:
								if isinstance(t, str) and t.strip():
									tickers.add(t.strip().upper())
	except Exception as e:
		tqdm.write(f"[interest] failed to parse stock.yml: {e}")
	return tickers


def _get_last_date_for_ticker(db, ticker: str) -> date | None:
	doc = db[STOCK_COLLECTION].find_one({"ticker": ticker}, sort=[("date", -1)])
	if not doc:
		return None
	d = doc.get("date")
	if isinstance(d, datetime):
		return d.date()
	return d


def _is_trading_day(d: date) -> bool:
	# Basic filter: Mon-Fri; (Could be enhanced with exchange holiday calendar if needed)
	return d.weekday() < 5


def _per_ticker_open_close(client: RESTClient, ticker: str, date_str: str):
	"""Wrapper to fetch per-ticker daily open/close with backwards compatibility on SDK versions."""
	# Avoid DRY_RUN data writes
	if DRY_RUN:
		tqdm.write(f"[interest][dry-run] would fetch {ticker} {date_str}")
		return None
	try:
		try:
			return client.get_daily_open_close_agg(ticker=ticker, date=date_str, adjusted=True)
		except TypeError:
			# older signature maybe positional
			return client.get_daily_open_close_agg(ticker, date_str, adjusted=True)  # type: ignore[call-arg]
	except Exception as e:
		msg = str(e)
		rate_limited = ("429" in msg) or ("rate" in msg.lower() and "limit" in msg.lower())
		if rate_limited:
			tqdm.write(f"[interest] rate limit fetching {ticker} {date_str}; will retry once after backoff")
			time.sleep(2.5)
			try:
				return client.get_daily_open_close_agg(ticker=ticker, date=date_str, adjusted=True)
			except Exception as e2:
				tqdm.write(f"[interest] retry failed {ticker} {date_str}: {e2}")
				return None
		else:
			tqdm.write(f"[interest] error fetching {ticker} {date_str}: {e}")
			return None


def backfill_interest_tickers(db):
	"""Ensure the configured interest tickers have data up to the latest global stock date.

	Strategy:
	  1. Determine global max date across stock collection (reference date).
	  2. For each interest ticker, check its last stored date.
	  3. If missing days up to global max, fetch each missing trading day via per-ticker open/close endpoint.
	"""
	interest = _load_interest_tickers()
	if not interest:
		return
	global_last = get_last_stock_date(db)
	if global_last is None:
		# Nothing in DB yet, grouped ingestion likely skipped; skip
		tqdm.write("[interest] no global stock data yet; skipping backfill")
		return
	client = RESTClient(polygon_api_key)
	coll = db[STOCK_COLLECTION]
	total_inserted = 0
	tqdm.write(f"[interest] validating {len(interest)} tickers against global_last={global_last}")
	for ticker in tqdm(sorted(interest), desc="interest"):
		last_ticker_date = _get_last_date_for_ticker(db, ticker)
		# If we already have the ticker up to global last date, skip
		if last_ticker_date == global_last:
			continue
		# Determine start of gap
		start_date = DEFAULT_START_DATE if last_ticker_date is None else last_ticker_date + timedelta(days=1)
		if start_date > global_last:
			continue
		missing_days = [d for d in _daterange(start_date, global_last) if _is_trading_day(d)]
		if not missing_days:
			continue
		inserted_for_ticker = 0
		for d in missing_days:
			date_str = d.strftime("%Y-%m-%d")
			oc = _per_ticker_open_close(client, ticker, date_str)
			if not oc:
				continue
			try:
				# Map fields with graceful degradation
				open_p = getattr(oc, "open", None) or getattr(oc, "o", None)
				close_p = getattr(oc, "close", None) or getattr(oc, "c", None)
				high_p = getattr(oc, "high", None) or getattr(oc, "h", None)
				low_p = getattr(oc, "low", None) or getattr(oc, "l", None)
				volume_v = getattr(oc, "volume", None) or getattr(oc, "v", None)
				if open_p is None and close_p is None:
					# Nothing meaningful; skip
					continue
				doc_date = datetime(d.year, d.month, d.day)
				doc = {
					"ticker": ticker,
					"date": doc_date,
					"open": open_p,
					"high": high_p,
					"low": low_p,
					"close": close_p,
					"volume": volume_v,
					"transactions": None,
					"otc": getattr(oc, "otc", None),
					"weighted_average_price": None,
					"source": "polygon-open-close",
					"ingested_at": datetime.now(timezone.utc),
				}
				coll.update_one({"ticker": ticker, "date": doc_date}, {"$set": doc}, upsert=True)
				inserted_for_ticker += 1
				total_inserted += 1
			except PyMongoError as me:
				tqdm.write(f"[interest] mongo error {ticker} {date_str}: {me}")
			except Exception as e:
				tqdm.write(f"[interest] generic error {ticker} {date_str}: {e}")
			time.sleep(0.25)  # gentle pacing per ticker/day
		if inserted_for_ticker:
			tqdm.write(f"[interest] {ticker} backfilled {inserted_for_ticker} days")
	if total_inserted:
		tqdm.write(f"[interest] completed backfill; total new docs={total_inserted}")
	else:
		tqdm.write("[interest] all interest tickers already up-to-date")


def _init_cert_override_if_needed():
	# mimic logic from test.py for corporate proxy TLS issues
	try:
		import certifi  # type: ignore
		os.environ.setdefault("CURL_CA_BUNDLE", certifi.where())
		os.environ.setdefault("REQUESTS_CA_BUNDLE", certifi.where())
		os.environ.setdefault("SSL_CERT_FILE", certifi.where())
	except Exception:
		pass


def fetch_and_upsert_indices(db):
	tqdm.write("[indices] starting yfinance ingestion")
	_init_cert_override_if_needed()
	coll = db[INDEX_COLLECTION]
	today = datetime.now(timezone.utc).date()
	rows_total = 0
	tickers_with_data = 0

	for ticker in tqdm(INDEX_LIST, desc="indices"):
		last_date = get_last_index_date(db, ticker)
		if last_date is None:
			start_date = DEFAULT_START_DATE
			tqdm.write(f"[indices] {ticker}: no data, defaulting start_date={start_date} (configured DEFAULT_START_DATE)")
		else:
			start_date = last_date + timedelta(days=1)
			tqdm.write(f"[indices] {ticker}: last={last_date} start={start_date}")
		if start_date > today:
			# up to date
			continue
		end_date = today + timedelta(days=1)  # yfinance end exclusive
		yf_ticker = yf.Ticker(ticker)
		try:
			hist = yf_ticker.history(start=start_date, end=end_date)
		except Exception as e:
			msg = str(e)
			is_cert_err = "SSL certificate" in msg or "self signed certificate" in msg
			if is_cert_err:
				try:
					from curl_cffi import requests as curl_requests  # type: ignore
					from yfinance import data as yf_data  # type: ignore
					insecure_session = curl_requests.Session(verify=False, impersonate="chrome")
					yf_data.YfData(session=insecure_session)
					tqdm.write(f"[indices] {ticker}: retrying with insecure TLS (NOT FOR PROD)")
					hist = yf_ticker.history(start=start_date, end=end_date)
				except Exception as e2:
					tqdm.write(f"[indices] {ticker}: failed insecure retry {e2}")
					continue
			else:
				qdm = tqdm.write
				qdm(f"[indices] {ticker}: error fetching history: {e}")
				continue
		if hist.empty:
			continue
		inserted = 0
		for idx, row in hist.iterrows():
			d = idx.date()
			doc_date = datetime(d.year, d.month, d.day)
			doc = {
				"ticker": ticker,
				"date": doc_date,
				"open": float(row.get("Open", float("nan"))),
				"high": float(row.get("High", float("nan"))),
				"low": float(row.get("Low", float("nan"))),
				"close": float(row.get("Close", float("nan"))),
				"volume": None if "Volume" not in row or row.get("Volume") != row.get("Volume") else float(row.get("Volume")),
				"source": "yfinance",
				"ingested_at": datetime.now(timezone.utc),
			}
			try:
				coll.update_one({"ticker": ticker, "date": doc_date}, {"$set": doc}, upsert=True)
				inserted += 1
			except PyMongoError as me:
				qdm = tqdm.write
				qdm(f"[indices] {ticker}: mongo error {me}")
		if inserted:
			rows_total += inserted
			tickers_with_data += 1
		time.sleep(0.2)
	tqdm.write(f"[indices] done. tickers_with_data={tickers_with_data} total_rows_upserted={rows_total}")


def main():
	mongo_client = MongoClient(MONGO_URI)
	db = mongo_client[DB_NAME]
	_ensure_indexes(db)
	fetch_and_upsert_stocks(db)
	# After bulk grouped ingestion, ensure interest tickers are filled using per-ticker endpoint
	backfill_interest_tickers(db)
	fetch_and_upsert_indices(db)
	tqdm.write("All ingestion tasks complete.")


if __name__ == "__main__":
	main()
