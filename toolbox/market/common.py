"""Shared market utilities for scripts/notebooks.

Provide a lazy loader `load_context()` that returns a dict of commonly used
variables (NAME_MAP, ALL_TICKERS, Mongo collections, price frames, and
week window bounds). This keeps heavy operations behind an explicit call
so importing the module is cheap and safe.
"""
import os
from datetime import datetime, timedelta, date
from typing import Dict
import yaml
import pandas as pd
import numpy as np
from pymongo import MongoClient

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
DB_NAME = os.getenv('DB_NAME', 'FIN')
STOCK_COLLECTION = os.getenv('STOCK_COLLECTION', 'stock_daily')
INDEX_COLLECTION = os.getenv('INDEX_COLLECTION', 'index_daily')
INDEX_TICKER = os.getenv('SP500_TICKER', '^SPX')


def load_context(lookback_days: int = None) -> Dict:
    """Return a dict with shared variables used by analysis scripts.

    Keys returned include:
    - NAME_MAP: mapping ticker -> human name from directory.yml
    - ALL_TICKERS: list of tickers (index + sectors)
    - client, db, stock_coll, index_coll
    - prices: pivoted close prices (index=date, columns=ticker)
    - idx_df: flat dataframe of index records (date, ticker, close, high, low)
    - start_week, end_week, start_dt, end_dt
    - idx_proj: standard projection used for index queries

    The function is defensive: if MongoDB is not available it will still
    return NAME_MAP and empty dataframes with helpful defaults.
    """
    # Config and directory
    cwd = os.getcwd()
    dir_path = os.path.join(cwd, 'directory.yml')
    NAME_MAP: Dict[str, str] = {}
    try:
        with open(dir_path, 'r', encoding='utf-8') as f:
            directory = yaml.safe_load(f) or {}
            NAME_MAP = directory.get('indexes', {})
    except FileNotFoundError:
        # best-effort: continue with empty NAME_MAP
        NAME_MAP = {}

    TODAY = date.today()
    if lookback_days is None:
        LOOKBACK_DAYS = int(os.getenv('LOOKBACK_DAYS', '180'))
    else:
        LOOKBACK_DAYS = lookback_days
    END_DATE = datetime.strptime(os.getenv('END_DATE', TODAY.strftime('%Y-%m-%d')), '%Y-%m-%d').date()
    START_DATE = datetime.strptime(os.getenv('START_DATE', (END_DATE - timedelta(days=LOOKBACK_DAYS)).strftime('%Y-%m-%d')), '%Y-%m-%d').date()

    ALL_TICKERS = list(NAME_MAP.keys()) if NAME_MAP else []
    if INDEX_TICKER and INDEX_TICKER not in ALL_TICKERS:
        # ensure index ticker present first
        ALL_TICKERS = [INDEX_TICKER] + [t for t in ALL_TICKERS if t != INDEX_TICKER]

    # Connect to MongoDB (best-effort)
    client = None
    db = None
    stock_coll = None
    index_coll = None
    prices = pd.DataFrame()
    idx_df = pd.DataFrame()
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        db = client[DB_NAME]
        stock_coll = db[STOCK_COLLECTION]
        index_coll = db[INDEX_COLLECTION]

        # Load index records (close/high/low) for the configured tickers
        if ALL_TICKERS:
            query = {
                'ticker': {'$in': ALL_TICKERS},
                'date': {'$gte': datetime(START_DATE.year, START_DATE.month, START_DATE.day),
                         '$lte': datetime(END_DATE.year, END_DATE.month, END_DATE.day)}
            }
            idx_proj = {'_id': 0, 'ticker': 1, 'date': 1, 'close': 1, 'high': 1, 'low': 1}
            rows = list(index_coll.find(query, idx_proj))
            if rows:
                idx_df = pd.DataFrame(rows)
                idx_df['date'] = pd.to_datetime(idx_df['date'])
                # prices pivot
                prices = idx_df[['date', 'ticker', 'close']].pivot(index='date', columns='ticker', values='close')
                prices = prices.reindex(columns=ALL_TICKERS)
            else:
                idx_proj = {'_id': 0, 'ticker': 1, 'date': 1, 'close': 1, 'high': 1, 'low': 1}
        else:
            idx_proj = {'_id': 0, 'ticker': 1, 'date': 1, 'close': 1, 'high': 1, 'low': 1}

    except Exception:
        # If Mongo not accessible, return graceful defaults
        idx_proj = {'_id': 0, 'ticker': 1, 'date': 1, 'close': 1, 'high': 1, 'low': 1}

    # Determine weekly window using available price index if possible
    if not prices.empty:
        all_dates = prices.index
        last_day = all_dates.max().date()
        # find the latest friday on or before last_day
        # last_friday = last_day - timedelta(days=last_day.weekday() + 2)
        if last_day.weekday() >= 4:
            last_friday = last_day - timedelta(days=(last_day.weekday() - 4))
        else:
            last_friday = last_day - timedelta(days=last_day.weekday() + 2)
        end_week = last_friday
        start_week = end_week - timedelta(days=5)
        start_dt = datetime(start_week.year, start_week.month, start_week.day)
        end_dt = datetime(end_week.year, end_week.month, end_week.day)
    else:
        # fallback to end date environment
        end_week = END_DATE
        start_week = END_DATE - timedelta(days=5)
        start_dt = datetime(start_week.year, start_week.month, start_week.day)
        end_dt = datetime(end_week.year, end_week.month, end_week.day)

    return {
        'NAME_MAP': NAME_MAP,
        'ALL_TICKERS': ALL_TICKERS,
        'client': client,
        'db': db,
        'stock_coll': stock_coll,
        'index_coll': index_coll,
        'prices': prices,
        'idx_df': idx_df,
        'start_week': start_week,
        'end_week': end_week,
        'start_dt': start_dt,
        'end_dt': end_dt,
        'idx_proj': idx_proj,
        'INDEX_TICKER': INDEX_TICKER,
        'prior_end': end_dt - timedelta(days=1),
    }