# Market Data Ingestion (Polygon + yfinance)

Purpose: Fetch US equities daily OHLCV (bulk via Polygon) plus selected index / sector benchmarks via yfinance.

## Data Sources
- Polygon: bulk daily aggregates (all tickers in one call).
- yfinance: indices and sector benchmarks not fully exposed via Polygon.

## Observed Indices / Sectors
The SP500 indecies are hiarchichared in to the following:
- sector
  - industry
    - sub-industry
| Description | Ticker |
|-------------|--------|
| S&P 500 | ^SPX |
| S&P 500 Communication Services | ^SP500-50 |
| S&P 500 Consumer Discretionary | ^SP500-25 |
| S&P 500 Consumer Staples | ^SP500-30 |
| S&P 500 Energy | ^SP500-10TR |
| S&P 500 Financials | ^SP500-40 |
| S&P 500 Health Care | ^SP500-35 |
| S&P 500 Industrials | ^SP500-20 |
| S&P 500 Information Technology | ^SP500-45 |
| S&P 500 Materials | ^SP500-15 |
| S&P 500 Real Estate | ^SP500-60 |
| S&P 500 Utilities | ^SP500-55 |
| VIX | ^VIX |

## Quick Start
```
source toolbox/venv/bin/activate
python toolbox/market/update.py      # ingest equities via Polygon and ingest indices via yfinance
```

## Collections (Mongo-style)
stock_daily:
  ticker (string)
  date (date)
  open, high, low, close (float)
  volume (float)
  pre_market (float|null)
  after_hours (float|null)
  source = "polygon"

index_daily:
  ticker (string)
  date (date or ISO string)
  open, high, low, close (float)
  volume (float|null)
  source = "yfinance"

## Notes
- Polygon dashboard useful for request inspection & quota tracking.
- yfinance mirrors publicly visible Yahoo Finance frontend data.
- Keep ingestion idempotent: upsert by (ticker, date).
- Validate null / missing fields (e.g., pre/after hours not always present).

## Next Ideas (optional)
- Add retry + backoff wrapper.
- Add checksum / last updated metadata collection.
- Parallelize yfinance pulls (bounded concurrency).