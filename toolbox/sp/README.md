AlphaVantage fetcher

This folder contains a small utility to fetch weekly time series from AlphaVantage and store the raw response and a small summary into a local MongoDB.

Requirements
- Python 3.10+
- pip install requests pymongo

Optional (recommended)
- pip install python-dotenv  # allow placing ALPHAVANTAGE_KEY in toolbox/sp/.env or top-level .env

Configuration
- Set environment variable ALPHAVANTAGE_KEY with your API key, or put the key as plain text in `toolbox/sp/alphaventage.api.local` (first line).
	Alternatively you can create `toolbox/sp/.env` with a line:
	ALPHAVANTAGE_KEY=your_key_here
- Optionally set MONGO_URI environment variable, otherwise defaults to `mongodb://localhost:27017`.

Example

```sh
python toolbox/sp/av_fetch_and_store.py --symbol SPY
```

The script will insert documents into DB `investment_weekly` by default:
- `alpha_weekly_raw` - raw JSON responses with `fetched_at` timestamp
- `alpha_weekly_summary` - compact summary (week_end, last_close, prev_close, pct_change, raw_id)

Avoiding duplicates
- The script checks the latest `alpha_weekly_raw` for the symbol and will skip inserting a new raw document if the newest week_end matches the latest stored week_end (to avoid repeated entries).
