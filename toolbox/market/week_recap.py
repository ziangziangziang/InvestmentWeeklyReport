
"""Weekly recap utilities that reuse shared context from common.py.

This script computes the top stock range movers for the configured
stock groups and prints a mermaid radar for sector weekly H/L/C vs prior
week. Missing variables are loaded from the notebook-style context in
`common.load_context()` so the script can be run standalone or in the
notebook.
"""

from datetime import timedelta, date, datetime
import yaml
import pandas as pd

from common import load_context


# Load shared context (this will attempt MongoDB but return graceful
# defaults if not available). You can pass lookback_days to override.
CTX = load_context()
NAME_MAP = CTX['NAME_MAP']
ALL_TICKERS = CTX['ALL_TICKERS']
stock_coll = CTX['stock_coll']
index_coll = CTX['index_coll']
idx_df = CTX['idx_df']
start_week = CTX['start_week']
end_week = CTX['end_week']
start_dt = CTX['start_dt']
end_dt = CTX['end_dt']
idx_proj = CTX['idx_proj']
INDEX_TICKER = CTX.get('INDEX_TICKER')
prior_end = CTX.get('prior_end')


def top_stock_range_movers(stock_yml_path: str = 'directory.yml', top_n: int = 10):
    interest_set = set()
    try:
        with open(stock_yml_path, 'r', encoding='utf-8') as f:
            stock_groups = yaml.safe_load(f).get('stocks', {})
        for name, group in stock_groups.items():
            tickers = group.get('tickers', [])
            interest_set.update(tickers)
    except FileNotFoundError:
        print('No directory.yml found; skipping top movers')
        return None

    if not interest_set:
        print('No interest tickers found in directory.yml')
        return None

    start_dt_local = start_dt
    end_dt_local = end_dt
    if stock_coll is None:
        print('No stock collection available; cannot query DB for stock data')
        return None

    stock_query = {
        'ticker': {'$in': sorted(list(interest_set))},
        'date': {'$gte': start_dt_local, '$lte': end_dt_local}
    }
    stock_proj = {'_id': 0, 'ticker': 1, 'date': 1, 'high': 1, 'low': 1, 'close': 1}
    stock_rows = list(stock_coll.find(stock_query, stock_proj))
    stock_df = pd.DataFrame(stock_rows)
    if stock_df.empty:
        print('No stock data for interest list in week window.')
        return None

    stock_df['date'] = pd.to_datetime(stock_df['date'])
    agg = stock_df.groupby('ticker').agg(week_high=('high', 'max'), week_low=('low', 'min'))
    agg['range'] = agg['week_high'] - agg['week_low']
    top_movers = agg.sort_values('range', ascending=False).head(top_n)
    print(f'Top {min(top_n, len(top_movers))} Stock Range Movers This Week:')
    print(top_movers)
    print()
    return top_movers


def mermaid_radar(start_date, end_date, max_axes: int = 12):
    """Build a mermaid radar for the window start_date..end_date.

    The function computes percentages of this week's High/Low/Close vs the
    prior week's last close (prior_start = start_date - 7 days; prior_end = start_date - 1).
    """
    # normalize inputs to date-like (keep time portion if provided)
    try:
        sd = pd.to_datetime(start_date)
        ed = pd.to_datetime(end_date)
    except Exception:
        raise ValueError('start_date and end_date must be parseable by pandas.to_datetime')

    prior_start = sd - timedelta(days=7)
    prior_end = sd - timedelta(days=1)

    if index_coll is None:
        print('No index collection available; cannot build radar')
        return None

    # Query prior-week last closes (prior_start..prior_end) and this-week details
    prior_query = {
        'ticker': {'$in': ALL_TICKERS},
        'date': {'$gte': datetime(prior_start.year, prior_start.month, prior_start.day),
                 '$lte': datetime(prior_end.year, prior_end.month, prior_end.day)}
    }
    prior_rows = list(index_coll.find(prior_query, idx_proj))
    prior_df = pd.DataFrame(prior_rows)
    if prior_df.empty:
        print('Not enough prior week data for radar.')
        return None

    prior_df['date'] = pd.to_datetime(prior_df['date'])
    prior_last = prior_df.sort_values('date').groupby('ticker').last()['close']

    # current week slice from provided idx_df (must contain high/low/close and date)
    wk_slice = idx_df[(idx_df['date'] >= sd) & (idx_df['date'] <= ed)]
    if wk_slice.empty:
        print('No index data in weekly slice; cannot build radar')
        return None

    week_high = wk_slice.groupby('ticker')['high'].max()
    week_low = wk_slice.groupby('ticker')['low'].min()
    week_close = wk_slice.sort_values('date').groupby('ticker').last()['close']

    pct_close = (week_close / prior_last) * 100
    pct_high = (week_high / prior_last) * 100
    pct_low = (week_low / prior_last) * 100

    common = pct_close.index.intersection(pct_high.index).intersection(pct_low.index)
    common = [t for t in common if t in ALL_TICKERS]
    if len(common) > max_axes:
        common = common[:max_axes]

    axes_tokens = [f"axis {tk.replace('^','').replace('-','')}['{NAME_MAP.get(tk, tk)}']" for tk in common]
    close_series = ', '.join(f"{pct_close[tk]:.2f}" for tk in common)
    high_series = ', '.join(f"{pct_high[tk]:.2f}" for tk in common)
    low_series = ', '.join(f"{pct_low[tk]:.2f}" for tk in common)

    curve_tokens = []
    curve_tokens.append(f"  curve ref['Reference']{{{','.join(['100.00']*len(common))}}}")
    curve_tokens.append(f"  curve h['High']{{{high_series}}}")
    curve_tokens.append(f"  curve c['Close']{{{close_series}}}")
    curve_tokens.append(f"  curve l['Low']{{{low_series}}}")

    max_val = float(pd.concat([pct_high.loc[common], pct_low.loc[common], pct_close.loc[common]]).max())
    min_val = float(pd.concat([pct_high.loc[common], pct_low.loc[common], pct_close.loc[common]]).min())
    pad = max(5, abs(max_val - min_val) * 0.05)
    mmax = round(max_val + pad, 2)
    mmin = round(min_val - pad, 2)

    mermaid_lines = [
        '---',
        f'title: "Weekly Sector H/L/C % vs {sd.date()} - {ed.date()}"',
        'config:',
        '  themeVariables:',
        '    cScale0: "#D3D3D3"',
        '---',
        'radar-beta'
    ]
    mermaid_lines += ["  " + a for a in axes_tokens]
    mermaid_lines += curve_tokens
    mermaid_lines.append(f"  max {mmax}")
    mermaid_lines.append(f"  min {mmin}")

    mermaid_code = '\n'.join(mermaid_lines)
    print(mermaid_code)

if __name__ == '__main__':
    # call mermaid_radar with the computed week bounds
    mermaid_radar(start_week, end_week)