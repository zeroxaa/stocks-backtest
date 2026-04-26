# stocks-backtest

Streaming, resumable ingest of US equity OHLCV bars into a local DuckDB file for backtesting.

## What's in the DB

Three tables:

| Table | Purpose |
|---|---|
| `bars` | OHLCV rows: `ticker, interval, t, o, h, l, c, v, vw, n` |
| `coverage` | Per `(ticker, interval)`: `min_t`, `max_t`, `bars_count`, `updated_at` — drives resume |
| `ingest_log` | Per chunk: range, rows returned, elapsed ms |

`bars` has an index on `(ticker, interval, t)` but no uniqueness constraint — PKs on a growing DuckDB table tank insert throughput, so idempotency lives at the fetch layer (the `coverage` row tells the next run where to resume from, and the most recently covered day is re-pulled and trimmed to absorb any partial-day rows from an interrupted run).

## Usage

```bash
pip install duckdb pandas requests

# Default: Mag7, 1-minute bars, last 5 years -> /code/stocks.duckdb
python ingest.py

# Custom
python ingest.py --tickers SPY QQQ IWM --interval 5min --years 2 --db ./mydata.duckdb
```

Re-running is safe: each ticker/interval resumes from the last covered day.

## Supported intervals

`1min`, `5min`, `15min`, `30min`, `1hour`, `1day`. Chunk size auto-scales per interval.

## Auth

The script reads a sandbox token from `/home/user/.rebyte.ai/auth.json` and POSTs to the Rebyte stocks bars endpoint. Adapt `fetch_bars()` if you're calling a different upstream (Polygon, Alpaca, etc.) — the rest of the pipeline is provider-agnostic.

## Backtest

`backtest.py` runs a long-only trend-following strategy on any ticker in the DB:
long when daily close > N-day SMA, cash otherwise.

```bash
pip install vectorbt
python backtest.py                        # SPY, 200d SMA, $10k
python backtest.py --ticker QQQ --sma 100 # different ticker / window
```

Sample output (SPY, 200d SMA, 2022-01-22 → 2026-04-24):

| Metric | Strategy | Buy & Hold |
|---|---:|---:|
| Total return | 49.73% | 63.56% |
| Max drawdown | **11.57%** | 22.66% |
| Sharpe | **1.11** | 0.86 |
| Calmar | **1.18** | 0.75 |
| Time in market | 72.72% | 100% |

Classic trend-filter result: gives up some upside, cuts drawdown roughly in half, improves risk-adjusted return. The strategy spent most of 2022 in cash and avoided that bear market.

## DB file

The actual `.duckdb` file is gitignored — at 1-minute resolution × 5 years × ~40 large-cap tickers it grows to ~2 GB. Re-generate locally with `python ingest.py`.
