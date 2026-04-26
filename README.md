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

`backtest.py` compares several long-only timing strategies on a single ticker.
All strategies are pure on/off (full position or cash); signals decided at
today's close apply to tomorrow's close-to-close return. Overnight-only is
the exception — it captures only the close→next-open gap.

```bash
python backtest.py                # SPY, default $10k
python backtest.py --ticker QQQ   # any ticker in the DB
```

Strategies compared:
- **Buy & Hold** — baseline
- **SMA(N) trend** — long when close > N-day SMA, cash otherwise (N = 50/100/200)
- **EMA(N) trend** — same with exponential MA (N = 50/200)
- **Connors RSI(2) 10/70** — buy when 2-day RSI < 10, exit when > 70 (mean reversion)
- **Overnight-only** — capture the close → next-open gap, sit out the day

### Sample output (SPY, 2022-02-08 → 2026-04-24, 1056 sessions, $10k start)

| Strategy | End $ | CAGR | Sharpe | MaxDD | TIM | Trades |
|---|---:|---:|---:|---:|---:|---:|
| Buy & Hold | $15,884 | 11.68% | 0.73 | 22.68% | 100% | 1 |
| SMA(50) trend | $13,683 | 7.77% | 0.77 | 17.33% | 66.7% | 32 |
| **SMA(100) trend** | **$14,883** | **9.95%** | **0.95** | **12.09%** | 70.9% | 17 |
| SMA(200) trend | $14,079 | 8.51% | 0.82 | 14.25% | 72.7% | 16 |
| EMA(50) trend | $13,740 | 7.88% | 0.77 | 16.00% | 68.3% | 38 |
| EMA(200) trend | $14,338 | 8.98% | 0.85 | 13.35% | 72.6% | 17 |
| Connors RSI(2) 10/70 | $14,124 | 8.59% | 0.86 | **11.36%** | 23.0% | 54 |
| Overnight-only | $12,448 | 5.37% | 0.55 | 19.50% | 99.9% | 1 |

### Reading the table

- **Buy & Hold wins on raw return** but the worst Sharpe and the biggest drawdown of the trend strategies.
- **SMA(100)** is the sweet spot here — best Sharpe, tightest drawdown of the trend group, only 17 trades.
- **Connors RSI(2)** has the smallest drawdown and decent Sharpe while only being in the market 23% of the time — capital efficient if you have other things to do with your cash.
- **Overnight-only** is a famous "anomaly" but on this 2022-2026 window it's been weak — the historical edge has degraded materially since ~2019. Not a standalone strategy anymore on SPY.
- **EMA vs SMA**: very similar; EMA reacts faster, so more trades and more whipsaw.

Treat these as illustrative, not investment advice — no fees / slippage / tax modeled, and 4 years is a small sample (one bear, one recovery). To make any of these production-real you'd want to test across multiple decades + multi-ticker robustness checks.

## DB file

The actual `.duckdb` file is gitignored — at 1-minute resolution × 5 years × ~40 large-cap tickers it grows to ~2 GB. Re-generate locally with `python ingest.py`.
