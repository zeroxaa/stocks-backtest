"""Incremental, streaming ingest of stock OHLCV bars into DuckDB.

Design:
- `bars` table: no uniqueness constraint, just an index for query speed.
  PK constraints in DuckDB tank insert throughput on growing tables, so we
  enforce idempotency at the *fetch* layer instead.
- `coverage` table: per (ticker, interval), tracks the [min, max] date range
  that has already been ingested. Rerunning resumes from `max+1` and skips
  redundant API calls. The most recent fetched day is re-pulled on each run
  to fill any partial-day bars from a prior interrupted run.
- Each fetched chunk is INSERTed straight into `bars`. Nothing accumulates
  across chunks in memory.
"""
import argparse
import datetime as dt
import json
import time

import duckdb
import pandas as pd
import requests

with open("/home/user/.rebyte.ai/auth.json") as f:
    _AUTH = json.load(f)
TOKEN = _AUTH["sandbox"]["token"]
URL = _AUTH["sandbox"]["relay_url"]
H = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

DB_PATH = "/code/stocks.duckdb"
DEFAULT_TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA"]

CHUNK_DAYS = {
    "1min": 10,
    "5min": 60,
    "15min": 180,
    "30min": 360,
    "1hour": 720,
    "1day": 1825,
}


def init_db(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS bars (
            ticker    VARCHAR   NOT NULL,
            interval  VARCHAR   NOT NULL,
            t         TIMESTAMP NOT NULL,
            o         DOUBLE,
            h         DOUBLE,
            l         DOUBLE,
            c         DOUBLE,
            v         BIGINT,
            vw        DOUBLE,
            n         INTEGER
        );
        """
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS bars_lookup_idx ON bars(ticker, interval, t);"
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS coverage (
            ticker      VARCHAR NOT NULL,
            interval    VARCHAR NOT NULL,
            min_t       TIMESTAMP,
            max_t       TIMESTAMP,
            bars_count  BIGINT,
            updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ticker, interval)
        );
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS ingest_log (
            ticker          VARCHAR,
            interval        VARCHAR,
            range_from      DATE,
            range_to        DATE,
            bars_returned   BIGINT,
            elapsed_ms      INTEGER,
            fetched_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )


def fetch_bars(ticker: str, interval: str, frm: dt.date, to: dt.date) -> list[dict]:
    r = requests.post(
        f"{URL}/api/data/stocks/bars",
        headers=H,
        json={"ticker": ticker, "interval": interval, "from": str(frm), "to": str(to)},
        timeout=180,
    )
    r.raise_for_status()
    return r.json().get("bars", [])


def get_coverage(con, ticker: str, interval: str):
    row = con.execute(
        "SELECT min_t, max_t, bars_count FROM coverage WHERE ticker=? AND interval=?",
        [ticker, interval],
    ).fetchone()
    return row  # (min_t, max_t, count) or None


def upsert_coverage(con, ticker: str, interval: str) -> None:
    con.execute(
        """
        INSERT INTO coverage (ticker, interval, min_t, max_t, bars_count, updated_at)
        SELECT ?, ?, MIN(t), MAX(t), COUNT(*), CURRENT_TIMESTAMP
        FROM bars WHERE ticker=? AND interval=?
        ON CONFLICT (ticker, interval) DO UPDATE SET
            min_t = EXCLUDED.min_t,
            max_t = EXCLUDED.max_t,
            bars_count = EXCLUDED.bars_count,
            updated_at = EXCLUDED.updated_at;
        """,
        [ticker, interval, ticker, interval],
    )


def insert_bars(con, ticker: str, interval: str, bars: list[dict]) -> int:
    """Stream rows straight into bars via DuckDB's DataFrame path. Caller
    is responsible for not re-fetching ranges already covered (see coverage
    table). DataFrame INSERT is ~400x faster than executemany for batches."""
    if not bars:
        return 0
    df = pd.DataFrame(bars)
    df["ticker"] = ticker
    df["interval"] = interval
    df["t"] = pd.to_datetime(df["t"])
    for col in ("o", "h", "l", "c", "vw"):
        if col not in df:
            df[col] = None
    for col in ("v", "n"):
        if col not in df:
            df[col] = None
    df = df[["ticker", "interval", "t", "o", "h", "l", "c", "v", "vw", "n"]]
    con.register("_chunk_df", df)
    con.execute("INSERT INTO bars SELECT * FROM _chunk_df")
    con.unregister("_chunk_df")
    return len(df)


def trim_overlap(con, ticker: str, interval: str, frm_ts) -> int:
    """Delete any bars at or after frm_ts so re-fetching the latest day
    doesn't double-count rows. Returns how many rows were removed."""
    res = con.execute(
        "DELETE FROM bars WHERE ticker=? AND interval=? AND t >= ? RETURNING 1",
        [ticker, interval, frm_ts],
    ).fetchall()
    return len(res)


def ingest_ticker(
    con,
    ticker: str,
    interval: str,
    target_start: dt.date,
    target_end: dt.date,
) -> None:
    chunk_days = CHUNK_DAYS.get(interval, 30)
    cov = get_coverage(con, ticker, interval)
    if cov is None or cov[1] is None:
        start = target_start
        print(f"  [{ticker} {interval}] new — fetching {start} -> {target_end}", flush=True)
    else:
        max_t = cov[1]
        max_date = max_t.date() if hasattr(max_t, "date") else dt.date.fromisoformat(str(max_t)[:10])
        # Re-fetch the last covered day to fill any partial bars.
        start = max_date
        if start > target_end:
            print(f"  [{ticker} {interval}] up-to-date (max={max_t}, count={cov[2]})", flush=True)
            return
        # Wipe rows from `start` onward so re-fetched bars don't duplicate.
        removed = trim_overlap(con, ticker, interval, dt.datetime.combine(start, dt.time(0, 0)))
        print(
            f"  [{ticker} {interval}] resume from {start} (db max={max_t}, "
            f"count={cov[2]}, trimmed={removed})",
            flush=True,
        )

    cur = start
    total_returned = 0
    n_chunks = 0
    t0 = time.time()
    while cur <= target_end:
        nxt = min(cur + dt.timedelta(days=chunk_days - 1), target_end)
        chunk_t0 = time.time()
        try:
            bars = fetch_bars(ticker, interval, cur, nxt)
        except Exception as e:
            print(f"    fetch error {cur}->{nxt}: {e}; retrying in 3s", flush=True)
            time.sleep(3)
            continue
        insert_bars(con, ticker, interval, bars)
        chunk_ms = int((time.time() - chunk_t0) * 1000)
        con.execute(
            "INSERT INTO ingest_log "
            "(ticker, interval, range_from, range_to, bars_returned, elapsed_ms) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [ticker, interval, cur, nxt, len(bars), chunk_ms],
        )
        total_returned += len(bars)
        n_chunks += 1
        print(
            f"    chunk {n_chunks:3d} {cur}->{nxt}: got={len(bars):>5} "
            f"({chunk_ms} ms, total={total_returned})",
            flush=True,
        )
        cur = nxt + dt.timedelta(days=1)
    upsert_coverage(con, ticker, interval)
    elapsed = time.time() - t0
    cov = get_coverage(con, ticker, interval)
    print(
        f"  [{ticker} {interval}] done: {n_chunks} chunks in {elapsed:.1f}s — "
        f"db now has {cov[2]} bars from {cov[0]} to {cov[1]}",
        flush=True,
    )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--tickers", nargs="+", default=DEFAULT_TICKERS)
    p.add_argument("--interval", default="1min")
    p.add_argument("--years", type=float, default=5.0)
    p.add_argument("--db", default=DB_PATH)
    args = p.parse_args()

    end = dt.date.today()
    start = end - dt.timedelta(days=int(args.years * 365))

    con = duckdb.connect(args.db)
    init_db(con)

    print(
        f"Ingest: tickers={args.tickers} interval={args.interval} "
        f"range={start} -> {end} db={args.db}",
        flush=True,
    )
    t0 = time.time()
    for tk in args.tickers:
        ingest_ticker(con, tk, args.interval, start, end)
    print(f"\nTotal elapsed: {time.time()-t0:.1f}s", flush=True)

    print("\nDB summary:", flush=True)
    rows = con.execute(
        "SELECT ticker, interval, bars_count, min_t, max_t "
        "FROM coverage ORDER BY ticker, interval"
    ).fetchall()
    print(f"{'ticker':<8}{'interval':<8}{'bars':>10}  {'min_t':<22}{'max_t':<22}")
    for r in rows:
        print(f"{r[0]:<8}{r[1]:<8}{r[2]:>10}  {str(r[3]):<22}{str(r[4]):<22}", flush=True)
    con.close()


if __name__ == "__main__":
    main()
