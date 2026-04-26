"""SPY trend-following backtest: long when daily close > N-day SMA, else cash.

Reads 1-minute bars from the local DuckDB file, resamples to a daily close series,
runs the strategy via vectorbt, and prints stats vs buy-and-hold.
"""
import argparse

import duckdb
import pandas as pd
import vectorbt as vbt

DB_PATH = "/code/stocks.duckdb"


def load_daily_close(db_path: str, ticker: str) -> pd.Series:
    con = duckdb.connect(db_path, read_only=True)
    df = con.execute(
        """
        SELECT t::DATE AS d, last(c ORDER BY t) AS close
        FROM bars
        WHERE ticker = ? AND interval = '1min'
        GROUP BY d
        ORDER BY d
        """,
        [ticker],
    ).df()
    con.close()
    return pd.Series(df["close"].values, index=pd.to_datetime(df["d"]), name=ticker)


def run(close: pd.Series, sma_window: int, init_cash: float):
    sma = close.rolling(sma_window).mean()
    in_market = (close > sma).fillna(False)
    valid = sma.notna()
    close_v = close[valid]
    in_v = in_market[valid]

    entries = in_v & ~in_v.shift(1, fill_value=False)
    exits = ~in_v & in_v.shift(1, fill_value=False)

    pf_strat = vbt.Portfolio.from_signals(
        close_v, entries, exits, init_cash=init_cash, fees=0.0, freq="1D"
    )
    pf_bh = vbt.Portfolio.from_holding(close_v, init_cash=init_cash, freq="1D")
    return pf_strat, pf_bh, float(in_v.mean())


def fmt(pf, label: str, time_in_market: float | None = None) -> str:
    s = pf.stats()
    lines = [
        f"=== {label} ===",
        f"  Period              : {s['Start'].date()} → {s['End'].date()}",
        f"  End equity          : ${pf.value().iloc[-1]:>12,.2f}",
        f"  Total return        : {s['Total Return [%]']:>8.2f}%",
        f"  Max drawdown        : {s['Max Drawdown [%]']:>8.2f}%",
        f"  Sharpe (annualized) : {s['Sharpe Ratio']:>8.2f}",
        f"  Sortino             : {s['Sortino Ratio']:>8.2f}",
        f"  Calmar              : {s['Calmar Ratio']:>8.2f}",
        f"  # trades            : {int(s['Total Trades']):>8d}",
    ]
    if s["Total Trades"] > 0:
        lines.append(f"  Win rate            : {s['Win Rate [%]']:>8.2f}%")
        lines.append(f"  Profit factor       : {s['Profit Factor']:>8.2f}")
    if time_in_market is not None:
        lines.append(f"  Time in market      : {time_in_market * 100:>8.2f}%")
    return "\n".join(lines) + "\n"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--ticker", default="SPY")
    p.add_argument("--sma", type=int, default=200)
    p.add_argument("--cash", type=float, default=10_000.0)
    p.add_argument("--db", default=DB_PATH)
    args = p.parse_args()

    print(f"Loading {args.ticker} daily closes from {args.db} ...", flush=True)
    close = load_daily_close(args.db, args.ticker)
    print(
        f"  {len(close)} trading days, "
        f"{close.index.min().date()} → {close.index.max().date()}\n",
        flush=True,
    )

    pf_strat, pf_bh, time_in_market = run(close, args.sma, args.cash)
    print(fmt(pf_strat, f"{args.ticker} {args.sma}d SMA trend-following", time_in_market))
    print(fmt(pf_bh, f"{args.ticker} buy & hold", 1.0))


if __name__ == "__main__":
    main()
