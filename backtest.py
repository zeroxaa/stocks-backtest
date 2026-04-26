"""Compare long-only timing strategies on a single ticker.

All strategies output a daily position {0, 1}. Signals are lagged one day —
decided at today's close, applied to tomorrow's close-to-close return.
The overnight-only strategy is the exception: it captures the close→next-open
return rather than close→next-close.

Usage:
    python backtest.py                  # SPY, default strategy set
    python backtest.py --ticker QQQ     # any ticker in the DB
"""
import argparse

import duckdb
import numpy as np
import pandas as pd

DB_PATH = "/code/stocks.duckdb"
TRADING_DAYS = 252  # for annualization


# ---------- data ----------

def load_sessions(db_path: str, ticker: str) -> pd.DataFrame:
    """Per-trading-day DataFrame with regular-session open and close.

    Regular session = 13:30..20:00 UTC (9:30..16:00 ET). `open` is the open
    of the 9:30 ET bar; `close` is the close of the last bar in the session.
    """
    con = duckdb.connect(db_path, read_only=True)
    df = con.execute(
        """
        WITH reg AS (
          SELECT t::DATE AS d, t, o, c,
                 ROW_NUMBER() OVER (PARTITION BY t::DATE ORDER BY t)      AS rn_asc,
                 ROW_NUMBER() OVER (PARTITION BY t::DATE ORDER BY t DESC) AS rn_desc
          FROM bars
          WHERE ticker = ?
            AND interval = '1min'
            AND date_part('hour', t) * 60 + date_part('minute', t) BETWEEN 810 AND 1200
        )
        SELECT
          d,
          MIN(CASE WHEN rn_asc  = 1 THEN o END) AS open,
          MAX(CASE WHEN rn_desc = 1 THEN c END) AS close
        FROM reg
        GROUP BY d
        ORDER BY d
        """,
        [ticker],
    ).df()
    con.close()
    df["date"] = pd.to_datetime(df["d"])
    df = df.set_index("date").drop(columns="d").dropna(subset=["open", "close"])
    df["next_open"] = df["open"].shift(-1)
    return df


# ---------- indicators ----------

def rsi(close: pd.Series, period: int) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    # Wilder-style EMA: alpha = 1/period
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - 100 / (1 + rs)


# ---------- strategies ----------
# Each returns (position_series, daily_return_series), both indexed like df.
# `position` is the bool/int weight of the asset for that day's return.

def strat_buy_hold(df: pd.DataFrame):
    pos = pd.Series(1, index=df.index)
    ret = df["close"].pct_change().fillna(0)
    return pos, ret


def _trend_strategy(df: pd.DataFrame, ma: pd.Series):
    close = df["close"]
    raw_signal = (close > ma).astype(int)
    pos = raw_signal.shift(1).fillna(0).astype(int)  # decide at close, apply next day
    ret = close.pct_change().fillna(0) * pos
    return pos, ret


def strat_sma(df: pd.DataFrame, window: int):
    return _trend_strategy(df, df["close"].rolling(window).mean())


def strat_ema(df: pd.DataFrame, span: int):
    return _trend_strategy(df, df["close"].ewm(span=span, adjust=False).mean())


def strat_connors_rsi2(df: pd.DataFrame, lo: float = 10, hi: float = 70):
    """Buy when RSI(2) < lo, exit when RSI(2) > hi. Classic Larry Connors."""
    r = rsi(df["close"], 2)
    raw = np.zeros(len(r), dtype=int)
    in_pos = 0
    for i, v in enumerate(r.values):
        if np.isnan(v):
            raw[i] = 0
            continue
        if not in_pos and v < lo:
            in_pos = 1
        elif in_pos and v > hi:
            in_pos = 0
        raw[i] = in_pos
    raw = pd.Series(raw, index=r.index)
    pos = raw.shift(1).fillna(0).astype(int)
    ret = df["close"].pct_change().fillna(0) * pos
    return pos, ret


def strat_overnight(df: pd.DataFrame):
    """Always hold close→next_open; daily return is the overnight gap."""
    overnight = ((df["next_open"] - df["close"]) / df["close"]).fillna(0)
    pos = df["next_open"].notna().astype(int)
    return pos, overnight


# ---------- summary ----------

def summarize(name: str, pos: pd.Series, ret: pd.Series, init_cash: float) -> dict:
    eq = (1 + ret).cumprod() * init_cash
    n = len(ret)
    years = n / TRADING_DAYS
    cagr = (eq.iloc[-1] / init_cash) ** (1 / years) - 1 if years > 0 else 0
    std = ret.std()
    sharpe = (ret.mean() / std) * np.sqrt(TRADING_DAYS) if std > 0 else 0
    cummax = eq.cummax()
    max_dd = (1 - eq / cummax).max()
    time_in_market = (pos > 0).mean()
    # Trades: each rising edge of pos
    edges = pos.diff().fillna(pos).astype(int)
    n_trades = int((edges > 0).sum())
    return {
        "name": name,
        "end_equity": float(eq.iloc[-1]),
        "total_return": float(eq.iloc[-1] / init_cash - 1),
        "cagr": float(cagr),
        "sharpe": float(sharpe),
        "max_dd": float(max_dd),
        "time_in_market": float(time_in_market),
        "n_trades": n_trades,
    }


def print_table(rows: list[dict], start_d, end_d, n_days: int):
    print(f"\nPeriod: {start_d.date()} → {end_d.date()}  ({n_days} sessions)\n")
    hdr = f"{'Strategy':<26} {'End $':>12} {'Total':>8} {'CAGR':>7} {'Sharpe':>7} {'MaxDD':>8} {'TIM':>7} {'Trades':>7}"
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        print(
            f"{r['name']:<26} "
            f"${r['end_equity']:>11,.0f} "
            f"{r['total_return']*100:>7.2f}% "
            f"{r['cagr']*100:>6.2f}% "
            f"{r['sharpe']:>7.2f} "
            f"{r['max_dd']*100:>7.2f}% "
            f"{r['time_in_market']*100:>6.1f}% "
            f"{r['n_trades']:>7d}"
        )


# ---------- main ----------

STRATEGIES = [
    ("Buy & Hold",          strat_buy_hold,          {}),
    ("SMA(50) trend",       strat_sma,               {"window": 50}),
    ("SMA(100) trend",      strat_sma,               {"window": 100}),
    ("SMA(200) trend",      strat_sma,               {"window": 200}),
    ("EMA(50) trend",       strat_ema,               {"span": 50}),
    ("EMA(200) trend",      strat_ema,               {"span": 200}),
    ("Connors RSI(2) 10/70", strat_connors_rsi2,     {}),
    ("Overnight-only",      strat_overnight,         {}),
]
WARMUP = 200  # max indicator window — drop these days from every strategy for comparability


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--ticker", default="SPY")
    p.add_argument("--cash", type=float, default=10_000.0)
    p.add_argument("--db", default=DB_PATH)
    args = p.parse_args()

    print(f"Loading {args.ticker} sessions from {args.db} ...")
    df = load_sessions(args.db, args.ticker)
    if len(df) <= WARMUP:
        raise SystemExit(f"need > {WARMUP} sessions, got {len(df)}")
    print(f"  {len(df)} sessions, {df.index.min().date()} → {df.index.max().date()}")

    rows = []
    for name, fn, kwargs in STRATEGIES:
        pos, ret = fn(df, **kwargs)
        # Trim warmup so all strategies cover the same period
        pos = pos.iloc[WARMUP:].copy()
        ret = ret.iloc[WARMUP:].copy()
        rows.append(summarize(name, pos, ret, args.cash))

    start_d = df.index[WARMUP]
    end_d = df.index[-1]
    print_table(rows, start_d, end_d, len(df) - WARMUP)


if __name__ == "__main__":
    main()
