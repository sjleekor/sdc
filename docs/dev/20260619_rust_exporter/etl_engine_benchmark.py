#!/usr/bin/env python3
"""Benchmark candidate ETL engines on the real Parquet lake.

Compares DuckDB vs Polars vs DataFusion on the core operations the
01_20_access_return_rank ETL actually needs (see etl_01 §4):

  Q1 dedup    KRX-priority dedupe of krx_security_flow_raw (window QUALIFY)   -- 2.2 GB, heaviest
  Q2 label    20-trading-day forward return via d_idx self-join (daily_ohlcv)
  Q3 rank     cross-sectional percentile rank of excess return within (date, market)
  Q4 asof     PIT as-of join of stock_metric_fact onto a price universe

Each query is run cold (fresh process not feasible here, so we re-create the
engine/context per engine and run each query 2x: first = cold-ish, report best).
We measure wall time and peak RSS delta.
"""
import gc
import glob
import resource
import time

RAW = "data_lake/raw_postgres/snapshot_date=2026-06-19/source=local_mydb"
CAN = "data_lake/canonical_postgres/snapshot_date=2026-06-19/source=local_mydb"
OHLCV = f"{RAW}/daily_ohlcv/**/*.parquet"
FLOW = f"{RAW}/krx_security_flow_raw/**/*.parquet"
SMF = f"{CAN}/stock_metric_fact/**/*.parquet"

REPEATS = 2


def peak_mb():
    # ru_maxrss is bytes on macOS, kilobytes on Linux
    r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return r / (1024 * 1024) if r > 10_000_000 else r / 1024


def timed(fn):
    best = None
    rows = None
    for _ in range(REPEATS):
        gc.collect()
        t0 = time.perf_counter()
        rows = fn()
        dt = time.perf_counter() - t0
        best = dt if best is None else min(best, dt)
    return best, rows


# ---------------------------------------------------------------- DuckDB
def bench_duckdb():
    import duckdb

    out = {}
    con = duckdb.connect()
    con.execute("PRAGMA threads=14")

    def q1():
        return con.execute(f"""
            SELECT count(*) FROM (
              SELECT trade_date, ticker, market, metric_code, value,
                ROW_NUMBER() OVER (PARTITION BY trade_date,ticker,market,metric_code
                  ORDER BY CASE source WHEN 'KRX' THEN 0 ELSE 1 END) rn
              FROM read_parquet('{FLOW}')
            ) WHERE rn=1
        """).fetchone()[0]

    def q2():
        return con.execute(f"""
            WITH px AS (
              SELECT trade_date,ticker,market,close,
                ROW_NUMBER() OVER (PARTITION BY ticker,market ORDER BY trade_date) d_idx
              FROM read_parquet('{OHLCV}')
              WHERE NOT (open=0 AND high=0 AND low=0)
            )
            SELECT count(*) FROM px a JOIN px f
              ON f.ticker=a.ticker AND f.market=a.market AND f.d_idx=a.d_idx+20
        """).fetchone()[0]

    def q3():
        return con.execute(f"""
            WITH px AS (
              SELECT trade_date,ticker,market,close,
                ROW_NUMBER() OVER (PARTITION BY ticker,market ORDER BY trade_date) d_idx
              FROM read_parquet('{OHLCV}')
              WHERE NOT (open=0 AND high=0 AND low=0)
            ),
            fwd AS (
              SELECT a.trade_date,a.ticker,a.market,
                f.close::DOUBLE/NULLIF(a.close,0)-1 AS r
              FROM px a JOIN px f
                ON f.ticker=a.ticker AND f.market=a.market AND f.d_idx=a.d_idx+20
            ),
            ex AS (
              SELECT trade_date,ticker,market,
                r - AVG(r) OVER (PARTITION BY trade_date,market) AS ex
              FROM fwd
            )
            SELECT count(*) FROM (
              SELECT PERCENT_RANK() OVER (PARTITION BY trade_date,market ORDER BY ex) pr
              FROM ex
            )
        """).fetchone()[0]

    def q4():
        return con.execute(f"""
            WITH u AS (
              SELECT trade_date,ticker FROM read_parquet('{OHLCV}')
              WHERE trade_date >= DATE '2024-01-01'
            ),
            f AS (
              SELECT ticker,metric_code,value_numeric,
                (period_end + INTERVAL 90 DAY) AS avail
              FROM read_parquet('{SMF}') WHERE metric_code='roa' OR true
            )
            SELECT count(*) FROM (
              SELECT u.trade_date,u.ticker,f.metric_code,f.value_numeric,
                ROW_NUMBER() OVER (PARTITION BY u.trade_date,u.ticker,f.metric_code
                  ORDER BY f.avail DESC) rn
              FROM u JOIN f ON f.ticker=u.ticker AND f.avail <= u.trade_date
            ) WHERE rn=1
        """).fetchone()[0]

    for name, fn in [("Q1_dedup", q1), ("Q2_label", q2), ("Q3_rank", q3), ("Q4_asof", q4)]:
        try:
            dt, rows = timed(fn)
            out[name] = (dt, rows)
        except Exception as e:
            out[name] = (None, f"ERR {type(e).__name__}: {str(e)[:80]}")
    con.close()
    return out


# ---------------------------------------------------------------- Polars (lazy)
def bench_polars():
    import polars as pl

    out = {}

    def q1():
        lf = pl.scan_parquet(FLOW)
        return (
            lf.with_columns(pri=pl.when(pl.col("source") == "KRX").then(0).otherwise(1))
            .sort("pri")
            .group_by(["trade_date", "ticker", "market", "metric_code"])
            .first()
            .select(pl.len())
            .collect()
            .item()
        )

    def fwd_lf():
        px = (
            pl.scan_parquet(OHLCV)
            .filter(~((pl.col("open") == 0) & (pl.col("high") == 0) & (pl.col("low") == 0)))
            .with_columns(
                d_idx=pl.int_range(pl.len()).over(["ticker", "market"], order_by="trade_date")
            )
        )
        f = px.select(
            ["ticker", "market", (pl.col("d_idx") - 20).alias("d_idx"),
             pl.col("close").alias("close_f")]
        )
        return px.join(f, on=["ticker", "market", "d_idx"]).with_columns(
            r=pl.col("close_f").cast(pl.Float64) / pl.col("close") - 1
        )

    def q2():
        return fwd_lf().select(pl.len()).collect().item()

    def q3():
        ex = fwd_lf().with_columns(
            ex=pl.col("r") - pl.col("r").mean().over(["trade_date", "market"])
        )
        return (
            ex.select(
                pl.col("ex").rank(method="average").over(["trade_date", "market"])
            )
            .select(pl.len())
            .collect()
            .item()
        )

    def q4():
        u = pl.scan_parquet(OHLCV).filter(pl.col("trade_date") >= pl.date(2024, 1, 1)).select(
            ["trade_date", "ticker"]
        )
        f = pl.scan_parquet(SMF).with_columns(
            avail=pl.col("period_end").dt.offset_by("90d")
        ).select(["ticker", "metric_code", "value_numeric", "avail"])
        # as-of: join on ticker, keep latest avail <= trade_date per metric
        j = u.join(f, on="ticker").filter(pl.col("avail") <= pl.col("trade_date"))
        return (
            j.with_columns(
                rn=pl.col("avail").rank(method="ordinal", descending=True).over(
                    ["trade_date", "ticker", "metric_code"]
                )
            )
            .filter(pl.col("rn") == 1)
            .select(pl.len())
            .collect()
            .item()
        )

    for name, fn in [("Q1_dedup", q1), ("Q2_label", q2), ("Q3_rank", q3), ("Q4_asof", q4)]:
        try:
            dt, rows = timed(fn)
            out[name] = (dt, rows)
        except Exception as e:
            out[name] = (None, f"ERR {type(e).__name__}: {str(e)[:80]}")
    return out


# ---------------------------------------------------------------- DataFusion
def bench_datafusion():
    from datafusion import SessionContext

    out = {}
    ctx = SessionContext()
    # register parquet dirs as tables (datafusion reads a dir of parquet)
    ctx.register_parquet("flow", f"{RAW}/krx_security_flow_raw")
    ctx.register_parquet("ohlcv", f"{RAW}/daily_ohlcv")
    ctx.register_parquet("smf", f"{CAN}/stock_metric_fact")

    def q1():
        return ctx.sql("""
            SELECT count(*) FROM (
              SELECT ROW_NUMBER() OVER (PARTITION BY trade_date,ticker,market,metric_code
                ORDER BY CASE source WHEN 'KRX' THEN 0 ELSE 1 END) rn
              FROM flow
            ) WHERE rn=1
        """).collect()[0].column(0)[0].as_py()

    def q2():
        return ctx.sql("""
            WITH px AS (
              SELECT trade_date,ticker,market,close,
                ROW_NUMBER() OVER (PARTITION BY ticker,market ORDER BY trade_date) d_idx
              FROM ohlcv WHERE NOT (open=0 AND high=0 AND low=0)
            )
            SELECT count(*) FROM px a JOIN px f
              ON f.ticker=a.ticker AND f.market=a.market AND f.d_idx=a.d_idx+20
        """).collect()[0].column(0)[0].as_py()

    def q3():
        return ctx.sql("""
            WITH px AS (
              SELECT trade_date,ticker,market,close,
                ROW_NUMBER() OVER (PARTITION BY ticker,market ORDER BY trade_date) d_idx
              FROM ohlcv WHERE NOT (open=0 AND high=0 AND low=0)
            ),
            fwd AS (
              SELECT a.trade_date,a.ticker,a.market,
                CAST(f.close AS DOUBLE)/NULLIF(a.close,0)-1 AS r
              FROM px a JOIN px f
                ON f.ticker=a.ticker AND f.market=a.market AND f.d_idx=a.d_idx+20
            ),
            ex AS (
              SELECT trade_date,ticker,market,
                r - AVG(r) OVER (PARTITION BY trade_date,market) AS ex FROM fwd
            )
            SELECT count(*) FROM (
              SELECT PERCENT_RANK() OVER (PARTITION BY trade_date,market ORDER BY ex) pr FROM ex
            )
        """).collect()[0].column(0)[0].as_py()

    def q4():
        return ctx.sql("""
            WITH u AS (SELECT trade_date,ticker FROM ohlcv WHERE trade_date >= DATE '2024-01-01'),
            f AS (SELECT ticker,metric_code,value_numeric,
                    (period_end + INTERVAL '90' DAY) AS avail FROM smf)
            SELECT count(*) FROM (
              SELECT ROW_NUMBER() OVER (PARTITION BY u.trade_date,u.ticker,f.metric_code
                ORDER BY f.avail DESC) rn
              FROM u JOIN f ON f.ticker=u.ticker AND f.avail <= u.trade_date
            ) WHERE rn=1
        """).collect()[0].column(0)[0].as_py()

    for name, fn in [("Q1_dedup", q1), ("Q2_label", q2), ("Q3_rank", q3), ("Q4_asof", q4)]:
        try:
            dt, rows = timed(fn)
            out[name] = (dt, rows)
        except Exception as e:
            out[name] = (None, f"ERR {type(e).__name__}: {str(e)[:80]}")
    return out


if __name__ == "__main__":
    import sys

    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    engines = {"duckdb": bench_duckdb, "polars": bench_polars, "datafusion": bench_datafusion}
    if which != "all":
        engines = {which: engines[which]}
    for name, fn in engines.items():
        t0 = time.perf_counter()
        res = fn()
        total = time.perf_counter() - t0
        print(f"\n=== {name} (peak RSS ~{peak_mb():.0f} MB, wall incl. repeats {total:.1f}s) ===")
        for q, (dt, rows) in res.items():
            if dt is None:
                print(f"  {q:10} {rows}")
            else:
                print(f"  {q:10} {dt*1000:8.0f} ms   rows={rows}")
