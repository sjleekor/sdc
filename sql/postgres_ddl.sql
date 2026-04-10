-- =============================================================================
-- KRX Data Pipeline — PostgreSQL DDL
-- =============================================================================
-- Run this file to initialise the database schema:
--   psql -d krx_data -f sql/postgres_ddl.sql
--
-- Design decisions:
--   • OHLCV values stored as BIGINT (Korean won, no decimals needed).
--   • daily_ohlcv upsert uses ON CONFLICT ... DO UPDATE so re-fetches overwrite
--     stale rows — preferred over DO NOTHING because source corrections should
--     propagate automatically.
--   • stock_master upsert by (ticker, market) keeps the latest state.
--   • All timestamps are TIMESTAMPTZ (UTC-stored, Asia/Seoul in application).
-- =============================================================================

-- 1) stock_master ─ latest state of each listed stock
CREATE TABLE IF NOT EXISTS stock_master (
    ticker          TEXT        NOT NULL,
    market          TEXT        NOT NULL,   -- KOSPI | KOSDAQ
    name            TEXT        NOT NULL,
    listing_date    DATE,
    status          TEXT        NOT NULL,   -- ACTIVE | DELISTED | UNKNOWN
    last_seen_date  DATE        NOT NULL,
    source          TEXT        NOT NULL,   -- FDR | PYKRX
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ticker, market)
);

-- 2) stock_master_snapshot ─ point-in-time universe fetch metadata
CREATE TABLE IF NOT EXISTS stock_master_snapshot (
    snapshot_id     UUID        PRIMARY KEY,
    as_of_date      DATE        NOT NULL,
    source          TEXT        NOT NULL,
    fetched_at      TIMESTAMPTZ NOT NULL,
    record_count    INT         NOT NULL
);

-- 3) stock_master_snapshot_items ─ individual stocks captured in a snapshot
--    Recommended for full auditability: you can diff any two snapshots to find
--    new listings, delistings, or name changes.
CREATE TABLE IF NOT EXISTS stock_master_snapshot_items (
    snapshot_id     UUID        NOT NULL REFERENCES stock_master_snapshot(snapshot_id),
    ticker          TEXT        NOT NULL,
    market          TEXT        NOT NULL,
    name            TEXT        NOT NULL,
    listing_date    DATE,
    status          TEXT        NOT NULL,
    UNIQUE (snapshot_id, ticker, market)
);

-- 4) daily_ohlcv ─ daily price bars
CREATE TABLE IF NOT EXISTS daily_ohlcv (
    trade_date      DATE        NOT NULL,
    ticker          TEXT        NOT NULL,
    market          TEXT        NOT NULL,
    open            BIGINT      NOT NULL,
    high            BIGINT      NOT NULL,
    low             BIGINT      NOT NULL,
    close           BIGINT      NOT NULL,
    volume          BIGINT      NOT NULL,
    source          TEXT        NOT NULL,
    fetched_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (trade_date, ticker, market)
);

-- Covering index for per-ticker queries ordered by date descending
CREATE INDEX IF NOT EXISTS ix_daily_ohlcv_ticker_date
    ON daily_ohlcv (ticker, market, trade_date DESC);

-- 5) ingestion_runs ─ audit log for every pipeline execution
CREATE TABLE IF NOT EXISTS ingestion_runs (
    run_id          UUID        PRIMARY KEY,
    run_type        TEXT        NOT NULL,   -- universe_sync | daily_backfill | validate
    started_at      TIMESTAMPTZ NOT NULL,
    ended_at        TIMESTAMPTZ,
    status          TEXT        NOT NULL,   -- running | success | failed
    params          JSONB,
    counts          JSONB,
    error_summary   TEXT
);

-- =============================================================================
-- Future extension: intraday_ohlcv (OUT OF SCOPE)
-- =============================================================================
-- CREATE TABLE IF NOT EXISTS intraday_ohlcv (
--     trade_ts    TIMESTAMPTZ NOT NULL,
--     ticker      TEXT        NOT NULL,
--     market      TEXT        NOT NULL,
--     interval    TEXT        NOT NULL,   -- 1m | 5m | 1h
--     open        BIGINT      NOT NULL,
--     high        BIGINT      NOT NULL,
--     low         BIGINT      NOT NULL,
--     close       BIGINT      NOT NULL,
--     volume      BIGINT      NOT NULL,
--     source      TEXT        NOT NULL,
--     fetched_at  TIMESTAMPTZ NOT NULL,
--     PRIMARY KEY (trade_ts, ticker, market, interval)
-- );
