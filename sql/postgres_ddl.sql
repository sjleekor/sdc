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

-- Sync cursor indexes for remote-to-local replication
CREATE INDEX IF NOT EXISTS ix_stock_master_sync_cursor
    ON stock_master (updated_at, ticker, market);

CREATE INDEX IF NOT EXISTS ix_stock_master_snapshot_sync_cursor
    ON stock_master_snapshot (fetched_at, snapshot_id);

CREATE INDEX IF NOT EXISTS ix_daily_ohlcv_sync_cursor
    ON daily_ohlcv (fetched_at, trade_date, ticker, market);

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

-- 6) sync_checkpoints ─ resume cursors for long-running sync jobs
CREATE TABLE IF NOT EXISTS sync_checkpoints (
    sync_name       TEXT        PRIMARY KEY,
    cursor_payload  JSONB       NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- =============================================================================
-- Phase 0 scaffold for account / flow ingestion
-- =============================================================================

-- 7) dart_corp_master ─ OpenDART corp_code to KRX ticker mapping
CREATE TABLE IF NOT EXISTS dart_corp_master (
    corp_code       TEXT        PRIMARY KEY,
    ticker          TEXT,
    corp_name       TEXT        NOT NULL,
    market          TEXT,
    stock_name      TEXT,
    modify_date     DATE,
    is_active       BOOLEAN     NOT NULL DEFAULT TRUE,
    source          TEXT        NOT NULL DEFAULT 'OPENDART',
    fetched_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_dart_corp_master_ticker
    ON dart_corp_master (ticker);

-- 8) dart_financial_statement_raw ─ raw rows from fnlttSinglAcntAll / XBRL facts
CREATE TABLE IF NOT EXISTS dart_financial_statement_raw (
    raw_id               BIGSERIAL   PRIMARY KEY,
    corp_code            TEXT        NOT NULL,
    ticker               TEXT,
    bsns_year            INT         NOT NULL,
    reprt_code           TEXT        NOT NULL,
    fs_div               TEXT        NOT NULL,
    sj_div               TEXT        NOT NULL,
    sj_nm                TEXT        NOT NULL DEFAULT '',
    account_id           TEXT        NOT NULL,
    account_nm           TEXT        NOT NULL,
    account_detail       TEXT        NOT NULL DEFAULT '',
    thstrm_nm            TEXT        NOT NULL DEFAULT '',
    thstrm_add_amount    NUMERIC(30, 4),
    frmtrm_nm            TEXT        NOT NULL DEFAULT '',
    frmtrm_q_nm          TEXT        NOT NULL DEFAULT '',
    frmtrm_q_amount      NUMERIC(30, 4),
    frmtrm_add_amount    NUMERIC(30, 4),
    bfefrmtrm_nm         TEXT        NOT NULL DEFAULT '',
    ord                  BIGINT      NOT NULL DEFAULT 0,
    thstrm_amount        NUMERIC(30, 4),
    frmtrm_amount        NUMERIC(30, 4),
    bfefrmtrm_amount     NUMERIC(30, 4),
    currency             TEXT,
    rcept_no             TEXT        NOT NULL DEFAULT '',
    source               TEXT        NOT NULL,
    fetched_at           TIMESTAMPTZ NOT NULL,
    raw_payload          JSONB       NOT NULL,
    CONSTRAINT uq_dart_financial_statement_raw
        UNIQUE (corp_code, bsns_year, reprt_code, fs_div, sj_div, account_id, ord, rcept_no)
);

CREATE INDEX IF NOT EXISTS ix_dart_financial_statement_raw_lookup
    ON dart_financial_statement_raw (ticker, bsns_year, reprt_code, fs_div, sj_div);

ALTER TABLE dart_financial_statement_raw
    ADD COLUMN IF NOT EXISTS sj_nm TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_financial_statement_raw
    ADD COLUMN IF NOT EXISTS account_detail TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_financial_statement_raw
    ADD COLUMN IF NOT EXISTS thstrm_nm TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_financial_statement_raw
    ADD COLUMN IF NOT EXISTS thstrm_add_amount NUMERIC(30, 4);
ALTER TABLE dart_financial_statement_raw
    ADD COLUMN IF NOT EXISTS frmtrm_nm TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_financial_statement_raw
    ADD COLUMN IF NOT EXISTS frmtrm_q_nm TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_financial_statement_raw
    ADD COLUMN IF NOT EXISTS frmtrm_q_amount NUMERIC(30, 4);
ALTER TABLE dart_financial_statement_raw
    ADD COLUMN IF NOT EXISTS frmtrm_add_amount NUMERIC(30, 4);
ALTER TABLE dart_financial_statement_raw
    ADD COLUMN IF NOT EXISTS bfefrmtrm_nm TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_financial_statement_raw
    ALTER COLUMN ord SET DEFAULT 0;
UPDATE dart_financial_statement_raw
SET ord = 0
WHERE ord IS NULL;
ALTER TABLE dart_financial_statement_raw
    ALTER COLUMN ord SET NOT NULL;

DO $$
DECLARE
    constraint_name TEXT;
BEGIN
    FOR constraint_name IN
        SELECT c.conname
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        WHERE t.relname = 'dart_financial_statement_raw'
          AND c.contype = 'u'
          AND c.conname <> 'uq_dart_financial_statement_raw'
    LOOP
        EXECUTE format(
            'ALTER TABLE dart_financial_statement_raw DROP CONSTRAINT IF EXISTS %I',
            constraint_name
        );
    END LOOP;
END $$;

ALTER TABLE dart_financial_statement_raw
    DROP CONSTRAINT IF EXISTS uq_dart_financial_statement_raw;
ALTER TABLE dart_financial_statement_raw
    ADD CONSTRAINT uq_dart_financial_statement_raw
    UNIQUE (corp_code, bsns_year, reprt_code, fs_div, sj_div, account_id, ord, rcept_no);

-- 9) dart_share_count_raw ─ stock count disclosures from stockTotqySttus
CREATE TABLE IF NOT EXISTS dart_share_count_raw (
    raw_id               BIGSERIAL   PRIMARY KEY,
    corp_code            TEXT        NOT NULL,
    ticker               TEXT,
    bsns_year            INT         NOT NULL,
    reprt_code           TEXT        NOT NULL,
    rcept_no             TEXT        NOT NULL DEFAULT '',
    corp_cls             TEXT        NOT NULL DEFAULT '',
    se                   TEXT        NOT NULL DEFAULT '',
    isu_stock_totqy      BIGINT,
    now_to_isu_stock_totqy BIGINT,
    now_to_dcrs_stock_totqy BIGINT,
    redc                 TEXT        NOT NULL DEFAULT '',
    profit_incnr         TEXT        NOT NULL DEFAULT '',
    rdmstk_repy          TEXT        NOT NULL DEFAULT '',
    etc                  TEXT        NOT NULL DEFAULT '',
    istc_totqy           BIGINT,
    tesstk_co            BIGINT,
    distb_stock_co       BIGINT,
    stlm_dt              DATE,
    source               TEXT        NOT NULL,
    fetched_at           TIMESTAMPTZ NOT NULL,
    raw_payload          JSONB       NOT NULL,
    CONSTRAINT uq_dart_share_count_raw
        UNIQUE (corp_code, bsns_year, reprt_code, se, rcept_no)
);

CREATE INDEX IF NOT EXISTS ix_dart_share_count_raw_lookup
    ON dart_share_count_raw (ticker, bsns_year, reprt_code);

ALTER TABLE dart_share_count_raw
    ADD COLUMN IF NOT EXISTS corp_cls TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_share_count_raw
    ADD COLUMN IF NOT EXISTS se TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_share_count_raw
    ADD COLUMN IF NOT EXISTS isu_stock_totqy BIGINT;
ALTER TABLE dart_share_count_raw
    ADD COLUMN IF NOT EXISTS now_to_isu_stock_totqy BIGINT;
ALTER TABLE dart_share_count_raw
    ADD COLUMN IF NOT EXISTS now_to_dcrs_stock_totqy BIGINT;
ALTER TABLE dart_share_count_raw
    ADD COLUMN IF NOT EXISTS redc TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_share_count_raw
    ADD COLUMN IF NOT EXISTS profit_incnr TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_share_count_raw
    ADD COLUMN IF NOT EXISTS rdmstk_repy TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_share_count_raw
    ADD COLUMN IF NOT EXISTS etc TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_share_count_raw
    ADD COLUMN IF NOT EXISTS istc_totqy BIGINT;
ALTER TABLE dart_share_count_raw
    ADD COLUMN IF NOT EXISTS tesstk_co BIGINT;
ALTER TABLE dart_share_count_raw
    ADD COLUMN IF NOT EXISTS distb_stock_co BIGINT;
ALTER TABLE dart_share_count_raw
    ADD COLUMN IF NOT EXISTS stlm_dt DATE;

DO $$
DECLARE
    constraint_name TEXT;
BEGIN
    FOR constraint_name IN
        SELECT c.conname
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        WHERE t.relname = 'dart_share_count_raw'
          AND c.contype = 'u'
          AND c.conname <> 'uq_dart_share_count_raw'
    LOOP
        EXECUTE format(
            'ALTER TABLE dart_share_count_raw DROP CONSTRAINT IF EXISTS %I',
            constraint_name
        );
    END LOOP;
END $$;

ALTER TABLE dart_share_count_raw
    DROP CONSTRAINT IF EXISTS uq_dart_share_count_raw;
ALTER TABLE dart_share_count_raw
    ADD CONSTRAINT uq_dart_share_count_raw
    UNIQUE (corp_code, bsns_year, reprt_code, se, rcept_no);

-- 10) dart_shareholder_return_raw ─ dividend / treasury stock disclosures
CREATE TABLE IF NOT EXISTS dart_shareholder_return_raw (
    raw_id               BIGSERIAL   PRIMARY KEY,
    corp_code            TEXT        NOT NULL,
    ticker               TEXT,
    bsns_year            INT         NOT NULL,
    reprt_code           TEXT        NOT NULL DEFAULT '',
    statement_type       TEXT        NOT NULL,
    row_name             TEXT        NOT NULL DEFAULT '',
    stock_knd            TEXT        NOT NULL DEFAULT '',
    dim1                 TEXT        NOT NULL DEFAULT '',
    dim2                 TEXT        NOT NULL DEFAULT '',
    dim3                 TEXT        NOT NULL DEFAULT '',
    metric_code          TEXT        NOT NULL,
    metric_name          TEXT        NOT NULL,
    value_numeric        NUMERIC(30, 4),
    value_text           TEXT        NOT NULL DEFAULT '',
    unit                 TEXT,
    rcept_no             TEXT        NOT NULL DEFAULT '',
    stlm_dt              DATE,
    source               TEXT        NOT NULL,
    fetched_at           TIMESTAMPTZ NOT NULL,
    raw_payload          JSONB       NOT NULL,
    CONSTRAINT uq_dart_shareholder_return_raw
        UNIQUE (
            corp_code,
            bsns_year,
            reprt_code,
            statement_type,
            row_name,
            stock_knd,
            dim1,
            dim2,
            dim3,
            metric_code,
            rcept_no
        )
);

CREATE INDEX IF NOT EXISTS ix_dart_shareholder_return_raw_lookup
    ON dart_shareholder_return_raw (ticker, bsns_year, reprt_code, statement_type);

ALTER TABLE dart_shareholder_return_raw
    ADD COLUMN IF NOT EXISTS row_name TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_shareholder_return_raw
    ADD COLUMN IF NOT EXISTS stock_knd TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_shareholder_return_raw
    ADD COLUMN IF NOT EXISTS dim1 TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_shareholder_return_raw
    ADD COLUMN IF NOT EXISTS dim2 TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_shareholder_return_raw
    ADD COLUMN IF NOT EXISTS dim3 TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_shareholder_return_raw
    ADD COLUMN IF NOT EXISTS value_numeric NUMERIC(30, 4);
ALTER TABLE dart_shareholder_return_raw
    ADD COLUMN IF NOT EXISTS value_text TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_shareholder_return_raw
    ADD COLUMN IF NOT EXISTS stlm_dt DATE;
ALTER TABLE dart_shareholder_return_raw
    ALTER COLUMN bsns_year SET NOT NULL;

DO $$
DECLARE
    constraint_name TEXT;
BEGIN
    FOR constraint_name IN
        SELECT c.conname
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        WHERE t.relname = 'dart_shareholder_return_raw'
          AND c.contype = 'u'
          AND c.conname <> 'uq_dart_shareholder_return_raw'
    LOOP
        EXECUTE format(
            'ALTER TABLE dart_shareholder_return_raw DROP CONSTRAINT IF EXISTS %I',
            constraint_name
        );
    END LOOP;
END $$;

ALTER TABLE dart_shareholder_return_raw
    DROP CONSTRAINT IF EXISTS uq_dart_shareholder_return_raw;
ALTER TABLE dart_shareholder_return_raw
    ADD CONSTRAINT uq_dart_shareholder_return_raw
    UNIQUE (
        corp_code,
        bsns_year,
        reprt_code,
        statement_type,
        row_name,
        stock_knd,
        dim1,
        dim2,
        dim3,
        metric_code,
        rcept_no
    );

-- 11) dart_xbrl_document ─ parsed XBRL ZIP document metadata
CREATE TABLE IF NOT EXISTS dart_xbrl_document (
    document_id            BIGSERIAL   PRIMARY KEY,
    corp_code              TEXT        NOT NULL,
    ticker                 TEXT,
    bsns_year              INT         NOT NULL,
    reprt_code             TEXT        NOT NULL,
    rcept_no               TEXT        NOT NULL,
    zip_entry_count        INT         NOT NULL DEFAULT 0,
    instance_document_name TEXT        NOT NULL DEFAULT '',
    label_ko_document_name TEXT        NOT NULL DEFAULT '',
    source                 TEXT        NOT NULL,
    fetched_at             TIMESTAMPTZ NOT NULL,
    raw_payload            JSONB       NOT NULL,
    CONSTRAINT uq_dart_xbrl_document
        UNIQUE (corp_code, bsns_year, reprt_code, rcept_no)
);

CREATE INDEX IF NOT EXISTS ix_dart_xbrl_document_lookup
    ON dart_xbrl_document (ticker, bsns_year, reprt_code);

ALTER TABLE dart_xbrl_document
    ADD COLUMN IF NOT EXISTS zip_entry_count INT NOT NULL DEFAULT 0;
ALTER TABLE dart_xbrl_document
    ADD COLUMN IF NOT EXISTS instance_document_name TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_xbrl_document
    ADD COLUMN IF NOT EXISTS label_ko_document_name TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_xbrl_document
    ADD COLUMN IF NOT EXISTS raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb;

-- 12) dart_xbrl_fact_raw ─ parsed XBRL facts
CREATE TABLE IF NOT EXISTS dart_xbrl_fact_raw (
    raw_id                 BIGSERIAL   PRIMARY KEY,
    corp_code              TEXT        NOT NULL,
    ticker                 TEXT,
    bsns_year              INT         NOT NULL,
    reprt_code             TEXT        NOT NULL,
    rcept_no               TEXT        NOT NULL,
    concept_id             TEXT        NOT NULL,
    concept_name           TEXT        NOT NULL DEFAULT '',
    namespace_uri          TEXT        NOT NULL DEFAULT '',
    context_id             TEXT        NOT NULL DEFAULT '',
    context_type           TEXT        NOT NULL DEFAULT '',
    period_start           DATE,
    period_end             DATE,
    instant_date           DATE,
    dimensions             JSONB       NOT NULL DEFAULT '[]'::jsonb,
    unit_id                TEXT        NOT NULL DEFAULT '',
    unit_measure           TEXT        NOT NULL DEFAULT '',
    decimals               TEXT        NOT NULL DEFAULT '',
    value_numeric          NUMERIC(30, 4),
    value_text             TEXT        NOT NULL DEFAULT '',
    is_nil                 BOOLEAN     NOT NULL DEFAULT FALSE,
    label_ko               TEXT        NOT NULL DEFAULT '',
    source                 TEXT        NOT NULL,
    fetched_at             TIMESTAMPTZ NOT NULL,
    raw_payload            JSONB       NOT NULL,
    CONSTRAINT uq_dart_xbrl_fact_raw
        UNIQUE (corp_code, bsns_year, reprt_code, rcept_no, context_id, concept_id)
);

CREATE INDEX IF NOT EXISTS ix_dart_xbrl_fact_raw_lookup
    ON dart_xbrl_fact_raw (ticker, bsns_year, reprt_code, concept_id);

ALTER TABLE dart_xbrl_fact_raw
    ADD COLUMN IF NOT EXISTS concept_name TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_xbrl_fact_raw
    ADD COLUMN IF NOT EXISTS namespace_uri TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_xbrl_fact_raw
    ADD COLUMN IF NOT EXISTS context_id TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_xbrl_fact_raw
    ADD COLUMN IF NOT EXISTS context_type TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_xbrl_fact_raw
    ADD COLUMN IF NOT EXISTS period_start DATE;
ALTER TABLE dart_xbrl_fact_raw
    ADD COLUMN IF NOT EXISTS period_end DATE;
ALTER TABLE dart_xbrl_fact_raw
    ADD COLUMN IF NOT EXISTS instant_date DATE;
ALTER TABLE dart_xbrl_fact_raw
    ADD COLUMN IF NOT EXISTS dimensions JSONB NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE dart_xbrl_fact_raw
    ADD COLUMN IF NOT EXISTS unit_id TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_xbrl_fact_raw
    ADD COLUMN IF NOT EXISTS unit_measure TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_xbrl_fact_raw
    ADD COLUMN IF NOT EXISTS decimals TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_xbrl_fact_raw
    ADD COLUMN IF NOT EXISTS value_numeric NUMERIC(30, 4);
ALTER TABLE dart_xbrl_fact_raw
    ADD COLUMN IF NOT EXISTS value_text TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_xbrl_fact_raw
    ADD COLUMN IF NOT EXISTS is_nil BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE dart_xbrl_fact_raw
    ADD COLUMN IF NOT EXISTS label_ko TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_xbrl_fact_raw
    ADD COLUMN IF NOT EXISTS raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb;

-- 13) metric_catalog ─ canonical metric dictionary
CREATE TABLE IF NOT EXISTS metric_catalog (
    metric_code      TEXT        PRIMARY KEY,
    metric_name      TEXT        NOT NULL,
    category         TEXT        NOT NULL,
    unit             TEXT        NOT NULL DEFAULT '',
    description      TEXT        NOT NULL DEFAULT '',
    is_active        BOOLEAN     NOT NULL DEFAULT TRUE,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 14) metric_mapping_rule ─ active raw-to-canonical mapping rules
CREATE TABLE IF NOT EXISTS metric_mapping_rule (
    rule_code        TEXT        PRIMARY KEY,
    metric_code      TEXT        NOT NULL REFERENCES metric_catalog(metric_code),
    source_table     TEXT        NOT NULL,
    value_selector   TEXT        NOT NULL,
    priority         INT         NOT NULL,
    statement_type   TEXT        NOT NULL DEFAULT '',
    fs_div           TEXT        NOT NULL DEFAULT '',
    sj_div           TEXT        NOT NULL DEFAULT '',
    account_id       TEXT        NOT NULL DEFAULT '',
    account_nm       TEXT        NOT NULL DEFAULT '',
    row_name         TEXT        NOT NULL DEFAULT '',
    stock_knd        TEXT        NOT NULL DEFAULT '',
    dim1             TEXT        NOT NULL DEFAULT '',
    dim2             TEXT        NOT NULL DEFAULT '',
    dim3             TEXT        NOT NULL DEFAULT '',
    metric_code_match TEXT       NOT NULL DEFAULT '',
    is_active        BOOLEAN     NOT NULL DEFAULT TRUE,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_metric_mapping_rule_metric
    ON metric_mapping_rule (metric_code, source_table, priority);

-- 15) stock_metric_fact ─ normalized canonical metric facts
CREATE TABLE IF NOT EXISTS stock_metric_fact (
    fact_id            BIGSERIAL   PRIMARY KEY,
    ticker             TEXT        NOT NULL,
    market             TEXT        NOT NULL,
    corp_code          TEXT        NOT NULL,
    metric_code        TEXT        NOT NULL REFERENCES metric_catalog(metric_code),
    period_type        TEXT        NOT NULL,
    period_end         DATE,
    bsns_year          INT         NOT NULL,
    reprt_code         TEXT        NOT NULL,
    fs_div             TEXT        NOT NULL DEFAULT '',
    value_numeric      NUMERIC(30, 4),
    value_text         TEXT        NOT NULL DEFAULT '',
    unit               TEXT        NOT NULL DEFAULT '',
    source_table       TEXT        NOT NULL,
    source_key         TEXT        NOT NULL DEFAULT '',
    mapping_rule_code  TEXT        NOT NULL REFERENCES metric_mapping_rule(rule_code),
    fetched_at         TIMESTAMPTZ NOT NULL,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_stock_metric_fact
        UNIQUE (ticker, metric_code, bsns_year, reprt_code)
);

CREATE INDEX IF NOT EXISTS ix_stock_metric_fact_lookup
    ON stock_metric_fact (ticker, metric_code, bsns_year DESC, reprt_code);

-- 16) krx_security_flow_raw ─ daily investor/short-selling/borrow flow metrics
CREATE TABLE IF NOT EXISTS krx_security_flow_raw (
    raw_id               BIGSERIAL   PRIMARY KEY,
    trade_date           DATE        NOT NULL,
    ticker               TEXT        NOT NULL,
    market               TEXT        NOT NULL,
    metric_code          TEXT        NOT NULL,
    metric_name          TEXT        NOT NULL,
    value                NUMERIC(30, 4),
    unit                 TEXT,
    source               TEXT        NOT NULL,
    fetched_at           TIMESTAMPTZ NOT NULL,
    raw_payload          JSONB       NOT NULL,
    UNIQUE (trade_date, ticker, market, metric_code, source)
);

CREATE INDEX IF NOT EXISTS ix_krx_security_flow_raw_lookup
    ON krx_security_flow_raw (ticker, market, trade_date DESC);

-- 17) operating_source_document ─ provenance for sector-specific KPI extraction
CREATE TABLE IF NOT EXISTS operating_source_document (
    document_key        TEXT        PRIMARY KEY,
    ticker              TEXT        NOT NULL,
    market              TEXT        NOT NULL,
    sector_key          TEXT        NOT NULL,
    document_type       TEXT        NOT NULL,
    title               TEXT        NOT NULL,
    document_date       DATE,
    period_end          DATE,
    source_system       TEXT        NOT NULL DEFAULT '',
    source_url          TEXT        NOT NULL DEFAULT '',
    language            TEXT        NOT NULL DEFAULT 'ko',
    content_text        TEXT        NOT NULL,
    fetched_at          TIMESTAMPTZ NOT NULL,
    raw_payload         JSONB       NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_operating_source_document_lookup
    ON operating_source_document (ticker, sector_key, period_end DESC);

-- 18) operating_metric_fact ─ extracted sector-specific KPI facts
CREATE TABLE IF NOT EXISTS operating_metric_fact (
    fact_id             BIGSERIAL   PRIMARY KEY,
    ticker              TEXT        NOT NULL,
    market              TEXT        NOT NULL,
    sector_key          TEXT        NOT NULL,
    metric_code         TEXT        NOT NULL,
    metric_name         TEXT        NOT NULL,
    period_end          DATE,
    value_numeric       NUMERIC(30, 4),
    value_text          TEXT        NOT NULL DEFAULT '',
    unit                TEXT        NOT NULL DEFAULT '',
    document_key        TEXT        NOT NULL REFERENCES operating_source_document(document_key),
    extractor_code      TEXT        NOT NULL,
    raw_snippet         TEXT        NOT NULL DEFAULT '',
    fetched_at          TIMESTAMPTZ NOT NULL,
    raw_payload         JSONB       NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_operating_metric_fact
        UNIQUE (ticker, metric_code, period_end, document_key, extractor_code)
);

CREATE INDEX IF NOT EXISTS ix_operating_metric_fact_lookup
    ON operating_metric_fact (ticker, sector_key, metric_code, period_end DESC);

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
