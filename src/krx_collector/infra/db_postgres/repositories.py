"""PostgreSQL storage implementation.

Implements the :class:`~krx_collector.ports.storage.Storage` protocol
using ``psycopg2`` against the schema defined in ``sql/postgres_ddl.sql``.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import psycopg2.extras

from krx_collector.domain.enums import ListingStatus, Market, RunStatus, RunType, Source
from krx_collector.domain.models import (
    DailyBar,
    DartCorp,
    DartFinancialStatementLine,
    DartShareCountLine,
    DartShareholderReturnLine,
    DartXbrlDocument,
    DartXbrlFactLine,
    IngestionRun,
    MetricCatalogEntry,
    MetricMappingRule,
    OperatingMetricFact,
    OperatingSourceDocument,
    SecurityFlowLine,
    Stock,
    StockMetricFact,
    StockUniverseSnapshot,
    UpsertResult,
)
from krx_collector.infra.calendar.trading_days import get_trading_days
from krx_collector.infra.db_postgres.connection import get_connection

logger = logging.getLogger(__name__)


class PostgresStorage:
    """PostgreSQL-backed storage conforming to the ``Storage`` protocol.

    Args:
        dsn: PostgreSQL connection string.
    """

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    # -- Schema management ----------------------------------------------------

    def init_schema(self) -> None:
        """Execute ``sql/postgres_ddl.sql`` to create / update tables."""
        # Find sql file relative to project root
        # Since this code is in src/krx_collector/infra/db_postgres,
        # we can go up 4 levels and into 'sql'.
        sql_path = Path(__file__).parent.parent.parent.parent.parent / "sql" / "postgres_ddl.sql"
        if not sql_path.exists():
            logger.error("DDL file not found at %s", sql_path)
            raise FileNotFoundError(f"DDL file not found at {sql_path}")

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql_path.read_text(encoding="utf-8"))
        logger.info("Schema initialized successfully.")

    # -- Stock master ---------------------------------------------------------

    def upsert_stock_master(
        self,
        stocks: list[Stock],
        snapshot: StockUniverseSnapshot,
    ) -> UpsertResult:
        """Upsert stock_master rows and persist the snapshot."""
        if not stocks:
            return UpsertResult()

        result = UpsertResult()

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                # 1. Insert snapshot metadata
                cur.execute(
                    """
                    INSERT INTO stock_master_snapshot (
                        snapshot_id,
                        as_of_date,
                        source,
                        fetched_at,
                        record_count
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        snapshot.snapshot_id,
                        snapshot.as_of_date,
                        snapshot.source.value,
                        snapshot.fetched_at,
                        snapshot.record_count,
                    ),
                )

                # 2. Insert snapshot items
                snapshot_items_args = [
                    (
                        snapshot.snapshot_id,
                        s.ticker,
                        s.market.value,
                        s.name,
                        s.status.value,
                    )
                    for s in snapshot.records
                ]
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO stock_master_snapshot_items (
                        snapshot_id,
                        ticker,
                        market,
                        name,
                        status
                    )
                    VALUES %s
                    ON CONFLICT (snapshot_id, ticker, market) DO NOTHING
                    """,
                    snapshot_items_args,
                    page_size=1000,
                )

                # 3. Upsert stock_master
                master_args = [
                    (
                        s.ticker,
                        s.market.value,
                        s.name,
                        s.status.value,
                        s.last_seen_date,
                        s.source.value,
                    )
                    for s in stocks
                ]

                # psycopg2 execute_values doesn't easily tell us inserted vs updated count directly
                # We will approximate or just use rowcount for total affected.
                # Actually, DO UPDATE returns the affected rows if we append RETURNING.

                # To accurately count, we can do a standard execute_values
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO stock_master (ticker, market, name, status, last_seen_date, source)
                    VALUES %s
                    ON CONFLICT (ticker, market) DO UPDATE SET
                        name = EXCLUDED.name,
                        status = EXCLUDED.status,
                        last_seen_date = EXCLUDED.last_seen_date,
                        source = EXCLUDED.source,
                        updated_at = now()
                    """,
                    master_args,
                    page_size=1000,
                )
                # Approximation: we can just count total as updated for upsert
                result.updated = cur.rowcount

        return result

    def get_active_stocks(self, market: Market | None = None) -> list[Stock]:
        """Return currently active stocks from stock_master."""
        stocks = []
        with get_connection(self._dsn) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                if market:
                    cur.execute(
                        "SELECT * FROM stock_master WHERE status = 'ACTIVE' AND market = %s",
                        (market.value,),
                    )
                else:
                    cur.execute("SELECT * FROM stock_master WHERE status = 'ACTIVE'")

                for row in cur.fetchall():
                    stocks.append(
                        Stock(
                            ticker=row["ticker"],
                            market=Market(row["market"]),
                            name=row["name"],
                            status=ListingStatus(row["status"]),
                            last_seen_date=row["last_seen_date"],
                            source=Source(row["source"]),
                        )
                    )
        return stocks

    def upsert_dart_corp_master(self, records: list[DartCorp]) -> UpsertResult:
        """Upsert OpenDART corp-code master rows."""
        if not records:
            return UpsertResult()

        result = UpsertResult()

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                args = [
                    (
                        record.corp_code,
                        record.ticker,
                        record.corp_name,
                        record.market.value if record.market else None,
                        record.stock_name,
                        record.modify_date,
                        record.is_active,
                        record.source.value,
                        record.fetched_at,
                    )
                    for record in records
                ]

                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO dart_corp_master (
                        corp_code,
                        ticker,
                        corp_name,
                        market,
                        stock_name,
                        modify_date,
                        is_active,
                        source,
                        fetched_at
                    )
                    VALUES %s
                    ON CONFLICT (corp_code) DO UPDATE SET
                        ticker = EXCLUDED.ticker,
                        corp_name = EXCLUDED.corp_name,
                        market = EXCLUDED.market,
                        stock_name = EXCLUDED.stock_name,
                        modify_date = EXCLUDED.modify_date,
                        is_active = EXCLUDED.is_active,
                        source = EXCLUDED.source,
                        fetched_at = EXCLUDED.fetched_at,
                        updated_at = now()
                    """,
                    args,
                    page_size=1000,
                )
                result.updated = cur.rowcount

        return result

    def get_dart_corp_master(
        self,
        active_only: bool = True,
        tickers: list[str] | None = None,
    ) -> list[DartCorp]:
        """Return OpenDART corp master rows mapped to local tickers."""
        records: list[DartCorp] = []
        with get_connection(self._dsn) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                sql = "SELECT * FROM dart_corp_master"
                conditions: list[str] = []
                params: list[object] = []

                if active_only:
                    conditions.append("is_active = TRUE")
                if tickers:
                    conditions.append("ticker = ANY(%s)")
                    params.append(tickers)

                if conditions:
                    sql += " WHERE " + " AND ".join(conditions)
                sql += " ORDER BY ticker NULLS LAST, corp_code"

                cur.execute(sql, params)
                for row in cur.fetchall():
                    records.append(
                        DartCorp(
                            corp_code=row["corp_code"],
                            corp_name=row["corp_name"],
                            ticker=row["ticker"],
                            market=Market(row["market"]) if row["market"] else None,
                            stock_name=row["stock_name"],
                            modify_date=row["modify_date"],
                            is_active=row["is_active"],
                            source=Source(row["source"]),
                            fetched_at=row["fetched_at"],
                        )
                    )
        return records

    def get_existing_dart_financial_statement_keys(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        fs_divs: list[str],
        corp_codes: list[str] | None = None,
    ) -> set[tuple[str, int, str, str]]:
        """Return (corp_code, bsns_year, reprt_code, fs_div) tuples already present in raw."""
        if not bsns_years or not reprt_codes or not fs_divs:
            return set()
        sql = """
            SELECT DISTINCT corp_code, bsns_year, reprt_code, fs_div
            FROM dart_financial_statement_raw
            WHERE bsns_year = ANY(%s)
              AND reprt_code = ANY(%s)
              AND fs_div = ANY(%s)
        """
        params: list[object] = [bsns_years, reprt_codes, fs_divs]
        if corp_codes:
            sql += " AND corp_code = ANY(%s)"
            params.append(corp_codes)
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                return {(row[0], row[1], row[2], row[3]) for row in cur.fetchall()}

    def get_existing_dart_share_count_keys(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        corp_codes: list[str] | None = None,
    ) -> set[tuple[str, int, str]]:
        """Return (corp_code, bsns_year, reprt_code) tuples already present."""
        if not bsns_years or not reprt_codes:
            return set()
        sql = """
            SELECT DISTINCT corp_code, bsns_year, reprt_code
            FROM dart_share_count_raw
            WHERE bsns_year = ANY(%s)
              AND reprt_code = ANY(%s)
        """
        params: list[object] = [bsns_years, reprt_codes]
        if corp_codes:
            sql += " AND corp_code = ANY(%s)"
            params.append(corp_codes)
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                return {(row[0], row[1], row[2]) for row in cur.fetchall()}

    def get_existing_dart_shareholder_return_keys(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        corp_codes: list[str] | None = None,
    ) -> set[tuple[str, int, str, str]]:
        """Return (corp_code, bsns_year, reprt_code, statement_type) tuples already present."""
        if not bsns_years or not reprt_codes:
            return set()
        sql = """
            SELECT DISTINCT corp_code, bsns_year, reprt_code, statement_type
            FROM dart_shareholder_return_raw
            WHERE bsns_year = ANY(%s)
              AND reprt_code = ANY(%s)
        """
        params: list[object] = [bsns_years, reprt_codes]
        if corp_codes:
            sql += " AND corp_code = ANY(%s)"
            params.append(corp_codes)
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                return {(row[0], row[1], row[2], row[3]) for row in cur.fetchall()}

    def get_existing_dart_xbrl_document_keys(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        corp_codes: list[str] | None = None,
    ) -> set[tuple[str, int, str, str]]:
        """Return (corp_code, bsns_year, reprt_code, rcept_no) tuples already parsed."""
        if not bsns_years or not reprt_codes:
            return set()
        sql = """
            SELECT corp_code, bsns_year, reprt_code, rcept_no
            FROM dart_xbrl_document
            WHERE bsns_year = ANY(%s)
              AND reprt_code = ANY(%s)
        """
        params: list[object] = [bsns_years, reprt_codes]
        if corp_codes:
            sql += " AND corp_code = ANY(%s)"
            params.append(corp_codes)
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                return {(row[0], row[1], row[2], row[3]) for row in cur.fetchall()}

    def get_last_successful_run(self, run_type: RunType) -> IngestionRun | None:
        """Return the most recent SUCCESS-status run for the given run_type, or None."""
        import json

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        run_id,
                        run_type,
                        started_at,
                        ended_at,
                        status,
                        params,
                        counts,
                        error_summary
                    FROM ingestion_runs
                    WHERE run_type = %s
                      AND status = %s
                    ORDER BY started_at DESC
                    LIMIT 1
                    """,
                    (run_type.value, RunStatus.SUCCESS.value),
                )
                row = cur.fetchone()
                if row is None:
                    return None
                params = (
                    row[5] if isinstance(row[5], dict) else (json.loads(row[5]) if row[5] else {})
                )
                counts = (
                    row[6] if isinstance(row[6], dict) else (json.loads(row[6]) if row[6] else {})
                )
                return IngestionRun(
                    run_id=str(row[0]),
                    run_type=RunType(row[1]),
                    started_at=row[2],
                    ended_at=row[3],
                    status=RunStatus(row[4]),
                    params=params,
                    counts=counts,
                    error_summary=row[7],
                )

    def upsert_dart_financial_statement_raw(
        self,
        records: list[DartFinancialStatementLine],
    ) -> UpsertResult:
        """Upsert OpenDART financial-statement raw rows."""
        if not records:
            return UpsertResult()

        result = UpsertResult()
        deduped_records = {
            (
                record.corp_code,
                record.bsns_year,
                record.reprt_code,
                record.fs_div,
                record.sj_div,
                record.account_id,
                record.ord,
                record.rcept_no,
            ): record
            for record in records
        }
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                args = [
                    (
                        record.corp_code,
                        record.ticker,
                        record.bsns_year,
                        record.reprt_code,
                        record.fs_div,
                        record.sj_div,
                        record.sj_nm,
                        record.account_id,
                        record.account_nm,
                        record.account_detail,
                        record.thstrm_nm,
                        record.thstrm_amount,
                        record.thstrm_add_amount,
                        record.frmtrm_nm,
                        record.frmtrm_amount,
                        record.frmtrm_q_nm,
                        record.frmtrm_q_amount,
                        record.frmtrm_add_amount,
                        record.bfefrmtrm_nm,
                        record.bfefrmtrm_amount,
                        record.ord,
                        record.currency,
                        record.rcept_no,
                        record.source.value,
                        record.fetched_at,
                        psycopg2.extras.Json(record.raw_payload),
                    )
                    for record in deduped_records.values()
                ]

                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO dart_financial_statement_raw (
                        corp_code,
                        ticker,
                        bsns_year,
                        reprt_code,
                        fs_div,
                        sj_div,
                        sj_nm,
                        account_id,
                        account_nm,
                        account_detail,
                        thstrm_nm,
                        thstrm_amount,
                        thstrm_add_amount,
                        frmtrm_nm,
                        frmtrm_amount,
                        frmtrm_q_nm,
                        frmtrm_q_amount,
                        frmtrm_add_amount,
                        bfefrmtrm_nm,
                        bfefrmtrm_amount,
                        ord,
                        currency,
                        rcept_no,
                        source,
                        fetched_at,
                        raw_payload
                    )
                    VALUES %s
                    ON CONFLICT (
                        corp_code,
                        bsns_year,
                        reprt_code,
                        fs_div,
                        sj_div,
                        account_id,
                        ord,
                        rcept_no
                    )
                    DO UPDATE SET
                        ticker = EXCLUDED.ticker,
                        sj_nm = EXCLUDED.sj_nm,
                        account_nm = EXCLUDED.account_nm,
                        account_detail = EXCLUDED.account_detail,
                        thstrm_nm = EXCLUDED.thstrm_nm,
                        thstrm_amount = EXCLUDED.thstrm_amount,
                        thstrm_add_amount = EXCLUDED.thstrm_add_amount,
                        frmtrm_nm = EXCLUDED.frmtrm_nm,
                        frmtrm_amount = EXCLUDED.frmtrm_amount,
                        frmtrm_q_nm = EXCLUDED.frmtrm_q_nm,
                        frmtrm_q_amount = EXCLUDED.frmtrm_q_amount,
                        frmtrm_add_amount = EXCLUDED.frmtrm_add_amount,
                        bfefrmtrm_nm = EXCLUDED.bfefrmtrm_nm,
                        bfefrmtrm_amount = EXCLUDED.bfefrmtrm_amount,
                        ord = EXCLUDED.ord,
                        currency = EXCLUDED.currency,
                        source = EXCLUDED.source,
                        fetched_at = EXCLUDED.fetched_at,
                        raw_payload = EXCLUDED.raw_payload
                    """,
                    args,
                    page_size=1000,
                )
                result.updated = cur.rowcount

        return result

    def upsert_dart_share_count_raw(
        self,
        records: list[DartShareCountLine],
    ) -> UpsertResult:
        """Upsert OpenDART share-count raw rows."""
        if not records:
            return UpsertResult()

        result = UpsertResult()
        deduped_records = {
            (
                record.corp_code,
                record.bsns_year,
                record.reprt_code,
                record.se,
                record.rcept_no,
            ): record
            for record in records
        }
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                args = [
                    (
                        record.corp_code,
                        record.ticker,
                        record.bsns_year,
                        record.reprt_code,
                        record.rcept_no,
                        record.corp_cls,
                        record.se,
                        record.isu_stock_totqy,
                        record.now_to_isu_stock_totqy,
                        record.now_to_dcrs_stock_totqy,
                        record.redc,
                        record.profit_incnr,
                        record.rdmstk_repy,
                        record.etc,
                        record.istc_totqy,
                        record.tesstk_co,
                        record.distb_stock_co,
                        record.stlm_dt,
                        record.source.value,
                        record.fetched_at,
                        psycopg2.extras.Json(record.raw_payload),
                    )
                    for record in deduped_records.values()
                ]

                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO dart_share_count_raw (
                        corp_code,
                        ticker,
                        bsns_year,
                        reprt_code,
                        rcept_no,
                        corp_cls,
                        se,
                        isu_stock_totqy,
                        now_to_isu_stock_totqy,
                        now_to_dcrs_stock_totqy,
                        redc,
                        profit_incnr,
                        rdmstk_repy,
                        etc,
                        istc_totqy,
                        tesstk_co,
                        distb_stock_co,
                        stlm_dt,
                        source,
                        fetched_at,
                        raw_payload
                    )
                    VALUES %s
                    ON CONFLICT (corp_code, bsns_year, reprt_code, se, rcept_no)
                    DO UPDATE SET
                        ticker = EXCLUDED.ticker,
                        corp_cls = EXCLUDED.corp_cls,
                        isu_stock_totqy = EXCLUDED.isu_stock_totqy,
                        now_to_isu_stock_totqy = EXCLUDED.now_to_isu_stock_totqy,
                        now_to_dcrs_stock_totqy = EXCLUDED.now_to_dcrs_stock_totqy,
                        redc = EXCLUDED.redc,
                        profit_incnr = EXCLUDED.profit_incnr,
                        rdmstk_repy = EXCLUDED.rdmstk_repy,
                        etc = EXCLUDED.etc,
                        istc_totqy = EXCLUDED.istc_totqy,
                        tesstk_co = EXCLUDED.tesstk_co,
                        distb_stock_co = EXCLUDED.distb_stock_co,
                        stlm_dt = EXCLUDED.stlm_dt,
                        source = EXCLUDED.source,
                        fetched_at = EXCLUDED.fetched_at,
                        raw_payload = EXCLUDED.raw_payload
                    """,
                    args,
                    page_size=1000,
                )
                result.updated = cur.rowcount

        return result

    def upsert_dart_shareholder_return_raw(
        self,
        records: list[DartShareholderReturnLine],
    ) -> UpsertResult:
        """Upsert OpenDART dividend / treasury-stock flattened raw rows."""
        if not records:
            return UpsertResult()

        result = UpsertResult()
        deduped_records = {
            (
                record.corp_code,
                record.bsns_year,
                record.reprt_code,
                record.statement_type,
                record.row_name,
                record.stock_knd,
                record.dim1,
                record.dim2,
                record.dim3,
                record.metric_code,
                record.rcept_no,
            ): record
            for record in records
        }
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                args = [
                    (
                        record.corp_code,
                        record.ticker,
                        record.bsns_year,
                        record.reprt_code,
                        record.statement_type,
                        record.row_name,
                        record.stock_knd,
                        record.dim1,
                        record.dim2,
                        record.dim3,
                        record.metric_code,
                        record.metric_name,
                        record.value_numeric,
                        record.value_text,
                        record.unit,
                        record.rcept_no,
                        record.stlm_dt,
                        record.source.value,
                        record.fetched_at,
                        psycopg2.extras.Json(record.raw_payload),
                    )
                    for record in deduped_records.values()
                ]

                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO dart_shareholder_return_raw (
                        corp_code,
                        ticker,
                        bsns_year,
                        reprt_code,
                        statement_type,
                        row_name,
                        stock_knd,
                        dim1,
                        dim2,
                        dim3,
                        metric_code,
                        metric_name,
                        value_numeric,
                        value_text,
                        unit,
                        rcept_no,
                        stlm_dt,
                        source,
                        fetched_at,
                        raw_payload
                    )
                    VALUES %s
                    ON CONFLICT (
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
                    DO UPDATE SET
                        ticker = EXCLUDED.ticker,
                        metric_name = EXCLUDED.metric_name,
                        value_numeric = EXCLUDED.value_numeric,
                        value_text = EXCLUDED.value_text,
                        unit = EXCLUDED.unit,
                        stlm_dt = EXCLUDED.stlm_dt,
                        source = EXCLUDED.source,
                        fetched_at = EXCLUDED.fetched_at,
                        raw_payload = EXCLUDED.raw_payload
                    """,
                    args,
                    page_size=1000,
                )
                result.updated = cur.rowcount

        return result

    def upsert_dart_xbrl_documents(
        self,
        records: list[DartXbrlDocument],
    ) -> UpsertResult:
        """Upsert parsed OpenDART XBRL document metadata."""
        if not records:
            return UpsertResult()

        result = UpsertResult()
        deduped_records = {
            (record.corp_code, record.bsns_year, record.reprt_code, record.rcept_no): record
            for record in records
        }
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                args = [
                    (
                        record.corp_code,
                        record.ticker,
                        record.bsns_year,
                        record.reprt_code,
                        record.rcept_no,
                        record.zip_entry_count,
                        record.instance_document_name,
                        record.label_ko_document_name,
                        record.source.value,
                        record.fetched_at,
                        psycopg2.extras.Json(record.raw_payload),
                    )
                    for record in deduped_records.values()
                ]

                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO dart_xbrl_document (
                        corp_code,
                        ticker,
                        bsns_year,
                        reprt_code,
                        rcept_no,
                        zip_entry_count,
                        instance_document_name,
                        label_ko_document_name,
                        source,
                        fetched_at,
                        raw_payload
                    )
                    VALUES %s
                    ON CONFLICT (corp_code, bsns_year, reprt_code, rcept_no)
                    DO UPDATE SET
                        ticker = EXCLUDED.ticker,
                        zip_entry_count = EXCLUDED.zip_entry_count,
                        instance_document_name = EXCLUDED.instance_document_name,
                        label_ko_document_name = EXCLUDED.label_ko_document_name,
                        source = EXCLUDED.source,
                        fetched_at = EXCLUDED.fetched_at,
                        raw_payload = EXCLUDED.raw_payload
                    """,
                    args,
                    page_size=1000,
                )
                result.updated = cur.rowcount

        return result

    def upsert_dart_xbrl_fact_raw(
        self,
        records: list[DartXbrlFactLine],
    ) -> UpsertResult:
        """Upsert parsed OpenDART XBRL fact rows."""
        if not records:
            return UpsertResult()

        result = UpsertResult()
        deduped_records = {
            (
                record.corp_code,
                record.bsns_year,
                record.reprt_code,
                record.rcept_no,
                record.context_id,
                record.concept_id,
            ): record
            for record in records
        }
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                args = [
                    (
                        record.corp_code,
                        record.ticker,
                        record.bsns_year,
                        record.reprt_code,
                        record.rcept_no,
                        record.concept_id,
                        record.concept_name,
                        record.namespace_uri,
                        record.context_id,
                        record.context_type,
                        record.period_start,
                        record.period_end,
                        record.instant_date,
                        psycopg2.extras.Json(record.dimensions),
                        record.unit_id,
                        record.unit_measure,
                        record.decimals,
                        record.value_numeric,
                        record.value_text,
                        record.is_nil,
                        record.label_ko,
                        record.source.value,
                        record.fetched_at,
                        psycopg2.extras.Json(record.raw_payload),
                    )
                    for record in deduped_records.values()
                ]

                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO dart_xbrl_fact_raw (
                        corp_code,
                        ticker,
                        bsns_year,
                        reprt_code,
                        rcept_no,
                        concept_id,
                        concept_name,
                        namespace_uri,
                        context_id,
                        context_type,
                        period_start,
                        period_end,
                        instant_date,
                        dimensions,
                        unit_id,
                        unit_measure,
                        decimals,
                        value_numeric,
                        value_text,
                        is_nil,
                        label_ko,
                        source,
                        fetched_at,
                        raw_payload
                    )
                    VALUES %s
                    ON CONFLICT (corp_code, bsns_year, reprt_code, rcept_no, context_id, concept_id)
                    DO UPDATE SET
                        ticker = EXCLUDED.ticker,
                        concept_name = EXCLUDED.concept_name,
                        namespace_uri = EXCLUDED.namespace_uri,
                        context_type = EXCLUDED.context_type,
                        period_start = EXCLUDED.period_start,
                        period_end = EXCLUDED.period_end,
                        instant_date = EXCLUDED.instant_date,
                        dimensions = EXCLUDED.dimensions,
                        unit_id = EXCLUDED.unit_id,
                        unit_measure = EXCLUDED.unit_measure,
                        decimals = EXCLUDED.decimals,
                        value_numeric = EXCLUDED.value_numeric,
                        value_text = EXCLUDED.value_text,
                        is_nil = EXCLUDED.is_nil,
                        label_ko = EXCLUDED.label_ko,
                        source = EXCLUDED.source,
                        fetched_at = EXCLUDED.fetched_at,
                        raw_payload = EXCLUDED.raw_payload
                    """,
                    args,
                    page_size=1000,
                )
                result.updated = cur.rowcount

        return result

    def upsert_krx_security_flow_raw(
        self,
        records: list[SecurityFlowLine],
    ) -> UpsertResult:
        """Upsert KRX security-flow raw rows."""
        if not records:
            return UpsertResult()

        result = UpsertResult()
        deduped_records = {
            (
                record.trade_date,
                record.ticker,
                record.market.value,
                record.metric_code,
                record.source.value,
            ): record
            for record in records
        }
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                args = [
                    (
                        record.trade_date,
                        record.ticker,
                        record.market.value,
                        record.metric_code,
                        record.metric_name,
                        record.value,
                        record.unit,
                        record.source.value,
                        record.fetched_at,
                        psycopg2.extras.Json(record.raw_payload),
                    )
                    for record in deduped_records.values()
                ]

                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO krx_security_flow_raw (
                        trade_date,
                        ticker,
                        market,
                        metric_code,
                        metric_name,
                        value,
                        unit,
                        source,
                        fetched_at,
                        raw_payload
                    )
                    VALUES %s
                    ON CONFLICT (trade_date, ticker, market, metric_code, source)
                    DO UPDATE SET
                        metric_name = EXCLUDED.metric_name,
                        value = EXCLUDED.value,
                        unit = EXCLUDED.unit,
                        fetched_at = EXCLUDED.fetched_at,
                        raw_payload = EXCLUDED.raw_payload
                    """,
                    args,
                    page_size=1000,
                )
                result.updated = cur.rowcount

        return result

    def count_krx_security_flow_daily_market_tickers(
        self,
        start: date,
        end: date,
        tickers: list[str],
        metric_code: str,
        source: Source,
    ) -> dict[tuple[date, str], int]:
        """Count existing tickers by trade_date/market for one flow metric."""
        if not tickers:
            return {}

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT trade_date, market, COUNT(DISTINCT ticker)
                    FROM krx_security_flow_raw
                    WHERE trade_date BETWEEN %s AND %s
                      AND ticker = ANY(%s)
                      AND metric_code = %s
                      AND source = %s
                    GROUP BY trade_date, market
                    """,
                    (start, end, tickers, metric_code, source.value),
                )
                return {(row[0], row[1]): int(row[2]) for row in cur.fetchall()}

    def count_krx_security_flow_ticker_metric_dates(
        self,
        start: date,
        end: date,
        tickers: list[str],
        metric_codes: list[str],
        source: Source,
    ) -> dict[str, int]:
        """Count existing distinct (trade_date, metric_code) pairs by ticker."""
        if not tickers or not metric_codes:
            return {}

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT ticker, COUNT(DISTINCT (trade_date, metric_code))
                    FROM krx_security_flow_raw
                    WHERE trade_date BETWEEN %s AND %s
                      AND ticker = ANY(%s)
                      AND metric_code = ANY(%s)
                      AND source = %s
                    GROUP BY ticker
                    """,
                    (start, end, tickers, metric_codes, source.value),
                )
                return {row[0]: int(row[1]) for row in cur.fetchall()}

    def upsert_operating_source_documents(
        self,
        records: list[OperatingSourceDocument],
    ) -> UpsertResult:
        """Upsert operating KPI source documents."""
        if not records:
            return UpsertResult()

        result = UpsertResult()
        deduped_records = {record.document_key: record for record in records}
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                args = [
                    (
                        record.document_key,
                        record.ticker,
                        record.market.value,
                        record.sector_key,
                        record.document_type,
                        record.title,
                        record.document_date,
                        record.period_end,
                        record.source_system,
                        record.source_url,
                        record.language,
                        record.content_text,
                        record.fetched_at,
                        psycopg2.extras.Json(record.raw_payload),
                    )
                    for record in deduped_records.values()
                ]

                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO operating_source_document (
                        document_key,
                        ticker,
                        market,
                        sector_key,
                        document_type,
                        title,
                        document_date,
                        period_end,
                        source_system,
                        source_url,
                        language,
                        content_text,
                        fetched_at,
                        raw_payload
                    )
                    VALUES %s
                    ON CONFLICT (document_key) DO UPDATE SET
                        ticker = EXCLUDED.ticker,
                        market = EXCLUDED.market,
                        sector_key = EXCLUDED.sector_key,
                        document_type = EXCLUDED.document_type,
                        title = EXCLUDED.title,
                        document_date = EXCLUDED.document_date,
                        period_end = EXCLUDED.period_end,
                        source_system = EXCLUDED.source_system,
                        source_url = EXCLUDED.source_url,
                        language = EXCLUDED.language,
                        content_text = EXCLUDED.content_text,
                        fetched_at = EXCLUDED.fetched_at,
                        raw_payload = EXCLUDED.raw_payload,
                        updated_at = now()
                    """,
                    args,
                    page_size=1000,
                )
                result.updated = cur.rowcount

        return result

    def upsert_operating_metric_facts(
        self,
        records: list[OperatingMetricFact],
    ) -> UpsertResult:
        """Upsert extracted operating KPI facts."""
        if not records:
            return UpsertResult()

        result = UpsertResult()
        deduped_records = {
            (
                record.ticker,
                record.metric_code,
                record.period_end,
                record.document_key,
                record.extractor_code,
            ): record
            for record in records
        }
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                args = [
                    (
                        record.ticker,
                        record.market.value,
                        record.sector_key,
                        record.metric_code,
                        record.metric_name,
                        record.period_end,
                        record.value_numeric,
                        record.value_text,
                        record.unit,
                        record.document_key,
                        record.extractor_code,
                        record.raw_snippet,
                        record.fetched_at,
                        psycopg2.extras.Json(record.raw_payload),
                    )
                    for record in deduped_records.values()
                ]

                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO operating_metric_fact (
                        ticker,
                        market,
                        sector_key,
                        metric_code,
                        metric_name,
                        period_end,
                        value_numeric,
                        value_text,
                        unit,
                        document_key,
                        extractor_code,
                        raw_snippet,
                        fetched_at,
                        raw_payload
                    )
                    VALUES %s
                    ON CONFLICT (ticker, metric_code, period_end, document_key, extractor_code)
                    DO UPDATE SET
                        market = EXCLUDED.market,
                        sector_key = EXCLUDED.sector_key,
                        metric_name = EXCLUDED.metric_name,
                        value_numeric = EXCLUDED.value_numeric,
                        value_text = EXCLUDED.value_text,
                        unit = EXCLUDED.unit,
                        raw_snippet = EXCLUDED.raw_snippet,
                        fetched_at = EXCLUDED.fetched_at,
                        raw_payload = EXCLUDED.raw_payload,
                        updated_at = now()
                    """,
                    args,
                    page_size=1000,
                )
                result.updated = cur.rowcount

        return result

    def upsert_metric_catalog(self, records: list[MetricCatalogEntry]) -> UpsertResult:
        """Upsert canonical metric catalog entries."""
        if not records:
            return UpsertResult()

        result = UpsertResult()
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                args = [
                    (
                        record.metric_code,
                        record.metric_name,
                        record.category,
                        record.unit,
                        record.description,
                        record.is_active,
                    )
                    for record in records
                ]
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO metric_catalog (
                        metric_code, metric_name, category, unit, description, is_active
                    )
                    VALUES %s
                    ON CONFLICT (metric_code) DO UPDATE SET
                        metric_name = EXCLUDED.metric_name,
                        category = EXCLUDED.category,
                        unit = EXCLUDED.unit,
                        description = EXCLUDED.description,
                        is_active = EXCLUDED.is_active,
                        updated_at = now()
                    """,
                    args,
                    page_size=1000,
                )
                result.updated = cur.rowcount
        return result

    def replace_metric_mapping_rules(self, records: list[MetricMappingRule]) -> UpsertResult:
        """Replace the active metric mapping rules with the provided set."""
        result = UpsertResult()
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE metric_mapping_rule SET is_active = FALSE, updated_at = now()")
                if not records:
                    return result

                args = [
                    (
                        record.rule_code,
                        record.metric_code,
                        record.source_table,
                        record.value_selector,
                        record.priority,
                        record.statement_type,
                        record.fs_div,
                        record.sj_div,
                        record.account_id,
                        record.account_nm,
                        record.row_name,
                        record.stock_knd,
                        record.dim1,
                        record.dim2,
                        record.dim3,
                        record.metric_code_match,
                        record.is_active,
                    )
                    for record in records
                ]
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO metric_mapping_rule (
                        rule_code,
                        metric_code,
                        source_table,
                        value_selector,
                        priority,
                        statement_type,
                        fs_div,
                        sj_div,
                        account_id,
                        account_nm,
                        row_name,
                        stock_knd,
                        dim1,
                        dim2,
                        dim3,
                        metric_code_match,
                        is_active
                    )
                    VALUES %s
                    ON CONFLICT (rule_code) DO UPDATE SET
                        metric_code = EXCLUDED.metric_code,
                        source_table = EXCLUDED.source_table,
                        value_selector = EXCLUDED.value_selector,
                        priority = EXCLUDED.priority,
                        statement_type = EXCLUDED.statement_type,
                        fs_div = EXCLUDED.fs_div,
                        sj_div = EXCLUDED.sj_div,
                        account_id = EXCLUDED.account_id,
                        account_nm = EXCLUDED.account_nm,
                        row_name = EXCLUDED.row_name,
                        stock_knd = EXCLUDED.stock_knd,
                        dim1 = EXCLUDED.dim1,
                        dim2 = EXCLUDED.dim2,
                        dim3 = EXCLUDED.dim3,
                        metric_code_match = EXCLUDED.metric_code_match,
                        is_active = EXCLUDED.is_active,
                        updated_at = now()
                    """,
                    args,
                    page_size=1000,
                )
                result.updated = cur.rowcount
        return result

    def get_metric_mapping_rules(self) -> list[MetricMappingRule]:
        """Return active metric mapping rules ordered by priority."""
        records: list[MetricMappingRule] = []
        with get_connection(self._dsn) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("""
                    SELECT *
                    FROM metric_mapping_rule
                    WHERE is_active = TRUE
                    ORDER BY priority ASC, rule_code
                    """)
                for row in cur.fetchall():
                    records.append(
                        MetricMappingRule(
                            rule_code=row["rule_code"],
                            metric_code=row["metric_code"],
                            source_table=row["source_table"],
                            value_selector=row["value_selector"],
                            priority=row["priority"],
                            statement_type=row["statement_type"],
                            fs_div=row["fs_div"],
                            sj_div=row["sj_div"],
                            account_id=row["account_id"],
                            account_nm=row["account_nm"],
                            row_name=row["row_name"],
                            stock_knd=row["stock_knd"],
                            dim1=row["dim1"],
                            dim2=row["dim2"],
                            dim3=row["dim3"],
                            metric_code_match=row["metric_code_match"],
                            is_active=row["is_active"],
                        )
                    )
        return records

    def get_dart_financial_statement_raw(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str] | None = None,
    ) -> list[DartFinancialStatementLine]:
        """Return financial statement raw rows for normalization."""
        records: list[DartFinancialStatementLine] = []
        with get_connection(self._dsn) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                sql = """
                    SELECT *
                    FROM dart_financial_statement_raw
                    WHERE bsns_year = ANY(%s)
                      AND reprt_code = ANY(%s)
                """
                params: list[object] = [bsns_years, reprt_codes]
                if tickers:
                    sql += " AND ticker = ANY(%s)"
                    params.append(tickers)
                sql += " ORDER BY ticker, bsns_year, reprt_code, fs_div, sj_div, ord"
                cur.execute(sql, params)
                for row in cur.fetchall():
                    records.append(
                        DartFinancialStatementLine(
                            corp_code=row["corp_code"],
                            ticker=row["ticker"],
                            bsns_year=row["bsns_year"],
                            reprt_code=row["reprt_code"],
                            fs_div=row["fs_div"],
                            sj_div=row["sj_div"],
                            sj_nm=row["sj_nm"],
                            account_id=row["account_id"],
                            account_nm=row["account_nm"],
                            account_detail=row["account_detail"],
                            thstrm_nm=row["thstrm_nm"],
                            thstrm_amount=row["thstrm_amount"],
                            thstrm_add_amount=row["thstrm_add_amount"],
                            frmtrm_nm=row["frmtrm_nm"],
                            frmtrm_amount=row["frmtrm_amount"],
                            frmtrm_q_nm=row["frmtrm_q_nm"],
                            frmtrm_q_amount=row["frmtrm_q_amount"],
                            frmtrm_add_amount=row["frmtrm_add_amount"],
                            bfefrmtrm_nm=row["bfefrmtrm_nm"],
                            bfefrmtrm_amount=row["bfefrmtrm_amount"],
                            ord=row["ord"],
                            currency=row["currency"] or "",
                            rcept_no=row["rcept_no"],
                            source=Source(row["source"]),
                            fetched_at=row["fetched_at"],
                            raw_payload=row["raw_payload"],
                        )
                    )
        return records

    def get_dart_share_count_raw(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str] | None = None,
    ) -> list[DartShareCountLine]:
        """Return share-count raw rows for normalization."""
        records: list[DartShareCountLine] = []
        with get_connection(self._dsn) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                sql = """
                    SELECT *
                    FROM dart_share_count_raw
                    WHERE bsns_year = ANY(%s)
                      AND reprt_code = ANY(%s)
                """
                params: list[object] = [bsns_years, reprt_codes]
                if tickers:
                    sql += " AND ticker = ANY(%s)"
                    params.append(tickers)
                sql += " ORDER BY ticker, bsns_year, reprt_code, se"
                cur.execute(sql, params)
                for row in cur.fetchall():
                    records.append(
                        DartShareCountLine(
                            corp_code=row["corp_code"],
                            ticker=row["ticker"],
                            bsns_year=row["bsns_year"],
                            reprt_code=row["reprt_code"],
                            rcept_no=row["rcept_no"],
                            corp_cls=row["corp_cls"],
                            se=row["se"],
                            isu_stock_totqy=row["isu_stock_totqy"],
                            now_to_isu_stock_totqy=row["now_to_isu_stock_totqy"],
                            now_to_dcrs_stock_totqy=row["now_to_dcrs_stock_totqy"],
                            redc=row["redc"],
                            profit_incnr=row["profit_incnr"],
                            rdmstk_repy=row["rdmstk_repy"],
                            etc=row["etc"],
                            istc_totqy=row["istc_totqy"],
                            tesstk_co=row["tesstk_co"],
                            distb_stock_co=row["distb_stock_co"],
                            stlm_dt=row["stlm_dt"],
                            source=Source(row["source"]),
                            fetched_at=row["fetched_at"],
                            raw_payload=row["raw_payload"],
                        )
                    )
        return records

    def get_dart_shareholder_return_raw(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str] | None = None,
    ) -> list[DartShareholderReturnLine]:
        """Return dividend / treasury-stock raw rows for normalization."""
        records: list[DartShareholderReturnLine] = []
        with get_connection(self._dsn) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                sql = """
                    SELECT *
                    FROM dart_shareholder_return_raw
                    WHERE bsns_year = ANY(%s)
                      AND reprt_code = ANY(%s)
                """
                params: list[object] = [bsns_years, reprt_codes]
                if tickers:
                    sql += " AND ticker = ANY(%s)"
                    params.append(tickers)
                sql += " ORDER BY ticker, bsns_year, reprt_code, statement_type, row_name"
                cur.execute(sql, params)
                for row in cur.fetchall():
                    records.append(
                        DartShareholderReturnLine(
                            corp_code=row["corp_code"],
                            ticker=row["ticker"],
                            bsns_year=row["bsns_year"],
                            reprt_code=row["reprt_code"],
                            statement_type=row["statement_type"],
                            row_name=row["row_name"],
                            stock_knd=row["stock_knd"],
                            dim1=row["dim1"],
                            dim2=row["dim2"],
                            dim3=row["dim3"],
                            metric_code=row["metric_code"],
                            metric_name=row["metric_name"],
                            value_numeric=row["value_numeric"],
                            value_text=row["value_text"],
                            unit=row["unit"] or "",
                            rcept_no=row["rcept_no"],
                            stlm_dt=row["stlm_dt"],
                            source=Source(row["source"]),
                            fetched_at=row["fetched_at"],
                            raw_payload=row["raw_payload"],
                        )
                    )
        return records

    def get_dart_xbrl_fact_raw(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str] | None = None,
    ) -> list[DartXbrlFactLine]:
        """Return parsed OpenDART XBRL fact rows for normalization."""
        records: list[DartXbrlFactLine] = []
        with get_connection(self._dsn) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                sql = """
                    SELECT *
                    FROM dart_xbrl_fact_raw
                    WHERE bsns_year = ANY(%s)
                      AND reprt_code = ANY(%s)
                """
                params: list[object] = [bsns_years, reprt_codes]
                if tickers:
                    sql += " AND ticker = ANY(%s)"
                    params.append(tickers)
                sql += " ORDER BY ticker, bsns_year, reprt_code, concept_id, context_id"
                cur.execute(sql, params)
                for row in cur.fetchall():
                    records.append(
                        DartXbrlFactLine(
                            corp_code=row["corp_code"],
                            ticker=row["ticker"],
                            bsns_year=row["bsns_year"],
                            reprt_code=row["reprt_code"],
                            rcept_no=row["rcept_no"],
                            concept_id=row["concept_id"],
                            concept_name=row["concept_name"],
                            namespace_uri=row["namespace_uri"],
                            context_id=row["context_id"],
                            context_type=row["context_type"],
                            period_start=row["period_start"],
                            period_end=row["period_end"],
                            instant_date=row["instant_date"],
                            dimensions=list(row["dimensions"] or []),
                            unit_id=row["unit_id"],
                            unit_measure=row["unit_measure"],
                            decimals=row["decimals"],
                            value_numeric=row["value_numeric"],
                            value_text=row["value_text"],
                            is_nil=row["is_nil"],
                            label_ko=row["label_ko"],
                            source=Source(row["source"]),
                            fetched_at=row["fetched_at"],
                            raw_payload=row["raw_payload"],
                        )
                    )
        return records

    def upsert_stock_metric_facts(self, records: list[StockMetricFact]) -> UpsertResult:
        """Upsert normalized canonical metric facts."""
        if not records:
            return UpsertResult()

        result = UpsertResult()
        deduped_records = {
            (record.ticker, record.metric_code, record.bsns_year, record.reprt_code): record
            for record in records
        }
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                args = [
                    (
                        record.ticker,
                        record.market.value,
                        record.corp_code,
                        record.metric_code,
                        record.period_type,
                        record.period_end,
                        record.bsns_year,
                        record.reprt_code,
                        record.fs_div,
                        record.value_numeric,
                        record.value_text,
                        record.unit,
                        record.source_table,
                        record.source_key,
                        record.mapping_rule_code,
                        record.fetched_at,
                    )
                    for record in deduped_records.values()
                ]
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO stock_metric_fact (
                        ticker,
                        market,
                        corp_code,
                        metric_code,
                        period_type,
                        period_end,
                        bsns_year,
                        reprt_code,
                        fs_div,
                        value_numeric,
                        value_text,
                        unit,
                        source_table,
                        source_key,
                        mapping_rule_code,
                        fetched_at
                    )
                    VALUES %s
                    ON CONFLICT (ticker, metric_code, bsns_year, reprt_code)
                    DO UPDATE SET
                        market = EXCLUDED.market,
                        corp_code = EXCLUDED.corp_code,
                        period_type = EXCLUDED.period_type,
                        period_end = EXCLUDED.period_end,
                        fs_div = EXCLUDED.fs_div,
                        value_numeric = EXCLUDED.value_numeric,
                        value_text = EXCLUDED.value_text,
                        unit = EXCLUDED.unit,
                        source_table = EXCLUDED.source_table,
                        source_key = EXCLUDED.source_key,
                        mapping_rule_code = EXCLUDED.mapping_rule_code,
                        fetched_at = EXCLUDED.fetched_at,
                        updated_at = now()
                    """,
                    args,
                    page_size=1000,
                )
                result.updated = cur.rowcount
        return result

    def get_metric_catalog_entries(self) -> list[MetricCatalogEntry]:
        """Return active canonical metric catalog entries."""
        records: list[MetricCatalogEntry] = []
        with get_connection(self._dsn) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("""
                    SELECT *
                    FROM metric_catalog
                    WHERE is_active = TRUE
                    ORDER BY metric_code
                    """)
                for row in cur.fetchall():
                    records.append(
                        MetricCatalogEntry(
                            metric_code=row["metric_code"],
                            metric_name=row["metric_name"],
                            category=row["category"],
                            unit=row["unit"],
                            description=row["description"],
                            is_active=row["is_active"],
                        )
                    )
        return records

    def get_stock_metric_facts(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str] | None = None,
    ) -> list[StockMetricFact]:
        """Return normalized canonical metric facts."""
        records: list[StockMetricFact] = []
        with get_connection(self._dsn) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                sql = """
                    SELECT *
                    FROM stock_metric_fact
                    WHERE bsns_year = ANY(%s)
                      AND reprt_code = ANY(%s)
                """
                params: list[object] = [bsns_years, reprt_codes]
                if tickers:
                    sql += " AND ticker = ANY(%s)"
                    params.append(tickers)
                sql += " ORDER BY ticker, bsns_year, reprt_code, metric_code"
                cur.execute(sql, params)
                for row in cur.fetchall():
                    records.append(
                        StockMetricFact(
                            ticker=row["ticker"],
                            market=Market(row["market"]),
                            corp_code=row["corp_code"],
                            metric_code=row["metric_code"],
                            period_type=row["period_type"],
                            period_end=row["period_end"],
                            bsns_year=row["bsns_year"],
                            reprt_code=row["reprt_code"],
                            fs_div=row["fs_div"],
                            value_numeric=row["value_numeric"],
                            value_text=row["value_text"],
                            unit=row["unit"],
                            source_table=row["source_table"],
                            source_key=row["source_key"],
                            mapping_rule_code=row["mapping_rule_code"],
                            fetched_at=row["fetched_at"],
                        )
                    )
        return records

    def get_operating_metric_facts(
        self,
        tickers: list[str] | None = None,
        sector_keys: list[str] | None = None,
    ) -> list[OperatingMetricFact]:
        """Return extracted operating KPI facts."""
        records: list[OperatingMetricFact] = []
        with get_connection(self._dsn) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                sql = "SELECT * FROM operating_metric_fact WHERE 1=1"
                params: list[object] = []
                if tickers:
                    sql += " AND ticker = ANY(%s)"
                    params.append(tickers)
                if sector_keys:
                    sql += " AND sector_key = ANY(%s)"
                    params.append(sector_keys)
                sql += " ORDER BY ticker, sector_key, metric_code, period_end DESC NULLS LAST"
                cur.execute(sql, params)
                for row in cur.fetchall():
                    records.append(
                        OperatingMetricFact(
                            ticker=row["ticker"],
                            market=Market(row["market"]),
                            sector_key=row["sector_key"],
                            metric_code=row["metric_code"],
                            metric_name=row["metric_name"],
                            period_end=row["period_end"],
                            value_numeric=row["value_numeric"],
                            value_text=row["value_text"],
                            unit=row["unit"],
                            document_key=row["document_key"],
                            extractor_code=row["extractor_code"],
                            raw_snippet=row["raw_snippet"],
                            fetched_at=row["fetched_at"],
                            raw_payload=row["raw_payload"],
                        )
                    )
        return records

    # -- Daily OHLCV ----------------------------------------------------------

    def upsert_daily_bars(self, bars: list[DailyBar]) -> UpsertResult:
        """Upsert daily OHLCV bars."""
        if not bars:
            return UpsertResult()

        result = UpsertResult()

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                args = [
                    (
                        b.trade_date,
                        b.ticker,
                        b.market.value,
                        b.open,
                        b.high,
                        b.low,
                        b.close,
                        b.volume,
                        b.source.value,
                        b.fetched_at,
                    )
                    for b in bars
                ]

                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO daily_ohlcv (
                        trade_date,
                        ticker,
                        market,
                        open,
                        high,
                        low,
                        close,
                        volume,
                        source,
                        fetched_at
                    )
                    VALUES %s
                    ON CONFLICT (trade_date, ticker, market) DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume,
                        source = EXCLUDED.source,
                        fetched_at = EXCLUDED.fetched_at
                    """,
                    args,
                    page_size=1000,
                )
                result.updated = cur.rowcount

        return result

    # -- Ingestion runs -------------------------------------------------------

    def record_run(self, run: IngestionRun) -> None:
        """Insert or update an ingestion-run audit record."""
        import json

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ingestion_runs (
                        run_id,
                        run_type,
                        started_at,
                        ended_at,
                        status,
                        params,
                        counts,
                        error_summary
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (run_id) DO UPDATE SET
                        ended_at = EXCLUDED.ended_at,
                        status = EXCLUDED.status,
                        params = EXCLUDED.params,
                        counts = EXCLUDED.counts,
                        error_summary = EXCLUDED.error_summary
                    """,
                    (
                        run.run_id,
                        run.run_type.value,
                        run.started_at,
                        run.ended_at,
                        run.status.value,
                        json.dumps(run.params) if run.params else None,
                        json.dumps(run.counts) if run.counts else None,
                        run.error_summary,
                    ),
                )

    # -- Query helpers --------------------------------------------------------

    def get_daily_bars(self, target_date: date, market: Market | None = None) -> list[DailyBar]:
        """Return all daily bars for a given date."""
        bars = []
        with get_connection(self._dsn) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                if market:
                    cur.execute(
                        "SELECT * FROM daily_ohlcv WHERE trade_date = %s AND market = %s",
                        (target_date, market.value),
                    )
                else:
                    cur.execute("SELECT * FROM daily_ohlcv WHERE trade_date = %s", (target_date,))

                for row in cur.fetchall():
                    bars.append(
                        DailyBar(
                            ticker=row["ticker"],
                            market=Market(row["market"]),
                            trade_date=row["trade_date"],
                            open=row["open"],
                            high=row["high"],
                            low=row["low"],
                            close=row["close"],
                            volume=row["volume"],
                            source=Source(row["source"]),
                            fetched_at=row["fetched_at"],
                        )
                    )
        return bars

    def query_missing_days(
        self,
        ticker: str,
        start: date,
        end: date,
    ) -> list[date]:
        """Return KRX trading days in [start, end] without stored bars.

        Uses the trading-day calendar to enumerate expected sessions
        (excluding weekends and known holidays), then subtracts the
        ``trade_date`` values already present in ``daily_ohlcv`` for the
        given ticker. This avoids ever flagging weekends/holidays as
        "missing", which would otherwise trigger pointless re-fetches on
        every backfill run.
        """
        if start > end:
            return []

        expected = get_trading_days(start, end)
        if not expected:
            return []

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT trade_date
                    FROM daily_ohlcv
                    WHERE ticker = %s
                      AND trade_date BETWEEN %s AND %s
                    """,
                    (ticker, start, end),
                )
                stored = {row[0] for row in cur.fetchall()}

        return [d for d in expected if d not in stored]

    def get_min_trade_date(self, ticker: str) -> date | None:
        """Return the earliest stored ``trade_date`` for *ticker*, if any.

        Used by the backfill service as a lower-bound clamp so that
        date ranges before the ticker's known data start are not
        re-requested on every run.
        """
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT MIN(trade_date) FROM daily_ohlcv WHERE ticker = %s",
                    (ticker,),
                )
                row = cur.fetchone()
        if not row or row[0] is None:
            return None
        return row[0]

    def get_max_trade_date(self, ticker: str) -> date | None:
        """Return the latest stored ``trade_date`` for *ticker*, if any.

        Used by the backfill service in incremental mode to fetch only
        days strictly after this date.
        """
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT MAX(trade_date) FROM daily_ohlcv WHERE ticker = %s",
                    (ticker,),
                )
                row = cur.fetchone()
        if not row or row[0] is None:
            return None
        return row[0]

    def get_daily_price_date_range(
        self,
        tickers: list[str] | None = None,
    ) -> tuple[date, date] | None:
        """Return the min/max stored daily OHLCV trade dates for selected tickers."""
        params: list[object] = []
        where_clause = ""
        if tickers:
            where_clause = "WHERE ticker = ANY(%s)"
            params.append(tickers)

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT MIN(trade_date), MAX(trade_date) FROM daily_ohlcv {where_clause}",
                    params,
                )
                row = cur.fetchone()

        if not row or row[0] is None or row[1] is None:
            return None
        return row[0], row[1]
