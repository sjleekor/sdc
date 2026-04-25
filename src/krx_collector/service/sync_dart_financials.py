"""Use-case: Sync OpenDART financial-statement raw rows."""

from __future__ import annotations

import logging

from krx_collector.adapters.opendart_common.client import OpenDartRequestExecutor
from krx_collector.domain.enums import RunStatus, RunType
from krx_collector.domain.models import DartFinancialSyncResult, IngestionRun
from krx_collector.ports.financials import FinancialStatementProvider
from krx_collector.ports.storage import Storage
from krx_collector.util.pipeline import (
    build_run_counts,
    call_with_retry,
    complete_run,
    fail_run,
    should_retry_opendart_result,
    sleep_with_jitter,
)
from krx_collector.util.time import now_kst

logger = logging.getLogger(__name__)


def _get_executor(provider: object) -> OpenDartRequestExecutor | None:
    executor = getattr(provider, "request_executor", None)
    return executor if isinstance(executor, OpenDartRequestExecutor) else None


def sync_dart_financial_statements(
    provider: FinancialStatementProvider,
    storage: Storage,
    bsns_years: list[int],
    reprt_codes: list[str],
    fs_divs: list[str],
    tickers: list[str] | None = None,
    rate_limit_seconds: float = 0.2,
) -> DartFinancialSyncResult:
    """Synchronise OpenDART financial raw rows into local storage."""
    run = IngestionRun(
        run_type=RunType.DART_FINANCIAL_SYNC,
        started_at=now_kst(),
        status=RunStatus.RUNNING,
        params={
            "bsns_years": bsns_years,
            "reprt_codes": reprt_codes,
            "fs_divs": fs_divs,
            "tickers": tickers,
            "rate_limit_seconds": rate_limit_seconds,
        },
    )
    executor = _get_executor(provider)
    if executor is not None:
        run.params["opendart_key_count"] = executor.configured_key_count
    storage.record_run(run)

    result = DartFinancialSyncResult()

    try:
        targets = storage.get_dart_corp_master(active_only=True, tickers=tickers)
        if not targets:
            raise RuntimeError("No active OpenDART corp mappings found. Run `dart sync-corp` first.")

        for corp in targets:
            result.targets_processed += 1

            for bsns_year in bsns_years:
                for reprt_code in reprt_codes:
                    for fs_div in fs_divs:
                        result.requests_attempted += 1
                        request_key = f"{corp.ticker}:{bsns_year}:{reprt_code}:{fs_div}"
                        fetch_result = call_with_retry(
                            lambda: provider.fetch_financial_statement(
                                corp=corp,
                                bsns_year=bsns_year,
                                reprt_code=reprt_code,
                                fs_div=fs_div,
                            ),
                            request_label=request_key,
                            logger_instance=logger,
                            should_retry_result=should_retry_opendart_result,
                        )

                        if fetch_result.error:
                            logger.warning("Financial sync failed for %s: %s", request_key, fetch_result.error)
                            result.errors[request_key] = fetch_result.error
                        elif fetch_result.no_data:
                            result.no_data_requests += 1
                        elif fetch_result.records:
                            upsert_result = storage.upsert_dart_financial_statement_raw(
                                fetch_result.records
                            )
                            result.upsert.updated += upsert_result.updated
                            result.upsert.errors += upsert_result.errors
                            result.rows_upserted += upsert_result.updated

                        sleep_with_jitter(rate_limit_seconds)

        complete_run(
            storage,
            run,
            counts=build_run_counts(
                targets_processed=result.targets_processed,
                requests_attempted=result.requests_attempted,
                rows_upserted=result.rows_upserted,
                no_data_requests=result.no_data_requests,
                **(executor.snapshot_metrics() if executor is not None else {}),
            ),
            errors=result.errors,
            partial_subject="financial sync requests",
        )
        return result
    except Exception as exc:
        logger.exception("OpenDART financial sync failed")
        fail_run(storage, run, exc)
        result.errors["pipeline"] = str(exc)
        return result
