"""Use-case: Sync parsed OpenDART XBRL documents and fact rows."""

from __future__ import annotations

import logging

from krx_collector.adapters.opendart_common.client import OpenDartRequestExecutor
from krx_collector.domain.enums import RunStatus, RunType
from krx_collector.domain.models import DartXbrlSyncResult, IngestionRun
from krx_collector.ports.storage import Storage
from krx_collector.ports.xbrl import XbrlProvider
from krx_collector.util.pipeline import (
    OpenDartKeyExhaustedError,
    build_run_counts,
    call_with_retry,
    complete_run,
    fail_run,
    is_opendart_daily_limit_exhausted,
    should_retry_opendart_result,
    sleep_with_jitter,
)
from krx_collector.util.time import now_kst

logger = logging.getLogger(__name__)


def _get_executor(provider: object) -> OpenDartRequestExecutor | None:
    executor = getattr(provider, "request_executor", None)
    return executor if isinstance(executor, OpenDartRequestExecutor) else None


def sync_dart_xbrl(
    provider: XbrlProvider,
    storage: Storage,
    bsns_years: list[int],
    reprt_codes: list[str],
    tickers: list[str] | None = None,
    rate_limit_seconds: float = 0.2,
    force: bool = False,
) -> DartXbrlSyncResult:
    """Synchronise parsed XBRL ZIP data for filings already present in financial raw."""
    run = IngestionRun(
        run_type=RunType.XBRL_PARSE,
        started_at=now_kst(),
        status=RunStatus.RUNNING,
        params={
            "bsns_years": bsns_years,
            "reprt_codes": reprt_codes,
            "tickers": tickers,
            "rate_limit_seconds": rate_limit_seconds,
            "force": force,
        },
    )
    executor = _get_executor(provider)
    if executor is not None:
        run.params["opendart_key_count"] = executor.configured_key_count
    storage.record_run(run)

    result = DartXbrlSyncResult()

    try:
        corp_rows = storage.get_dart_corp_master(active_only=True, tickers=tickers)
        corp_by_ticker = {corp.ticker: corp for corp in corp_rows if corp.ticker}
        if not corp_by_ticker:
            raise RuntimeError(
                "No active OpenDART corp mappings found. Run `dart sync-corp` first."
            )

        financial_rows = storage.get_dart_financial_statement_raw(bsns_years, reprt_codes, tickers)
        request_targets: dict[tuple[str, int, str, str], tuple[str, int, str, str]] = {}
        for row in financial_rows:
            if not row.ticker or not row.rcept_no:
                continue
            key = (row.ticker, row.bsns_year, row.reprt_code, row.rcept_no)
            request_targets.setdefault(key, key)

        if not request_targets:
            raise RuntimeError(
                "No financial raw rows with rcept_no found. Run `dart sync-financials` first."
            )

        existing_doc_keys: set[tuple[str, int, str, str]]
        if force:
            existing_doc_keys = set()
        else:
            existing_doc_keys = storage.get_existing_dart_xbrl_document_keys(
                bsns_years=bsns_years,
                reprt_codes=reprt_codes,
                corp_codes=[corp.corp_code for corp in corp_by_ticker.values()],
            )

        result.targets_processed = len(request_targets)
        for ticker, bsns_year, reprt_code, rcept_no in request_targets.values():
            corp = corp_by_ticker.get(ticker)
            if corp is None:
                continue

            request_key = f"{ticker}:{bsns_year}:{reprt_code}:{rcept_no}"
            if (corp.corp_code, bsns_year, reprt_code, rcept_no) in existing_doc_keys:
                logger.debug("Skipping existing XBRL document %s", request_key)
                result.requests_skipped += 1
                continue

            result.requests_attempted += 1
            fetch_result = call_with_retry(
                lambda: provider.fetch_xbrl(
                    corp=corp,
                    bsns_year=bsns_year,
                    reprt_code=reprt_code,
                    rcept_no=rcept_no,
                ),
                request_label=request_key,
                logger_instance=logger,
                should_retry_result=should_retry_opendart_result,
            )

            if is_opendart_daily_limit_exhausted(fetch_result):
                raise OpenDartKeyExhaustedError(
                    fetch_result.error or "All OpenDART API keys are temporarily rate limited."
                )
            if fetch_result.error:
                logger.warning("XBRL sync failed for %s: %s", request_key, fetch_result.error)
                result.errors[request_key] = fetch_result.error
            elif fetch_result.no_data:
                result.no_data_requests += 1
            else:
                if fetch_result.document is not None:
                    upsert_document = storage.upsert_dart_xbrl_documents([fetch_result.document])
                    result.document_upsert.updated += upsert_document.updated
                    result.document_upsert.errors += upsert_document.errors
                    result.documents_upserted += upsert_document.updated

                if fetch_result.facts:
                    upsert_facts = storage.upsert_dart_xbrl_fact_raw(fetch_result.facts)
                    result.fact_upsert.updated += upsert_facts.updated
                    result.fact_upsert.errors += upsert_facts.errors
                    result.facts_upserted += upsert_facts.updated

            sleep_with_jitter(rate_limit_seconds)

        complete_run(
            storage,
            run,
            counts=build_run_counts(
                targets_processed=result.targets_processed,
                requests_attempted=result.requests_attempted,
                requests_skipped=result.requests_skipped,
                documents_upserted=result.documents_upserted,
                facts_upserted=result.facts_upserted,
                no_data_requests=result.no_data_requests,
                **(executor.snapshot_metrics() if executor is not None else {}),
            ),
            errors=result.errors,
            partial_subject="XBRL sync requests",
        )
        return result
    except OpenDartKeyExhaustedError as exc:
        logger.warning("OpenDART XBRL sync stopped: %s", exc)
        fail_run(storage, run, exc)
        result.opendart_exhaustion_reason = "all_rate_limited"
        result.errors["pipeline"] = str(exc)
        return result
    except Exception as exc:
        logger.exception("OpenDART XBRL sync failed")
        fail_run(storage, run, exc)
        result.errors["pipeline"] = str(exc)
        return result
