"""Use-case: Sync OpenDART share-count and shareholder-return raw rows."""

from __future__ import annotations

import logging

from krx_collector.adapters.opendart_common.client import OpenDartRequestExecutor
from krx_collector.domain.enums import RunStatus, RunType
from krx_collector.domain.models import DartShareInfoSyncResult, IngestionRun
from krx_collector.ports.share_info import ShareCountProvider, ShareholderReturnProvider
from krx_collector.ports.storage import Storage
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


def sync_dart_share_info(
    share_count_provider: ShareCountProvider,
    shareholder_return_provider: ShareholderReturnProvider,
    storage: Storage,
    bsns_years: list[int],
    reprt_codes: list[str],
    tickers: list[str] | None = None,
    rate_limit_seconds: float = 0.2,
    force: bool = False,
) -> DartShareInfoSyncResult:
    """Synchronise OpenDART share-count/dividend/treasury-stock raw rows."""
    run = IngestionRun(
        run_type=RunType.DART_SHARE_INFO_SYNC,
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
    executor = _get_executor(share_count_provider) or _get_executor(shareholder_return_provider)
    if executor is not None:
        run.params["opendart_key_count"] = executor.configured_key_count
    storage.record_run(run)

    result = DartShareInfoSyncResult()
    try:
        targets = storage.get_dart_corp_master(active_only=True, tickers=tickers)
        if not targets:
            raise RuntimeError(
                "No active OpenDART corp mappings found. Run `dart sync-corp` first."
            )

        existing_share_count_keys: set[tuple[str, int, str]]
        existing_return_keys: set[tuple[str, int, str, str]]
        if force:
            existing_share_count_keys = set()
            existing_return_keys = set()
        else:
            corp_codes = [corp.corp_code for corp in targets]
            existing_share_count_keys = storage.get_existing_dart_share_count_keys(
                bsns_years=bsns_years,
                reprt_codes=reprt_codes,
                corp_codes=corp_codes,
            )
            existing_return_keys = storage.get_existing_dart_shareholder_return_keys(
                bsns_years=bsns_years,
                reprt_codes=reprt_codes,
                corp_codes=corp_codes,
            )

        for corp in targets:
            result.targets_processed += 1
            for bsns_year in bsns_years:
                for reprt_code in reprt_codes:
                    request_prefix = f"{corp.ticker}:{bsns_year}:{reprt_code}"
                    attempted_any = False

                    if (corp.corp_code, bsns_year, reprt_code) in existing_share_count_keys:
                        logger.debug("Skipping existing share_count request %s", request_prefix)
                        result.requests_skipped += 1
                    else:
                        result.requests_attempted += 1
                        attempted_any = True
                        share_count_result = call_with_retry(
                            lambda: share_count_provider.fetch_share_count(
                                corp=corp,
                                bsns_year=bsns_year,
                                reprt_code=reprt_code,
                            ),
                            request_label=f"{request_prefix}:share_count",
                            logger_instance=logger,
                            should_retry_result=should_retry_opendart_result,
                        )
                        if is_opendart_daily_limit_exhausted(share_count_result):
                            raise OpenDartKeyExhaustedError(
                                share_count_result.error
                                or "All OpenDART API keys are temporarily rate limited."
                            )
                        if share_count_result.error:
                            result.errors[f"{request_prefix}:share_count"] = (
                                share_count_result.error
                            )
                        elif share_count_result.no_data:
                            result.no_data_requests += 1
                        elif share_count_result.records:
                            upsert = storage.upsert_dart_share_count_raw(share_count_result.records)
                            result.share_count_upsert.updated += upsert.updated
                            result.share_count_upsert.errors += upsert.errors
                            result.share_count_rows_upserted += upsert.updated

                    if (
                        corp.corp_code,
                        bsns_year,
                        reprt_code,
                        "dividend",
                    ) in existing_return_keys:
                        logger.debug("Skipping existing dividend request %s", request_prefix)
                        result.requests_skipped += 1
                    else:
                        result.requests_attempted += 1
                        attempted_any = True
                        dividend_result = call_with_retry(
                            lambda: shareholder_return_provider.fetch_dividend(
                                corp=corp,
                                bsns_year=bsns_year,
                                reprt_code=reprt_code,
                            ),
                            request_label=f"{request_prefix}:dividend",
                            logger_instance=logger,
                            should_retry_result=should_retry_opendart_result,
                        )
                        if is_opendart_daily_limit_exhausted(dividend_result):
                            raise OpenDartKeyExhaustedError(
                                dividend_result.error
                                or "All OpenDART API keys are temporarily rate limited."
                            )
                        if dividend_result.error:
                            result.errors[f"{request_prefix}:dividend"] = dividend_result.error
                        elif dividend_result.no_data:
                            result.no_data_requests += 1
                        elif dividend_result.records:
                            upsert = storage.upsert_dart_shareholder_return_raw(
                                dividend_result.records
                            )
                            result.shareholder_return_upsert.updated += upsert.updated
                            result.shareholder_return_upsert.errors += upsert.errors
                            result.shareholder_return_rows_upserted += upsert.updated

                    if (
                        corp.corp_code,
                        bsns_year,
                        reprt_code,
                        "treasury_stock",
                    ) in existing_return_keys:
                        logger.debug("Skipping existing treasury_stock request %s", request_prefix)
                        result.requests_skipped += 1
                    else:
                        result.requests_attempted += 1
                        attempted_any = True
                        treasury_result = call_with_retry(
                            lambda: shareholder_return_provider.fetch_treasury_stock(
                                corp=corp,
                                bsns_year=bsns_year,
                                reprt_code=reprt_code,
                            ),
                            request_label=f"{request_prefix}:treasury_stock",
                            logger_instance=logger,
                            should_retry_result=should_retry_opendart_result,
                        )
                        if is_opendart_daily_limit_exhausted(treasury_result):
                            raise OpenDartKeyExhaustedError(
                                treasury_result.error
                                or "All OpenDART API keys are temporarily rate limited."
                            )
                        if treasury_result.error:
                            result.errors[f"{request_prefix}:treasury_stock"] = (
                                treasury_result.error
                            )
                        elif treasury_result.no_data:
                            result.no_data_requests += 1
                        elif treasury_result.records:
                            upsert = storage.upsert_dart_shareholder_return_raw(
                                treasury_result.records
                            )
                            result.shareholder_return_upsert.updated += upsert.updated
                            result.shareholder_return_upsert.errors += upsert.errors
                            result.shareholder_return_rows_upserted += upsert.updated

                    if attempted_any:
                        sleep_with_jitter(rate_limit_seconds)

        complete_run(
            storage,
            run,
            counts=build_run_counts(
                targets_processed=result.targets_processed,
                requests_attempted=result.requests_attempted,
                requests_skipped=result.requests_skipped,
                share_count_rows_upserted=result.share_count_rows_upserted,
                shareholder_return_rows_upserted=result.shareholder_return_rows_upserted,
                no_data_requests=result.no_data_requests,
                **(executor.snapshot_metrics() if executor is not None else {}),
            ),
            errors=result.errors,
            partial_subject="share info requests",
        )
        return result
    except OpenDartKeyExhaustedError as exc:
        logger.warning("OpenDART share info sync stopped: %s", exc)
        fail_run(storage, run, exc)
        result.opendart_exhaustion_reason = "all_rate_limited"
        result.errors["pipeline"] = str(exc)
        return result
    except Exception as exc:
        logger.exception("OpenDART share info sync failed")
        fail_run(storage, run, exc)
        result.errors["pipeline"] = str(exc)
        return result
