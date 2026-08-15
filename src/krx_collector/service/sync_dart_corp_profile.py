"""Use-case: Sync OpenDART company profiles (industry / incorporation / FY end).

``corpCode.xml``, which fills ``dart_corp_master`` today, carries only
corp_code / corp_name / stock_code / modify_date.  Industry code comes from a
separate per-corporation endpoint, and its absence is why every cross-sectional
z-score in ``fin_scan.py`` partitions on ``(trade_date, market)`` alone — banks,
biotech, shipbuilders and game studios all normalise against one KOSPI pool.

Targets are the ~3,959 corporations with a ticker mapping.  Including unlisted
entities would multiply the request count by about thirty for rows no feature
ever reads.

Skip-if-present is ``profile_fetched_at IS NOT NULL``.  One call per
corporation with no sub-parts, so there is no partial-response hazard here —
unlike the slice-based collectors, a row either has its profile or it does not.
"""

from __future__ import annotations

import logging

from krx_collector.adapters.opendart_common.client import OpenDartRequestExecutor
from krx_collector.domain.enums import RunStatus, RunType
from krx_collector.domain.models import CompanyProfile, CompanyProfileSyncResult, IngestionRun
from krx_collector.ports.corp_codes import CorpProfileProvider
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

UPSERT_BATCH_SIZE = 200


def _get_executor(provider: object) -> OpenDartRequestExecutor | None:
    executor = getattr(provider, "request_executor", None)
    return executor if isinstance(executor, OpenDartRequestExecutor) else None


def sync_dart_corp_profile(
    profile_provider: CorpProfileProvider,
    storage: Storage,
    tickers: list[str] | None = None,
    rate_limit_seconds: float = 0.2,
    force: bool = False,
    include_delisted: bool = False,
) -> CompanyProfileSyncResult:
    """Fetch ``company.json`` for every ticker-mapped corporation.

    Args:
        profile_provider: OpenDART company-profile provider.
        storage: Target storage.
        tickers: Optional ticker allowlist.
        rate_limit_seconds: Base delay between requests.
        force: Re-fetch corporations that already have a profile.

    Returns:
        ``CompanyProfileSyncResult`` with per-request counters and errors.
    """
    run = IngestionRun(
        run_type=RunType.DART_CORP_PROFILE_SYNC,
        started_at=now_kst(),
        status=RunStatus.RUNNING,
        params={
            "tickers": tickers,
            "rate_limit_seconds": rate_limit_seconds,
            "force": force,
            "include_delisted": include_delisted,
        },
    )
    executor = _get_executor(profile_provider)
    if executor is not None:
        run.params["opendart_key_count"] = executor.configured_key_count
    storage.record_run(run)

    result = CompanyProfileSyncResult()
    pending: list[CompanyProfile] = []

    def flush() -> None:
        if not pending:
            return
        upsert = storage.upsert_company_profiles(pending)
        result.rows_upserted += upsert.updated
        pending.clear()

    try:
        targets = [
            corp
            for corp in storage.get_dart_corp_master(
                active_only=True,
                tickers=tickers,
                include_delisted=include_delisted,
            )
            if corp.ticker
        ]
        if not targets:
            raise RuntimeError(
                "No ticker-mapped OpenDART corp rows found. Run `dart sync-corp` first."
            )

        already = set() if force else storage.get_profiled_corp_codes()
        logger.info(
            "Company profile sync: %d targets (%d already profiled)",
            len(targets),
            len(already & {c.corp_code for c in targets}),
        )

        for corp in targets:
            label = f"{corp.ticker}:company"

            if corp.corp_code in already:
                result.requests_skipped += 1
                continue

            result.requests_attempted += 1
            fetch = call_with_retry(
                lambda corp=corp: profile_provider.fetch_company_profile(corp),
                request_label=label,
                logger_instance=logger,
                should_retry_result=should_retry_opendart_result,
            )

            if is_opendart_daily_limit_exhausted(fetch):
                # Flush what is already fetched before exiting: the run resumes
                # from profile_fetched_at, so unsaved work is re-fetched.
                flush()
                raise OpenDartKeyExhaustedError(
                    fetch.error or "All OpenDART API keys are temporarily rate limited."
                )

            if fetch.error:
                result.errors[label] = fetch.error
            elif fetch.no_data or fetch.profile is None:
                result.no_data += 1
            else:
                pending.append(fetch.profile)
                if len(pending) >= UPSERT_BATCH_SIZE:
                    flush()

            sleep_with_jitter(rate_limit_seconds)

        flush()

        complete_run(
            storage,
            run,
            counts=build_run_counts(
                targets_processed=len(targets),
                requests_attempted=result.requests_attempted,
                requests_skipped=result.requests_skipped,
                rows_upserted=result.rows_upserted,
                no_data_requests=result.no_data,
                **(executor.snapshot_metrics() if executor is not None else {}),
            ),
            errors=result.errors,
            partial_subject="company profile requests",
        )
        return result

    except OpenDartKeyExhaustedError as exc:
        logger.warning("Company profile sync stopped: %s", exc)
        fail_run(storage, run, exc)
        result.opendart_exhaustion_reason = "all_rate_limited"
        result.errors["pipeline"] = str(exc)
        return result
    except Exception as exc:
        logger.exception("Company profile sync failed")
        flush()
        fail_run(storage, run, exc)
        result.errors["pipeline"] = str(exc)
        return result
