"""CLI entrypoint — argparse wiring for ``krx-collector``.

Subcommands::

    krx-collector db init
    krx-collector db sync-remote [--db-info-path ...] [--ssh-host ...] [--full-refresh]
                                  [--tables ...] [--all-tables]
    krx-collector universe sync  [--source fdr|pykrx] [--markets ...]
    krx-collector prices backfill [--market ...] [--tickers ...] [--start ...]
    krx-collector validate       [--date ...] [--market ...]

Each subcommand parses arguments and delegates to the corresponding
service function.  Providers and storage are instantiated here (dependency
wiring) but currently raise ``NotImplementedError`` since adapters are stubs.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path

from krx_collector.adapters.opendart_common.client import OpenDartRequestExecutor
from krx_collector.infra.config.settings import get_settings
from krx_collector.infra.logging.setup import setup_logging

logger = logging.getLogger(__name__)


def _build_opendart_request_executor() -> OpenDartRequestExecutor:
    """Construct the shared OpenDART executor for one CLI command."""
    settings = get_settings()
    return OpenDartRequestExecutor(settings.opendart_api_keys)


def _exit_if_opendart_key_exhausted(result: object, label: str) -> None:
    """Stop shell schedulers when every OpenDART key has hit its daily limit."""
    if getattr(result, "opendart_exhaustion_reason", None) != "all_rate_limited":
        return

    error = getattr(result, "errors", {}).get(
        "pipeline", "All OpenDART API keys are temporarily rate limited."
    )
    print(f"❌ {label} stopped: {error}", file=sys.stderr)
    sys.exit(75)


def _split_csv(value: str | None) -> list[str] | None:
    """Split a comma-separated CLI value into stripped non-empty tokens."""
    if value is None:
        return None
    values = [item.strip() for item in value.split(",") if item.strip()]
    return values or None


def _parse_common_sources(value: str) -> list[object]:
    """Parse a comma-separated common feature source allowlist."""
    from krx_collector.domain.enums import Source

    source_by_name = {
        "pykrx": Source.PYKRX,
        "krx": Source.KRX,
        "fdr": Source.FDR,
        "ecos": Source.ECOS,
        "fred": Source.FRED,
    }
    sources: list[Source] = []
    for raw_source in value.split(","):
        normalized = raw_source.strip().lower()
        if not normalized:
            continue
        source = source_by_name.get(normalized)
        if source is None:
            supported = ", ".join(sorted(source_by_name))
            raise argparse.ArgumentTypeError(
                f"Unsupported common feature source: {raw_source!r} (supported: {supported})"
            )
        sources.append(source)
    if not sources:
        raise argparse.ArgumentTypeError("At least one common feature source is required.")
    return sources


def _build_common_feature_providers(sources: list[object]) -> list[object]:
    """Instantiate common feature providers for a source allowlist."""
    from krx_collector.adapters.common_features_ecos import EcosCommonFeatureProvider
    from krx_collector.adapters.common_features_fdr import FdrCommonFeatureProvider
    from krx_collector.adapters.common_features_fred import FredCommonFeatureProvider
    from krx_collector.adapters.common_features_krx import KrxCommonFeatureProvider
    from krx_collector.adapters.common_features_pykrx import PykrxCommonFeatureProvider
    from krx_collector.domain.enums import Source

    provider_by_source = {
        Source.PYKRX: PykrxCommonFeatureProvider,
        Source.KRX: KrxCommonFeatureProvider,
        Source.FDR: FdrCommonFeatureProvider,
        Source.ECOS: EcosCommonFeatureProvider,
        Source.FRED: FredCommonFeatureProvider,
    }
    providers = []
    for source in sources:
        provider_factory = provider_by_source.get(source)
        if provider_factory is None:
            raise ValueError(f"Unsupported common feature source: {source}")
        providers.append(provider_factory())
    return providers


# ---------------------------------------------------------------------------
# Subcommand handlers
# ---------------------------------------------------------------------------


def _handle_db_init(args: argparse.Namespace) -> None:
    """Handle ``krx-collector db init``."""
    print("→ db init: initialising database schema…")
    settings = get_settings()
    from krx_collector.infra.db_postgres.repositories import PostgresStorage

    storage = PostgresStorage(settings.db_dsn)
    try:
        storage.init_schema()
        print("✅ Schema initialisation successful.")
    except Exception as exc:
        print(f"❌ Schema initialisation failed: {exc}", file=sys.stderr)
        sys.exit(1)


def _parse_remote_sync_tables(raw_value: str | None) -> tuple[str, ...] | None:
    """Parse a comma-separated remote sync table list."""
    if raw_value is None:
        return None
    table_names = tuple(part.strip() for part in raw_value.split(",") if part.strip())
    return table_names or None


def _handle_db_sync_remote(args: argparse.Namespace) -> None:
    """Handle ``krx-collector db sync-remote``."""
    settings = get_settings()

    db_info_path = args.db_info_path or str(settings.remote_db_info_path)
    batch_size = args.batch_size or settings.remote_db_batch_size
    remote_host_override = args.remote_host or settings.remote_db_host_override
    ssh_host = args.ssh_host or settings.remote_db_ssh_host
    ssh_local_port = args.ssh_local_port or settings.remote_db_ssh_local_port
    tables = _parse_remote_sync_tables(args.tables)

    print(
        f"→ db sync-remote: db_info_path={db_info_path}, "
        f"batch_size={batch_size}, full_refresh={args.full_refresh}, "
        f"all_tables={args.all_tables}, tables={tables}, "
        f"remote_host_override={remote_host_override}, ssh_host={ssh_host}, "
        f"ssh_local_port={ssh_local_port}"
    )

    from krx_collector.service.sync_local_db import sync_remote_db_to_local

    result = sync_remote_db_to_local(
        local_dsn=settings.db_dsn,
        remote_db_info_path=db_info_path,
        batch_size=batch_size,
        full_refresh=args.full_refresh,
        all_tables=args.all_tables,
        tables=tables,
        remote_host_override=remote_host_override,
        ssh_host=ssh_host,
        ssh_local_port=ssh_local_port,
    )

    if result.error:
        print(f"❌ Remote DB sync failed: {result.error}", file=sys.stderr)
        sys.exit(1)

    print("✅ Remote DB sync completed.")
    print(f"   - Remote host: {result.remote_host}")
    print(f"   - Total rows synced: {result.total_rows}")
    for table_name, row_count in result.table_counts.items():
        print(f"   - {table_name}: {row_count}")


def _handle_ops_freshness_report(args: argparse.Namespace) -> None:
    """Handle ``krx-collector ops freshness-report``."""
    settings = get_settings()

    from krx_collector.infra.db_postgres.repositories import PostgresStorage
    from krx_collector.service.freshness import build_freshness_report

    storage = PostgresStorage(settings.db_dsn)
    report = build_freshness_report(storage, running_limit=args.running_limit)

    print("✅ Freshness report generated.")
    print(f"   - daily_ohlcv latest: {report.price_latest_date or '-'}")

    print("   - flow group latest:")
    for group, latest in sorted(report.flow_group_latest_dates.items()):
        print(f"     {group}: {latest or '-'}")

    print("   - common raw latest by source:")
    common_by_source: dict[str, list[str]] = {}
    for row in report.common_series:
        common_by_source.setdefault(row.source.value, []).append(
            f"{row.series_id}={row.latest_observation_date or '-'}"
        )
    for source, rows in sorted(common_by_source.items()):
        preview = ", ".join(rows[:8])
        suffix = "" if len(rows) <= 8 else f", ... (+{len(rows) - 8})"
        print(f"     {source}: {preview}{suffix}")

    if report.common_fact_latest_dates:
        latest_fact = max(report.common_fact_latest_dates.values())
        print(f"   - common daily fact latest: {latest_fact}")
    else:
        print("   - common daily fact latest: -")

    print("   - DART/metric year ranges:")
    for row in report.dart_year_ranges:
        year_range = (
            f"{row.min_year}..{row.max_year}" if row.min_year is not None else "-"
        )
        print(f"     {row.table_name}: years={year_range} rows={row.rows}")

    print("   - running ingestion runs:")
    if not report.running_runs:
        print("     -")
    for run in report.running_runs:
        started_at = run.started_at.isoformat() if run.started_at else "-"
        print(f"     {run.run_id} {run.run_type.value} started_at={started_at}")


def _handle_ops_assert_common_freshness(args: argparse.Namespace) -> None:
    """Handle ``krx-collector ops assert-common-freshness``."""
    settings = get_settings()

    from krx_collector.infra.db_postgres.repositories import PostgresStorage
    from krx_collector.service.freshness import assert_common_freshness
    from krx_collector.util.time import today_kst

    end = args.end or today_kst()
    series_ids = _split_csv(args.series)
    storage = PostgresStorage(settings.db_dsn)
    result = assert_common_freshness(
        storage=storage,
        sources=args.sources,
        end=end,
        max_run_age_hours=args.max_run_age_hours,
        daily_max_lag_days=args.daily_max_lag_days,
        macro_max_lag_days=args.macro_max_lag_days,
        series_ids=series_ids,
    )

    source_text = ",".join(source.value.lower() for source in result.sources)
    if result.ok:
        print(
            "✅ Common freshness assertion passed. "
            f"sources={source_text} checked_series={result.checked_series} end={end}"
        )
        for row in result.run_freshness:
            age = "-" if row.age_hours is None else f"{row.age_hours:.1f}h"
            ended_at = row.ended_at.isoformat() if row.ended_at else "-"
            print(f"   - {row.source.value}: run={row.run_id or '-'} ended_at={ended_at} age={age}")
        return

    print(
        "❌ Common freshness assertion failed. "
        f"sources={source_text} checked_series={result.checked_series} end={end}",
        file=sys.stderr,
    )
    for violation in result.violations:
        series = f" series={violation.series_id}" if violation.series_id else ""
        print(
            f"   - {violation.source.value} {violation.check}{series}: "
            f"{violation.message}",
            file=sys.stderr,
        )
    sys.exit(2)


def _dart_financial_actual_attempt_estimate(
    *,
    storage: object,
    targets: list[object],
    allowed_pairs: set[tuple[int, str]],
    fs_divs: list[str],
    force: bool,
    skip_request_keys: set[str],
) -> int:
    if force:
        existing_keys: set[tuple[str, int, str, str]] = set()
        effective_skip_keys: set[str] = set()
    else:
        existing_keys = storage.get_existing_dart_financial_statement_keys(
            bsns_years=sorted({year for year, _ in allowed_pairs}),
            reprt_codes=sorted({reprt_code for _, reprt_code in allowed_pairs}),
            fs_divs=fs_divs,
            corp_codes=[corp.corp_code for corp in targets],
        )
        effective_skip_keys = skip_request_keys

    attempts = 0
    for corp in targets:
        for bsns_year, reprt_code in allowed_pairs:
            for fs_div in fs_divs:
                request_key = f"{corp.ticker}:{bsns_year}:{reprt_code}:{fs_div}"
                if (corp.corp_code, bsns_year, reprt_code, fs_div) in existing_keys:
                    continue
                if request_key in effective_skip_keys:
                    continue
                attempts += 1
    return attempts


def _dart_share_info_actual_attempt_estimate(
    *,
    storage: object,
    targets: list[object],
    allowed_pairs: set[tuple[int, str]],
    force: bool,
    skip_request_keys: set[str],
) -> int:
    if force:
        existing_share_count_keys: set[tuple[str, int, str]] = set()
        existing_return_keys: set[tuple[str, int, str, str]] = set()
        effective_skip_keys: set[str] = set()
    else:
        bsns_years = sorted({year for year, _ in allowed_pairs})
        reprt_codes = sorted({reprt_code for _, reprt_code in allowed_pairs})
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
        effective_skip_keys = skip_request_keys

    attempts = 0
    for corp in targets:
        for bsns_year, reprt_code in allowed_pairs:
            request_prefix = f"{corp.ticker}:{bsns_year}:{reprt_code}"
            if (
                (corp.corp_code, bsns_year, reprt_code) not in existing_share_count_keys
                and f"{request_prefix}:share_count" not in effective_skip_keys
            ):
                attempts += 1
            if (
                (corp.corp_code, bsns_year, reprt_code, "dividend") not in existing_return_keys
                and f"{request_prefix}:dividend" not in effective_skip_keys
            ):
                attempts += 1
            if (
                (corp.corp_code, bsns_year, reprt_code, "treasury_stock")
                not in existing_return_keys
                and f"{request_prefix}:treasury_stock" not in effective_skip_keys
            ):
                attempts += 1
    return attempts


def _dart_xbrl_actual_attempt_estimate(
    *,
    storage: object,
    allowed_pairs: set[tuple[int, str]],
    tickers: list[str] | None,
    force: bool,
    skip_request_keys: set[str],
) -> int:
    bsns_years = sorted({year for year, _ in allowed_pairs})
    reprt_codes = sorted({reprt_code for _, reprt_code in allowed_pairs})
    corp_rows = storage.get_dart_corp_master(active_only=True, tickers=tickers)
    corp_by_ticker = {corp.ticker: corp for corp in corp_rows if corp.ticker}
    financial_rows = storage.get_dart_financial_statement_raw(bsns_years, reprt_codes, tickers)
    request_targets: set[tuple[str, int, str, str]] = set()
    for row in financial_rows:
        if row.ticker and row.rcept_no and (row.bsns_year, row.reprt_code) in allowed_pairs:
            request_targets.add((row.ticker, row.bsns_year, row.reprt_code, row.rcept_no))

    if force:
        existing_doc_keys: set[tuple[str, int, str, str]] = set()
        effective_skip_keys: set[str] = set()
    else:
        existing_doc_keys = storage.get_existing_dart_xbrl_document_keys(
            bsns_years=bsns_years,
            reprt_codes=reprt_codes,
            corp_codes=[corp.corp_code for corp in corp_by_ticker.values()],
        )
        effective_skip_keys = skip_request_keys

    attempts = 0
    for ticker, bsns_year, reprt_code, rcept_no in request_targets:
        corp = corp_by_ticker.get(ticker)
        if corp is None:
            continue
        request_key = f"{ticker}:{bsns_year}:{reprt_code}:{rcept_no}"
        if (corp.corp_code, bsns_year, reprt_code, rcept_no) in existing_doc_keys:
            continue
        if request_key in effective_skip_keys:
            continue
        attempts += 1
    return attempts


def _handle_dart_sync_corp(args: argparse.Namespace) -> None:
    """Handle ``krx-collector dart sync-corp``."""
    settings = get_settings()

    print("→ dart sync-corp: downloading OpenDART corp master and validating ticker mappings")

    from krx_collector.adapters.opendart_corp.provider import OpenDartCorpCodeProvider
    from krx_collector.infra.db_postgres.repositories import PostgresStorage
    from krx_collector.service.sync_dart_corp import sync_dart_corp_master

    try:
        request_executor = _build_opendart_request_executor()
    except Exception as exc:
        print(f"❌ OpenDART corp sync failed: {exc}", file=sys.stderr)
        sys.exit(1)

    provider = OpenDartCorpCodeProvider(request_executor=request_executor)
    storage = PostgresStorage(settings.db_dsn)
    result = sync_dart_corp_master(provider=provider, storage=storage, force=args.force)

    if result.error:
        print(f"❌ OpenDART corp sync failed: {result.error}", file=sys.stderr)
        sys.exit(1)

    print("✅ OpenDART corp sync completed.")
    print(f"   - Total records fetched: {result.total_records}")
    print(f"   - Active tickers matched: {result.matched_active_tickers}")
    print(f"   - Active tickers unmatched: {len(result.unmatched_active_tickers)}")
    print(f"   - DART listed tickers missing in stock_master: {len(result.unmatched_dart_tickers)}")

    if result.unmatched_active_tickers:
        preview = ", ".join(result.unmatched_active_tickers[:10])
        print(f"   - Sample unmatched active tickers: {preview}")


def _handle_dart_sync_financials(args: argparse.Namespace) -> None:
    """Handle ``krx-collector dart sync-financials``."""
    settings = get_settings()
    bsns_years = [int(value.strip()) for value in args.bsns_years.split(",") if value.strip()]
    reprt_codes = [value.strip() for value in args.reprt_codes.split(",") if value.strip()]
    fs_divs = [value.strip().upper() for value in args.fs_divs.split(",") if value.strip()]
    tickers = [value.strip() for value in args.tickers.split(",")] if args.tickers else None
    if args.incremental and args.reprt_codes == "11011":
        reprt_codes = ["11011", "11012", "11013", "11014"]

    print(
        f"→ dart sync-financials: years={bsns_years}, reprt_codes={reprt_codes}, "
        f"fs_divs={fs_divs}, tickers={tickers}, rate_limit={args.rate_limit_seconds}"
    )

    from krx_collector.adapters.opendart_financials.provider import (
        OpenDartFinancialStatementProvider,
    )
    from krx_collector.infra.db_postgres.repositories import PostgresStorage
    from krx_collector.service.sync_dart_financials import sync_dart_financial_statements

    try:
        request_executor = _build_opendart_request_executor()
    except Exception as exc:
        print(f"❌ OpenDART financial sync failed: {exc}", file=sys.stderr)
        sys.exit(1)

    provider = OpenDartFinancialStatementProvider(request_executor=request_executor)
    storage = PostgresStorage(settings.db_dsn)
    allowed_year_report_pairs = None
    skip_request_keys = None
    run_params_extra = None
    if args.incremental:
        from krx_collector.domain.enums import RunStatus, RunType
        from krx_collector.service.dart_target_plan import build_dart_target_plan
        from krx_collector.util.pipeline import record_terminal_run

        active_targets = storage.get_dart_corp_master(active_only=True, tickers=tickers)
        active_count = len(active_targets)
        plan = build_dart_target_plan(
            storage,
            run_type=RunType.DART_FINANCIAL_SYNC,
            active_corp_count=active_count,
            requests_per_corp_target=len(fs_divs),
            lookback_years=args.lookback_years,
            reprt_codes=reprt_codes,
            negative_cache_ttl_days=args.negative_cache_ttl_days,
        )
        if not plan.allowed_year_report_pairs:
            record_terminal_run(
                storage,
                run_type=RunType.DART_FINANCIAL_SYNC,
                status=RunStatus.SUCCESS,
                params={**plan.audit_params(), "no_work": True},
                counts={"requests_attempted": 0, "requests_skipped": 0},
            )
            print("✅ OpenDART financial sync skipped: no available incremental targets.")
            return
        actual_attempt_estimate = _dart_financial_actual_attempt_estimate(
            storage=storage,
            targets=active_targets,
            allowed_pairs=plan.allowed_year_report_pairs,
            fs_divs=fs_divs,
            force=args.force,
            skip_request_keys=plan.negative_cache_request_keys,
        )
        if actual_attempt_estimate == 0:
            audit_params = {
                **plan.audit_params(),
                "prefilter_estimated_request_count": plan.estimated_request_count,
                "estimated_request_count": actual_attempt_estimate,
                "no_work": True,
            }
            record_terminal_run(
                storage,
                run_type=RunType.DART_FINANCIAL_SYNC,
                status=RunStatus.SUCCESS,
                params=audit_params,
                counts={"requests_attempted": 0, "requests_skipped": 0},
            )
            print("✅ OpenDART financial sync skipped: no incremental request candidates.")
            return
        if actual_attempt_estimate > args.max_attempt_targets:
            audit_params = {
                **plan.audit_params(),
                "prefilter_estimated_request_count": plan.estimated_request_count,
                "estimated_request_count": actual_attempt_estimate,
                "max_attempt_targets": args.max_attempt_targets,
            }
            record_terminal_run(
                storage,
                run_type=RunType.DART_FINANCIAL_SYNC,
                status=RunStatus.FAILED,
                params=audit_params,
                counts={"estimated_request_count": actual_attempt_estimate},
                error_summary=(
                    f"Estimated OpenDART requests exceed guard "
                    f"({actual_attempt_estimate} > {args.max_attempt_targets})."
                ),
            )
            print(
                "❌ OpenDART financial sync failed: estimated requests exceed guard "
                f"({actual_attempt_estimate} > {args.max_attempt_targets}).",
                file=sys.stderr,
            )
            sys.exit(1)
        bsns_years = plan.bsns_years
        reprt_codes = plan.reprt_codes
        allowed_year_report_pairs = plan.allowed_year_report_pairs
        skip_request_keys = set() if args.force else plan.negative_cache_request_keys
        run_params_extra = {
            **plan.audit_params(),
            "prefilter_estimated_request_count": plan.estimated_request_count,
            "estimated_request_count": actual_attempt_estimate,
            "force_bypasses_negative_cache": args.force,
        }
    result = sync_dart_financial_statements(
        provider=provider,
        storage=storage,
        bsns_years=bsns_years,
        reprt_codes=reprt_codes,
        fs_divs=fs_divs,
        tickers=tickers,
        rate_limit_seconds=args.rate_limit_seconds,
        force=args.force,
        allowed_year_report_pairs=allowed_year_report_pairs,
        skip_request_keys=skip_request_keys,
        run_params_extra=run_params_extra,
    )

    if result.errors:
        print(f"⚠ Financial sync completed with {len(result.errors)} errors.", file=sys.stderr)
    else:
        print("✅ OpenDART financial sync completed.")

    print(f"   - Targets processed: {result.targets_processed}")
    print(f"   - Requests attempted: {result.requests_attempted}")
    print(f"   - Requests skipped: {result.requests_skipped}")
    print(f"   - Rows upserted: {result.rows_upserted}")
    print(f"   - No-data requests: {result.no_data_requests}")
    if result.errors:
        for request_key, error in list(result.errors.items())[:10]:
            print(f"   - Error {request_key}: {error}")
    _exit_if_opendart_key_exhausted(result, "OpenDART financial sync")


def _handle_dart_sync_share_info(args: argparse.Namespace) -> None:
    """Handle ``krx-collector dart sync-share-info``."""
    settings = get_settings()
    bsns_years = [int(value.strip()) for value in args.bsns_years.split(",") if value.strip()]
    reprt_codes = [value.strip() for value in args.reprt_codes.split(",") if value.strip()]
    tickers = [value.strip() for value in args.tickers.split(",")] if args.tickers else None
    if args.incremental and args.reprt_codes == "11011":
        reprt_codes = ["11011", "11012", "11013", "11014"]

    print(
        f"→ dart sync-share-info: years={bsns_years}, reprt_codes={reprt_codes}, "
        f"tickers={tickers}, rate_limit={args.rate_limit_seconds}"
    )

    from krx_collector.adapters.opendart_share_info.provider import OpenDartShareInfoProvider
    from krx_collector.infra.db_postgres.repositories import PostgresStorage
    from krx_collector.service.sync_dart_share_info import sync_dart_share_info

    try:
        request_executor = _build_opendart_request_executor()
    except Exception as exc:
        print(f"❌ OpenDART share info sync failed: {exc}", file=sys.stderr)
        sys.exit(1)

    provider = OpenDartShareInfoProvider(request_executor=request_executor)
    storage = PostgresStorage(settings.db_dsn)
    allowed_year_report_pairs = None
    skip_request_keys = None
    run_params_extra = None
    if args.incremental:
        from krx_collector.domain.enums import RunStatus, RunType
        from krx_collector.service.dart_target_plan import build_dart_target_plan
        from krx_collector.util.pipeline import record_terminal_run

        active_targets = storage.get_dart_corp_master(active_only=True, tickers=tickers)
        active_count = len(active_targets)
        plan = build_dart_target_plan(
            storage,
            run_type=RunType.DART_SHARE_INFO_SYNC,
            active_corp_count=active_count,
            requests_per_corp_target=3,
            lookback_years=args.lookback_years,
            reprt_codes=reprt_codes,
            negative_cache_ttl_days=args.negative_cache_ttl_days,
        )
        if not plan.allowed_year_report_pairs:
            record_terminal_run(
                storage,
                run_type=RunType.DART_SHARE_INFO_SYNC,
                status=RunStatus.SUCCESS,
                params={**plan.audit_params(), "no_work": True},
                counts={"requests_attempted": 0, "requests_skipped": 0},
            )
            print("✅ OpenDART share info sync skipped: no available incremental targets.")
            return
        actual_attempt_estimate = _dart_share_info_actual_attempt_estimate(
            storage=storage,
            targets=active_targets,
            allowed_pairs=plan.allowed_year_report_pairs,
            force=args.force,
            skip_request_keys=plan.negative_cache_request_keys,
        )
        if actual_attempt_estimate == 0:
            audit_params = {
                **plan.audit_params(),
                "prefilter_estimated_request_count": plan.estimated_request_count,
                "estimated_request_count": actual_attempt_estimate,
                "no_work": True,
            }
            record_terminal_run(
                storage,
                run_type=RunType.DART_SHARE_INFO_SYNC,
                status=RunStatus.SUCCESS,
                params=audit_params,
                counts={"requests_attempted": 0, "requests_skipped": 0},
            )
            print("✅ OpenDART share info sync skipped: no incremental request candidates.")
            return
        if actual_attempt_estimate > args.max_attempt_targets:
            audit_params = {
                **plan.audit_params(),
                "prefilter_estimated_request_count": plan.estimated_request_count,
                "estimated_request_count": actual_attempt_estimate,
                "max_attempt_targets": args.max_attempt_targets,
            }
            record_terminal_run(
                storage,
                run_type=RunType.DART_SHARE_INFO_SYNC,
                status=RunStatus.FAILED,
                params=audit_params,
                counts={"estimated_request_count": actual_attempt_estimate},
                error_summary=(
                    f"Estimated OpenDART requests exceed guard "
                    f"({actual_attempt_estimate} > {args.max_attempt_targets})."
                ),
            )
            print(
                "❌ OpenDART share info sync failed: estimated requests exceed guard "
                f"({actual_attempt_estimate} > {args.max_attempt_targets}).",
                file=sys.stderr,
            )
            sys.exit(1)
        bsns_years = plan.bsns_years
        reprt_codes = plan.reprt_codes
        allowed_year_report_pairs = plan.allowed_year_report_pairs
        skip_request_keys = set() if args.force else plan.negative_cache_request_keys
        run_params_extra = {
            **plan.audit_params(),
            "prefilter_estimated_request_count": plan.estimated_request_count,
            "estimated_request_count": actual_attempt_estimate,
            "force_bypasses_negative_cache": args.force,
        }
    result = sync_dart_share_info(
        share_count_provider=provider,
        shareholder_return_provider=provider,
        storage=storage,
        bsns_years=bsns_years,
        reprt_codes=reprt_codes,
        tickers=tickers,
        rate_limit_seconds=args.rate_limit_seconds,
        force=args.force,
        allowed_year_report_pairs=allowed_year_report_pairs,
        skip_request_keys=skip_request_keys,
        run_params_extra=run_params_extra,
    )

    if result.errors:
        print(f"⚠ Share info sync completed with {len(result.errors)} errors.", file=sys.stderr)
    else:
        print("✅ OpenDART share info sync completed.")

    print(f"   - Targets processed: {result.targets_processed}")
    print(f"   - Requests attempted: {result.requests_attempted}")
    print(f"   - Requests skipped: {result.requests_skipped}")
    print(f"   - Share count rows upserted: {result.share_count_rows_upserted}")
    print(f"   - Shareholder return rows upserted: {result.shareholder_return_rows_upserted}")
    print(f"   - No-data requests: {result.no_data_requests}")
    if result.errors:
        for request_key, error in list(result.errors.items())[:10]:
            print(f"   - Error {request_key}: {error}")
    _exit_if_opendart_key_exhausted(result, "OpenDART share info sync")


def _handle_dart_sync_xbrl(args: argparse.Namespace) -> None:
    """Handle ``krx-collector dart sync-xbrl``."""
    settings = get_settings()
    bsns_years = [int(value.strip()) for value in args.bsns_years.split(",") if value.strip()]
    reprt_codes = [value.strip() for value in args.reprt_codes.split(",") if value.strip()]
    tickers = [value.strip() for value in args.tickers.split(",")] if args.tickers else None
    if args.incremental and args.reprt_codes == "11011":
        reprt_codes = ["11011", "11012", "11013", "11014"]

    print(
        f"→ dart sync-xbrl: years={bsns_years}, reprt_codes={reprt_codes}, "
        f"tickers={tickers}, rate_limit={args.rate_limit_seconds}"
    )

    from krx_collector.adapters.opendart_xbrl.provider import OpenDartXbrlProvider
    from krx_collector.infra.db_postgres.repositories import PostgresStorage
    from krx_collector.service.sync_dart_xbrl import sync_dart_xbrl

    try:
        request_executor = _build_opendart_request_executor()
    except Exception as exc:
        print(f"❌ OpenDART XBRL sync failed: {exc}", file=sys.stderr)
        sys.exit(1)

    provider = OpenDartXbrlProvider(request_executor=request_executor)
    storage = PostgresStorage(settings.db_dsn)
    allowed_year_report_pairs = None
    skip_request_keys = None
    run_params_extra = None
    if args.incremental:
        from krx_collector.domain.enums import RunStatus, RunType
        from krx_collector.service.dart_target_plan import build_dart_target_plan
        from krx_collector.util.pipeline import record_terminal_run

        active_count = len(storage.get_dart_corp_master(active_only=True, tickers=tickers))
        plan = build_dart_target_plan(
            storage,
            run_type=RunType.XBRL_PARSE,
            active_corp_count=active_count,
            requests_per_corp_target=1,
            lookback_years=args.lookback_years,
            reprt_codes=reprt_codes,
            negative_cache_ttl_days=args.negative_cache_ttl_days,
        )
        if not plan.allowed_year_report_pairs:
            record_terminal_run(
                storage,
                run_type=RunType.XBRL_PARSE,
                status=RunStatus.SUCCESS,
                params={**plan.audit_params(), "no_work": True},
                counts={"requests_attempted": 0, "requests_skipped": 0},
            )
            print("✅ OpenDART XBRL sync skipped: no available incremental targets.")
            return
        actual_attempt_estimate = _dart_xbrl_actual_attempt_estimate(
            storage=storage,
            allowed_pairs=plan.allowed_year_report_pairs,
            tickers=tickers,
            force=args.force,
            skip_request_keys=plan.negative_cache_request_keys,
        )
        if actual_attempt_estimate == 0:
            audit_params = {
                **plan.audit_params(),
                "prefilter_estimated_request_count": plan.estimated_request_count,
                "estimated_request_count": actual_attempt_estimate,
                "no_work": True,
            }
            record_terminal_run(
                storage,
                run_type=RunType.XBRL_PARSE,
                status=RunStatus.SUCCESS,
                params=audit_params,
                counts={"requests_attempted": 0, "requests_skipped": 0},
            )
            print("✅ OpenDART XBRL sync skipped: no incremental request candidates.")
            return
        if actual_attempt_estimate > args.max_attempt_targets:
            audit_params = {
                **plan.audit_params(),
                "prefilter_estimated_request_count": plan.estimated_request_count,
                "estimated_request_count": actual_attempt_estimate,
                "max_attempt_targets": args.max_attempt_targets,
            }
            record_terminal_run(
                storage,
                run_type=RunType.XBRL_PARSE,
                status=RunStatus.FAILED,
                params=audit_params,
                counts={"estimated_request_count": actual_attempt_estimate},
                error_summary=(
                    f"Estimated OpenDART requests exceed guard "
                    f"({actual_attempt_estimate} > {args.max_attempt_targets})."
                ),
            )
            print(
                "❌ OpenDART XBRL sync failed: estimated requests exceed guard "
                f"({actual_attempt_estimate} > {args.max_attempt_targets}).",
                file=sys.stderr,
            )
            sys.exit(1)
        bsns_years = plan.bsns_years
        reprt_codes = plan.reprt_codes
        allowed_year_report_pairs = plan.allowed_year_report_pairs
        skip_request_keys = set() if args.force else plan.negative_cache_request_keys
        run_params_extra = {
            **plan.audit_params(),
            "prefilter_estimated_request_count": plan.estimated_request_count,
            "estimated_request_count": actual_attempt_estimate,
            "force_bypasses_negative_cache": args.force,
        }
    result = sync_dart_xbrl(
        provider=provider,
        storage=storage,
        bsns_years=bsns_years,
        reprt_codes=reprt_codes,
        tickers=tickers,
        rate_limit_seconds=args.rate_limit_seconds,
        force=args.force,
        allowed_year_report_pairs=allowed_year_report_pairs,
        skip_request_keys=skip_request_keys,
        run_params_extra=run_params_extra,
    )

    if result.errors:
        print(f"⚠ XBRL sync completed with {len(result.errors)} errors.", file=sys.stderr)
    else:
        print("✅ OpenDART XBRL sync completed.")

    print(f"   - Targets processed: {result.targets_processed}")
    print(f"   - Requests attempted: {result.requests_attempted}")
    print(f"   - Requests skipped: {result.requests_skipped}")
    print(f"   - Documents upserted: {result.documents_upserted}")
    print(f"   - Facts upserted: {result.facts_upserted}")
    print(f"   - No-data requests: {result.no_data_requests}")
    if result.errors:
        for request_key, error in list(result.errors.items())[:10]:
            print(f"   - Error {request_key}: {error}")
    _exit_if_opendart_key_exhausted(result, "OpenDART XBRL sync")


def _handle_metrics_normalize(args: argparse.Namespace) -> None:
    """Handle ``krx-collector metrics normalize``."""
    bsns_years = [int(value.strip()) for value in args.bsns_years.split(",") if value.strip()]
    reprt_codes = [value.strip() for value in args.reprt_codes.split(",") if value.strip()]
    tickers = [value.strip() for value in args.tickers.split(",")] if args.tickers else None
    if args.incremental:
        current_year = date.today().year
        bsns_years = [current_year - offset for offset in range(args.lookback_years + 1)]

    print(
        f"→ metrics normalize: years={bsns_years}, reprt_codes={reprt_codes}, "
        f"tickers={tickers}, batch_size={args.batch_size}, "
        f"incremental={args.incremental}, lookback_years={args.lookback_years}"
    )

    from krx_collector.infra.config.settings import get_settings as _get_settings
    from krx_collector.infra.db_postgres.repositories import PostgresStorage
    from krx_collector.service.normalize_metrics import normalize_stock_metrics

    settings = _get_settings()
    storage = PostgresStorage(settings.db_dsn)
    result = normalize_stock_metrics(
        storage=storage,
        bsns_years=bsns_years,
        reprt_codes=reprt_codes,
        tickers=tickers,
        batch_size=args.batch_size,
        incremental=args.incremental,
    )

    if result.errors:
        print(
            f"⚠ Metric normalization completed with {len(result.errors)} errors.", file=sys.stderr
        )
        for error_key, error_value in list(result.errors.items())[:10]:
            print(f"   - Error {error_key}: {error_value}")
    else:
        print("✅ Metric normalization completed.")

    print(f"   - Targets processed: {result.targets_processed}")
    print(f"   - Metric catalog upserted: {result.catalog_upsert.updated}")
    print(f"   - Mapping rules upserted: {result.rule_upsert.updated}")
    print(f"   - Facts written: {result.facts_written}")


def _handle_metrics_coverage_report(args: argparse.Namespace) -> None:
    """Handle ``krx-collector metrics coverage-report``."""
    bsns_years = [int(value.strip()) for value in args.bsns_years.split(",") if value.strip()]
    reprt_codes = [value.strip() for value in args.reprt_codes.split(",") if value.strip()]
    tickers = [value.strip() for value in args.tickers.split(",")] if args.tickers else None

    print(
        f"→ metrics coverage-report: years={bsns_years}, reprt_codes={reprt_codes}, "
        f"tickers={tickers}"
    )

    from krx_collector.infra.config.settings import get_settings as _get_settings
    from krx_collector.infra.db_postgres.repositories import PostgresStorage
    from krx_collector.service.report_metric_coverage import build_metric_coverage_report

    settings = _get_settings()
    storage = PostgresStorage(settings.db_dsn)
    report = build_metric_coverage_report(
        storage=storage,
        bsns_years=bsns_years,
        reprt_codes=reprt_codes,
        tickers=tickers,
    )

    print(f"✅ Metric coverage report generated. Targets: {report.target_count}")
    for row in report.rows[:20]:
        print(
            f"   - {row.metric_code}: {row.covered_count}/{row.target_count} "
            f"({row.coverage_ratio})"
        )


def _handle_common_seed_catalog(args: argparse.Namespace) -> None:
    """Handle ``krx-collector common seed-catalog``."""
    settings = get_settings()

    print(f"→ common seed-catalog: init_schema={args.init_schema}")

    from krx_collector.infra.db_postgres.repositories import PostgresStorage
    from krx_collector.service.default_common_feature_catalog import seed_common_feature_catalog

    storage = PostgresStorage(settings.db_dsn)
    if args.init_schema:
        storage.init_schema()

    result = seed_common_feature_catalog(storage)

    print("✅ Common feature catalog seed completed.")
    print(f"   - Series upserted: {result.series_upsert.updated}")
    print(f"   - Feature catalog upserted: {result.catalog_upsert.updated}")


def _handle_common_sync(args: argparse.Namespace) -> None:
    """Handle ``krx-collector common sync``."""
    settings = get_settings()
    sources = args.sources
    series_ids = _split_csv(args.series)
    include_inactive = bool(args.include_inactive)
    if include_inactive and not series_ids:
        raise SystemExit("--include-inactive requires an explicit --series allowlist.")
    providers = _build_common_feature_providers(sources)

    print(
        f"→ common sync: sources={[source.value for source in sources]}, "
        f"series={series_ids}, start={args.start}, end={args.end}, "
        f"force={args.force}, rate_limit={args.rate_limit_seconds}, "
        f"include_inactive={include_inactive}, init_schema={args.init_schema}, "
        f"incremental={args.incremental}, lookback_days={args.lookback_days}, "
        f"max_auto_range_days={args.max_auto_range_days}, "
        f"allow_large_range={args.allow_large_range}"
    )

    from krx_collector.infra.db_postgres.repositories import PostgresStorage
    from krx_collector.service.sync_common_features import sync_common_features

    storage = PostgresStorage(settings.db_dsn)
    if args.init_schema:
        storage.init_schema()

    result = sync_common_features(
        providers=providers,
        storage=storage,
        start=args.start,
        end=args.end,
        sources=sources,
        series_ids=series_ids,
        active_only=not include_inactive,
        force=args.force,
        rate_limit_seconds=args.rate_limit_seconds,
        incremental=args.incremental,
        lookback_days=args.lookback_days,
        max_auto_range_days=args.max_auto_range_days,
        allow_large_range=args.allow_large_range,
    )

    if result.errors:
        print(f"⚠ Common feature sync completed with {len(result.errors)} errors.", file=sys.stderr)
    else:
        print("✅ Common feature sync completed.")

    print(f"   - Series processed: {result.series_processed}")
    print(f"   - Requests attempted: {result.requests_attempted}")
    print(f"   - Requests skipped: {result.requests_skipped}")
    print(f"   - Rows upserted: {result.rows_upserted}")
    print(f"   - No-data requests: {result.no_data_requests}")
    if result.errors:
        for request_key, error in list(result.errors.items())[:10]:
            print(f"   - Error {request_key}: {error}")
        if args.incremental:
            sys.exit(1)


def _handle_common_build_daily(args: argparse.Namespace) -> None:
    """Handle ``krx-collector common build-daily``."""
    settings = get_settings()
    feature_codes = _split_csv(args.feature_codes)
    include_inactive = bool(args.include_inactive)
    if include_inactive and not feature_codes:
        raise SystemExit("--include-inactive requires an explicit --feature-codes allowlist.")

    print(
        f"→ common build-daily: feature_codes={feature_codes}, "
        f"start={args.start}, end={args.end}, include_inactive={include_inactive}, "
        f"init_schema={args.init_schema}, incremental={args.incremental}, "
        f"lookback_days={args.lookback_days}, "
        f"max_auto_range_days={args.max_auto_range_days}, "
        f"allow_large_range={args.allow_large_range}"
    )

    from krx_collector.infra.db_postgres.repositories import PostgresStorage
    from krx_collector.service.build_common_feature_daily_facts import (
        build_common_feature_daily_facts,
    )

    storage = PostgresStorage(settings.db_dsn)
    if args.init_schema:
        storage.init_schema()

    result = build_common_feature_daily_facts(
        storage=storage,
        start=args.start,
        end=args.end,
        feature_codes=feature_codes,
        active_only=not include_inactive,
        incremental=args.incremental,
        lookback_days=args.lookback_days,
        max_auto_range_days=args.max_auto_range_days,
        allow_large_range=args.allow_large_range,
    )

    if result.errors:
        print(
            f"⚠ Common feature daily build completed with {len(result.errors)} errors.",
            file=sys.stderr,
        )
    else:
        print("✅ Common feature daily build completed.")

    print(f"   - Features processed: {result.features_processed}")
    print(f"   - Feature dates processed: {result.feature_dates_processed}")
    print(f"   - Facts built: {result.facts_built}")
    print(f"   - Null facts: {result.null_facts}")
    print(f"   - Facts upserted: {result.facts_upserted}")
    if result.errors:
        for feature_code, error in list(result.errors.items())[:10]:
            print(f"   - Error {feature_code}: {error}")
        if args.incremental:
            sys.exit(1)


def _handle_common_coverage_report(args: argparse.Namespace) -> None:
    """Handle ``krx-collector common coverage-report``."""
    settings = get_settings()
    feature_codes = _split_csv(args.feature_codes)
    include_inactive = bool(args.include_inactive)
    if include_inactive and not feature_codes:
        raise SystemExit("--include-inactive requires an explicit --feature-codes allowlist.")

    print(
        f"→ common coverage-report: feature_codes={feature_codes}, "
        f"start={args.start}, end={args.end}, include_inactive={include_inactive}"
    )

    from krx_collector.infra.db_postgres.repositories import PostgresStorage
    from krx_collector.service.report_common_feature_coverage import (
        build_common_feature_coverage_report,
    )

    storage = PostgresStorage(settings.db_dsn)
    report = build_common_feature_coverage_report(
        storage=storage,
        start=args.start,
        end=args.end,
        feature_codes=feature_codes,
        active_only=not include_inactive,
    )

    print(f"✅ Common feature coverage report generated. Target dates: {report.target_count}")
    print("   feature_code | facts | non_null | nulls | missing | coverage | pit_violations")
    for row in report.rows[:50]:
        print(
            f"   {row.feature_code}: "
            f"{row.fact_count}/{row.non_null_count}/{row.null_count}/"
            f"{row.missing_count} coverage={row.coverage_ratio} "
            f"pit_violations={row.pit_violation_count}"
        )


def _handle_common_readiness_report(args: argparse.Namespace) -> None:
    """Handle ``krx-collector common readiness-report``."""
    settings = get_settings()
    feature_codes = _split_csv(args.feature_codes)
    include_inactive = bool(args.include_inactive)
    if include_inactive and not feature_codes:
        raise SystemExit("--include-inactive requires an explicit --feature-codes allowlist.")

    print(
        f"→ common readiness-report: feature_codes={feature_codes}, "
        f"start={args.start}, end={args.end}, include_inactive={include_inactive}, "
        f"required_coverage_ratio={args.required_coverage_ratio}"
    )

    from krx_collector.infra.db_postgres.repositories import PostgresStorage
    from krx_collector.service.report_common_feature_readiness import (
        build_common_feature_readiness_report,
    )

    storage = PostgresStorage(settings.db_dsn)
    report = build_common_feature_readiness_report(
        storage=storage,
        start=args.start,
        end=args.end,
        feature_codes=feature_codes,
        active_only=not include_inactive,
        required_coverage_ratio=args.required_coverage_ratio,
    )

    print(f"✅ Common feature readiness report generated. Target dates: {report.target_count}")
    print("   feature_code | ready | coverage | nulls | missing | pit_violations | blockers")
    for row in report.rows[:50]:
        blockers = ", ".join(row.blockers) if row.blockers else "-"
        print(
            f"   {row.feature_code}: ready={row.ready} "
            f"coverage={row.coverage_ratio}/{row.required_coverage_ratio} "
            f"nulls={row.null_count} missing={row.missing_count} "
            f"pit_violations={row.pit_violation_count} blockers={blockers}"
        )

    not_ready = [row for row in report.rows if not row.ready]
    if args.fail_on_not_ready and (report.errors or not_ready):
        if report.errors:
            print(
                f"❌ Common feature readiness report has {len(report.errors)} errors.",
                file=sys.stderr,
            )
        if not_ready:
            print(
                f"❌ Common feature readiness failed for {len(not_ready)} features.",
                file=sys.stderr,
            )
        raise SystemExit(2)


def _handle_flows_sync(args: argparse.Namespace) -> None:
    """Handle ``krx-collector flows sync``."""
    settings = get_settings()
    tickers = [value.strip() for value in args.tickers.split(",")] if args.tickers else None
    default_flow_date = date.today() - timedelta(days=1)
    start = args.start or default_flow_date
    end = args.end or default_flow_date

    if args.incremental and (args.start is not None or args.end is not None):
        print(
            "❌ Flow sync failed: --incremental cannot be combined with --start/--end in v1.",
            file=sys.stderr,
        )
        sys.exit(2)
    if args.incremental and args.use_price_range:
        print(
            "❌ Flow sync failed: --incremental cannot be combined with --use-price-range.",
            file=sys.stderr,
        )
        sys.exit(2)

    timeout_seconds = (
        args.timeout_seconds
        if args.timeout_seconds is not None
        else settings.krx_mdc_timeout_seconds
    )
    rate_limit_seconds = (
        args.rate_limit_seconds
        if args.rate_limit_seconds is not None
        else settings.krx_logical_rate_limit_seconds
    )
    http_min_delay_seconds = (
        args.http_min_delay_seconds
        if args.http_min_delay_seconds is not None
        else settings.krx_min_delay_seconds
    )
    http_max_delay_seconds = (
        args.http_max_delay_seconds
        if args.http_max_delay_seconds is not None
        else settings.krx_max_delay_seconds
    )
    long_rest_every = (
        args.long_rest_every if args.long_rest_every is not None else settings.krx_long_rest_every
    )
    long_rest_min_seconds = (
        args.long_rest_min_seconds
        if args.long_rest_min_seconds is not None
        else settings.krx_long_rest_min_seconds
    )
    long_rest_max_seconds = (
        args.long_rest_max_seconds
        if args.long_rest_max_seconds is not None
        else settings.krx_long_rest_max_seconds
    )
    auth_cooldown_seconds = (
        args.auth_cooldown_seconds
        if args.auth_cooldown_seconds is not None
        else settings.krx_auth_cooldown_seconds
    )
    error_backoff_min_seconds = (
        args.error_backoff_min_seconds
        if args.error_backoff_min_seconds is not None
        else settings.krx_error_backoff_min_seconds
    )
    error_backoff_max_seconds = (
        args.error_backoff_max_seconds
        if args.error_backoff_max_seconds is not None
        else settings.krx_error_backoff_max_seconds
    )

    print(
        f"→ flows sync: start={start}, end={end}, "
        f"tickers={tickers}, logical_rate_limit={rate_limit_seconds}, "
        f"http_delay={http_min_delay_seconds}..{http_max_delay_seconds}s, "
        f"long_rest_every={long_rest_every}, "
        f"long_rest={long_rest_min_seconds}..{long_rest_max_seconds}s, "
        f"auth_cooldown={auth_cooldown_seconds}s, "
        f"error_backoff={error_backoff_min_seconds}..{error_backoff_max_seconds}s, "
        f"randomize_requests={not args.ordered_requests}, "
        f"timeout={timeout_seconds}, "
        f"progress_interval={args.progress_log_interval_seconds}, "
        f"progress_every={args.progress_log_every_items}, "
        f"incremental={args.incremental}, "
        f"lookback_days={args.lookback_days}, "
        f"max_auto_range_days={args.max_auto_range_days}, "
        f"exclude_groups={args.exclude_groups}"
    )

    from krx_collector.adapters.flows_krx.provider import KrxDirectFlowProvider
    from krx_collector.domain.enums import RunStatus, RunType, Source
    from krx_collector.infra.db_postgres.repositories import PostgresStorage
    from krx_collector.service.sync_krx_flows import (
        FLOW_METRIC_GROUPS,
        resolve_incremental_flow_range,
        sync_krx_security_flows,
    )
    from krx_collector.util.pipeline import HumanThrottle, HumanThrottlePolicy, record_terminal_run

    storage = PostgresStorage(settings.db_dsn)
    run_params_extra: dict[str, object] | None = None
    exclude_groups = _split_csv(args.exclude_groups) or []
    enabled_flow_groups = [
        group for group in sorted(FLOW_METRIC_GROUPS) if group not in set(exclude_groups)
    ]

    if args.incremental:
        metric_codes = sorted(
            {metric for metrics in FLOW_METRIC_GROUPS.values() for metric in metrics}
        )
        try:
            incremental_range = resolve_incremental_flow_range(
                latest_price_date=storage.get_latest_daily_price_date(tickers=tickers),
                metric_latest_dates=storage.get_krx_security_flow_metric_max_dates(
                    metric_codes=metric_codes,
                    source=Source.KRX,
                ),
                lookback_days=args.lookback_days,
                exclude_groups=exclude_groups,
            )
        except ValueError as exc:
            print(f"❌ Flow sync failed: {exc}", file=sys.stderr)
            sys.exit(1)

        if (
            incremental_range.auto_range_days > args.max_auto_range_days
            and not args.allow_large_range
        ):
            print(
                "❌ Flow sync failed: resolved incremental range is too large "
                f"({incremental_range.auto_range_days} days > "
                f"{args.max_auto_range_days}). Use --allow-large-range to override.",
                file=sys.stderr,
            )
            sys.exit(1)

        start = incremental_range.start
        end = incremental_range.end
        print("   - Flows incremental range resolved:")
        print(f"     latest_price_date={incremental_range.latest_price_date}")
        for group, latest in sorted(incremental_range.group_latest_dates.items()):
            lag = incremental_range.group_lag_days[group]
            print(f"     latest_{group}_date={latest} lag_days={lag}")
        print(f"     excluded_groups={incremental_range.excluded_groups}")
        print(f"     start={start}")
        print(f"     end={end}")
        print(f"     lookback_days={incremental_range.lookback_days}")
        print(f"     auto_range_days={incremental_range.auto_range_days}")
        run_params_extra = incremental_range.as_run_params()
        run_params_extra["max_auto_range_days"] = args.max_auto_range_days
        run_params_extra["allow_large_range"] = args.allow_large_range

        if incremental_range.no_work:
            record_terminal_run(
                storage,
                run_type=RunType.KRX_FLOW_SYNC,
                status=RunStatus.SUCCESS,
                params={
                    **(run_params_extra or {}),
                    "tickers": tickers,
                    "no_work": True,
                    "skip_reason": "flow metrics are current",
                },
                counts={
                    "targets_processed": 0,
                    "requests_attempted": 0,
                    "requests_skipped": 0,
                    "rows_upserted": 0,
                    "no_data_requests": 0,
                },
            )
            print("✅ KRX flow sync skipped: no incremental work.")
            return

    if args.use_price_range:
        price_range = storage.get_daily_price_date_range(tickers=tickers)
        if price_range is None:
            print(
                "❌ Flow sync failed: no daily OHLCV rows found for price range.",
                file=sys.stderr,
            )
            sys.exit(1)
        price_start, price_end = price_range
        start = max(start, price_start) if args.start else price_start
        end = min(end, price_end) if args.end else price_end
        if start > end:
            print(
                f"❌ Flow sync failed: resolved price range is empty "
                f"(start={start}, end={end}).",
                file=sys.stderr,
            )
            sys.exit(1)
        price_range_days = (end - start).days + 1
        if price_range_days > args.max_price_range_days and not args.allow_large_range:
            print(
                "❌ Flow sync failed: resolved price range is too large "
                f"({price_range_days} days > {args.max_price_range_days}). "
                "Use --allow-large-range to override.",
                file=sys.stderr,
            )
            sys.exit(1)
        print(f"   - Price range resolved: start={start}, end={end}")

    if not args.incremental:
        resolved_range_days = (end - start).days + 1
        if resolved_range_days > args.max_price_range_days and not args.allow_large_range:
            print(
                "❌ Flow sync failed: resolved range is too large "
                f"({resolved_range_days} days > {args.max_price_range_days}). "
                "Use --allow-large-range to override.",
                file=sys.stderr,
            )
            sys.exit(1)

    human_throttle = HumanThrottle(
        HumanThrottlePolicy(
            min_delay_seconds=http_min_delay_seconds,
            max_delay_seconds=http_max_delay_seconds,
            long_rest_every=long_rest_every,
            long_rest_min_seconds=long_rest_min_seconds,
            long_rest_max_seconds=long_rest_max_seconds,
            auth_cooldown_seconds=auth_cooldown_seconds,
            error_backoff_min_seconds=error_backoff_min_seconds,
            error_backoff_max_seconds=error_backoff_max_seconds,
        ),
        logger_instance=logger,
    )

    provider = KrxDirectFlowProvider(
        timeout_seconds=timeout_seconds,
        login_id=settings.krx_id,
        login_pw=settings.krx_pw,
        human_throttle=human_throttle,
    )

    result = sync_krx_security_flows(
        provider=provider,
        storage=storage,
        start=start,
        end=end,
        tickers=tickers,
        rate_limit_seconds=rate_limit_seconds,
        progress_log_interval_seconds=args.progress_log_interval_seconds,
        progress_log_every_items=args.progress_log_every_items,
        randomize_request_order=not args.ordered_requests,
        run_params_extra=run_params_extra,
        enabled_flow_groups=enabled_flow_groups,
    )

    if result.errors:
        print(f"⚠ Flow sync completed with {len(result.errors)} errors.", file=sys.stderr)
    else:
        print("✅ KRX flow sync completed.")

    print(f"   - Targets processed: {result.targets_processed}")
    print(f"   - Source: {provider.source().value}")
    print(f"   - Requests attempted: {result.requests_attempted}")
    print(f"   - Requests skipped: {result.requests_skipped}")
    print(f"   - Rows upserted: {result.rows_upserted}")
    print(f"   - No-data requests: {result.no_data_requests}")
    if result.pending_metrics:
        print(f"   - Pending metrics: {', '.join(result.pending_metrics)}")
    if result.errors:
        for request_key, error in list(result.errors.items())[:10]:
            print(f"   - Error {request_key}: {error}")


def _handle_operating_process_document(args: argparse.Namespace) -> None:
    """Handle ``krx-collector operating process-document``."""
    from krx_collector.domain.enums import Market
    from krx_collector.domain.models import OperatingSourceDocument
    from krx_collector.infra.db_postgres.repositories import PostgresStorage
    from krx_collector.service.default_operating_registry import build_default_operating_registry
    from krx_collector.service.process_operating_document import (
        build_operating_document_key,
        process_operating_document,
    )
    from krx_collector.util.time import now_kst

    settings = get_settings()
    text_path = Path(args.text_file)
    content_text = text_path.read_text(encoding="utf-8")
    market = Market(args.market.upper())
    document_key = build_operating_document_key(
        ticker=args.ticker,
        sector_key=args.sector_key,
        document_type=args.document_type,
        title=args.title,
        period_end=args.period_end.isoformat(),
        content_text=content_text,
    )
    document = OperatingSourceDocument(
        document_key=document_key,
        ticker=args.ticker,
        market=market,
        sector_key=args.sector_key,
        document_type=args.document_type,
        title=args.title,
        document_date=args.document_date,
        period_end=args.period_end,
        source_system=args.source_system,
        source_url=args.source_url or "",
        language=args.language,
        content_text=content_text,
        fetched_at=now_kst(),
        raw_payload={
            "text_file": str(text_path),
        },
    )

    print(
        f"→ operating process-document: ticker={args.ticker}, market={market.value}, "
        f"sector_key={args.sector_key}, period_end={args.period_end}, text_file={text_path}"
    )

    storage = PostgresStorage(settings.db_dsn)
    registry = build_default_operating_registry()
    result = process_operating_document(storage=storage, registry=registry, document=document)

    if result.errors:
        print(
            f"⚠ Operating KPI processing completed with {len(result.errors)} errors.",
            file=sys.stderr,
        )
        for request_key, error in list(result.errors.items())[:10]:
            print(f"   - Error {request_key}: {error}")
    else:
        print("✅ Operating KPI processing completed.")

    print(f"   - Documents processed: {result.documents_processed}")
    print(f"   - Facts upserted: {result.facts_upserted}")
    if result.extracted_metric_codes:
        print(f"   - Extracted metrics: {', '.join(result.extracted_metric_codes)}")


def _handle_universe_sync(args: argparse.Namespace) -> None:
    """Handle ``krx-collector universe sync``."""
    settings = get_settings()

    # 1. Parse arguments
    source_str = args.source or settings.universe_source_default
    source_str = source_str.upper()

    from krx_collector.domain.enums import Market

    markets = []
    for m in args.markets.split(","):
        m_upper = m.strip().upper()
        if m_upper == "KOSPI":
            markets.append(Market.KOSPI)
        elif m_upper == "KOSDAQ":
            markets.append(Market.KOSDAQ)
        else:
            print(f"❌ Unknown market: {m}", file=sys.stderr)
            sys.exit(1)

    print(
        f"→ universe sync: source={source_str}, markets={[m.value for m in markets]}, "
        f"as_of={args.as_of}, full_refresh={args.full_refresh}"
    )

    # 2. Instantiate dependencies
    from krx_collector.infra.db_postgres.repositories import PostgresStorage

    storage = PostgresStorage(settings.db_dsn)

    provider = None
    if source_str == "FDR":
        from krx_collector.adapters.universe_fdr.provider import FdrUniverseProvider

        provider = FdrUniverseProvider()
    elif source_str == "PYKRX":
        from krx_collector.adapters.universe_pykrx.provider import PykrxUniverseProvider

        provider = PykrxUniverseProvider()
    else:
        print(f"❌ Unsupported universe source: {source_str}", file=sys.stderr)
        sys.exit(1)

    # 3. Execute use case
    from krx_collector.service.sync_universe import sync_universe

    result = sync_universe(
        provider=provider,
        storage=storage,
        markets=markets,
        as_of=args.as_of,
        full_refresh=args.full_refresh,
    )

    if result.error:
        print(f"❌ Universe sync failed: {result.error}", file=sys.stderr)
        sys.exit(1)

    print("✅ Universe sync completed.")
    print(f"   - Upserted: {result.upsert.updated} records")
    if result.new_tickers:
        print(f"   - New tickers: {len(result.new_tickers)}")
    if result.delisted_tickers:
        print(f"   - Delisted tickers: {len(result.delisted_tickers)}")


def _handle_prices_backfill(args: argparse.Namespace) -> None:
    """Handle ``krx-collector prices backfill``."""
    settings = get_settings()

    rate_limit = args.rate_limit_seconds
    if rate_limit is None:
        rate_limit = settings.rate_limit_seconds

    long_rest_interval = args.long_rest_interval
    if long_rest_interval is None:
        long_rest_interval = settings.long_rest_interval

    long_rest_seconds = args.long_rest_seconds
    if long_rest_seconds is None:
        long_rest_seconds = settings.long_rest_seconds

    print(
        f"→ prices backfill: market={args.market}, tickers={args.tickers}, "
        f"start={args.start}, end={args.end}, "
        f"rate_limit={rate_limit}, "
        f"long_rest_interval={long_rest_interval}, "
        f"long_rest_seconds={long_rest_seconds}, "
        f"incremental={args.incremental}, "
        f"lookback_days={args.lookback_days}, "
        f"max_auto_range_days={args.max_auto_range_days}, "
        f"new_ticker_start={args.new_ticker_start}, "
        f"allow_new_ticker_backfill={args.allow_new_ticker_backfill}, "
        f"allow_large_range={args.allow_large_range}"
    )

    from krx_collector.domain.enums import Market

    market_filter = None
    if args.market and args.market.upper() != "ALL":
        market_str = args.market.upper()
        if market_str == "KOSPI":
            market_filter = Market.KOSPI
        elif market_str == "KOSDAQ":
            market_filter = Market.KOSDAQ
        else:
            print(f"❌ Unknown market: {args.market}", file=sys.stderr)
            sys.exit(1)

    tickers_list = None
    if args.tickers:
        tickers_list = [t.strip() for t in args.tickers.split(",")]

    from krx_collector.adapters.prices_pykrx.provider import PykrxDailyPriceProvider

    provider = PykrxDailyPriceProvider()

    from krx_collector.infra.db_postgres.repositories import PostgresStorage

    storage = PostgresStorage(settings.db_dsn)

    from krx_collector.service.backfill_daily import backfill_daily_prices

    result = backfill_daily_prices(
        provider=provider,
        storage=storage,
        market=market_filter,
        tickers=tickers_list,
        start=args.start,
        end=args.end,
        rate_limit_seconds=rate_limit,
        long_rest_interval=long_rest_interval,
        long_rest_seconds=long_rest_seconds,
        incremental=args.incremental,
        lookback_days=args.lookback_days,
        max_auto_range_days=args.max_auto_range_days,
        new_ticker_start=args.new_ticker_start,
        allow_new_ticker_backfill=args.allow_new_ticker_backfill,
        allow_large_range=args.allow_large_range,
    )

    if result.errors:
        print(f"⚠ Backfill completed with {len(result.errors)} errors.", file=sys.stderr)
    else:
        print("✅ Backfill completed successfully.")

    print(f"   - Tickers processed: {result.tickers_processed}")
    print(f"   - Bars upserted: {result.bars_upserted}")
    if args.incremental and result.errors:
        sys.exit(1)


def _handle_validate(args: argparse.Namespace) -> None:
    """Handle ``krx-collector validate``."""
    settings = get_settings()
    print(f"→ validate: date={args.date}, market={args.market}")

    from krx_collector.domain.enums import Market

    market_filter = None
    if args.market and args.market.upper() != "ALL":
        market_str = args.market.upper()
        if market_str == "KOSPI":
            market_filter = Market.KOSPI
        elif market_str == "KOSDAQ":
            market_filter = Market.KOSDAQ
        else:
            print(f"❌ Unknown market: {args.market}", file=sys.stderr)
            sys.exit(1)

    from krx_collector.infra.db_postgres.repositories import PostgresStorage

    storage = PostgresStorage(settings.db_dsn)

    from krx_collector.service.validate import validate

    try:
        validate(storage=storage, market=market_filter, target_date=args.date)
        print("✅ Validation completed. Check logs for details.")
    except Exception as exc:
        print(f"❌ Validation failed: {exc}", file=sys.stderr)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Parser construction
# ---------------------------------------------------------------------------


def _parse_date(value: str) -> date:
    """Parse a YYYY-MM-DD string into a ``date``."""
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: {value!r} (expected YYYY-MM-DD)")


def _parse_coverage_ratio(value: str) -> Decimal:
    """Parse a coverage ratio in the inclusive 0..1 range."""
    try:
        ratio = Decimal(value)
    except InvalidOperation as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid coverage ratio: {value!r} (expected decimal between 0 and 1)"
        ) from exc
    if not ratio.is_finite() or ratio < Decimal("0") or ratio > Decimal("1"):
        raise argparse.ArgumentTypeError(
            f"Invalid coverage ratio: {value!r} (expected decimal between 0 and 1)"
        )
    return ratio.quantize(Decimal("0.0001"))


def _parse_positive_seconds(value: str) -> float:
    """Parse a positive second value, accepting an optional ``s`` suffix."""
    normalized = value.strip().lower()
    for suffix in ("seconds", "second", "secs", "sec", "s"):
        if normalized.endswith(suffix):
            normalized = normalized[: -len(suffix)].strip()
            break
    try:
        seconds = float(normalized)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid seconds value: {value!r} (expected positive seconds)"
        )
    if seconds <= 0:
        raise argparse.ArgumentTypeError(
            f"Invalid seconds value: {value!r} (must be greater than zero)"
        )
    return seconds


def build_parser() -> argparse.ArgumentParser:
    """Build and return the top-level argument parser."""
    parser = argparse.ArgumentParser(
        prog="krx-collector",
        description="KRX stock data pipeline — universe sync & daily OHLCV collection.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # -- db -------------------------------------------------------------------
    db_parser = subparsers.add_parser("db", help="Database management commands.")
    db_sub = db_parser.add_subparsers(dest="db_command", required=True)

    db_init = db_sub.add_parser("init", help="Initialise database schema (run DDL).")
    db_init.set_defaults(handler=_handle_db_init)

    db_sync_remote = db_sub.add_parser(
        "sync-remote",
        help="Sync the remote sj2-server PostgreSQL data into the local PostgreSQL DB.",
    )
    db_sync_remote.add_argument(
        "--db-info-path",
        default=None,
        help="Path to the remote DB metadata file (default: from config).",
    )
    db_sync_remote.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Number of rows to fetch from the remote DB per batch (default: from config).",
    )
    db_sync_remote.add_argument(
        "--full-refresh",
        action="store_true",
        default=False,
        help="Truncate the local synced tables and copy everything from scratch.",
    )
    db_sync_scope = db_sync_remote.add_mutually_exclusive_group()
    db_sync_scope.add_argument(
        "--all-tables",
        action="store_true",
        default=False,
        help=(
            "Sync the managed mirror tables through the schema-reset copy path. "
            "Requires --full-refresh. "
            "Drops only those local tables and re-applies sql/postgres_ddl.sql "
            "before copying so stale local schemas are replaced."
        ),
    )
    db_sync_scope.add_argument(
        "--tables",
        default=None,
        help=(
            "Comma-separated managed table names to sync. FK parent tables are "
            "included automatically. Omit to sync all managed mirror tables."
        ),
    )
    db_sync_remote.add_argument(
        "--remote-host",
        default=None,
        help="Override the remote DB hostname from db_info (default: from config/file).",
    )
    db_sync_remote.add_argument(
        "--ssh-host",
        default=None,
        help="Optional SSH host for port forwarding to the remote PostgreSQL server.",
    )
    db_sync_remote.add_argument(
        "--ssh-local-port",
        type=int,
        default=None,
        help="Optional fixed local port for the SSH tunnel (default: random free port).",
    )
    db_sync_remote.set_defaults(handler=_handle_db_sync_remote)

    # -- ops ------------------------------------------------------------------
    ops_parser = subparsers.add_parser("ops", help="Read-only operational reports.")
    ops_sub = ops_parser.add_subparsers(dest="ops_command", required=True)
    ops_freshness = ops_sub.add_parser(
        "freshness-report",
        help="Report latest stored data points and running ingestion jobs.",
    )
    ops_freshness.add_argument(
        "--running-limit",
        type=int,
        default=20,
        help="Maximum running ingestion runs to show (default: 20).",
    )
    ops_freshness.set_defaults(handler=_handle_ops_freshness_report)

    ops_common_freshness = ops_sub.add_parser(
        "assert-common-freshness",
        help="Fail unless required common feature sources are fresh enough for build.",
    )
    ops_common_freshness.add_argument(
        "--sources",
        type=_parse_common_sources,
        default=_parse_common_sources("fdr,fred,ecos,krx"),
        help="Comma-separated required common sources (default: fdr,fred,ecos,krx).",
    )
    ops_common_freshness.add_argument(
        "--end",
        type=_parse_date,
        default=None,
        help="Freshness reference date (YYYY-MM-DD). Default: today in KST.",
    )
    ops_common_freshness.add_argument(
        "--max-run-age-hours",
        type=int,
        default=30,
        help="Maximum age in hours for the latest successful source sync run.",
    )
    ops_common_freshness.add_argument(
        "--daily-max-lag-days",
        type=int,
        default=2,
        help="Maximum latest-observation lag for FDR/KRX/PYKRX daily sources.",
    )
    ops_common_freshness.add_argument(
        "--macro-max-lag-days",
        type=int,
        default=45,
        help="Maximum latest-observation lag for FRED/ECOS macro sources.",
    )
    ops_common_freshness.add_argument(
        "--series",
        default=None,
        help="Optional comma-separated source series allowlist.",
    )
    ops_common_freshness.set_defaults(handler=_handle_ops_assert_common_freshness)

    # -- dart -----------------------------------------------------------------
    dart_parser = subparsers.add_parser("dart", help="OpenDART ingestion commands.")
    dart_sub = dart_parser.add_subparsers(dest="dart_command", required=True)

    dart_sync_corp = dart_sub.add_parser(
        "sync-corp",
        help="Download the OpenDART corp-code master and map it to active KRX tickers.",
    )
    dart_sync_corp.add_argument(
        "--force",
        action="store_true",
        help="Re-download even if a previous successful corp sync is recorded.",
    )
    dart_sync_corp.set_defaults(handler=_handle_dart_sync_corp)

    dart_sync_financials = dart_sub.add_parser(
        "sync-financials",
        help="Download OpenDART single-company full financial statements into raw storage.",
    )
    dart_sync_financials.add_argument(
        "--bsns-years",
        default=str(date.today().year - 1),
        help="Comma-separated business years (default: previous year).",
    )
    dart_sync_financials.add_argument(
        "--reprt-codes",
        default="11011",
        help="Comma-separated report codes (default: 11011 for annual report).",
    )
    dart_sync_financials.add_argument(
        "--fs-divs",
        default="CFS",
        help="Comma-separated fs_div values (default: CFS).",
    )
    dart_sync_financials.add_argument(
        "--tickers",
        default=None,
        help="Optional comma-separated ticker allowlist.",
    )
    dart_sync_financials.add_argument(
        "--rate-limit-seconds",
        type=float,
        default=0.2,
        help="Seconds between OpenDART requests (default: 0.2).",
    )
    dart_sync_financials.add_argument(
        "--force",
        action="store_true",
        help="Re-download even when raw rows already exist for a request key.",
    )
    dart_sync_financials.add_argument(
        "--incremental",
        action="store_true",
        default=False,
        help="Resolve business years/report codes from filing availability and recent no-data.",
    )
    dart_sync_financials.add_argument(
        "--lookback-years",
        type=int,
        default=1,
        help="Number of prior business years to include in --incremental mode.",
    )
    dart_sync_financials.add_argument(
        "--max-attempt-targets",
        type=int,
        default=10000,
        help="Maximum estimated OpenDART requests allowed in --incremental mode.",
    )
    dart_sync_financials.add_argument(
        "--negative-cache-ttl-days",
        type=int,
        default=3,
        help="Days to skip request keys that recently returned no-data.",
    )
    dart_sync_financials.set_defaults(handler=_handle_dart_sync_financials)

    dart_sync_share_info = dart_sub.add_parser(
        "sync-share-info",
        help="Download OpenDART stock count, dividend, and treasury-stock disclosures.",
    )
    dart_sync_share_info.add_argument(
        "--bsns-years",
        default=str(date.today().year - 1),
        help="Comma-separated business years (default: previous year).",
    )
    dart_sync_share_info.add_argument(
        "--reprt-codes",
        default="11011",
        help="Comma-separated report codes (default: 11011 for annual report).",
    )
    dart_sync_share_info.add_argument(
        "--tickers",
        default=None,
        help="Optional comma-separated ticker allowlist.",
    )
    dart_sync_share_info.add_argument(
        "--rate-limit-seconds",
        type=float,
        default=0.2,
        help="Seconds between OpenDART request groups (default: 0.2).",
    )
    dart_sync_share_info.add_argument(
        "--force",
        action="store_true",
        help="Re-download even when raw rows already exist for a request key.",
    )
    dart_sync_share_info.add_argument(
        "--incremental",
        action="store_true",
        default=False,
        help="Resolve business years/report codes from filing availability and recent no-data.",
    )
    dart_sync_share_info.add_argument(
        "--lookback-years",
        type=int,
        default=1,
        help="Number of prior business years to include in --incremental mode.",
    )
    dart_sync_share_info.add_argument(
        "--max-attempt-targets",
        type=int,
        default=10000,
        help="Maximum estimated OpenDART requests allowed in --incremental mode.",
    )
    dart_sync_share_info.add_argument(
        "--negative-cache-ttl-days",
        type=int,
        default=3,
        help="Days to skip request keys that recently returned no-data.",
    )
    dart_sync_share_info.set_defaults(handler=_handle_dart_sync_share_info)

    dart_sync_xbrl = dart_sub.add_parser(
        "sync-xbrl",
        help="Download and parse OpenDART XBRL ZIP filings into raw fact storage.",
    )
    dart_sync_xbrl.add_argument(
        "--bsns-years",
        default=str(date.today().year - 1),
        help="Comma-separated business years (default: previous year).",
    )
    dart_sync_xbrl.add_argument(
        "--reprt-codes",
        default="11011",
        help="Comma-separated report codes (default: 11011 for annual report).",
    )
    dart_sync_xbrl.add_argument(
        "--tickers",
        default=None,
        help="Optional comma-separated ticker allowlist.",
    )
    dart_sync_xbrl.add_argument(
        "--rate-limit-seconds",
        type=float,
        default=0.2,
        help="Seconds between OpenDART XBRL requests (default: 0.2).",
    )
    dart_sync_xbrl.add_argument(
        "--force",
        action="store_true",
        help="Re-parse even when an XBRL document is already stored for a filing.",
    )
    dart_sync_xbrl.add_argument(
        "--incremental",
        action="store_true",
        default=False,
        help="Resolve business years/report codes from filing availability and recent no-data.",
    )
    dart_sync_xbrl.add_argument(
        "--lookback-years",
        type=int,
        default=1,
        help="Number of prior business years to include in --incremental mode.",
    )
    dart_sync_xbrl.add_argument(
        "--max-attempt-targets",
        type=int,
        default=10000,
        help="Maximum estimated OpenDART requests allowed in --incremental mode.",
    )
    dart_sync_xbrl.add_argument(
        "--negative-cache-ttl-days",
        type=int,
        default=3,
        help="Days to skip request keys that recently returned no-data.",
    )
    dart_sync_xbrl.set_defaults(handler=_handle_dart_sync_xbrl)

    # -- metrics --------------------------------------------------------------
    metrics_parser = subparsers.add_parser("metrics", help="Canonical metric commands.")
    metrics_sub = metrics_parser.add_subparsers(dest="metrics_command", required=True)

    metrics_normalize = metrics_sub.add_parser(
        "normalize",
        help="Seed metric mapping rules and normalize canonical metric facts.",
    )
    metrics_normalize.add_argument(
        "--bsns-years",
        default=str(date.today().year - 1),
        help="Comma-separated business years (default: previous year).",
    )
    metrics_normalize.add_argument(
        "--reprt-codes",
        default="11011",
        help="Comma-separated report codes (default: 11011 for annual report).",
    )
    metrics_normalize.add_argument(
        "--tickers",
        default=None,
        help="Optional comma-separated ticker allowlist.",
    )
    metrics_normalize.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help=("Ticker batch size (env: SDC_METRICS_NORMALIZE_BATCH_SIZE, " "default 100)."),
    )
    metrics_normalize.add_argument(
        "--incremental",
        action="store_true",
        default=False,
        help="Normalize only recent business years instead of an explicit full range.",
    )
    metrics_normalize.add_argument(
        "--lookback-years",
        type=int,
        default=2,
        help="Number of prior business years to include in --incremental mode.",
    )
    metrics_normalize.set_defaults(handler=_handle_metrics_normalize)

    metrics_coverage = metrics_sub.add_parser(
        "coverage-report",
        help="Report canonical metric coverage for the selected periods.",
    )
    metrics_coverage.add_argument(
        "--bsns-years",
        default=str(date.today().year - 1),
        help="Comma-separated business years (default: previous year).",
    )
    metrics_coverage.add_argument(
        "--reprt-codes",
        default="11011",
        help="Comma-separated report codes (default: 11011 for annual report).",
    )
    metrics_coverage.add_argument(
        "--tickers",
        default=None,
        help="Optional comma-separated ticker allowlist.",
    )
    metrics_coverage.set_defaults(handler=_handle_metrics_coverage_report)

    # -- common ---------------------------------------------------------------
    common_parser = subparsers.add_parser(
        "common",
        help="Common market and macro feature commands.",
    )
    common_sub = common_parser.add_subparsers(dest="common_command", required=True)

    common_seed = common_sub.add_parser(
        "seed-catalog",
        help="Seed Phase 1 common feature source series and feature catalog rows.",
    )
    common_seed.add_argument(
        "--init-schema",
        action="store_true",
        default=False,
        help="Initialise/update the database schema before seeding.",
    )
    common_seed.set_defaults(handler=_handle_common_seed_catalog)

    common_sync = common_sub.add_parser(
        "sync",
        help="Sync common feature raw observations from configured providers.",
    )
    common_sync.add_argument(
        "--sources",
        type=_parse_common_sources,
        default=_parse_common_sources("pykrx,fdr"),
        help="Comma-separated source allowlist: pykrx,krx,fdr,ecos,fred (default: pykrx,fdr).",
    )
    common_sync.add_argument(
        "--series",
        default=None,
        help="Optional comma-separated common feature series_id allowlist.",
    )
    common_sync.add_argument(
        "--start",
        type=_parse_date,
        default=None,
        help="Start date (YYYY-MM-DD). Required unless --incremental is used.",
    )
    common_sync.add_argument(
        "--end",
        type=_parse_date,
        default=date.today(),
        help="End date (YYYY-MM-DD). Default: today.",
    )
    common_sync.add_argument(
        "--incremental",
        action="store_true",
        default=False,
        help="Resolve start from stored raw observation latest dates.",
    )
    common_sync.add_argument(
        "--lookback-days",
        type=int,
        default=0,
        help="Recent calendar-day window to rescan in --incremental mode.",
    )
    common_sync.add_argument(
        "--max-auto-range-days",
        type=int,
        default=90,
        help="Maximum inclusive day range allowed for --incremental without override.",
    )
    common_sync.add_argument(
        "--allow-large-range",
        action="store_true",
        default=False,
        help="Allow resolved common sync ranges larger than the safety guard.",
    )
    common_sync.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Re-fetch even when existing raw observations are present.",
    )
    common_sync.add_argument(
        "--rate-limit-seconds",
        type=float,
        default=0.0,
        help="Seconds between provider series requests (default: 0.0).",
    )
    common_sync.add_argument(
        "--include-inactive",
        action="store_true",
        default=False,
        help="Allow explicitly selected inactive source series for smoke verification.",
    )
    common_sync.add_argument(
        "--init-schema",
        action="store_true",
        default=False,
        help="Initialise/update the database schema before syncing.",
    )
    common_sync.set_defaults(handler=_handle_common_sync)

    common_build_daily = common_sub.add_parser(
        "build-daily",
        help="Build KRX-date-aligned common feature daily facts.",
    )
    common_build_daily.add_argument(
        "--feature-codes",
        default=None,
        help="Optional comma-separated common feature_code allowlist.",
    )
    common_build_daily.add_argument(
        "--start",
        type=_parse_date,
        default=None,
        help="Start date (YYYY-MM-DD). Required unless --incremental is used.",
    )
    common_build_daily.add_argument(
        "--end",
        type=_parse_date,
        default=date.today(),
        help="End date (YYYY-MM-DD). Default: today.",
    )
    common_build_daily.add_argument(
        "--incremental",
        action="store_true",
        default=False,
        help="Resolve start from stored daily fact latest dates.",
    )
    common_build_daily.add_argument(
        "--lookback-days",
        type=int,
        default=0,
        help="Recent calendar-day window to rebuild in --incremental mode.",
    )
    common_build_daily.add_argument(
        "--max-auto-range-days",
        type=int,
        default=180,
        help="Maximum inclusive day range allowed for --incremental without override.",
    )
    common_build_daily.add_argument(
        "--allow-large-range",
        action="store_true",
        default=False,
        help="Allow resolved common build ranges larger than the safety guard.",
    )
    common_build_daily.add_argument(
        "--init-schema",
        action="store_true",
        default=False,
        help="Initialise/update the database schema before building daily facts.",
    )
    common_build_daily.add_argument(
        "--include-inactive",
        action="store_true",
        default=False,
        help="Allow explicitly selected inactive feature codes for verification.",
    )
    common_build_daily.set_defaults(handler=_handle_common_build_daily)

    common_coverage = common_sub.add_parser(
        "coverage-report",
        help="Report common feature daily fact coverage and PIT violations.",
    )
    common_coverage.add_argument(
        "--feature-codes",
        default=None,
        help="Optional comma-separated common feature_code allowlist.",
    )
    common_coverage.add_argument(
        "--start",
        type=_parse_date,
        required=True,
        help="Start date (YYYY-MM-DD).",
    )
    common_coverage.add_argument(
        "--end",
        type=_parse_date,
        required=True,
        help="End date (YYYY-MM-DD).",
    )
    common_coverage.add_argument(
        "--include-inactive",
        action="store_true",
        default=False,
        help="Allow explicitly selected inactive feature codes in the report.",
    )
    common_coverage.set_defaults(handler=_handle_common_coverage_report)

    common_readiness = common_sub.add_parser(
        "readiness-report",
        help="Report common feature active-transition readiness.",
    )
    common_readiness.add_argument(
        "--feature-codes",
        default=None,
        help="Optional comma-separated common feature_code allowlist.",
    )
    common_readiness.add_argument(
        "--start",
        type=_parse_date,
        required=True,
        help="Start date (YYYY-MM-DD).",
    )
    common_readiness.add_argument(
        "--end",
        type=_parse_date,
        required=True,
        help="End date (YYYY-MM-DD).",
    )
    common_readiness.add_argument(
        "--required-coverage-ratio",
        type=_parse_coverage_ratio,
        default=Decimal("1.0000"),
        help="Required non-null coverage ratio for readiness (default: 1.0000).",
    )
    common_readiness.add_argument(
        "--include-inactive",
        action="store_true",
        default=False,
        help="Allow explicitly selected inactive feature codes in the report.",
    )
    common_readiness.add_argument(
        "--fail-on-not-ready",
        action="store_true",
        default=False,
        help="Exit non-zero when any selected feature is not ready or report errors exist.",
    )
    common_readiness.set_defaults(handler=_handle_common_readiness_report)

    # -- flows ----------------------------------------------------------------
    flows_parser = subparsers.add_parser("flows", help="Security flow ingestion commands.")
    flows_sub = flows_parser.add_subparsers(dest="flows_command", required=True)

    flows_sync = flows_sub.add_parser(
        "sync",
        help="Sync daily investor/foreign/shorting raw flow metrics.",
    )
    flows_sync.add_argument(
        "--start",
        type=_parse_date,
        default=None,
        help="Start date (YYYY-MM-DD). Default: yesterday.",
    )
    flows_sync.add_argument(
        "--end",
        type=_parse_date,
        default=None,
        help="End date (YYYY-MM-DD). Default: yesterday.",
    )
    flows_sync.add_argument(
        "--tickers",
        default=None,
        help="Optional comma-separated ticker allowlist.",
    )
    flows_sync.add_argument(
        "--rate-limit-seconds",
        type=float,
        default=None,
        help=(
            "Seconds between higher-level flow requests "
            "(default: env KRX_LOGICAL_RATE_LIMIT_SECONDS or 8.0)."
        ),
    )
    flows_sync.add_argument(
        "--timeout-seconds",
        type=_parse_positive_seconds,
        default=None,
        help=(
            "KRX MDC HTTP timeout in seconds "
            "(default: env KRX_MDC_TIMEOUT_SECONDS or 20; accepts 150 or 150s)."
        ),
    )
    flows_sync.add_argument(
        "--use-price-range",
        action="store_true",
        help=(
            "Use the stored daily OHLCV min/max trade_date as the flow sync range. "
            "Optional --start/--end further clamp that range."
        ),
    )
    flows_sync.add_argument(
        "--incremental",
        action="store_true",
        default=False,
        help=(
            "Resolve the flow sync range from stored daily OHLCV and KRX flow "
            "latest dates. Intended for daily catch-up runs."
        ),
    )
    flows_sync.add_argument(
        "--lookback-days",
        type=int,
        default=14,
        help=(
            "Recent calendar-day window to always rescan in --incremental mode "
            "(default: 14)."
        ),
    )
    flows_sync.add_argument(
        "--max-auto-range-days",
        type=int,
        default=30,
        help="Maximum inclusive day range allowed for --incremental without override.",
    )
    flows_sync.add_argument(
        "--max-price-range-days",
        type=int,
        default=90,
        help="Maximum inclusive day range allowed for --use-price-range without override.",
    )
    flows_sync.add_argument(
        "--allow-large-range",
        action="store_true",
        default=False,
        help="Allow resolved flow ranges larger than the configured safety guard.",
    )
    flows_sync.add_argument(
        "--exclude-groups",
        default=None,
        help=(
            "Comma-separated flow metric groups to exclude from range resolution and "
            "collection (foreign_holding,investor,shorting)."
        ),
    )
    flows_sync.add_argument(
        "--http-min-delay-seconds",
        type=float,
        default=None,
        help=(
            "Minimum seconds between actual KRX HTTP calls "
            "(default: env KRX_MIN_DELAY_SECONDS or 1.5)."
        ),
    )
    flows_sync.add_argument(
        "--http-max-delay-seconds",
        type=float,
        default=None,
        help=(
            "Maximum seconds between actual KRX HTTP calls "
            "(default: env KRX_MAX_DELAY_SECONDS or 4.0)."
        ),
    )
    flows_sync.add_argument(
        "--long-rest-every",
        type=int,
        default=None,
        help=(
            "Take a long random rest after this many KRX HTTP calls "
            "(default: env KRX_LONG_REST_EVERY or 15)."
        ),
    )
    flows_sync.add_argument(
        "--long-rest-min-seconds",
        type=float,
        default=None,
        help="Minimum seconds for a long KRX rest (default: env KRX_LONG_REST_MIN_SECONDS or 30).",
    )
    flows_sync.add_argument(
        "--long-rest-max-seconds",
        type=float,
        default=None,
        help="Maximum seconds for a long KRX rest (default: env KRX_LONG_REST_MAX_SECONDS or 90).",
    )
    flows_sync.add_argument(
        "--auth-cooldown-seconds",
        type=float,
        default=None,
        help=(
            "Seconds to wait after a successful KRX login "
            "(default: env KRX_AUTH_COOLDOWN_SECONDS or 10)."
        ),
    )
    flows_sync.add_argument(
        "--error-backoff-min-seconds",
        type=float,
        default=None,
        help=(
            "Minimum seconds to wait after a KRX error "
            "(default: env KRX_ERROR_BACKOFF_MIN_SECONDS or 45)."
        ),
    )
    flows_sync.add_argument(
        "--error-backoff-max-seconds",
        type=float,
        default=None,
        help=(
            "Maximum seconds to wait after a KRX error "
            "(default: env KRX_ERROR_BACKOFF_MAX_SECONDS or 180)."
        ),
    )
    flows_sync.add_argument(
        "--progress-log-interval-seconds",
        type=float,
        default=30.0,
        help="Emit flow sync progress at least this often in seconds (0 disables time-based logs).",
    )
    flows_sync.add_argument(
        "--progress-log-every-items",
        type=int,
        default=100,
        help="Emit flow sync progress every N handled items (0 disables count-based logs).",
    )
    flows_sync.add_argument(
        "--ordered-requests",
        action="store_true",
        help="Disable randomized request order and preserve deterministic traversal.",
    )
    flows_sync.set_defaults(handler=_handle_flows_sync)

    # -- operating ------------------------------------------------------------
    operating_parser = subparsers.add_parser(
        "operating", help="Sector-specific operating KPI commands."
    )
    operating_sub = operating_parser.add_subparsers(dest="operating_command", required=True)

    operating_process = operating_sub.add_parser(
        "process-document",
        help="Persist one source document and run a sector-specific KPI extractor.",
    )
    operating_process.add_argument("--ticker", required=True, help="6-digit ticker code.")
    operating_process.add_argument(
        "--market",
        required=True,
        choices=["KOSPI", "KOSDAQ", "kospi", "kosdaq"],
        help="Market segment.",
    )
    operating_process.add_argument(
        "--sector-key",
        required=True,
        help="Sector extractor key, e.g. shipbuilding_defense.",
    )
    operating_process.add_argument(
        "--document-type",
        default="manual_text",
        help="Document type label for provenance.",
    )
    operating_process.add_argument(
        "--title",
        required=True,
        help="Document title for provenance.",
    )
    operating_process.add_argument(
        "--document-date",
        type=_parse_date,
        default=None,
        help="Document date (YYYY-MM-DD).",
    )
    operating_process.add_argument(
        "--period-end",
        type=_parse_date,
        required=True,
        help="Metric period end date (YYYY-MM-DD).",
    )
    operating_process.add_argument(
        "--source-system",
        default="LOCAL",
        help="Document source system label.",
    )
    operating_process.add_argument(
        "--source-url",
        default="",
        help="Optional source URL for provenance.",
    )
    operating_process.add_argument(
        "--language",
        default="ko",
        help="Document language code.",
    )
    operating_process.add_argument(
        "--text-file",
        required=True,
        help="UTF-8 text file containing extracted document text.",
    )
    operating_process.set_defaults(handler=_handle_operating_process_document)

    # -- universe -------------------------------------------------------------
    universe_parser = subparsers.add_parser("universe", help="Stock universe management.")
    universe_sub = universe_parser.add_subparsers(dest="universe_command", required=True)

    universe_sync = universe_sub.add_parser("sync", help="Sync listed stock universe.")
    universe_sync.add_argument(
        "--source",
        choices=["fdr", "pykrx"],
        default=None,
        help="Data source (default: from config UNIVERSE_SOURCE_DEFAULT).",
    )
    universe_sync.add_argument(
        "--markets",
        default="kospi,kosdaq",
        help="Comma-separated market list (default: kospi,kosdaq).",
    )
    universe_sync.add_argument(
        "--as-of",
        type=_parse_date,
        default=None,
        help="Reference date (YYYY-MM-DD). Default: today (KST).",
    )
    universe_sync.add_argument(
        "--full-refresh",
        action="store_true",
        default=False,
        help="Replace all stock_master rows instead of incremental diff.",
    )
    universe_sync.set_defaults(handler=_handle_universe_sync)

    # -- prices ---------------------------------------------------------------
    prices_parser = subparsers.add_parser("prices", help="Price data commands.")
    prices_sub = prices_parser.add_subparsers(dest="prices_command", required=True)

    prices_backfill = prices_sub.add_parser("backfill", help="Backfill daily OHLCV data.")
    prices_backfill.add_argument(
        "--market",
        choices=["kospi", "kosdaq", "all"],
        default="all",
        help="Market filter (default: all).",
    )
    prices_backfill.add_argument(
        "--tickers",
        default=None,
        help="Comma-separated ticker codes (default: all active tickers).",
    )
    prices_backfill.add_argument(
        "--start",
        type=_parse_date,
        default=None,
        help="Start date (YYYY-MM-DD). Default: 2000-01-01.",
    )
    prices_backfill.add_argument(
        "--end",
        type=_parse_date,
        default=None,
        help="End date (YYYY-MM-DD). Default: today (KST).",
    )
    prices_backfill.add_argument(
        "--rate-limit-seconds",
        type=float,
        default=None,
        help="Seconds between API calls (default: from config).",
    )
    prices_backfill.add_argument(
        "--long-rest-interval",
        type=int,
        default=None,
        help=("Number of API requests between long rests " "(0 disables; default: from config)."),
    )
    prices_backfill.add_argument(
        "--long-rest-seconds",
        type=float,
        default=None,
        help="Duration of each long rest in seconds (default: from config).",
    )
    prices_backfill.add_argument(
        "--incremental",
        action="store_true",
        default=False,
        help=(
            "Skip per-day gap detection and fetch only days after each "
            "ticker's MAX(trade_date). Intended for fast daily catch-up runs."
        ),
    )
    prices_backfill.add_argument(
        "--lookback-days",
        type=int,
        default=0,
        help="Recent calendar-day window to rescan in --incremental mode (default: 0).",
    )
    prices_backfill.add_argument(
        "--max-auto-range-days",
        type=int,
        default=10,
        help="Maximum inclusive day range allowed for --incremental without override.",
    )
    prices_backfill.add_argument(
        "--new-ticker-start",
        type=_parse_date,
        default=None,
        help="Explicit start date for tickers with no stored price baseline.",
    )
    prices_backfill.add_argument(
        "--allow-new-ticker-backfill",
        action="store_true",
        default=False,
        help="Allow baseline-missing tickers to use --start or the default early start.",
    )
    prices_backfill.add_argument(
        "--allow-large-range",
        action="store_true",
        default=False,
        help="Allow resolved incremental price ranges larger than the safety guard.",
    )
    prices_backfill.set_defaults(handler=_handle_prices_backfill)

    # -- validate -------------------------------------------------------------
    validate_parser = subparsers.add_parser("validate", help="Run data-quality validations.")
    validate_parser.add_argument(
        "--date",
        type=_parse_date,
        default=None,
        help="Target date (YYYY-MM-DD). Default: today (KST).",
    )
    validate_parser.add_argument(
        "--market",
        choices=["kospi", "kosdaq", "all"],
        default="all",
        help="Market filter (default: all).",
    )
    validate_parser.set_defaults(handler=_handle_validate)

    return parser


# ---------------------------------------------------------------------------
# Main entry
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> None:
    """Parse CLI arguments, configure logging, and dispatch to handler.

    Args:
        argv: Argument list (defaults to ``sys.argv[1:]``).
    """
    settings = get_settings()
    setup_logging(
        level=settings.log_level,
        fmt=settings.log_format.value,
        log_dir=settings.log_dir,
    )

    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        args.handler(args)
    except NotImplementedError as exc:
        logger.warning("Command not yet implemented: %s", exc)
        print(f"⚠  Not implemented yet: {exc}", file=sys.stderr)
        sys.exit(1)


def dart_main(argv: list[str] | None = None) -> None:
    """Entrypoint for the ``dart`` console script."""
    main(["dart", *(sys.argv[1:] if argv is None else argv)])


if __name__ == "__main__":
    main()
