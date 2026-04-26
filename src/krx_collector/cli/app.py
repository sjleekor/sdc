"""CLI entrypoint — argparse wiring for ``krx-collector``.

Subcommands::

    krx-collector db init
    krx-collector db sync-remote [--db-info-path ...] [--ssh-host ...] [--full-refresh]
                                  [--all-tables]
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


def _handle_db_sync_remote(args: argparse.Namespace) -> None:
    """Handle ``krx-collector db sync-remote``."""
    settings = get_settings()

    db_info_path = args.db_info_path or str(settings.remote_db_info_path)
    batch_size = args.batch_size or settings.remote_db_batch_size
    remote_host_override = args.remote_host or settings.remote_db_host_override
    ssh_host = args.ssh_host or settings.remote_db_ssh_host
    ssh_local_port = args.ssh_local_port or settings.remote_db_ssh_local_port

    print(
        f"→ db sync-remote: db_info_path={db_info_path}, "
        f"batch_size={batch_size}, full_refresh={args.full_refresh}, "
        f"all_tables={args.all_tables}, "
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
    result = sync_dart_financial_statements(
        provider=provider,
        storage=storage,
        bsns_years=bsns_years,
        reprt_codes=reprt_codes,
        fs_divs=fs_divs,
        tickers=tickers,
        rate_limit_seconds=args.rate_limit_seconds,
        force=args.force,
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
    result = sync_dart_share_info(
        share_count_provider=provider,
        shareholder_return_provider=provider,
        storage=storage,
        bsns_years=bsns_years,
        reprt_codes=reprt_codes,
        tickers=tickers,
        rate_limit_seconds=args.rate_limit_seconds,
        force=args.force,
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
    result = sync_dart_xbrl(
        provider=provider,
        storage=storage,
        bsns_years=bsns_years,
        reprt_codes=reprt_codes,
        tickers=tickers,
        rate_limit_seconds=args.rate_limit_seconds,
        force=args.force,
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

    print(f"→ metrics normalize: years={bsns_years}, reprt_codes={reprt_codes}, tickers={tickers}")

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


def _handle_flows_sync(args: argparse.Namespace) -> None:
    """Handle ``krx-collector flows sync``."""
    settings = get_settings()
    tickers = [value.strip() for value in args.tickers.split(",")] if args.tickers else None
    default_flow_date = date.today() - timedelta(days=1)
    start = args.start or default_flow_date
    end = args.end or default_flow_date

    print(
        f"→ flows sync: start={start}, end={end}, "
        f"tickers={tickers}, rate_limit={args.rate_limit_seconds}, "
        f"progress_interval={args.progress_log_interval_seconds}, "
        f"progress_every={args.progress_log_every_items}"
    )

    from krx_collector.adapters.flows_krx.provider import KrxDirectFlowProvider
    from krx_collector.infra.db_postgres.repositories import PostgresStorage
    from krx_collector.service.sync_krx_flows import sync_krx_security_flows

    provider = KrxDirectFlowProvider(login_id=settings.krx_id, login_pw=settings.krx_pw)
    storage = PostgresStorage(settings.db_dsn)
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
        print(f"   - Price range resolved: start={start}, end={end}")

    result = sync_krx_security_flows(
        provider=provider,
        storage=storage,
        start=start,
        end=end,
        tickers=tickers,
        rate_limit_seconds=args.rate_limit_seconds,
        progress_log_interval_seconds=args.progress_log_interval_seconds,
        progress_log_every_items=args.progress_log_every_items,
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
        f"incremental={args.incremental}"
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
    )

    if result.errors:
        print(f"⚠ Backfill completed with {len(result.errors)} errors.", file=sys.stderr)
    else:
        print("✅ Backfill completed successfully.")

    print(f"   - Tickers processed: {result.tickers_processed}")
    print(f"   - Bars upserted: {result.bars_upserted}")


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
    db_sync_remote.add_argument(
        "--all-tables",
        action="store_true",
        default=False,
        help=(
            "Sync the managed pipeline data tables. Requires --full-refresh. "
            "Drops only those local tables and re-applies sql/postgres_ddl.sql "
            "before copying so stale local schemas are replaced."
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
        default=0.2,
        help="Seconds between KRX MDC requests (default: 0.2).",
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
