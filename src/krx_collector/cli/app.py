"""CLI entrypoint — argparse wiring for ``krx-collector``.

Subcommands::

    krx-collector db init
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
from datetime import date

from krx_collector.infra.config.settings import get_settings
from krx_collector.infra.logging.setup import setup_logging

logger = logging.getLogger(__name__)


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

    print(
        f"→ prices backfill: market={args.market}, tickers={args.tickers}, "
        f"start={args.start}, end={args.end}, "
        f"rate_limit={args.rate_limit_seconds}, incremental={args.incremental}"
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

    rate_limit = args.rate_limit_seconds
    if rate_limit is None:
        rate_limit = settings.rate_limit_seconds

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


if __name__ == "__main__":
    main()
