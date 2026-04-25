"""Use-case: Sync KRX / pykrx security-flow raw rows."""

from __future__ import annotations

import logging
from datetime import date

from krx_collector.domain.enums import ListingStatus, RunStatus, RunType, Source
from krx_collector.domain.models import IngestionRun, KrxFlowSyncResult, Stock
from krx_collector.infra.calendar.trading_days import get_trading_days
from krx_collector.ports.flows import FlowProvider
from krx_collector.ports.storage import Storage
from krx_collector.util.pipeline import (
    build_run_counts,
    call_with_retry,
    complete_run,
    fail_run,
    sleep_with_jitter,
)
from krx_collector.util.time import now_kst

logger = logging.getLogger(__name__)

FOREIGN_HOLDING_METRIC = "foreign_holding_shares"
INVESTOR_METRICS = [
    "institution_net_buy_volume",
    "individual_net_buy_volume",
    "foreign_net_buy_volume",
]
SHORTING_METRICS = [
    "short_selling_volume",
    "short_selling_value",
    "short_selling_balance_quantity",
]


def _filter_targets(stocks: list[Stock], tickers: list[str] | None) -> list[Stock]:
    if tickers is None:
        return stocks
    ticker_filter = set(tickers)
    return [stock for stock in stocks if stock.ticker in ticker_filter]


def _load_targets(storage: Storage, tickers: list[str] | None) -> list[Stock]:
    stocks = _filter_targets(storage.get_active_stocks(), tickers)
    if stocks:
        return stocks

    dart_rows = storage.get_dart_corp_master(active_only=True, tickers=tickers)
    return [
        Stock(
            ticker=row.ticker or "",
            market=row.market,
            name=row.stock_name or row.corp_name,
            status=ListingStatus.ACTIVE,
            last_seen_date=row.modify_date or date.today(),
            source=Source.OPENDART,
        )
        for row in dart_rows
        if row.ticker and row.market is not None
    ]


def sync_krx_security_flows(
    provider: FlowProvider,
    storage: Storage,
    start: date,
    end: date,
    tickers: list[str] | None = None,
    rate_limit_seconds: float = 0.2,
) -> KrxFlowSyncResult:
    """Synchronise daily investor/shorting/ownership raw metrics."""
    run = IngestionRun(
        run_type=RunType.KRX_FLOW_SYNC,
        started_at=now_kst(),
        status=RunStatus.RUNNING,
        params={
            "start": start.isoformat(),
            "end": end.isoformat(),
            "tickers": tickers,
            "rate_limit_seconds": rate_limit_seconds,
        },
    )
    storage.record_run(run)

    result = KrxFlowSyncResult(pending_metrics=provider.unsupported_metric_codes())

    try:
        targets = _load_targets(storage, tickers)
        if not targets:
            raise RuntimeError("No active stocks found for the requested flow sync.")

        result.targets_processed = len(targets)
        trading_days = get_trading_days(start, end)
        if not trading_days:
            raise RuntimeError("No trading days found in the requested range.")

        stocks_by_market: dict[str, list[Stock]] = {}
        for stock in targets:
            stocks_by_market.setdefault(stock.market.value, []).append(stock)

        target_tickers = [stock.ticker for stock in targets]
        foreign_ticker_counts = storage.count_krx_security_flow_daily_market_tickers(
            start=start,
            end=end,
            tickers=target_tickers,
            metric_code=FOREIGN_HOLDING_METRIC,
            source=Source.PYKRX,
        )
        investor_metric_counts = storage.count_krx_security_flow_ticker_metric_dates(
            start=start,
            end=end,
            tickers=target_tickers,
            metric_codes=INVESTOR_METRICS,
            source=Source.PYKRX,
        )
        shorting_metric_counts = storage.count_krx_security_flow_ticker_metric_dates(
            start=start,
            end=end,
            tickers=target_tickers,
            metric_codes=SHORTING_METRICS,
            source=Source.PYKRX,
        )
        investor_expected_count = len(trading_days) * len(INVESTOR_METRICS)
        shorting_expected_count = len(trading_days) * len(SHORTING_METRICS)
        completed_investor_tickers = {
            ticker
            for ticker, count in investor_metric_counts.items()
            if count >= investor_expected_count
        }
        completed_shorting_tickers = {
            ticker
            for ticker, count in shorting_metric_counts.items()
            if count >= shorting_expected_count
        }

        for trade_date in trading_days:
            for market_stocks in stocks_by_market.values():
                market = market_stocks[0].market
                market_tickers = [stock.ticker for stock in market_stocks]
                if foreign_ticker_counts.get((trade_date, market.value), 0) >= len(market_tickers):
                    logger.debug(
                        "Skipping existing foreign holding flow request %s:%s",
                        trade_date.isoformat(),
                        market.value,
                    )
                    result.requests_skipped += 1
                    continue

                result.requests_attempted += 1
                request_key = f"foreign:{trade_date.isoformat()}:{market.value}"
                foreign_result = call_with_retry(
                    lambda: provider.fetch_foreign_holding_shares(
                        trade_date=trade_date,
                        market=market,
                        tickers=market_tickers,
                    ),
                    request_label=request_key,
                    logger_instance=logger,
                )
                if foreign_result.error:
                    logger.warning(
                        "Foreign holding sync failed for %s: %s", request_key, foreign_result.error
                    )
                    result.errors[request_key] = foreign_result.error
                elif foreign_result.no_data:
                    result.no_data_requests += 1
                elif foreign_result.records:
                    upsert = storage.upsert_krx_security_flow_raw(foreign_result.records)
                    result.upsert.updated += upsert.updated
                    result.upsert.errors += upsert.errors
                    result.rows_upserted += upsert.updated
                sleep_with_jitter(rate_limit_seconds)

        for stock in targets:
            for fetch_kind, fetch_fn in [
                ("investor", provider.fetch_investor_net_volume),
                ("shorting", provider.fetch_shorting_metrics),
            ]:
                if fetch_kind == "investor" and stock.ticker in completed_investor_tickers:
                    logger.debug("Skipping existing investor flow request %s", stock.ticker)
                    result.requests_skipped += 1
                    continue
                if fetch_kind == "shorting" and stock.ticker in completed_shorting_tickers:
                    logger.debug("Skipping existing shorting flow request %s", stock.ticker)
                    result.requests_skipped += 1
                    continue

                result.requests_attempted += 1
                request_key = f"{fetch_kind}:{stock.ticker}:{start.isoformat()}:{end.isoformat()}"
                fetch_result = call_with_retry(
                    lambda: fetch_fn(stock.ticker, stock.market, start, end),
                    request_label=request_key,
                    logger_instance=logger,
                )
                if fetch_result.error:
                    logger.warning("Flow sync failed for %s: %s", request_key, fetch_result.error)
                    result.errors[request_key] = fetch_result.error
                elif fetch_result.no_data:
                    result.no_data_requests += 1
                elif fetch_result.records:
                    upsert = storage.upsert_krx_security_flow_raw(fetch_result.records)
                    result.upsert.updated += upsert.updated
                    result.upsert.errors += upsert.errors
                    result.rows_upserted += upsert.updated
                sleep_with_jitter(rate_limit_seconds)

        complete_run(
            storage,
            run,
            counts=build_run_counts(
                targets_processed=result.targets_processed,
                requests_attempted=result.requests_attempted,
                requests_skipped=result.requests_skipped,
                rows_upserted=result.rows_upserted,
                no_data_requests=result.no_data_requests,
                pending_metric_count=len(result.pending_metrics),
            ),
            errors=result.errors,
            partial_subject="flow sync requests",
        )
        return result
    except Exception as exc:
        logger.exception("KRX flow sync failed")
        fail_run(storage, run, exc)
        result.errors["pipeline"] = str(exc)
        return result
