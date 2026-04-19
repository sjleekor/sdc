"""Use-case: Sync the OpenDART corporation-code master."""

from __future__ import annotations

import logging

from krx_collector.domain.enums import RunStatus, RunType, Source
from krx_collector.domain.models import DartCorp, DartCorpSyncResult, IngestionRun
from krx_collector.ports.corp_codes import CorpCodeProvider
from krx_collector.ports.storage import Storage
from krx_collector.util.time import now_kst

logger = logging.getLogger(__name__)


def sync_dart_corp_master(
    provider: CorpCodeProvider,
    storage: Storage,
) -> DartCorpSyncResult:
    """Synchronise OpenDART corp codes into local storage."""
    run = IngestionRun(
        run_type=RunType.DART_CORP_SYNC,
        started_at=now_kst(),
        status=RunStatus.RUNNING,
        params={},
    )
    storage.record_run(run)

    try:
        fetch_result = provider.fetch_corp_codes()
        if fetch_result.error:
            raise RuntimeError(fetch_result.error)

        active_stocks = storage.get_active_stocks()
        active_stocks_by_ticker = {stock.ticker: stock for stock in active_stocks}

        matched_active_tickers: set[str] = set()
        unmatched_dart_tickers: set[str] = set()
        enriched_records: list[DartCorp] = []

        for record in fetch_result.records:
            matched_stock = active_stocks_by_ticker.get(record.ticker or "")
            if matched_stock is None:
                if record.ticker:
                    unmatched_dart_tickers.add(record.ticker)
                enriched_records.append(record)
                continue

            matched_active_tickers.add(matched_stock.ticker)
            enriched_records.append(
                DartCorp(
                    corp_code=record.corp_code,
                    corp_name=record.corp_name,
                    ticker=matched_stock.ticker,
                    market=matched_stock.market,
                    stock_name=matched_stock.name,
                    modify_date=record.modify_date,
                    is_active=True,
                    source=Source.OPENDART,
                    fetched_at=record.fetched_at,
                )
            )

        unmatched_active_tickers = sorted(set(active_stocks_by_ticker) - matched_active_tickers)
        upsert_result = storage.upsert_dart_corp_master(enriched_records)

        run.ended_at = now_kst()
        run.status = RunStatus.SUCCESS
        run.counts = {
            "fetched_records": len(fetch_result.records),
            "matched_active_tickers": len(matched_active_tickers),
            "unmatched_active_tickers": len(unmatched_active_tickers),
            "unmatched_dart_tickers": len(unmatched_dart_tickers),
            "upsert_updated": upsert_result.updated,
            "upsert_errors": upsert_result.errors,
        }
        storage.record_run(run)

        return DartCorpSyncResult(
            upsert=upsert_result,
            total_records=len(fetch_result.records),
            matched_active_tickers=len(matched_active_tickers),
            unmatched_active_tickers=unmatched_active_tickers,
            unmatched_dart_tickers=sorted(unmatched_dart_tickers),
        )
    except Exception as exc:
        logger.exception("OpenDART corp code sync failed")
        run.ended_at = now_kst()
        run.status = RunStatus.FAILED
        run.error_summary = str(exc)
        storage.record_run(run)
        return DartCorpSyncResult(error=str(exc))
