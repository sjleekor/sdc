"""Use-case: persist one source document and extract operating KPI facts."""

from __future__ import annotations

import hashlib
import logging

from krx_collector.domain.enums import RunStatus, RunType
from krx_collector.domain.models import (
    IngestionRun,
    OperatingMetricSyncResult,
    OperatingSourceDocument,
)
from krx_collector.ports.storage import Storage
from krx_collector.service.operating_registry import OperatingMetricExtractorRegistry
from krx_collector.util.pipeline import build_run_counts, complete_run, fail_run
from krx_collector.util.time import now_kst

logger = logging.getLogger(__name__)


def build_operating_document_key(
    ticker: str,
    sector_key: str,
    document_type: str,
    title: str,
    period_end: str,
    content_text: str,
) -> str:
    """Build a stable document key from core identity fields."""
    digest = hashlib.sha256(
        "|".join([ticker, sector_key, document_type, title, period_end, content_text]).encode(
            "utf-8"
        )
    ).hexdigest()
    return digest


def process_operating_document(
    storage: Storage,
    registry: OperatingMetricExtractorRegistry,
    document: OperatingSourceDocument,
) -> OperatingMetricSyncResult:
    """Persist one source document and extract sector-specific KPI facts."""
    run = IngestionRun(
        run_type=RunType.OPERATING_METRIC_SYNC,
        started_at=now_kst(),
        status=RunStatus.RUNNING,
        params={
            "ticker": document.ticker,
            "sector_key": document.sector_key,
            "document_type": document.document_type,
            "period_end": document.period_end.isoformat() if document.period_end else None,
            "document_key": document.document_key,
        },
    )
    storage.record_run(run)

    result = OperatingMetricSyncResult()
    try:
        extractor = registry.get(document.sector_key)
        if extractor is None:
            raise RuntimeError(
                f"No operating extractor registered for sector_key={document.sector_key!r}."
            )

        result.document_upsert = storage.upsert_operating_source_documents([document])
        result.documents_processed = result.document_upsert.updated

        extraction = extractor.extract(document)
        if extraction.error:
            raise RuntimeError(extraction.error)

        if extraction.facts:
            result.fact_upsert = storage.upsert_operating_metric_facts(extraction.facts)
            result.facts_upserted = result.fact_upsert.updated
            result.extracted_metric_codes = sorted({fact.metric_code for fact in extraction.facts})

        complete_run(
            storage,
            run,
            counts=build_run_counts(
                documents_processed=result.documents_processed,
                facts_upserted=result.facts_upserted,
                metric_code_count=len(result.extracted_metric_codes),
            ),
            errors=result.errors,
            partial_subject="operating metric extractions",
        )
        return result
    except Exception as exc:
        logger.exception("Operating KPI processing failed")
        fail_run(storage, run, exc)
        result.errors["pipeline"] = str(exc)
        return result
