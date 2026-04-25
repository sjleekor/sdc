from datetime import date
from decimal import Decimal
from pathlib import Path

from krx_collector.adapters.operating_extractors.shipbuilding_defense_order import (
    ShipbuildingDefenseOrderExtractor,
    parse_korean_amount_to_won,
)
from krx_collector.domain.enums import Market, RunStatus, RunType
from krx_collector.domain.models import (
    IngestionRun,
    OperatingMetricFact,
    OperatingSourceDocument,
    UpsertResult,
)
from krx_collector.service.default_operating_registry import build_default_operating_registry
from krx_collector.service.process_operating_document import (
    build_operating_document_key,
    process_operating_document,
)
from krx_collector.util.time import now_kst

FIXTURE_PATH = (
    Path(__file__).resolve().parent.parent
    / "fixtures"
    / "operating"
    / "shipbuilding_defense_sample.txt"
)


def _sample_document() -> OperatingSourceDocument:
    content_text = FIXTURE_PATH.read_text(encoding="utf-8")
    return OperatingSourceDocument(
        document_key=build_operating_document_key(
            ticker="009540",
            sector_key="shipbuilding_defense",
            document_type="manual_text",
            title="조선 방산 수주 샘플",
            period_end="2025-12-31",
            content_text=content_text,
        ),
        ticker="009540",
        market=Market.KOSPI,
        sector_key="shipbuilding_defense",
        document_type="manual_text",
        title="조선 방산 수주 샘플",
        document_date=date(2026, 4, 19),
        period_end=date(2025, 12, 31),
        source_system="LOCAL",
        source_url="",
        language="ko",
        content_text=content_text,
        fetched_at=now_kst(),
        raw_payload={},
    )


def test_parse_korean_amount_to_won() -> None:
    assert parse_korean_amount_to_won("3조 2,500억원") == Decimal("3250000000000")
    assert parse_korean_amount_to_won("7,200억원") == Decimal("720000000000")
    assert parse_korean_amount_to_won("123백만원") == Decimal("123000000")


def test_shipbuilding_defense_order_extractor_extracts_metrics() -> None:
    extractor = ShipbuildingDefenseOrderExtractor()
    result = extractor.extract(_sample_document())

    assert result.error is None
    facts = {fact.metric_code: fact for fact in result.facts}
    assert facts["order_intake_amount"].value_numeric == Decimal("3250000000000")
    assert facts["order_backlog_amount"].value_numeric == Decimal("24130000000000")


class MockOperatingStorage:
    def __init__(self) -> None:
        self.runs: list[IngestionRun] = []
        self.documents: list[OperatingSourceDocument] = []
        self.facts: list[OperatingMetricFact] = []

    def record_run(self, run: IngestionRun) -> None:
        self.runs.append(run)

    def upsert_operating_source_documents(
        self, records: list[OperatingSourceDocument]
    ) -> UpsertResult:
        self.documents.extend(records)
        return UpsertResult(updated=len(records))

    def upsert_operating_metric_facts(self, records: list[OperatingMetricFact]) -> UpsertResult:
        self.facts.extend(records)
        return UpsertResult(updated=len(records))


def test_process_operating_document_persists_doc_and_facts() -> None:
    storage = MockOperatingStorage()
    registry = build_default_operating_registry()

    result = process_operating_document(
        storage=storage,  # type: ignore[arg-type]
        registry=registry,
        document=_sample_document(),
    )

    assert result.errors == {}
    assert result.documents_processed == 1
    assert result.facts_upserted == 2
    assert sorted(result.extracted_metric_codes) == ["order_backlog_amount", "order_intake_amount"]
    assert len(storage.documents) == 1
    assert len(storage.facts) == 2
    assert storage.runs[0].run_type == RunType.OPERATING_METRIC_SYNC
    assert storage.runs[-1].status == RunStatus.SUCCESS
