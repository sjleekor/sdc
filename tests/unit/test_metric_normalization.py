from dataclasses import replace
from datetime import date
from decimal import Decimal

from krx_collector.domain.enums import Market, RunStatus, RunType, Source
from krx_collector.domain.models import (
    DartCorp,
    DartFinancialStatementLine,
    DartShareCountLine,
    DartShareholderReturnLine,
    DartXbrlFactLine,
    IngestionRun,
    MetricCatalogEntry,
    MetricMappingRule,
    StockMetricFact,
    UpsertResult,
)
from krx_collector.service.normalize_metrics import normalize_stock_metrics
from krx_collector.util.time import now_kst


def _filter_rows(rows, bsns_years, reprt_codes, tickers):
    return [
        row
        for row in rows
        if row.bsns_year in bsns_years
        and row.reprt_code in reprt_codes
        and (tickers is None or row.ticker in tickers)
    ]


def _fact_signature(facts: list[StockMetricFact]) -> set[tuple[str, str, int, str, Decimal, str]]:
    return {
        (
            fact.ticker,
            fact.metric_code,
            fact.bsns_year,
            fact.reprt_code,
            fact.value_numeric,
            fact.source_key,
        )
        for fact in facts
    }


class MockMetricStorage:
    def __init__(self) -> None:
        self.runs: list[IngestionRun] = []
        self.catalog: list[MetricCatalogEntry] = []
        self.rules: list[MetricMappingRule] = []
        self.facts: list[StockMetricFact] = []
        self.fact_by_key: dict[tuple[str, str, int, str], StockMetricFact] = {}
        self.extra_financial_rows: list[DartFinancialStatementLine] = []

    def record_run(self, run: IngestionRun) -> None:
        self.runs.append(run)

    def upsert_metric_catalog(self, records: list[MetricCatalogEntry]) -> UpsertResult:
        self.catalog = records
        return UpsertResult(updated=len(records))

    def replace_metric_mapping_rules(self, records: list[MetricMappingRule]) -> UpsertResult:
        self.rules = records
        return UpsertResult(updated=len(records))

    def get_metric_mapping_rules(self) -> list[MetricMappingRule]:
        return self.rules

    def get_dart_corp_master(
        self,
        active_only: bool = True,
        tickers: list[str] | None = None,
    ) -> list[DartCorp]:
        records = [
            DartCorp(
                corp_code="00126380",
                corp_name="삼성전자",
                ticker="005930",
                market=Market.KOSPI,
                stock_name="삼성전자",
                modify_date=date(2026, 3, 10),
                is_active=True,
                source=Source.OPENDART,
                fetched_at=now_kst(),
            ),
            DartCorp(
                corp_code="00164779",
                corp_name="SK하이닉스",
                ticker="000660",
                market=Market.KOSPI,
                stock_name="SK하이닉스",
                modify_date=date(2026, 3, 10),
                is_active=True,
                source=Source.OPENDART,
                fetched_at=now_kst(),
            ),
        ]
        if tickers is None:
            return records
        return [record for record in records if record.ticker in tickers]

    def get_dart_financial_statement_raw(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str] | None = None,
    ) -> list[DartFinancialStatementLine]:
        fetched_at = now_kst()
        rows = [
            DartFinancialStatementLine(
                corp_code="00126380",
                ticker="005930",
                bsns_year=2025,
                reprt_code="11011",
                fs_div="OFS",
                sj_div="IS",
                sj_nm="손익계산서",
                account_id="ifrs-full_Revenue",
                account_nm="매출액",
                account_detail="",
                thstrm_nm="제56기",
                thstrm_amount=Decimal("100"),
                thstrm_add_amount=Decimal("100"),
                frmtrm_nm="제55기",
                frmtrm_amount=Decimal("90"),
                frmtrm_q_nm="",
                frmtrm_q_amount=None,
                frmtrm_add_amount=Decimal("90"),
                bfefrmtrm_nm="제54기",
                bfefrmtrm_amount=Decimal("80"),
                ord=1,
                currency="KRW",
                rcept_no="r1",
                source=Source.OPENDART,
                fetched_at=fetched_at,
                raw_payload={},
            ),
            DartFinancialStatementLine(
                corp_code="00126380",
                ticker="005930",
                bsns_year=2025,
                reprt_code="11011",
                fs_div="CFS",
                sj_div="IS",
                sj_nm="손익계산서",
                account_id="ifrs-full_Revenue",
                account_nm="매출액",
                account_detail="",
                thstrm_nm="제56기",
                thstrm_amount=Decimal("200"),
                thstrm_add_amount=Decimal("200"),
                frmtrm_nm="제55기",
                frmtrm_amount=Decimal("180"),
                frmtrm_q_nm="",
                frmtrm_q_amount=None,
                frmtrm_add_amount=Decimal("180"),
                bfefrmtrm_nm="제54기",
                bfefrmtrm_amount=Decimal("160"),
                ord=1,
                currency="KRW",
                rcept_no="r2",
                source=Source.OPENDART,
                fetched_at=fetched_at,
                raw_payload={},
            ),
            DartFinancialStatementLine(
                corp_code="00126380",
                ticker="005930",
                bsns_year=2025,
                reprt_code="11011",
                fs_div="CFS",
                sj_div="BS",
                sj_nm="재무상태표",
                account_id="ifrs-full_Assets",
                account_nm="자산총계",
                account_detail="",
                thstrm_nm="제56기말",
                thstrm_amount=Decimal("500"),
                thstrm_add_amount=None,
                frmtrm_nm="제55기말",
                frmtrm_amount=Decimal("450"),
                frmtrm_q_nm="",
                frmtrm_q_amount=None,
                frmtrm_add_amount=None,
                bfefrmtrm_nm="제54기말",
                bfefrmtrm_amount=Decimal("400"),
                ord=1,
                currency="KRW",
                rcept_no="r3",
                source=Source.OPENDART,
                fetched_at=fetched_at,
                raw_payload={},
            ),
            DartFinancialStatementLine(
                corp_code="00126380",
                ticker="005930",
                bsns_year=2025,
                reprt_code="11011",
                fs_div="CFS",
                sj_div="CF",
                sj_nm="현금흐름표",
                account_id="ifrs-full_InterestReceivedClassifiedAsOperatingActivities",
                account_nm="이자의 수취",
                account_detail="",
                thstrm_nm="제56기",
                thstrm_amount=Decimal("15"),
                thstrm_add_amount=None,
                frmtrm_nm="제55기",
                frmtrm_amount=Decimal("14"),
                frmtrm_q_nm="",
                frmtrm_q_amount=None,
                frmtrm_add_amount=None,
                bfefrmtrm_nm="제54기",
                bfefrmtrm_amount=Decimal("13"),
                ord=9,
                currency="KRW",
                rcept_no="r4",
                source=Source.OPENDART,
                fetched_at=fetched_at,
                raw_payload={},
            ),
        ]
        rows.append(
            replace(
                rows[1],
                corp_code="00164779",
                ticker="000660",
                thstrm_amount=Decimal("300"),
                thstrm_add_amount=Decimal("300"),
                rcept_no="h1",
            )
        )
        rows.extend(self.extra_financial_rows)
        return _filter_rows(rows, bsns_years, reprt_codes, tickers)

    def get_dart_share_count_raw(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str] | None = None,
    ) -> list[DartShareCountLine]:
        rows = [
            DartShareCountLine(
                corp_code="00126380",
                ticker="005930",
                bsns_year=2025,
                reprt_code="11011",
                rcept_no="s1",
                corp_cls="Y",
                se="합계",
                isu_stock_totqy=25000000000,
                now_to_isu_stock_totqy=8975138200,
                now_to_dcrs_stock_totqy=2239525614,
                redc="-",
                profit_incnr="2239525614",
                rdmstk_repy="-",
                etc="-",
                istc_totqy=6735612586,
                tesstk_co=105432448,
                distb_stock_co=6630180138,
                stlm_dt=date(2025, 12, 31),
                source=Source.OPENDART,
                fetched_at=now_kst(),
                raw_payload={},
            )
        ]
        return _filter_rows(rows, bsns_years, reprt_codes, tickers)

    def get_dart_shareholder_return_raw(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str] | None = None,
    ) -> list[DartShareholderReturnLine]:
        rows = [
            DartShareholderReturnLine(
                corp_code="00126380",
                ticker="005930",
                bsns_year=2025,
                reprt_code="11011",
                statement_type="dividend",
                row_name="주당 현금배당금(원)",
                stock_knd="보통주",
                dim1="",
                dim2="",
                dim3="",
                metric_code="thstrm",
                metric_name="당기",
                value_numeric=Decimal("1668"),
                value_text="1,668",
                unit="",
                rcept_no="d1",
                stlm_dt=date(2025, 12, 31),
                source=Source.OPENDART,
                fetched_at=now_kst(),
                raw_payload={},
            )
        ]
        return _filter_rows(rows, bsns_years, reprt_codes, tickers)

    def get_dart_xbrl_fact_raw(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str] | None = None,
    ) -> list[DartXbrlFactLine]:
        rows = [
            DartXbrlFactLine(
                corp_code="00126380",
                ticker="005930",
                bsns_year=2025,
                reprt_code="11011",
                rcept_no="x1",
                concept_id="ifrs-full_WeightedAverageNumberOfOrdinarySharesOutstandingBasic",
                concept_name="WeightedAverageNumberOfOrdinarySharesOutstandingBasic",
                namespace_uri="http://xbrl.ifrs.org/taxonomy/2023-03-23/ifrs-full",
                context_id="ctx_annual",
                context_type="duration",
                period_start=date(2025, 1, 1),
                period_end=date(2025, 12, 31),
                instant_date=None,
                dimensions=[],
                unit_id="shares",
                unit_measure="shares",
                decimals="0",
                value_numeric=Decimal("6630180138"),
                value_text="6630180138",
                is_nil=False,
                label_ko="기본주당이익 계산에 사용된 가중평균주식수",
                source=Source.OPENDART,
                fetched_at=now_kst(),
                raw_payload={},
            )
        ]
        return _filter_rows(rows, bsns_years, reprt_codes, tickers)

    def iter_dart_financial_statement_for_normalize(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str],
        rule_account_ids: list[str] | None = None,
        page_size: int = 5000,
    ):
        del page_size
        rows = self.get_dart_financial_statement_raw(bsns_years, reprt_codes, tickers)
        if rule_account_ids is not None:
            rows = [row for row in rows if row.account_id in rule_account_ids]
        yield from rows

    def iter_dart_share_count_for_normalize(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str],
        rule_se_values: list[str] | None = None,
        page_size: int = 5000,
    ):
        del page_size
        rows = self.get_dart_share_count_raw(bsns_years, reprt_codes, tickers)
        if rule_se_values is not None:
            rows = [row for row in rows if row.se in rule_se_values]
        yield from rows

    def iter_dart_shareholder_return_for_normalize(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str],
        page_size: int = 5000,
    ):
        del page_size
        yield from self.get_dart_shareholder_return_raw(bsns_years, reprt_codes, tickers)

    def iter_dart_xbrl_fact_for_normalize(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str],
        rule_concept_ids: list[str] | None = None,
        page_size: int = 5000,
    ):
        del page_size
        rows = self.get_dart_xbrl_fact_raw(bsns_years, reprt_codes, tickers)
        if rule_concept_ids is not None:
            rows = [row for row in rows if row.concept_id in rule_concept_ids]
        yield from rows

    def upsert_stock_metric_facts(self, records: list[StockMetricFact]) -> UpsertResult:
        for record in records:
            self.fact_by_key[
                (record.ticker, record.metric_code, record.bsns_year, record.reprt_code)
            ] = record
        self.facts = list(self.fact_by_key.values())
        return UpsertResult(updated=len(records))


def test_normalize_stock_metrics_prefers_cfs_and_writes_share_metrics() -> None:
    storage = MockMetricStorage()

    result = normalize_stock_metrics(
        storage=storage,
        bsns_years=[2025],
        reprt_codes=["11011"],
        tickers=["005930"],
    )

    assert result.errors == {}
    assert result.targets_processed == 1
    assert result.facts_written >= 4
    assert storage.runs[0].run_type == RunType.METRIC_NORMALIZE
    assert storage.runs[-1].status == RunStatus.SUCCESS

    facts = {fact.metric_code: fact for fact in storage.facts}
    assert facts["revenue"].value_numeric == Decimal("200")
    assert facts["revenue"].fs_div == "CFS"
    assert facts["total_assets"].value_numeric == Decimal("500")
    assert facts["issued_shares"].value_numeric == Decimal("6735612586")
    assert facts["treasury_shares"].value_numeric == Decimal("105432448")
    assert facts["dps"].value_numeric == Decimal("1668")
    assert facts["interest_received"].value_numeric == Decimal("15")
    assert facts["weighted_avg_shares"].value_numeric == Decimal("6630180138")
    assert facts["weighted_avg_shares"].source_table == "dart_xbrl_fact_raw"


def test_normalize_stock_metrics_is_chunk_size_equivalent() -> None:
    signatures = []

    for batch_size in (1, 2, 10):
        storage = MockMetricStorage()

        result = normalize_stock_metrics(
            storage=storage,
            bsns_years=[2025],
            reprt_codes=["11011"],
            tickers=["005930", "000660"],
            batch_size=batch_size,
        )

        assert result.errors == {}
        assert result.targets_processed == 2
        signatures.append(_fact_signature(storage.facts))

    assert signatures[0] == signatures[1] == signatures[2]
    assert ("000660", "revenue", 2025, "11011", Decimal("300"), "h1:ifrs-full_Revenue:1") in (
        signatures[0]
    )


def test_normalize_stock_metrics_uses_source_key_tiebreaker() -> None:
    storage = MockMetricStorage()
    duplicate_revenue = storage.get_dart_financial_statement_raw([2025], ["11011"], ["005930"])[1]
    storage.extra_financial_rows.append(
        replace(
            duplicate_revenue,
            thstrm_amount=Decimal("175"),
            thstrm_add_amount=Decimal("175"),
            rcept_no="r0",
        )
    )

    result = normalize_stock_metrics(
        storage=storage,
        bsns_years=[2025],
        reprt_codes=["11011"],
        tickers=["005930"],
        batch_size=1,
    )

    assert result.errors == {}
    revenue = next(fact for fact in storage.facts if fact.metric_code == "revenue")
    assert revenue.value_numeric == Decimal("175")
    assert revenue.source_key == "r0:ifrs-full_Revenue:1"
