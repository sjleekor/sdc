from datetime import date
from decimal import Decimal

from krx_collector.domain.enums import Market, Source
from krx_collector.domain.models import DartFinancialStatementLine, MetricCatalogEntry, StockMetricFact
from krx_collector.service.report_metric_coverage import build_metric_coverage_report
from krx_collector.util.time import now_kst


class MockCoverageStorage:
    def get_metric_catalog_entries(self) -> list[MetricCatalogEntry]:
        return [
            MetricCatalogEntry("revenue", "매출액", "financial", "KRW", "매출액"),
            MetricCatalogEntry("weighted_avg_shares", "가중평균주식수", "xbrl", "shares", "가중평균주식수"),
        ]

    def get_dart_financial_statement_raw(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str] | None = None,
    ) -> list[DartFinancialStatementLine]:
        return [
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
                thstrm_amount=Decimal("100"),
                thstrm_add_amount=None,
                frmtrm_nm="제55기",
                frmtrm_amount=Decimal("90"),
                frmtrm_q_nm="",
                frmtrm_q_amount=None,
                frmtrm_add_amount=None,
                bfefrmtrm_nm="제54기",
                bfefrmtrm_amount=Decimal("80"),
                ord=1,
                currency="KRW",
                rcept_no="r1",
                source=Source.OPENDART,
                fetched_at=now_kst(),
                raw_payload={},
            )
        ]

    def get_stock_metric_facts(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str] | None = None,
    ) -> list[StockMetricFact]:
        return [
            StockMetricFact(
                ticker="005930",
                market=Market.KOSPI,
                corp_code="00126380",
                metric_code="revenue",
                period_type="annual",
                period_end=date(2025, 12, 31),
                bsns_year=2025,
                reprt_code="11011",
                fs_div="CFS",
                value_numeric=Decimal("100"),
                value_text="100",
                unit="KRW",
                source_table="dart_financial_statement_raw",
                source_key="r1",
                mapping_rule_code="fin.revenue",
                fetched_at=now_kst(),
            )
        ]


def test_build_metric_coverage_report_counts_per_metric() -> None:
    report = build_metric_coverage_report(
        storage=MockCoverageStorage(),  # type: ignore[arg-type]
        bsns_years=[2025],
        reprt_codes=["11011"],
        tickers=["005930"],
    )

    assert report.target_count == 1
    rows = {row.metric_code: row for row in report.rows}
    assert rows["revenue"].covered_count == 1
    assert rows["weighted_avg_shares"].covered_count == 0
