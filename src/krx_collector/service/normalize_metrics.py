"""Use-case: seed metric rules and normalize canonical stock metrics."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import date
from decimal import Decimal

from krx_collector.domain.enums import RunStatus, RunType
from krx_collector.domain.models import (
    DartFinancialStatementLine,
    DartShareCountLine,
    DartShareholderReturnLine,
    DartXbrlFactLine,
    IngestionRun,
    MetricCatalogEntry,
    MetricMappingRule,
    MetricNormalizationResult,
    StockMetricFact,
)
from krx_collector.ports.storage import Storage
from krx_collector.util.time import now_kst

logger = logging.getLogger(__name__)


def _default_metric_catalog() -> list[MetricCatalogEntry]:
    return [
        MetricCatalogEntry("revenue", "매출액", "financial", "KRW", "손익계산서 매출액"),
        MetricCatalogEntry("cogs", "매출원가", "financial", "KRW", "손익계산서 매출원가"),
        MetricCatalogEntry(
            "gross_profit", "매출총이익", "financial", "KRW", "손익계산서 매출총이익"
        ),
        MetricCatalogEntry("sga", "판매비와관리비", "financial", "KRW", "판매비와관리비"),
        MetricCatalogEntry("operating_income", "영업이익", "financial", "KRW", "영업이익"),
        MetricCatalogEntry("net_income", "당기순이익", "financial", "KRW", "당기순이익"),
        MetricCatalogEntry(
            "controlling_net_income",
            "지배주주순이익",
            "financial",
            "KRW",
            "지배기업 소유주지분 순이익",
        ),
        MetricCatalogEntry("total_assets", "총자산", "financial", "KRW", "자산총계"),
        MetricCatalogEntry("total_liabilities", "총부채", "financial", "KRW", "부채총계"),
        MetricCatalogEntry("total_equity", "총자본", "financial", "KRW", "자본총계"),
        MetricCatalogEntry(
            "cash_and_cash_equivalents",
            "현금및현금성자산",
            "financial",
            "KRW",
            "재무상태표 현금및현금성자산",
        ),
        MetricCatalogEntry(
            "operating_cash_flow",
            "영업활동현금흐름",
            "financial",
            "KRW",
            "현금흐름표 영업활동현금흐름",
        ),
        MetricCatalogEntry(
            "investing_cash_flow",
            "투자활동현금흐름",
            "financial",
            "KRW",
            "현금흐름표 투자활동현금흐름",
        ),
        MetricCatalogEntry(
            "financing_cash_flow",
            "재무활동현금흐름",
            "financial",
            "KRW",
            "현금흐름표 재무활동현금흐름",
        ),
        MetricCatalogEntry("issued_shares", "발행주식수", "share_count", "shares", "발행주식 총수"),
        MetricCatalogEntry("treasury_shares", "자기주식수", "share_count", "shares", "자기주식 수"),
        MetricCatalogEntry(
            "dps", "주당 현금배당금", "shareholder_return", "KRW", "보통주 기준 DPS"
        ),
        MetricCatalogEntry(
            "interest_received",
            "이자수익",
            "financial",
            "KRW",
            "현금흐름표 이자수취액",
        ),
        MetricCatalogEntry(
            "interest_paid",
            "이자비용",
            "financial",
            "KRW",
            "현금흐름표 이자지급액",
        ),
        MetricCatalogEntry(
            "dividends_paid",
            "배당금 지급액",
            "financial",
            "KRW",
            "현금흐름표 배당금 지급액",
        ),
        MetricCatalogEntry(
            "capex_ppe",
            "유형자산 취득액",
            "financial",
            "KRW",
            "현금흐름표 유형자산 취득액",
        ),
        MetricCatalogEntry(
            "capex_intangible",
            "무형자산 취득액",
            "financial",
            "KRW",
            "현금흐름표 무형자산 취득액",
        ),
        MetricCatalogEntry(
            "borrowing_proceeds_long_term",
            "장기차입금 증가액",
            "financial",
            "KRW",
            "장기차입금 조달 현금유입",
        ),
        MetricCatalogEntry(
            "borrowing_repayments_long_term",
            "장기차입금 상환액",
            "financial",
            "KRW",
            "장기차입금 상환 현금유출",
        ),
        MetricCatalogEntry(
            "treasury_share_acquisition_amount",
            "자사주 매입금액",
            "financial",
            "KRW",
            "자기주식 취득 현금유출",
        ),
        MetricCatalogEntry(
            "weighted_avg_shares",
            "가중평균주식수",
            "xbrl",
            "shares",
            "기본주당이익 계산용 가중평균주식수",
        ),
        MetricCatalogEntry(
            "diluted_shares",
            "희석주식수",
            "xbrl",
            "shares",
            "희석주당이익 계산용 가중평균주식수",
        ),
        MetricCatalogEntry(
            "depreciation_expense",
            "감가상각비",
            "xbrl",
            "KRW",
            "당기 감가상각비",
        ),
        MetricCatalogEntry(
            "amortization_intangible_assets",
            "무형자산상각비",
            "xbrl",
            "KRW",
            "당기 무형자산상각비",
        ),
    ]


def _financial_rule(
    metric_code: str, account_id: str, sj_div: str, priority: int, fs_div: str
) -> MetricMappingRule:
    return MetricMappingRule(
        rule_code=f"fin.{metric_code}.{fs_div.lower()}.{sj_div.lower()}.{account_id}",
        metric_code=metric_code,
        source_table="dart_financial_statement_raw",
        value_selector="thstrm_amount",
        priority=priority,
        fs_div=fs_div,
        sj_div=sj_div,
        account_id=account_id,
    )


def _default_metric_mapping_rules() -> list[MetricMappingRule]:
    rules: list[MetricMappingRule] = []
    financial_specs = [
        ("revenue", "ifrs-full_Revenue", "IS"),
        ("cogs", "ifrs-full_CostOfSales", "IS"),
        ("gross_profit", "ifrs-full_GrossProfit", "IS"),
        ("sga", "dart_TotalSellingGeneralAdministrativeExpenses", "IS"),
        ("operating_income", "dart_OperatingIncomeLoss", "IS"),
        ("net_income", "ifrs-full_ProfitLoss", "IS"),
        ("controlling_net_income", "ifrs-full_ProfitLossAttributableToOwnersOfParent", "IS"),
        ("total_assets", "ifrs-full_Assets", "BS"),
        ("total_liabilities", "ifrs-full_Liabilities", "BS"),
        ("total_equity", "ifrs-full_Equity", "BS"),
        ("cash_and_cash_equivalents", "ifrs-full_CashAndCashEquivalents", "BS"),
        ("operating_cash_flow", "ifrs-full_CashFlowsFromUsedInOperatingActivities", "CF"),
        ("investing_cash_flow", "ifrs-full_CashFlowsFromUsedInInvestingActivities", "CF"),
        ("financing_cash_flow", "ifrs-full_CashFlowsFromUsedInFinancingActivities", "CF"),
        (
            "interest_received",
            "ifrs-full_InterestReceivedClassifiedAsOperatingActivities",
            "CF",
        ),
        (
            "interest_paid",
            "ifrs-full_InterestPaidClassifiedAsOperatingActivities",
            "CF",
        ),
        (
            "dividends_paid",
            "ifrs-full_DividendsPaidClassifiedAsFinancingActivities",
            "CF",
        ),
        (
            "capex_ppe",
            "ifrs-full_PurchaseOfPropertyPlantAndEquipmentClassifiedAsInvestingActivities",
            "CF",
        ),
        (
            "capex_intangible",
            "ifrs-full_PurchaseOfIntangibleAssetsClassifiedAsInvestingActivities",
            "CF",
        ),
        (
            "borrowing_proceeds_long_term",
            "dart_ProceedsFromLongTermBorrowings",
            "CF",
        ),
        (
            "borrowing_repayments_long_term",
            "ifrs-full_RepaymentsOfNoncurrentBorrowings",
            "CF",
        ),
        (
            "treasury_share_acquisition_amount",
            "dart_AcquisitionOfTreasuryShares",
            "CF",
        ),
    ]
    for metric_code, account_id, sj_div in financial_specs:
        rules.append(_financial_rule(metric_code, account_id, sj_div, 10, "CFS"))
        rules.append(_financial_rule(metric_code, account_id, sj_div, 20, "OFS"))

    rules.extend(
        [
            MetricMappingRule(
                rule_code="share.issued_shares.total",
                metric_code="issued_shares",
                source_table="dart_share_count_raw",
                value_selector="istc_totqy",
                priority=10,
                row_name="합계",
            ),
            MetricMappingRule(
                rule_code="share.treasury_shares.total",
                metric_code="treasury_shares",
                source_table="dart_share_count_raw",
                value_selector="tesstk_co",
                priority=10,
                row_name="합계",
            ),
            MetricMappingRule(
                rule_code="return.dps.common",
                metric_code="dps",
                source_table="dart_shareholder_return_raw",
                value_selector="value_numeric",
                priority=10,
                statement_type="dividend",
                row_name="주당 현금배당금(원)",
                stock_knd="보통주",
                metric_code_match="thstrm",
            ),
            MetricMappingRule(
                rule_code="return.dps.default",
                metric_code="dps",
                source_table="dart_shareholder_return_raw",
                value_selector="value_numeric",
                priority=20,
                statement_type="dividend",
                row_name="주당 현금배당금(원)",
                metric_code_match="thstrm",
            ),
        ]
    )

    xbrl_specs = [
        (
            "weighted_avg_shares",
            [
                "ifrs-full_WeightedAverageShares",
                "ifrs-full_WeightedAverageNumberOfOrdinarySharesOutstandingBasic",
                "ifrs-full_WeightedAverageNumberOfSharesOutstandingBasic",
            ],
        ),
        (
            "diluted_shares",
            [
                "ifrs-full_AdjustedWeightedAverageShares",
                "ifrs-full_WeightedAverageNumberOfOrdinarySharesOutstandingDiluted",
                "ifrs-full_WeightedAverageNumberOfSharesOutstandingDiluted",
            ],
        ),
        (
            "depreciation_expense",
            [
                "ifrs-full_DepreciationExpense",
                "ifrs-full_DepreciationAndAmortisationExpense",
                "ifrs-full_DepreciationAmortisationAndImpairmentExpense",
            ],
        ),
        (
            "amortization_intangible_assets",
            [
                "ifrs-full_AmortisationExpense",
                "dart_AmortizationOfIntangibleAssetsExpense",
            ],
        ),
    ]
    for metric_code, concept_ids in xbrl_specs:
        for priority_offset, concept_id in enumerate(concept_ids):
            rules.append(
                MetricMappingRule(
                    rule_code=f"xbrl.{metric_code}.{concept_id.lower()}",
                    metric_code=metric_code,
                    source_table="dart_xbrl_fact_raw",
                    value_selector="value_numeric",
                    priority=10 + priority_offset,
                    account_id=concept_id,
                )
            )
    return rules


def _reprt_code_to_period_type(reprt_code: str) -> str:
    return {
        "11013": "q1",
        "11012": "half",
        "11014": "q3",
        "11011": "annual",
    }.get(reprt_code, "unknown")


def _infer_period_end(bsns_year: int, reprt_code: str) -> date | None:
    month_day = {
        "11013": (3, 31),
        "11012": (6, 30),
        "11014": (9, 30),
        "11011": (12, 31),
    }.get(reprt_code)
    if month_day is None:
        return None
    month, day = month_day
    return date(bsns_year, month, day)


def _matches_financial(rule: MetricMappingRule, row: DartFinancialStatementLine) -> bool:
    return (
        rule.source_table == "dart_financial_statement_raw"
        and (not rule.fs_div or row.fs_div == rule.fs_div)
        and (not rule.sj_div or row.sj_div == rule.sj_div)
        and (not rule.account_id or row.account_id == rule.account_id)
        and (not rule.account_nm or row.account_nm == rule.account_nm)
    )


def _matches_share_count(rule: MetricMappingRule, row: DartShareCountLine) -> bool:
    return rule.source_table == "dart_share_count_raw" and (
        not rule.row_name or row.se == rule.row_name
    )


def _matches_shareholder_return(rule: MetricMappingRule, row: DartShareholderReturnLine) -> bool:
    return (
        rule.source_table == "dart_shareholder_return_raw"
        and (not rule.statement_type or row.statement_type == rule.statement_type)
        and (not rule.row_name or row.row_name == rule.row_name)
        and (not rule.stock_knd or row.stock_knd == rule.stock_knd)
        and (not rule.dim1 or row.dim1 == rule.dim1)
        and (not rule.dim2 or row.dim2 == rule.dim2)
        and (not rule.dim3 or row.dim3 == rule.dim3)
        and (not rule.metric_code_match or row.metric_code == rule.metric_code_match)
    )


def _matches_xbrl(rule: MetricMappingRule, row: DartXbrlFactLine) -> bool:
    return (
        rule.source_table == "dart_xbrl_fact_raw"
        and (not rule.account_id or row.concept_id == rule.account_id)
        and (
            not rule.account_nm
            or row.label_ko == rule.account_nm
            or row.concept_name == rule.account_nm
        )
    )


def _extract_value(obj: object, selector: str) -> Decimal | None:
    value = getattr(obj, selector)
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _xbrl_candidate_rank(row: DartXbrlFactLine) -> int:
    rank = len(row.dimensions) * 10
    joined_dimensions = "|".join(row.dimensions)
    if "ConsolidatedMember" in joined_dimensions:
        rank -= 5
    elif "SeparateMember" in joined_dimensions:
        rank += 5
    if "ReportedAmountMember" in joined_dimensions:
        rank += 1
    if "OperatingSegmentsMember" in joined_dimensions:
        rank += 3
    return rank


def normalize_stock_metrics(
    storage: Storage,
    bsns_years: list[int],
    reprt_codes: list[str],
    tickers: list[str] | None = None,
) -> MetricNormalizationResult:
    """Seed metric rules and normalize canonical facts from raw tables."""
    run = IngestionRun(
        run_type=RunType.METRIC_NORMALIZE,
        started_at=now_kst(),
        status=RunStatus.RUNNING,
        params={
            "bsns_years": bsns_years,
            "reprt_codes": reprt_codes,
            "tickers": tickers,
        },
    )
    storage.record_run(run)

    result = MetricNormalizationResult()
    try:
        catalog = _default_metric_catalog()
        rules = _default_metric_mapping_rules()
        result.catalog_upsert = storage.upsert_metric_catalog(catalog)
        result.rule_upsert = storage.replace_metric_mapping_rules(rules)

        corp_rows = storage.get_dart_corp_master(active_only=True, tickers=tickers)
        corp_by_ticker = {
            corp.ticker: corp for corp in corp_rows if corp.ticker and corp.market is not None
        }
        result.targets_processed = len(corp_by_ticker)

        financial_rows = storage.get_dart_financial_statement_raw(bsns_years, reprt_codes, tickers)
        share_count_rows = storage.get_dart_share_count_raw(bsns_years, reprt_codes, tickers)
        shareholder_return_rows = storage.get_dart_shareholder_return_raw(
            bsns_years, reprt_codes, tickers
        )
        xbrl_rows = storage.get_dart_xbrl_fact_raw(bsns_years, reprt_codes, tickers)

        rules_by_source: dict[str, list[MetricMappingRule]] = {}
        for rule in storage.get_metric_mapping_rules():
            rules_by_source.setdefault(rule.source_table, []).append(rule)

        candidate_builders: list[
            tuple[
                str,
                list[object],
                Callable[[MetricMappingRule, object], bool],
                Callable[[object], tuple[str, int, str, str]],
                Callable[[object], str],
                Callable[[object], date | None],
                Callable[[object], int],
            ]
        ] = [
            (
                "dart_financial_statement_raw",
                list(financial_rows),
                _matches_financial,
                lambda row: (row.ticker, row.bsns_year, row.reprt_code, row.fs_div),
                lambda row: f"{row.rcept_no}:{row.account_id}:{row.ord}",
                lambda row: _infer_period_end(row.bsns_year, row.reprt_code),
                lambda row: 0,
            ),
            (
                "dart_share_count_raw",
                list(share_count_rows),
                _matches_share_count,
                lambda row: (row.ticker, row.bsns_year, row.reprt_code, ""),
                lambda row: f"{row.rcept_no}:{row.se}",
                lambda row: row.stlm_dt or _infer_period_end(row.bsns_year, row.reprt_code),
                lambda row: 0,
            ),
            (
                "dart_shareholder_return_raw",
                list(shareholder_return_rows),
                _matches_shareholder_return,
                lambda row: (row.ticker, row.bsns_year, row.reprt_code, ""),
                lambda row: (
                    f"{row.rcept_no}:{row.statement_type}:{row.row_name}:{row.stock_knd}:"
                    f"{row.dim1}:{row.dim2}:{row.dim3}:{row.metric_code}"
                ),
                lambda row: row.stlm_dt or _infer_period_end(row.bsns_year, row.reprt_code),
                lambda row: 0,
            ),
            (
                "dart_xbrl_fact_raw",
                list(xbrl_rows),
                _matches_xbrl,
                lambda row: (row.ticker, row.bsns_year, row.reprt_code, ""),
                lambda row: f"{row.rcept_no}:{row.context_id}:{row.concept_id}",
                lambda row: row.instant_date
                or row.period_end
                or _infer_period_end(row.bsns_year, row.reprt_code),
                _xbrl_candidate_rank,
            ),
        ]

        selected_facts: dict[tuple[str, str, int, str], tuple[int, int, StockMetricFact]] = {}
        for (
            source_table,
            rows,
            matcher,
            key_parts,
            source_key_builder,
            period_end_builder,
            candidate_rank_builder,
        ) in candidate_builders:
            source_rules = rules_by_source.get(source_table, [])
            for row in rows:
                ticker, bsns_year, reprt_code, fs_div = key_parts(row)
                corp = corp_by_ticker.get(ticker)
                if corp is None or corp.market is None:
                    continue

                for rule in source_rules:
                    if not matcher(rule, row):
                        continue

                    value_numeric = _extract_value(row, rule.value_selector)
                    if value_numeric is None:
                        continue

                    fact = StockMetricFact(
                        ticker=ticker,
                        market=corp.market,
                        corp_code=corp.corp_code,
                        metric_code=rule.metric_code,
                        period_type=_reprt_code_to_period_type(reprt_code),
                        period_end=period_end_builder(row),
                        bsns_year=bsns_year,
                        reprt_code=reprt_code,
                        fs_div=fs_div,
                        value_numeric=value_numeric,
                        value_text=str(value_numeric),
                        unit=next(
                            (
                                entry.unit
                                for entry in catalog
                                if entry.metric_code == rule.metric_code
                            ),
                            "",
                        ),
                        source_table=source_table,
                        source_key=source_key_builder(row),
                        mapping_rule_code=rule.rule_code,
                        fetched_at=row.fetched_at,
                    )
                    fact_key = (ticker, rule.metric_code, bsns_year, reprt_code)
                    candidate_rank = candidate_rank_builder(row)
                    current = selected_facts.get(fact_key)
                    if current is None or (rule.priority, candidate_rank) < (
                        current[0],
                        current[1],
                    ):
                        selected_facts[fact_key] = (rule.priority, candidate_rank, fact)

        facts = [fact for _, _, fact in selected_facts.values()]
        result.fact_upsert = storage.upsert_stock_metric_facts(facts)
        result.facts_written = result.fact_upsert.updated

        run.ended_at = now_kst()
        run.status = RunStatus.SUCCESS
        run.counts = {
            "targets_processed": result.targets_processed,
            "catalog_upserted": result.catalog_upsert.updated,
            "rules_upserted": result.rule_upsert.updated,
            "facts_written": result.facts_written,
            "error_count": len(result.errors),
        }
        storage.record_run(run)
        return result
    except Exception as exc:
        logger.exception("Metric normalization failed")
        run.ended_at = now_kst()
        run.status = RunStatus.FAILED
        run.error_summary = str(exc)
        storage.record_run(run)
        result.errors["pipeline"] = str(exc)
        return result
