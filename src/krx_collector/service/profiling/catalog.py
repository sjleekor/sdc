"""The declarative profile catalog — one :class:`TableProfileSpec` per table.

Adding a table to the profiler is a single entry here.  The service layer
reads only these specs to decide which standard checks (``C1``–``C13``) to
run; checks whose required columns are absent auto-skip.

The catalog is intentionally a Python dataclass registry (type-checked,
refactor-safe) rather than YAML — a YAML loader can be layered on later if a
non-developer editing workflow is needed.
"""

from __future__ import annotations

from krx_collector.domain.profiling import (
    CostClass,
    ForeignKeyProfileSpec,
    ProfileTableRole,
    ProfileWeight,
    SamplingPolicy,
    TableProfileSpec,
)

# ---------------------------------------------------------------------------
# Wave 0 — reference table (parity-validated against the manual profile)
# ---------------------------------------------------------------------------

DAILY_OHLCV = TableProfileSpec(
    table="daily_ohlcv",
    weight=ProfileWeight.FULL,
    role=ProfileTableRole.RAW,
    entity_key="ticker",
    time_col="trade_date",
    natural_key=("trade_date", "ticker", "market"),
    numeric_cols=("open", "high", "low", "close", "volume"),
    category_cols=("market", "source"),
    null_cols=(),  # all columns NOT NULL by DDL; default = all
    ingest_col="fetched_at",
    fk_relations=(
        ForeignKeyProfileSpec(
            ref_table="stock_master",
            columns=(("ticker", "ticker"), ("market", "market")),
        ),
    ),
    cost_class=CostClass.EXPENSIVE,
    sampling=SamplingPolicy(sample_pct=1.0, large_row_threshold=5_000_000),
    domain_checks=(
        "ohlc_identity",
        "halted_zero_ratio",
        "listing_span",
    ),
)


# ---------------------------------------------------------------------------
# Wave 1 — large / long-format tables (sampling + drilldown)
# ---------------------------------------------------------------------------

KRX_SECURITY_FLOW_RAW = TableProfileSpec(
    table="krx_security_flow_raw",
    weight=ProfileWeight.FULL,
    role=ProfileTableRole.RAW,
    entity_key="ticker",
    time_col="trade_date",
    natural_key=("trade_date", "ticker", "market", "metric_code", "source"),
    numeric_cols=("value",),
    category_cols=("market", "source", "unit"),
    top_n_cols=("metric_code",),
    # raw_payload is a large JSONB blob; exclude from per-column null scan.
    null_cols=("value", "unit", "metric_name", "source", "metric_code"),
    ingest_col="fetched_at",
    drilldown_dim="metric_code",  # 7 metrics → split per-metric
    fk_relations=(
        ForeignKeyProfileSpec(
            ref_table="daily_ohlcv",
            columns=(
                ("trade_date", "trade_date"),
                ("ticker", "ticker"),
                ("market", "market"),
            ),
        ),
    ),
    cost_class=CostClass.EXPENSIVE,
    sampling=SamplingPolicy(sample_pct=1.0, large_row_threshold=5_000_000),
    domain_checks=("flow_source_dedupe", "flow_pit_join_coverage"),
)

DART_XBRL_FACT_RAW = TableProfileSpec(
    table="dart_xbrl_fact_raw",
    weight=ProfileWeight.FULL,
    role=ProfileTableRole.RAW,
    entity_key="corp_code",
    time_col="bsns_year",  # INT business-year axis (type-aware checks)
    natural_key=(
        "corp_code",
        "bsns_year",
        "reprt_code",
        "rcept_no",
        "context_id",
        "concept_id",
    ),
    numeric_cols=("value_numeric",),
    category_cols=("reprt_code", "context_type", "is_nil", "source"),
    top_n_cols=("concept_id", "unit_measure"),
    null_cols=("value_numeric", "value_text", "unit_measure", "decimals"),
    ingest_col="fetched_at",
    cost_class=CostClass.EXPENSIVE,
    # Mandatory sampling — 80M rows; quantiles/Top-N over a 1% sample.
    sampling=SamplingPolicy(sample_pct=1.0, large_row_threshold=2_000_000),
    domain_checks=("xbrl_concept_top", "xbrl_orphan_fact"),
)

DART_FINANCIAL_STATEMENT_RAW = TableProfileSpec(
    table="dart_financial_statement_raw",
    weight=ProfileWeight.FULL,
    role=ProfileTableRole.RAW,
    entity_key="corp_code",
    time_col="bsns_year",
    natural_key=(
        "corp_code",
        "bsns_year",
        "reprt_code",
        "fs_div",
        "sj_div",
        "account_id",
        "ord",
        "rcept_no",
    ),
    numeric_cols=("thstrm_amount", "frmtrm_amount", "bfefrmtrm_amount"),
    category_cols=("fs_div", "sj_div", "reprt_code", "currency", "source"),
    top_n_cols=("account_id",),
    null_cols=(
        "thstrm_amount",
        "frmtrm_amount",
        "bfefrmtrm_amount",
        "currency",
        "rcept_no",
    ),
    unit_cols=("currency",),
    ingest_col="fetched_at",
    cost_class=CostClass.EXPENSIVE,
    sampling=SamplingPolicy(sample_pct=5.0, large_row_threshold=2_000_000),
    domain_checks=("fin_sj_div_dist",),
)

COMMON_FEATURE_OBSERVATION_RAW = TableProfileSpec(
    table="common_feature_observation_raw",
    weight=ProfileWeight.FULL,
    role=ProfileTableRole.RAW,
    entity_key="series_id",
    time_col="observation_date",
    natural_key=(
        "source",
        "series_id",
        "observation_date",
        "period_end_date",
        "release_date",
        "vintage",
    ),
    numeric_cols=("value_numeric",),
    category_cols=("source", "frequency", "unit"),
    top_n_cols=("series_id",),
    null_cols=(
        "value_numeric",
        "value_text",
        "period_end_date",
        "release_date",
        "vintage",
    ),
    ingest_col="fetched_at",
    drilldown_dim="series_id",  # 26 series → split per-series
    # Vintage availability must not precede the observation it describes.
    pit_pairs=(("available_from_date", "observation_date"),),
    fk_relations=(
        ForeignKeyProfileSpec(
            ref_table="common_feature_series",
            columns=(("series_id", "series_id"),),
        ),
    ),
    cost_class=CostClass.CHEAP,
    sampling=SamplingPolicy(sample_pct=None),
    domain_checks=(),
)

COMMON_FEATURE_SERIES = TableProfileSpec(
    table="common_feature_series",
    weight=ProfileWeight.LIGHT,
    role=ProfileTableRole.REFERENCE,
    natural_key=("series_id",),
    category_cols=("source", "category", "frequency", "active", "availability_policy"),
    null_cols=("history_start_date", "default_transform", "unit"),
    ingest_col="updated_at",
    cost_class=CostClass.CHEAP,
    sampling=SamplingPolicy(sample_pct=None),
    domain_checks=(),
)

DART_SHAREHOLDER_RETURN_RAW = TableProfileSpec(
    table="dart_shareholder_return_raw",
    weight=ProfileWeight.FULL,
    role=ProfileTableRole.RAW,
    entity_key="corp_code",
    time_col="bsns_year",
    natural_key=(
        "corp_code",
        "bsns_year",
        "reprt_code",
        "statement_type",
        "row_name",
        "stock_knd",
        "dim1",
        "dim2",
        "dim3",
        "metric_code",
        "rcept_no",
    ),
    numeric_cols=("value_numeric",),
    category_cols=("statement_type", "reprt_code", "unit", "source"),
    top_n_cols=("metric_code", "stock_knd", "row_name"),
    null_cols=("value_numeric", "value_text", "unit", "stlm_dt"),
    ingest_col="fetched_at",
    drilldown_dim="metric_code",
    cost_class=CostClass.EXPENSIVE,
    sampling=SamplingPolicy(sample_pct=2.0, large_row_threshold=2_000_000),
    domain_checks=(),
)

DART_SHARE_COUNT_RAW = TableProfileSpec(
    table="dart_share_count_raw",
    weight=ProfileWeight.FULL,
    role=ProfileTableRole.RAW,
    entity_key="corp_code",
    time_col="bsns_year",
    natural_key=("corp_code", "bsns_year", "reprt_code", "se", "rcept_no"),
    numeric_cols=("isu_stock_totqy", "istc_totqy", "tesstk_co", "distb_stock_co"),
    category_cols=("reprt_code", "corp_cls", "source"),
    top_n_cols=("se",),
    null_cols=("isu_stock_totqy", "istc_totqy", "tesstk_co", "stlm_dt"),
    ingest_col="fetched_at",
    cost_class=CostClass.CHEAP,
    sampling=SamplingPolicy(sample_pct=None),
    domain_checks=(),
)

DART_XBRL_DOCUMENT = TableProfileSpec(
    table="dart_xbrl_document",
    weight=ProfileWeight.FULL,
    role=ProfileTableRole.RAW,
    entity_key="corp_code",
    time_col="bsns_year",
    natural_key=("corp_code", "bsns_year", "reprt_code", "rcept_no"),
    numeric_cols=("zip_entry_count",),
    category_cols=("reprt_code", "source"),
    null_cols=("ticker", "instance_document_name", "label_ko_document_name"),
    ingest_col="fetched_at",
    cost_class=CostClass.CHEAP,
    sampling=SamplingPolicy(sample_pct=None),
    domain_checks=(),
)

DART_CORP_MASTER = TableProfileSpec(
    table="dart_corp_master",
    weight=ProfileWeight.LIGHT,
    role=ProfileTableRole.RAW,
    natural_key=("corp_code",),
    category_cols=("market", "is_active", "source"),
    null_cols=("ticker", "market", "stock_name", "modify_date"),
    ingest_col="updated_at",
    cost_class=CostClass.CHEAP,
    sampling=SamplingPolicy(sample_pct=None),
    domain_checks=("corp_master_listing",),
)

STOCK_MASTER = TableProfileSpec(
    table="stock_master",
    weight=ProfileWeight.LIGHT,
    role=ProfileTableRole.RAW,
    entity_key="ticker",
    natural_key=("ticker", "market"),
    category_cols=("market", "status", "source"),
    null_cols=("name", "last_seen_date"),
    ingest_col="updated_at",
    cost_class=CostClass.CHEAP,
    sampling=SamplingPolicy(sample_pct=None),
    domain_checks=(),
)

STOCK_MASTER_SNAPSHOT = TableProfileSpec(
    table="stock_master_snapshot",
    weight=ProfileWeight.LIGHT,
    role=ProfileTableRole.RAW,
    time_col="as_of_date",
    natural_key=("snapshot_id",),
    numeric_cols=("record_count",),
    category_cols=("source",),
    ingest_col="fetched_at",
    cost_class=CostClass.CHEAP,
    sampling=SamplingPolicy(sample_pct=None),
    domain_checks=(),
)

STOCK_MASTER_SNAPSHOT_ITEMS = TableProfileSpec(
    table="stock_master_snapshot_items",
    weight=ProfileWeight.LIGHT,
    role=ProfileTableRole.RAW,
    entity_key="ticker",
    natural_key=("snapshot_id", "ticker", "market"),
    category_cols=("market", "status"),
    null_cols=("name",),
    cost_class=CostClass.CHEAP,
    sampling=SamplingPolicy(sample_pct=None),
    domain_checks=(),
)

INGESTION_RUNS = TableProfileSpec(
    table="ingestion_runs",
    weight=ProfileWeight.LIGHT,
    role=ProfileTableRole.OPERATIONAL,
    time_col="started_at",
    natural_key=("run_id",),
    category_cols=("run_type", "status"),
    ingest_col="started_at",
    cost_class=CostClass.CHEAP,
    sampling=SamplingPolicy(sample_pct=None),
    domain_checks=("ingest_run_status",),
)

SYNC_CHECKPOINTS = TableProfileSpec(
    table="sync_checkpoints",
    weight=ProfileWeight.LIGHT,
    role=ProfileTableRole.OPERATIONAL,
    natural_key=("sync_name",),
    null_cols=("cursor_payload",),
    ingest_col="updated_at",
    cost_class=CostClass.CHEAP,
    sampling=SamplingPolicy(sample_pct=None),
    domain_checks=(),
)

# operating_* are pre-load (0 rows) — checks are written but auto-skip until
# data lands, then activate without a catalog change.
_CATALOG: tuple[TableProfileSpec, ...] = (
    # Wave 0–2: model-input core (full weight). The derived facts
    # (stock_metric_fact / common_feature_daily_fact) and the catalog/rule +
    # operating tables were decommissioned (refactor §5) — recomputed by the
    # DuckDB marts — so they are no longer profiled here.
    DAILY_OHLCV,
    KRX_SECURITY_FLOW_RAW,
    COMMON_FEATURE_OBSERVATION_RAW,
    DART_FINANCIAL_STATEMENT_RAW,
    DART_XBRL_FACT_RAW,
    DART_SHAREHOLDER_RETURN_RAW,
    DART_SHARE_COUNT_RAW,
    DART_XBRL_DOCUMENT,
    # Wave 3: masters / config (light weight)
    COMMON_FEATURE_SERIES,
    DART_CORP_MASTER,
    STOCK_MASTER,
    STOCK_MASTER_SNAPSHOT,
    STOCK_MASTER_SNAPSHOT_ITEMS,
    INGESTION_RUNS,
    SYNC_CHECKPOINTS,
)

_BY_TABLE: dict[str, TableProfileSpec] = {spec.table: spec for spec in _CATALOG}


def all_specs() -> tuple[TableProfileSpec, ...]:
    """Return every registered table profile spec, in priority order."""
    return _CATALOG


def get_spec(table: str) -> TableProfileSpec:
    """Return the spec for ``table`` or raise ``KeyError`` with the catalog."""
    try:
        return _BY_TABLE[table]
    except KeyError as exc:
        known = ", ".join(sorted(_BY_TABLE))
        raise KeyError(f"No profile spec for table {table!r}. Known tables: {known}") from exc


def specs_for_weights(weights: list[str]) -> list[TableProfileSpec]:
    """Return specs whose weight is in ``weights`` (catalog order preserved)."""
    wanted = {w.strip().lower() for w in weights if w.strip()}
    return [spec for spec in _CATALOG if spec.weight.value in wanted]


def specs_for_roles(roles: list[str]) -> list[TableProfileSpec]:
    """Return specs whose logical role is in ``roles`` (catalog order preserved)."""
    wanted = {r.strip().lower() for r in roles if r.strip()}
    return [spec for spec in _CATALOG if spec.role.value in wanted]


def specs_for_weights_and_roles(
    weights: list[str],
    roles: list[str] | None = None,
) -> list[TableProfileSpec]:
    """Return specs matching both weight and optional role filters."""
    wanted_weights = {w.strip().lower() for w in weights if w.strip()}
    wanted_roles = None if roles is None else {r.strip().lower() for r in roles if r.strip()}
    return [
        spec
        for spec in _CATALOG
        if spec.weight.value in wanted_weights
        and (wanted_roles is None or spec.role.value in wanted_roles)
    ]


def known_roles() -> frozenset[str]:
    """Return supported table-role labels."""
    return frozenset(role.value for role in ProfileTableRole)


def known_tables() -> frozenset[str]:
    """Return the set of table names the catalog knows about."""
    return frozenset(_BY_TABLE)
