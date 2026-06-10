from krx_collector.domain.enums import Source
from krx_collector.domain.models import (
    CommonFeatureCatalogEntry,
    CommonFeatureSeries,
    UpsertResult,
)
from krx_collector.service.default_common_feature_catalog import (
    default_common_feature_catalog,
    default_common_feature_series,
    seed_common_feature_catalog,
)


class MockCommonFeatureSeedStorage:
    def __init__(self) -> None:
        self.series: list[CommonFeatureSeries] = []
        self.catalog: list[CommonFeatureCatalogEntry] = []

    def upsert_common_feature_series(
        self,
        records: list[CommonFeatureSeries],
    ) -> UpsertResult:
        self.series = records
        return UpsertResult(updated=len(records))

    def upsert_common_feature_catalog(
        self,
        records: list[CommonFeatureCatalogEntry],
    ) -> UpsertResult:
        self.catalog = records
        return UpsertResult(updated=len(records))


def test_default_common_feature_series_ids_are_unique() -> None:
    series = default_common_feature_series()
    series_ids = [item.series_id for item in series]

    assert len(series_ids) == len(set(series_ids))


def test_default_common_feature_catalog_codes_are_unique() -> None:
    catalog = default_common_feature_catalog()
    feature_codes = [item.feature_code for item in catalog]

    assert len(feature_codes) == len(set(feature_codes))


def test_default_common_feature_catalog_inputs_exist_in_series_seed() -> None:
    series_ids = {item.series_id for item in default_common_feature_series()}

    missing_inputs = sorted(
        series_id
        for feature in default_common_feature_catalog()
        for series_id in feature.input_series_ids
        if series_id not in series_ids
    )

    assert missing_inputs == []


def test_default_common_feature_series_policies_match_pit_contract() -> None:
    series_by_id = {item.series_id: item for item in default_common_feature_series()}

    assert series_by_id["market_kospi"].availability_policy == "next_krx_session"
    assert series_by_id["market_kospi"].source_timezone == "Asia/Seoul"
    assert series_by_id["market_kosdaq"].availability_policy == "next_krx_session"
    assert series_by_id["global_sp500"].availability_policy == "same_krx_session_morning"
    assert series_by_id["global_sp500"].source_timezone == "America/New_York"
    assert series_by_id["global_nasdaq"].source_timezone == "America/New_York"
    assert series_by_id["global_vix"].source_timezone == "America/New_York"
    assert series_by_id["rate_kr_gov3y"].availability_policy == "next_krx_session"
    assert series_by_id["macro_cpi"].availability_policy == "manual_lag_days"
    assert series_by_id["macro_cpi"].manual_lag_days == 20


def test_default_common_feature_series_declares_stale_limits() -> None:
    series_by_id = {item.series_id: item for item in default_common_feature_series()}

    assert series_by_id["market_kospi"].max_stale_business_days == 5
    assert series_by_id["market_kosdaq"].max_stale_business_days == 5
    assert series_by_id["market_kospi200"].max_stale_business_days == 5
    assert series_by_id["global_sp500"].max_stale_business_days == 5
    assert series_by_id["global_nasdaq"].max_stale_business_days == 5
    assert series_by_id["global_vix"].max_stale_business_days == 5
    assert series_by_id["commodity_wti"].max_stale_business_days == 5
    assert series_by_id["fx_usdkrw"].max_stale_business_days == 10
    assert series_by_id["rate_kr_gov3y"].max_stale_business_days == 5
    assert series_by_id["macro_cpi"].max_stale_business_days == 45


def test_default_common_feature_catalog_contains_phase1_mvp_features() -> None:
    feature_codes = {item.feature_code for item in default_common_feature_catalog()}

    assert "market_kospi_close" in feature_codes
    assert "market_kospi_ret_1d" in feature_codes
    assert "global_sp500_ret_1d" in feature_codes
    assert "global_vix_level" in feature_codes
    assert "fx_usdkrw_level" in feature_codes
    assert "commodity_wti_ret_20d" in feature_codes


def test_default_common_feature_rate_kr_gov3y_is_active_after_pr4j() -> None:
    series_by_id = {item.series_id: item for item in default_common_feature_series()}
    catalog_by_code = {item.feature_code: item for item in default_common_feature_catalog()}

    rate = series_by_id["rate_kr_gov3y"]
    assert rate.source == Source.ECOS
    assert rate.active is True
    assert rate.source_series_key == "817Y002"
    assert rate.endpoint_params == {
        "stat_code": "817Y002",
        "cycle": "D",
        "item_code1": "010200000",
    }

    rate_feature = catalog_by_code["rate_kr_gov3y_level"]
    assert rate_feature.active is True
    assert rate_feature.input_series_ids == ("rate_kr_gov3y",)


def test_default_common_feature_macro_cpi_remains_inactive() -> None:
    series_by_id = {item.series_id: item for item in default_common_feature_series()}
    catalog_by_code = {item.feature_code: item for item in default_common_feature_catalog()}

    cpi = series_by_id["macro_cpi"]
    assert cpi.source == Source.ECOS
    assert cpi.active is False
    assert cpi.source_series_key == "901Y009"
    assert cpi.frequency == "M"
    assert cpi.unit == "2020=100"
    assert cpi.endpoint_params == {
        "stat_code": "901Y009",
        "cycle": "M",
        "item_code1": "0",
    }

    assert catalog_by_code["macro_cpi_level"].active is False
    assert catalog_by_code["macro_cpi_level"].frequency == "M"
    assert catalog_by_code["macro_cpi_level"].unit == "2020=100"
    assert catalog_by_code["macro_cpi_level"].input_series_ids == ("macro_cpi",)


def test_seed_common_feature_catalog_upserts_series_and_catalog() -> None:
    storage = MockCommonFeatureSeedStorage()

    result = seed_common_feature_catalog(storage)  # type: ignore[arg-type]

    assert result.series_upsert.updated == len(default_common_feature_series())
    assert result.catalog_upsert.updated == len(default_common_feature_catalog())
    assert [item.series_id for item in storage.series][:3] == [
        "market_kospi",
        "market_kosdaq",
        "market_kospi200",
    ]
    assert storage.catalog
    assert storage.catalog[0].feature_code == "market_kospi_close"
