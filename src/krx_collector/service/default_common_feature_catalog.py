"""Default Phase 1 common feature series and model-facing feature catalog."""

from __future__ import annotations

from datetime import date

from krx_collector.domain.enums import Source
from krx_collector.domain.models import (
    CommonFeatureCatalogEntry,
    CommonFeatureCatalogSeedResult,
    CommonFeatureSeries,
)
from krx_collector.ports.storage import Storage


def default_common_feature_series() -> list[CommonFeatureSeries]:
    """Return source-series seeds for the Phase 1 common feature MVP."""
    return [
        CommonFeatureSeries(
            series_id="market_kospi",
            source=Source.PYKRX,
            source_series_key="1001",
            category="market_index",
            frequency="D",
            name_kr="KOSPI",
            name_en="KOSPI Composite Index",
            unit="index_point",
            country="KR",
            market="KOSPI",
            endpoint_params={"index_code": "1001"},
            availability_policy="next_krx_session",
            source_timezone="Asia/Seoul",
            history_start_date=date(2000, 1, 1),
            max_stale_business_days=5,
            default_transform="level",
            notes="pykrx index fallback for KOSPI close.",
        ),
        CommonFeatureSeries(
            series_id="market_kosdaq",
            source=Source.PYKRX,
            source_series_key="2001",
            category="market_index",
            frequency="D",
            name_kr="KOSDAQ",
            name_en="KOSDAQ Composite Index",
            unit="index_point",
            country="KR",
            market="KOSDAQ",
            endpoint_params={"index_code": "2001"},
            availability_policy="next_krx_session",
            source_timezone="Asia/Seoul",
            history_start_date=date(2000, 1, 1),
            max_stale_business_days=5,
            default_transform="level",
            notes="pykrx index fallback for KOSDAQ close.",
        ),
        CommonFeatureSeries(
            series_id="market_kospi200",
            source=Source.PYKRX,
            source_series_key="1028",
            category="market_index",
            frequency="D",
            name_kr="KOSPI200",
            name_en="KOSPI 200 Index",
            unit="index_point",
            country="KR",
            market="KOSPI",
            endpoint_params={"index_code": "1028"},
            availability_policy="next_krx_session",
            source_timezone="Asia/Seoul",
            history_start_date=date(2000, 1, 1),
            max_stale_business_days=5,
            default_transform="level",
            notes="pykrx index fallback for KOSPI200 close.",
        ),
        CommonFeatureSeries(
            series_id="global_sp500",
            source=Source.FDR,
            source_series_key="US500",
            category="global_index",
            frequency="D",
            name_kr="S&P500",
            name_en="S&P 500 Index",
            unit="index_point",
            country="US",
            market="US",
            endpoint_params={"symbol": "US500"},
            availability_policy="same_krx_session_morning",
            source_timezone="America/New_York",
            history_start_date=date(2000, 1, 1),
            max_stale_business_days=5,
            default_transform="level",
            notes="FDR global index fallback; usable on the next KRX morning.",
        ),
        CommonFeatureSeries(
            series_id="global_nasdaq",
            source=Source.FDR,
            source_series_key="IXIC",
            category="global_index",
            frequency="D",
            name_kr="NASDAQ Composite",
            name_en="NASDAQ Composite Index",
            unit="index_point",
            country="US",
            market="US",
            endpoint_params={"symbol": "IXIC"},
            availability_policy="same_krx_session_morning",
            source_timezone="America/New_York",
            history_start_date=date(2000, 1, 1),
            max_stale_business_days=5,
            default_transform="level",
            notes="FDR global index fallback; usable on the next KRX morning.",
        ),
        CommonFeatureSeries(
            series_id="global_vix",
            source=Source.FDR,
            source_series_key="VIX",
            category="global_risk",
            frequency="D",
            name_kr="VIX",
            name_en="CBOE Volatility Index",
            unit="index_point",
            country="US",
            market="US",
            endpoint_params={"symbol": "VIX"},
            availability_policy="same_krx_session_morning",
            source_timezone="America/New_York",
            history_start_date=date(2000, 1, 1),
            max_stale_business_days=5,
            default_transform="level",
            notes="FDR volatility index fallback; usable on the next KRX morning.",
        ),
        CommonFeatureSeries(
            series_id="fx_usdkrw",
            source=Source.FDR,
            source_series_key="USD/KRW",
            category="fx",
            frequency="D",
            name_kr="USD/KRW",
            name_en="US dollar to Korean won",
            unit="KRW",
            country="KR",
            market="FX",
            endpoint_params={"symbol": "USD/KRW"},
            availability_policy="next_krx_session",
            source_timezone="Asia/Seoul",
            history_start_date=date(2000, 1, 1),
            max_stale_business_days=10,
            default_transform="level",
            notes="Conservative FDR FX fallback until an official ECOS source is wired.",
        ),
        CommonFeatureSeries(
            series_id="commodity_wti",
            source=Source.FDR,
            source_series_key="CL=F",
            category="commodity",
            frequency="D",
            name_kr="WTI",
            name_en="WTI crude oil front-month futures",
            unit="USD/bbl",
            country="US",
            market="COMMODITY",
            endpoint_params={"symbol": "CL=F"},
            availability_policy="same_krx_session_morning",
            source_timezone="America/New_York",
            history_start_date=date(2000, 1, 1),
            max_stale_business_days=5,
            default_transform="level",
            notes="FDR commodity fallback; official provider can replace later.",
        ),
        CommonFeatureSeries(
            series_id="rate_kr_gov3y",
            source=Source.ECOS,
            source_series_key="817Y002",
            category="rate",
            frequency="D",
            name_kr="국고채 3년 수익률",
            name_en="Korea Treasury Bond 3Y Yield",
            unit="pct",
            country="KR",
            market="RATE",
            endpoint_params={
                "stat_code": "817Y002",
                "cycle": "D",
                "item_code1": "010200000",
            },
            availability_policy="next_krx_session",
            source_timezone="Asia/Seoul",
            history_start_date=date(2001, 1, 1),
            max_stale_business_days=5,
            default_transform="level",
            notes=(
                "ECOS official source. Activated in PR 4-J after 3M/12M "
                "operational-range verification passed (coverage 1.0000, "
                "null/missing/PIT violations 0)."
            ),
        ),
        CommonFeatureSeries(
            series_id="macro_cpi",
            source=Source.ECOS,
            source_series_key="901Y009",
            category="macro_price",
            frequency="M",
            name_kr="소비자물가지수",
            name_en="Consumer Price Index",
            unit="2020=100",
            country="KR",
            market="MACRO",
            endpoint_params={
                "stat_code": "901Y009",
                "cycle": "M",
                "item_code1": "0",
            },
            availability_policy="manual_lag_days",
            manual_lag_days=20,
            source_timezone="Asia/Seoul",
            history_start_date=date(2000, 1, 1),
            max_stale_business_days=45,
            default_transform="level",
            active=False,
            notes=(
                "ECOS source catalog candidate. Live smoke verified stat/item "
                "code and unit; keep inactive until release lag policy is "
                "accepted for model exposure."
            ),
        ),
    ]


def default_common_feature_catalog() -> list[CommonFeatureCatalogEntry]:
    """Return model-facing Phase 1 common feature catalog seeds."""
    return [
        _feature(
            "market_kospi_close",
            "KOSPI 종가",
            "market_index",
            "index_point",
            "level",
            "market_kospi",
        ),
        _feature(
            "market_kospi_ret_1d",
            "KOSPI 1일 수익률",
            "market_index",
            "pct",
            "ret_1d",
            "market_kospi",
        ),
        _feature(
            "market_kospi_ret_5d",
            "KOSPI 5일 수익률",
            "market_index",
            "pct",
            "ret_5d",
            "market_kospi",
        ),
        _feature(
            "market_kospi_ret_20d",
            "KOSPI 20일 수익률",
            "market_index",
            "pct",
            "ret_20d",
            "market_kospi",
        ),
        _feature(
            "market_kosdaq_ret_1d",
            "KOSDAQ 1일 수익률",
            "market_index",
            "pct",
            "ret_1d",
            "market_kosdaq",
        ),
        _feature(
            "market_kospi200_ret_1d",
            "KOSPI200 1일 수익률",
            "market_index",
            "pct",
            "ret_1d",
            "market_kospi200",
        ),
        _feature(
            "global_sp500_ret_1d",
            "S&P500 1일 수익률",
            "global_index",
            "pct",
            "ret_1d",
            "global_sp500",
        ),
        _feature(
            "global_nasdaq_ret_1d",
            "NASDAQ 1일 수익률",
            "global_index",
            "pct",
            "ret_1d",
            "global_nasdaq",
        ),
        _feature(
            "global_vix_level",
            "VIX 레벨",
            "global_risk",
            "index_point",
            "level",
            "global_vix",
        ),
        _feature(
            "fx_usdkrw_level",
            "USD/KRW 레벨",
            "fx",
            "KRW",
            "level",
            "fx_usdkrw",
        ),
        _feature(
            "fx_usdkrw_ret_5d",
            "USD/KRW 5일 수익률",
            "fx",
            "pct",
            "ret_5d",
            "fx_usdkrw",
        ),
        _feature(
            "commodity_wti_ret_20d",
            "WTI 20일 수익률",
            "commodity",
            "pct",
            "ret_20d",
            "commodity_wti",
        ),
        _feature(
            "rate_kr_gov3y_level",
            "국고채 3년 수익률",
            "rate",
            "pct",
            "level",
            "rate_kr_gov3y",
            description=(
                "feature_date is a KRX session date; the value is the latest "
                "Korea Treasury Bond 3Y yield available by that session under "
                "the next_krx_session policy. Activated in PR 4-J."
            ),
        ),
        _feature(
            "macro_cpi_level",
            "소비자물가지수",
            "macro_price",
            "2020=100",
            "level",
            "macro_cpi",
            frequency="M",
            active=False,
            description=(
                "Inactive ECOS candidate. Activate only after ECOS code smoke "
                "verifies the item code and conservative release lag."
            ),
        ),
    ]


def seed_common_feature_catalog(storage: Storage) -> CommonFeatureCatalogSeedResult:
    """Seed Phase 1 common feature source series and model catalog rows."""
    result = CommonFeatureCatalogSeedResult()
    result.series_upsert = storage.upsert_common_feature_series(default_common_feature_series())
    result.catalog_upsert = storage.upsert_common_feature_catalog(default_common_feature_catalog())
    return result


def _feature(
    feature_code: str,
    feature_name_kr: str,
    category: str,
    unit: str,
    transform_code: str,
    series_id: str,
    frequency: str = "D",
    active: bool = True,
    description: str | None = None,
) -> CommonFeatureCatalogEntry:
    return CommonFeatureCatalogEntry(
        feature_code=feature_code,
        feature_name_kr=feature_name_kr,
        category=category,
        frequency=frequency,
        unit=unit,
        transform_code=transform_code,
        description=description
        or (
            "feature_date is a KRX session date; the value is built only from "
            "finite numeric observations available by that session. Non-finite "
            "source values are skipped during raw sync."
        ),
        input_series_ids=(series_id,),
        active=active,
    )
