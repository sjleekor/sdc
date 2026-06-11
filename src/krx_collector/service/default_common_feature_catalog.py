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
            active=False,
            notes="pykrx index fallback retained after KRX direct validation passed.",
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
            active=False,
            notes="pykrx index fallback retained after KRX direct validation passed.",
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
            active=False,
            notes="pykrx index fallback retained after KRX direct validation passed.",
        ),
        CommonFeatureSeries(
            series_id="market_kospi_krx",
            source=Source.KRX,
            source_series_key="1001",
            category="market_index",
            frequency="D",
            name_kr="KOSPI(KRX direct)",
            name_en="KOSPI Composite Index via KRX MDC",
            unit="index_point",
            country="KR",
            market="KOSPI",
            endpoint_params={
                "bld": "dbms/MDC/STAT/standard/MDCSTAT00301",
                "output_key": "output",
                "index_code": "1001",
                "indIdx": "1",
                "indIdx2": "001",
            },
            availability_policy="next_krx_session",
            source_timezone="Asia/Seoul",
            history_start_date=date(2000, 1, 1),
            max_stale_business_days=5,
            default_transform="level",
            notes=(
                "KRX direct source for KOSPI close. Activated after provider "
                "smoke, 3M DB build/readiness, and exact pykrx fallback "
                "comparison passed."
            ),
        ),
        CommonFeatureSeries(
            series_id="market_kosdaq_krx",
            source=Source.KRX,
            source_series_key="2001",
            category="market_index",
            frequency="D",
            name_kr="KOSDAQ(KRX direct)",
            name_en="KOSDAQ Composite Index via KRX MDC",
            unit="index_point",
            country="KR",
            market="KOSDAQ",
            endpoint_params={
                "bld": "dbms/MDC/STAT/standard/MDCSTAT00301",
                "output_key": "output",
                "index_code": "2001",
                "indIdx": "2",
                "indIdx2": "001",
            },
            availability_policy="next_krx_session",
            source_timezone="Asia/Seoul",
            history_start_date=date(2000, 1, 1),
            max_stale_business_days=5,
            default_transform="level",
            notes=(
                "KRX direct source for KOSDAQ close. Activated after provider "
                "smoke, 3M DB build/readiness, and exact pykrx fallback "
                "comparison passed."
            ),
        ),
        CommonFeatureSeries(
            series_id="market_kospi200_krx",
            source=Source.KRX,
            source_series_key="1028",
            category="market_index",
            frequency="D",
            name_kr="KOSPI200(KRX direct)",
            name_en="KOSPI 200 Index via KRX MDC",
            unit="index_point",
            country="KR",
            market="KOSPI",
            endpoint_params={
                "bld": "dbms/MDC/STAT/standard/MDCSTAT00301",
                "output_key": "output",
                "index_code": "1028",
                "indIdx": "1",
                "indIdx2": "028",
            },
            availability_policy="next_krx_session",
            source_timezone="Asia/Seoul",
            history_start_date=date(2000, 1, 1),
            max_stale_business_days=5,
            default_transform="level",
            notes=(
                "KRX direct source for KOSPI200 close. Activated after provider "
                "smoke, 3M DB build/readiness, and exact pykrx fallback "
                "comparison passed."
            ),
        ),
        CommonFeatureSeries(
            series_id="market_kospi_advancers_krx",
            source=Source.KRX,
            source_series_key="MDCSTAT01501:STK:advancers",
            category="market_breadth",
            frequency="D",
            name_kr="KOSPI 상승 종목 수",
            name_en="KOSPI advancing issues count",
            unit="count",
            country="KR",
            market="KOSPI",
            endpoint_params={
                "kind": "market_breadth",
                "bld": "dbms/MDC/STAT/standard/MDCSTAT01501",
                "output_key": "OutBlock_1",
                "mktId": "STK",
                "metric": "advancers",
            },
            availability_policy="next_krx_session",
            source_timezone="Asia/Seoul",
            history_start_date=date(2000, 1, 1),
            max_stale_business_days=5,
            default_transform="level",
            notes=(
                "KRX MDCSTAT01501 market breadth source. Activated after "
                "provider smoke and 3M DB build/coverage/readiness passed."
            ),
        ),
        CommonFeatureSeries(
            series_id="market_kospi_decliners_krx",
            source=Source.KRX,
            source_series_key="MDCSTAT01501:STK:decliners",
            category="market_breadth",
            frequency="D",
            name_kr="KOSPI 하락 종목 수",
            name_en="KOSPI declining issues count",
            unit="count",
            country="KR",
            market="KOSPI",
            endpoint_params={
                "kind": "market_breadth",
                "bld": "dbms/MDC/STAT/standard/MDCSTAT01501",
                "output_key": "OutBlock_1",
                "mktId": "STK",
                "metric": "decliners",
            },
            availability_policy="next_krx_session",
            source_timezone="Asia/Seoul",
            history_start_date=date(2000, 1, 1),
            max_stale_business_days=5,
            default_transform="level",
            notes=(
                "KRX MDCSTAT01501 market breadth source. Activated after "
                "provider smoke and 3M DB build/coverage/readiness passed."
            ),
        ),
        CommonFeatureSeries(
            series_id="market_kospi_unchanged_krx",
            source=Source.KRX,
            source_series_key="MDCSTAT01501:STK:unchanged",
            category="market_breadth",
            frequency="D",
            name_kr="KOSPI 보합 종목 수",
            name_en="KOSPI unchanged issues count",
            unit="count",
            country="KR",
            market="KOSPI",
            endpoint_params={
                "kind": "market_breadth",
                "bld": "dbms/MDC/STAT/standard/MDCSTAT01501",
                "output_key": "OutBlock_1",
                "mktId": "STK",
                "metric": "unchanged",
            },
            availability_policy="next_krx_session",
            source_timezone="Asia/Seoul",
            history_start_date=date(2000, 1, 1),
            max_stale_business_days=5,
            default_transform="level",
            notes=(
                "KRX MDCSTAT01501 market breadth source. Activated after "
                "provider smoke and 3M DB build/coverage/readiness passed."
            ),
        ),
        CommonFeatureSeries(
            series_id="market_kospi_turnover_value_krx",
            source=Source.KRX,
            source_series_key="MDCSTAT01501:STK:total_turnover_value",
            category="market_liquidity",
            frequency="D",
            name_kr="KOSPI 거래대금",
            name_en="KOSPI aggregate traded value",
            unit="KRW",
            country="KR",
            market="KOSPI",
            endpoint_params={
                "kind": "market_breadth",
                "bld": "dbms/MDC/STAT/standard/MDCSTAT01501",
                "output_key": "OutBlock_1",
                "mktId": "STK",
                "metric": "total_turnover_value",
            },
            availability_policy="next_krx_session",
            source_timezone="Asia/Seoul",
            history_start_date=date(2000, 1, 1),
            max_stale_business_days=5,
            default_transform="level",
            notes=(
                "KRX MDCSTAT01501 aggregate liquidity source. Activated after "
                "provider smoke and 3M DB build/coverage/readiness passed."
            ),
        ),
        CommonFeatureSeries(
            series_id="market_kosdaq_advancers_krx",
            source=Source.KRX,
            source_series_key="MDCSTAT01501:KSQ:advancers",
            category="market_breadth",
            frequency="D",
            name_kr="KOSDAQ 상승 종목 수",
            name_en="KOSDAQ advancing issues count",
            unit="count",
            country="KR",
            market="KOSDAQ",
            endpoint_params={
                "kind": "market_breadth",
                "bld": "dbms/MDC/STAT/standard/MDCSTAT01501",
                "output_key": "OutBlock_1",
                "mktId": "KSQ",
                "metric": "advancers",
            },
            availability_policy="next_krx_session",
            source_timezone="Asia/Seoul",
            history_start_date=date(2000, 1, 1),
            max_stale_business_days=5,
            default_transform="level",
            notes=(
                "KRX MDCSTAT01501 market breadth source. Activated after "
                "provider smoke and 3M DB build/coverage/readiness passed."
            ),
        ),
        CommonFeatureSeries(
            series_id="market_kosdaq_decliners_krx",
            source=Source.KRX,
            source_series_key="MDCSTAT01501:KSQ:decliners",
            category="market_breadth",
            frequency="D",
            name_kr="KOSDAQ 하락 종목 수",
            name_en="KOSDAQ declining issues count",
            unit="count",
            country="KR",
            market="KOSDAQ",
            endpoint_params={
                "kind": "market_breadth",
                "bld": "dbms/MDC/STAT/standard/MDCSTAT01501",
                "output_key": "OutBlock_1",
                "mktId": "KSQ",
                "metric": "decliners",
            },
            availability_policy="next_krx_session",
            source_timezone="Asia/Seoul",
            history_start_date=date(2000, 1, 1),
            max_stale_business_days=5,
            default_transform="level",
            notes=(
                "KRX MDCSTAT01501 market breadth source. Activated after "
                "provider smoke and 3M DB build/coverage/readiness passed."
            ),
        ),
        CommonFeatureSeries(
            series_id="market_kosdaq_unchanged_krx",
            source=Source.KRX,
            source_series_key="MDCSTAT01501:KSQ:unchanged",
            category="market_breadth",
            frequency="D",
            name_kr="KOSDAQ 보합 종목 수",
            name_en="KOSDAQ unchanged issues count",
            unit="count",
            country="KR",
            market="KOSDAQ",
            endpoint_params={
                "kind": "market_breadth",
                "bld": "dbms/MDC/STAT/standard/MDCSTAT01501",
                "output_key": "OutBlock_1",
                "mktId": "KSQ",
                "metric": "unchanged",
            },
            availability_policy="next_krx_session",
            source_timezone="Asia/Seoul",
            history_start_date=date(2000, 1, 1),
            max_stale_business_days=5,
            default_transform="level",
            notes=(
                "KRX MDCSTAT01501 market breadth source. Activated after "
                "provider smoke and 3M DB build/coverage/readiness passed."
            ),
        ),
        CommonFeatureSeries(
            series_id="market_kosdaq_turnover_value_krx",
            source=Source.KRX,
            source_series_key="MDCSTAT01501:KSQ:total_turnover_value",
            category="market_liquidity",
            frequency="D",
            name_kr="KOSDAQ 거래대금",
            name_en="KOSDAQ aggregate traded value",
            unit="KRW",
            country="KR",
            market="KOSDAQ",
            endpoint_params={
                "kind": "market_breadth",
                "bld": "dbms/MDC/STAT/standard/MDCSTAT01501",
                "output_key": "OutBlock_1",
                "mktId": "KSQ",
                "metric": "total_turnover_value",
            },
            availability_policy="next_krx_session",
            source_timezone="Asia/Seoul",
            history_start_date=date(2000, 1, 1),
            max_stale_business_days=5,
            default_transform="level",
            notes=(
                "KRX MDCSTAT01501 aggregate liquidity source. Activated after "
                "provider smoke and 3M DB build/coverage/readiness passed."
            ),
        ),
        CommonFeatureSeries(
            series_id="industry_krx_semiconductor_krx",
            source=Source.KRX,
            source_series_key="5044",
            category="industry_index",
            frequency="D",
            name_kr="KRX 반도체",
            name_en="KRX Semiconductor Index",
            unit="index_point",
            country="KR",
            market="KRX",
            endpoint_params={
                "bld": "dbms/MDC/STAT/standard/MDCSTAT00301",
                "output_key": "output",
                "index_code": "5044",
                "indIdx": "5",
                "indIdx2": "044",
            },
            availability_policy="next_krx_session",
            source_timezone="Asia/Seoul",
            history_start_date=date(2000, 1, 1),
            max_stale_business_days=5,
            default_transform="level",
            active=False,
            notes="Inactive KRX direct industry index candidate from finder_equidx.",
        ),
        CommonFeatureSeries(
            series_id="industry_kospi_electronics_krx",
            source=Source.KRX,
            source_series_key="1013",
            category="industry_index",
            frequency="D",
            name_kr="KOSPI 전기전자",
            name_en="KOSPI Electrical/Electronics Index",
            unit="index_point",
            country="KR",
            market="KOSPI",
            endpoint_params={
                "bld": "dbms/MDC/STAT/standard/MDCSTAT00301",
                "output_key": "output",
                "index_code": "1013",
                "indIdx": "1",
                "indIdx2": "013",
            },
            availability_policy="next_krx_session",
            source_timezone="Asia/Seoul",
            history_start_date=date(2000, 1, 1),
            max_stale_business_days=5,
            default_transform="level",
            active=False,
            notes="Inactive KRX direct industry index candidate from finder_equidx.",
        ),
        CommonFeatureSeries(
            series_id="industry_kospi_financials_krx",
            source=Source.KRX,
            source_series_key="1021",
            category="industry_index",
            frequency="D",
            name_kr="KOSPI 금융",
            name_en="KOSPI Financials Index",
            unit="index_point",
            country="KR",
            market="KOSPI",
            endpoint_params={
                "bld": "dbms/MDC/STAT/standard/MDCSTAT00301",
                "output_key": "output",
                "index_code": "1021",
                "indIdx": "1",
                "indIdx2": "021",
            },
            availability_policy="next_krx_session",
            source_timezone="Asia/Seoul",
            history_start_date=date(2000, 1, 1),
            max_stale_business_days=5,
            default_transform="level",
            active=False,
            notes="Inactive KRX direct industry index candidate from finder_equidx.",
        ),
        CommonFeatureSeries(
            series_id="industry_kosdaq_pharma_krx",
            source=Source.KRX,
            source_series_key="2066",
            category="industry_index",
            frequency="D",
            name_kr="KOSDAQ 제약",
            name_en="KOSDAQ Pharmaceuticals Index",
            unit="index_point",
            country="KR",
            market="KOSDAQ",
            endpoint_params={
                "bld": "dbms/MDC/STAT/standard/MDCSTAT00301",
                "output_key": "output",
                "index_code": "2066",
                "indIdx": "2",
                "indIdx2": "066",
            },
            availability_policy="next_krx_session",
            source_timezone="Asia/Seoul",
            history_start_date=date(2000, 1, 1),
            max_stale_business_days=5,
            default_transform="level",
            active=False,
            notes="Inactive KRX direct industry index candidate from finder_equidx.",
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
            series_id="fx_usdkrw_ecos",
            source=Source.ECOS,
            source_series_key="731Y001",
            category="fx",
            frequency="D",
            name_kr="원/미국달러 매매기준율",
            name_en="KRW per USD reference rate",
            unit="KRW",
            country="KR",
            market="FX",
            endpoint_params={
                "stat_code": "731Y001",
                "cycle": "D",
                "item_code1": "0000001",
            },
            availability_policy="next_krx_session",
            source_timezone="Asia/Seoul",
            history_start_date=date(1964, 5, 4),
            max_stale_business_days=10,
            default_transform="level",
            notes=(
                "ECOS official source for 원/미국달러(매매기준율). Activated after "
                "short smoke and 3M/12M coverage/readiness passed; existing "
                "fx_usdkrw_* feature inputs now use this series."
            ),
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
            series_id="rate_us2y",
            source=Source.FRED,
            source_series_key="DGS2",
            category="rate",
            frequency="D",
            name_kr="미국 국채 2년 수익률",
            name_en="Market Yield on U.S. Treasury Securities at 2-Year Constant Maturity",
            unit="pct",
            country="US",
            market="RATE",
            endpoint_params={"series_id": "DGS2"},
            availability_policy="same_krx_session_morning",
            source_timezone="America/New_York",
            history_start_date=date(1976, 6, 1),
            max_stale_business_days=5,
            default_transform="level",
            notes=(
                "FRED official source for US 2Y Treasury yield. Activated after "
                "short smoke and 3M/12M coverage/readiness passed."
            ),
        ),
        CommonFeatureSeries(
            series_id="rate_us10y",
            source=Source.FRED,
            source_series_key="DGS10",
            category="rate",
            frequency="D",
            name_kr="미국 국채 10년 수익률",
            name_en="Market Yield on U.S. Treasury Securities at 10-Year Constant Maturity",
            unit="pct",
            country="US",
            market="RATE",
            endpoint_params={"series_id": "DGS10"},
            availability_policy="same_krx_session_morning",
            source_timezone="America/New_York",
            history_start_date=date(1962, 1, 2),
            max_stale_business_days=5,
            default_transform="level",
            notes=(
                "FRED official source for US 10Y Treasury yield. Activated after "
                "short smoke and 3M/12M coverage/readiness passed."
            ),
        ),
        CommonFeatureSeries(
            series_id="commodity_wti_fred",
            source=Source.FRED,
            source_series_key="DCOILWTICO",
            category="commodity",
            frequency="D",
            name_kr="WTI 현물",
            name_en="Crude Oil Prices: West Texas Intermediate (WTI) - Cushing, Oklahoma",
            unit="USD/bbl",
            country="US",
            market="COMMODITY",
            endpoint_params={"series_id": "DCOILWTICO"},
            availability_policy="same_krx_session_morning",
            source_timezone="America/New_York",
            history_start_date=date(1986, 1, 2),
            max_stale_business_days=5,
            default_transform="level",
            notes=(
                "FRED official WTI spot source. Active as a separate spot "
                "feature after comparison showed material differences from the "
                "FDR CL=F futures fallback."
            ),
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
            series_id="rate_kr_gov10y",
            source=Source.ECOS,
            source_series_key="817Y002",
            category="rate",
            frequency="D",
            name_kr="국고채 10년 수익률",
            name_en="Korea Treasury Bond 10Y Yield",
            unit="pct",
            country="KR",
            market="RATE",
            endpoint_params={
                "stat_code": "817Y002",
                "cycle": "D",
                "item_code1": "010210000",
            },
            availability_policy="next_krx_session",
            source_timezone="Asia/Seoul",
            history_start_date=date(2000, 12, 18),
            max_stale_business_days=5,
            default_transform="level",
            notes=(
                "ECOS official source. Activated after 3M/12M operational-range "
                "verification passed (coverage 1.0000, null/missing/PIT "
                "violations 0). StatisticItemList metadata identifies "
                "item_code 010210000 as 국고채(10년)."
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
            notes=(
                "ECOS official CPI source. Activated after conservative "
                "period-end + 20 calendar day availability smoke passed for "
                "level, YoY, and MoM features. Replace with official release "
                "calendar when wired."
            ),
        ),
        CommonFeatureSeries(
            series_id="macro_ppi",
            source=Source.ECOS,
            source_series_key="404Y014",
            category="macro_price",
            frequency="M",
            name_kr="생산자물가지수",
            name_en="Producer Price Index",
            unit="2020=100",
            country="KR",
            market="MACRO",
            endpoint_params={
                "stat_code": "404Y014",
                "cycle": "M",
                "item_code1": "*AA",
            },
            availability_policy="manual_lag_days",
            manual_lag_days=20,
            source_timezone="Asia/Seoul",
            history_start_date=date(1965, 1, 1),
            max_stale_business_days=45,
            default_transform="level",
            notes=(
                "ECOS Producer Price Index total index. Activated after inactive "
                "smoke and active-only coverage/readiness passed under the "
                "conservative period-end + 20 calendar day availability policy."
            ),
        ),
        CommonFeatureSeries(
            series_id="macro_m2",
            source=Source.ECOS,
            source_series_key="161Y005",
            category="macro_money",
            frequency="M",
            name_kr="M2 광의통화",
            name_en="M2 broad money average outstanding seasonally adjusted",
            unit="KRW_bn",
            country="KR",
            market="MACRO",
            endpoint_params={
                "stat_code": "161Y005",
                "cycle": "M",
                "item_code1": "BBHS00",
            },
            availability_policy="manual_lag_days",
            manual_lag_days=20,
            source_timezone="Asia/Seoul",
            history_start_date=date(2003, 10, 1),
            max_stale_business_days=45,
            default_transform="level",
            notes=(
                "ECOS M2 average seasonally adjusted source. Activated after "
                "inactive smoke and active-only coverage/readiness passed under "
                "the conservative period-end + 20 calendar day availability policy."
            ),
        ),
        CommonFeatureSeries(
            series_id="macro_consumer_sentiment",
            source=Source.ECOS,
            source_series_key="511Y002",
            category="macro_sentiment",
            frequency="M",
            name_kr="소비자심리지수",
            name_en="Consumer Sentiment Index",
            unit="index",
            country="KR",
            market="MACRO",
            endpoint_params={
                "stat_code": "511Y002",
                "cycle": "M",
                "item_code1": "FME",
                "item_code2": "99988",
            },
            availability_policy="manual_lag_days",
            manual_lag_days=20,
            source_timezone="Asia/Seoul",
            history_start_date=date(2008, 7, 1),
            max_stale_business_days=45,
            default_transform="level",
            notes=(
                "ECOS consumer sentiment index. Activated after inactive smoke "
                "and active-only coverage/readiness passed under the conservative "
                "period-end + 20 calendar day availability policy."
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
            "market_kospi_krx",
        ),
        _feature(
            "market_kospi_ret_1d",
            "KOSPI 1일 수익률",
            "market_index",
            "pct",
            "ret_1d",
            "market_kospi_krx",
        ),
        _feature(
            "market_kospi_ret_5d",
            "KOSPI 5일 수익률",
            "market_index",
            "pct",
            "ret_5d",
            "market_kospi_krx",
        ),
        _feature(
            "market_kospi_ret_20d",
            "KOSPI 20일 수익률",
            "market_index",
            "pct",
            "ret_20d",
            "market_kospi_krx",
        ),
        _feature(
            "market_kosdaq_ret_1d",
            "KOSDAQ 1일 수익률",
            "market_index",
            "pct",
            "ret_1d",
            "market_kosdaq_krx",
        ),
        _feature(
            "market_kospi200_ret_1d",
            "KOSPI200 1일 수익률",
            "market_index",
            "pct",
            "ret_1d",
            "market_kospi200_krx",
        ),
        _feature(
            "market_kospi_krx_close",
            "KOSPI 종가(KRX direct)",
            "market_index",
            "index_point",
            "level",
            "market_kospi_krx",
            active=False,
            description=(
                "Inactive validation feature for KRX direct KOSPI close via "
                "MDCSTAT00301. Promote existing market_kospi_* inputs only "
                "after live smoke and readiness pass."
            ),
        ),
        _feature(
            "market_kospi_krx_ret_1d",
            "KOSPI 1일 수익률(KRX direct)",
            "market_index",
            "pct",
            "ret_1d",
            "market_kospi_krx",
            active=False,
            description="Inactive validation feature for KRX direct KOSPI 1-day return.",
        ),
        _feature(
            "market_kospi_krx_ret_5d",
            "KOSPI 5일 수익률(KRX direct)",
            "market_index",
            "pct",
            "ret_5d",
            "market_kospi_krx",
            active=False,
            description="Inactive validation feature for KRX direct KOSPI 5-day return.",
        ),
        _feature(
            "market_kospi_krx_ret_20d",
            "KOSPI 20일 수익률(KRX direct)",
            "market_index",
            "pct",
            "ret_20d",
            "market_kospi_krx",
            active=False,
            description="Inactive validation feature for KRX direct KOSPI 20-day return.",
        ),
        _feature(
            "market_kosdaq_krx_ret_1d",
            "KOSDAQ 1일 수익률(KRX direct)",
            "market_index",
            "pct",
            "ret_1d",
            "market_kosdaq_krx",
            active=False,
            description="Inactive validation feature for KRX direct KOSDAQ 1-day return.",
        ),
        _feature(
            "market_kospi200_krx_ret_1d",
            "KOSPI200 1일 수익률(KRX direct)",
            "market_index",
            "pct",
            "ret_1d",
            "market_kospi200_krx",
            active=False,
            description="Inactive validation feature for KRX direct KOSPI200 1-day return.",
        ),
        _feature(
            "market_kospi_advancers_count",
            "KOSPI 상승 종목 수",
            "market_breadth",
            "count",
            "level",
            "market_kospi_advancers_krx",
            description="KRX MDCSTAT01501 breadth feature activated after 3M readiness.",
        ),
        _feature(
            "market_kospi_decliners_count",
            "KOSPI 하락 종목 수",
            "market_breadth",
            "count",
            "level",
            "market_kospi_decliners_krx",
            description="KRX MDCSTAT01501 breadth feature activated after 3M readiness.",
        ),
        _feature(
            "market_kospi_unchanged_count",
            "KOSPI 보합 종목 수",
            "market_breadth",
            "count",
            "level",
            "market_kospi_unchanged_krx",
            description="KRX MDCSTAT01501 breadth feature activated after 3M readiness.",
        ),
        _feature(
            "market_kospi_turnover_value",
            "KOSPI 거래대금",
            "market_liquidity",
            "KRW",
            "level",
            "market_kospi_turnover_value_krx",
            description=(
                "KRX MDCSTAT01501 aggregate traded value feature activated after "
                "3M readiness."
            ),
        ),
        _feature(
            "market_kosdaq_advancers_count",
            "KOSDAQ 상승 종목 수",
            "market_breadth",
            "count",
            "level",
            "market_kosdaq_advancers_krx",
            description="KRX MDCSTAT01501 breadth feature activated after 3M readiness.",
        ),
        _feature(
            "market_kosdaq_decliners_count",
            "KOSDAQ 하락 종목 수",
            "market_breadth",
            "count",
            "level",
            "market_kosdaq_decliners_krx",
            description="KRX MDCSTAT01501 breadth feature activated after 3M readiness.",
        ),
        _feature(
            "market_kosdaq_unchanged_count",
            "KOSDAQ 보합 종목 수",
            "market_breadth",
            "count",
            "level",
            "market_kosdaq_unchanged_krx",
            description="KRX MDCSTAT01501 breadth feature activated after 3M readiness.",
        ),
        _feature(
            "market_kosdaq_turnover_value",
            "KOSDAQ 거래대금",
            "market_liquidity",
            "KRW",
            "level",
            "market_kosdaq_turnover_value_krx",
            description=(
                "KRX MDCSTAT01501 aggregate traded value feature activated after "
                "3M readiness."
            ),
        ),
        _feature(
            "industry_krx_semiconductor_level",
            "KRX 반도체 지수",
            "industry_index",
            "index_point",
            "level",
            "industry_krx_semiconductor_krx",
            active=False,
            description="Inactive KRX direct industry index validation feature.",
        ),
        _feature(
            "industry_krx_semiconductor_ret_1d",
            "KRX 반도체 1일 수익률",
            "industry_index",
            "pct",
            "ret_1d",
            "industry_krx_semiconductor_krx",
            active=False,
            description="Inactive KRX direct industry index validation feature.",
        ),
        _feature(
            "industry_kospi_electronics_level",
            "KOSPI 전기전자 지수",
            "industry_index",
            "index_point",
            "level",
            "industry_kospi_electronics_krx",
            active=False,
            description="Inactive KRX direct industry index validation feature.",
        ),
        _feature(
            "industry_kospi_electronics_ret_1d",
            "KOSPI 전기전자 1일 수익률",
            "industry_index",
            "pct",
            "ret_1d",
            "industry_kospi_electronics_krx",
            active=False,
            description="Inactive KRX direct industry index validation feature.",
        ),
        _feature(
            "industry_kospi_financials_level",
            "KOSPI 금융 지수",
            "industry_index",
            "index_point",
            "level",
            "industry_kospi_financials_krx",
            active=False,
            description="Inactive KRX direct industry index validation feature.",
        ),
        _feature(
            "industry_kospi_financials_ret_1d",
            "KOSPI 금융 1일 수익률",
            "industry_index",
            "pct",
            "ret_1d",
            "industry_kospi_financials_krx",
            active=False,
            description="Inactive KRX direct industry index validation feature.",
        ),
        _feature(
            "industry_kosdaq_pharma_level",
            "KOSDAQ 제약 지수",
            "industry_index",
            "index_point",
            "level",
            "industry_kosdaq_pharma_krx",
            active=False,
            description="Inactive KRX direct industry index validation feature.",
        ),
        _feature(
            "industry_kosdaq_pharma_ret_1d",
            "KOSDAQ 제약 1일 수익률",
            "industry_index",
            "pct",
            "ret_1d",
            "industry_kosdaq_pharma_krx",
            active=False,
            description="Inactive KRX direct industry index validation feature.",
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
            "fx_usdkrw_ecos",
        ),
        _feature(
            "fx_usdkrw_ret_5d",
            "USD/KRW 5일 수익률",
            "fx",
            "pct",
            "ret_5d",
            "fx_usdkrw_ecos",
        ),
        _feature(
            "fx_usdkrw_ecos_level",
            "USD/KRW 레벨(ECOS)",
            "fx",
            "KRW",
            "level",
            "fx_usdkrw_ecos",
            active=False,
            description=(
                "Inactive validation feature for ECOS official "
                "원/미국달러(매매기준율). Retained as a traceable validation "
                "feature after fx_usdkrw_level switched to ECOS."
            ),
        ),
        _feature(
            "fx_usdkrw_ecos_ret_5d",
            "USD/KRW 5일 수익률(ECOS)",
            "fx",
            "pct",
            "ret_5d",
            "fx_usdkrw_ecos",
            active=False,
            description=(
                "Inactive validation feature for ECOS official USD/KRW 5-day return. "
                "Retained as a traceable validation feature after fx_usdkrw_ret_5d "
                "switched to ECOS."
            ),
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
            "commodity_wti_spot_ret_20d",
            "WTI 현물 20일 수익률",
            "commodity",
            "pct",
            "ret_20d",
            "commodity_wti_fred",
            description=(
                "FRED WTI spot return based on DCOILWTICO. Kept as a separate "
                "active feature from commodity_wti_ret_20d because the existing "
                "feature uses FDR CL=F futures data and differs materially over "
                "the 12M comparison window."
            ),
        ),
        _feature(
            "commodity_wti_fred_ret_20d",
            "WTI 20일 수익률(FRED)",
            "commodity",
            "pct",
            "ret_20d",
            "commodity_wti_fred",
            active=False,
            description=(
                "Inactive FRED official WTI spot candidate. Activate only after "
                "FRED live smoke and coverage/readiness validation pass."
            ),
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
            "rate_kr_gov10y_level",
            "국고채 10년 수익률",
            "rate",
            "pct",
            "level",
            "rate_kr_gov10y",
            description=(
                "feature_date is a KRX session date; the value is the latest "
                "Korea Treasury Bond 10Y yield available by that session under "
                "the next_krx_session policy."
            ),
        ),
        _multi_input_feature(
            "rate_kr_term_spread_10y_3y",
            "국고채 10년-3년 스프레드",
            "rate",
            "pctp",
            "spread",
            inputs=(
                ("rate_kr_gov10y", "spread_long"),
                ("rate_kr_gov3y", "spread_short"),
            ),
            description=(
                "Calculated as Korea Treasury Bond 10Y yield minus 3Y yield "
                "using independently point-in-time aligned inputs."
            ),
        ),
        _feature(
            "rate_us2y_level",
            "미국 국채 2년 수익률",
            "rate",
            "pct",
            "level",
            "rate_us2y",
            description=(
                "feature_date is a KRX session date; the US 2Y yield is used "
                "only after same_krx_session_morning availability alignment. "
                "Activated after FRED short smoke and 3M/12M readiness passed."
            ),
        ),
        _feature(
            "rate_us10y_level",
            "미국 국채 10년 수익률",
            "rate",
            "pct",
            "level",
            "rate_us10y",
            description=(
                "feature_date is a KRX session date; the US 10Y yield is used "
                "only after same_krx_session_morning availability alignment. "
                "Activated after FRED short smoke and 3M/12M readiness passed."
            ),
        ),
        _multi_input_feature(
            "rate_us_term_spread_10y_2y",
            "미국 국채 10년-2년 스프레드",
            "rate",
            "pctp",
            "spread",
            inputs=(
                ("rate_us10y", "spread_long"),
                ("rate_us2y", "spread_short"),
            ),
            description=(
                "Calculated as US 10Y yield minus US 2Y yield using independently "
                "point-in-time aligned FRED inputs."
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
            description=(
                "Active ECOS CPI feature using conservative period-end + 20 "
                "calendar day availability until an official release calendar "
                "is wired."
            ),
        ),
        _feature(
            "macro_cpi_yoy_latest",
            "소비자물가지수 전년동월비",
            "macro_price",
            "pct",
            "yoy",
            "macro_cpi",
            frequency="M",
            description=(
                "Active CPI YoY feature calculated from exact calendar "
                "year-month matches after conservative monthly availability "
                "alignment."
            ),
        ),
        _feature(
            "macro_cpi_mom_latest",
            "소비자물가지수 전월비",
            "macro_price",
            "pct",
            "mom",
            "macro_cpi",
            frequency="M",
            description=(
                "Active CPI MoM feature calculated from the exact prior "
                "calendar month after conservative monthly availability "
                "alignment."
            ),
        ),
        _feature(
            "macro_ppi_level",
            "생산자물가지수",
            "macro_price",
            "2020=100",
            "level",
            "macro_ppi",
            frequency="M",
            description=(
                "Active ECOS PPI feature using conservative period-end + "
                "20 calendar day availability until an official release calendar "
                "is wired."
            ),
        ),
        _feature(
            "macro_ppi_yoy_latest",
            "생산자물가지수 전년동월비",
            "macro_price",
            "pct",
            "yoy",
            "macro_ppi",
            frequency="M",
            description=(
                "Active PPI YoY feature calculated from exact calendar year-month "
                "matches after conservative monthly availability alignment."
            ),
        ),
        _feature(
            "macro_ppi_mom_latest",
            "생산자물가지수 전월비",
            "macro_price",
            "pct",
            "mom",
            "macro_ppi",
            frequency="M",
            description=(
                "Active PPI MoM feature calculated from the exact prior calendar "
                "month after conservative monthly availability alignment."
            ),
        ),
        _feature(
            "macro_m2_level",
            "M2 광의통화",
            "macro_money",
            "KRW_bn",
            "level",
            "macro_m2",
            frequency="M",
            description=(
                "Active ECOS M2 feature using conservative period-end + "
                "20 calendar day availability until an official release calendar "
                "is wired."
            ),
        ),
        _feature(
            "macro_m2_yoy_latest",
            "M2 광의통화 전년동월비",
            "macro_money",
            "pct",
            "yoy",
            "macro_m2",
            frequency="M",
            description=(
                "Active M2 YoY feature calculated from exact calendar year-month "
                "matches after conservative monthly availability alignment."
            ),
        ),
        _feature(
            "macro_m2_mom_latest",
            "M2 광의통화 전월비",
            "macro_money",
            "pct",
            "mom",
            "macro_m2",
            frequency="M",
            description=(
                "Active M2 MoM feature calculated from the exact prior calendar "
                "month after conservative monthly availability alignment."
            ),
        ),
        _feature(
            "macro_consumer_sentiment_level",
            "소비자심리지수",
            "macro_sentiment",
            "index",
            "level",
            "macro_consumer_sentiment",
            frequency="M",
            description=(
                "Active ECOS consumer sentiment feature using conservative "
                "period-end + 20 calendar day availability until an official "
                "release calendar is wired."
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


def _multi_input_feature(
    feature_code: str,
    feature_name_kr: str,
    category: str,
    unit: str,
    transform_code: str,
    *,
    inputs: tuple[tuple[str, str], ...],
    frequency: str = "D",
    active: bool = True,
    description: str = "",
) -> CommonFeatureCatalogEntry:
    return CommonFeatureCatalogEntry(
        feature_code=feature_code,
        feature_name_kr=feature_name_kr,
        category=category,
        frequency=frequency,
        unit=unit,
        transform_code=transform_code,
        description=description,
        input_series_ids=tuple(series_id for series_id, _ in inputs),
        input_roles=tuple(role for _, role in inputs),
        active=active,
    )
