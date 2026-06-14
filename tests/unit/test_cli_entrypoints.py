from datetime import date
from decimal import Decimal
from types import SimpleNamespace

import pytest

from krx_collector.cli import app
from krx_collector.domain.enums import Source
from krx_collector.domain.models import (
    CommonFeatureBuildResult,
    CommonFeatureCoverageReport,
    CommonFeatureCoverageRow,
    CommonFeatureReadinessReport,
    CommonFeatureReadinessRow,
    CommonFeatureSyncResult,
    UpsertResult,
)
from krx_collector.service.freshness import (
    CommonFreshnessAssertResult,
    CommonFreshnessViolation,
)


def test_dart_main_prefixes_dart_subcommand(monkeypatch) -> None:
    captured: dict[str, list[str] | None] = {}

    def fake_main(argv: list[str] | None = None) -> None:
        captured["argv"] = argv

    monkeypatch.setattr(app, "main", fake_main)

    app.dart_main(["sync-corp"])

    assert captured["argv"] == ["dart", "sync-corp"]


def test_dart_main_uses_sys_argv_when_not_given(monkeypatch) -> None:
    captured: dict[str, list[str] | None] = {}

    def fake_main(argv: list[str] | None = None) -> None:
        captured["argv"] = argv

    monkeypatch.setattr(app, "main", fake_main)
    monkeypatch.setattr("sys.argv", ["dart", "sync-corp", "--force"])

    app.dart_main()

    assert captured["argv"] == ["dart", "sync-corp", "--force"]


def test_common_seed_catalog_parser_supports_init_schema() -> None:
    args = app.build_parser().parse_args(["common", "seed-catalog", "--init-schema"])

    assert args.command == "common"
    assert args.common_command == "seed-catalog"
    assert args.init_schema is True
    assert args.handler == app._handle_common_seed_catalog


def test_common_sync_parser_supports_sources_and_filters() -> None:
    args = app.build_parser().parse_args(
        [
            "common",
            "sync",
            "--sources",
            "pykrx,fdr",
            "--series",
            "market_kospi,global_sp500",
            "--start",
            "2026-06-01",
            "--end",
            "2026-06-08",
            "--force",
            "--rate-limit-seconds",
            "0.5",
            "--include-inactive",
            "--init-schema",
        ]
    )

    assert args.command == "common"
    assert args.common_command == "sync"
    assert args.sources == [Source.PYKRX, Source.FDR]
    assert args.series == "market_kospi,global_sp500"
    assert args.start == date(2026, 6, 1)
    assert args.end == date(2026, 6, 8)
    assert args.force is True
    assert args.rate_limit_seconds == 0.5
    assert args.include_inactive is True
    assert args.init_schema is True
    assert args.handler == app._handle_common_sync


def test_common_sync_parser_defaults_to_pykrx_and_fdr() -> None:
    args = app.build_parser().parse_args(
        [
            "common",
            "sync",
            "--start",
            "2026-06-01",
            "--end",
            "2026-06-08",
        ]
    )

    assert args.sources == [Source.PYKRX, Source.FDR]
    assert args.series is None
    assert args.force is False
    assert args.rate_limit_seconds == 0.0
    assert args.include_inactive is False


def test_common_sync_parser_supports_ecos_source() -> None:
    args = app.build_parser().parse_args(
        [
            "common",
            "sync",
            "--sources",
            "ecos",
            "--series",
            "rate_kr_gov3y",
            "--start",
            "2026-06-01",
            "--end",
            "2026-06-08",
            "--include-inactive",
        ]
    )

    assert args.sources == [Source.ECOS]
    assert args.series == "rate_kr_gov3y"
    assert args.include_inactive is True


def test_common_sync_parser_supports_fred_source() -> None:
    args = app.build_parser().parse_args(
        [
            "common",
            "sync",
            "--sources",
            "fred",
            "--series",
            "rate_us10y",
            "--start",
            "2026-06-01",
            "--end",
            "2026-06-08",
            "--include-inactive",
        ]
    )

    assert args.sources == [Source.FRED]
    assert args.series == "rate_us10y"
    assert args.include_inactive is True


def test_common_sync_parser_rejects_unknown_source() -> None:
    with pytest.raises(SystemExit):
        app.build_parser().parse_args(
            [
                "common",
                "sync",
                "--sources",
                "kosis",
                "--start",
                "2026-06-01",
                "--end",
                "2026-06-08",
            ]
        )


def test_common_build_daily_parser_supports_feature_codes_and_init_schema() -> None:
    args = app.build_parser().parse_args(
        [
            "common",
            "build-daily",
            "--feature-codes",
            "market_kospi_close,global_sp500_ret_1d",
            "--start",
            "2026-06-01",
            "--end",
            "2026-06-08",
            "--include-inactive",
            "--init-schema",
        ]
    )

    assert args.command == "common"
    assert args.common_command == "build-daily"
    assert args.feature_codes == "market_kospi_close,global_sp500_ret_1d"
    assert args.start == date(2026, 6, 1)
    assert args.end == date(2026, 6, 8)
    assert args.include_inactive is True
    assert args.init_schema is True
    assert args.handler == app._handle_common_build_daily


def test_common_coverage_report_parser_supports_feature_codes() -> None:
    args = app.build_parser().parse_args(
        [
            "common",
            "coverage-report",
            "--feature-codes",
            "global_vix_level,fx_usdkrw_level",
            "--start",
            "2026-06-01",
            "--end",
            "2026-06-08",
            "--include-inactive",
        ]
    )

    assert args.command == "common"
    assert args.common_command == "coverage-report"
    assert args.feature_codes == "global_vix_level,fx_usdkrw_level"
    assert args.start == date(2026, 6, 1)
    assert args.end == date(2026, 6, 8)
    assert args.include_inactive is True
    assert args.handler == app._handle_common_coverage_report


def test_common_readiness_report_parser_supports_feature_codes_and_threshold() -> None:
    args = app.build_parser().parse_args(
        [
            "common",
            "readiness-report",
            "--feature-codes",
            "rate_kr_gov3y_level,macro_cpi_level",
            "--start",
            "2026-06-01",
            "--end",
            "2026-06-08",
            "--required-coverage-ratio",
            "0.995",
            "--include-inactive",
        ]
    )

    assert args.command == "common"
    assert args.common_command == "readiness-report"
    assert args.feature_codes == "rate_kr_gov3y_level,macro_cpi_level"
    assert args.start == date(2026, 6, 1)
    assert args.end == date(2026, 6, 8)
    assert args.required_coverage_ratio == Decimal("0.9950")
    assert args.include_inactive is True
    assert args.fail_on_not_ready is False
    assert args.handler == app._handle_common_readiness_report


def test_common_readiness_report_parser_defaults_to_strict_coverage() -> None:
    args = app.build_parser().parse_args(
        [
            "common",
            "readiness-report",
            "--start",
            "2026-06-01",
            "--end",
            "2026-06-08",
        ]
    )

    assert args.required_coverage_ratio == Decimal("1.0000")
    assert args.include_inactive is False
    assert args.fail_on_not_ready is False


def test_common_readiness_report_parser_supports_fail_on_not_ready() -> None:
    args = app.build_parser().parse_args(
        [
            "common",
            "readiness-report",
            "--start",
            "2026-06-01",
            "--end",
            "2026-06-08",
            "--fail-on-not-ready",
        ]
    )

    assert args.fail_on_not_ready is True


def test_ops_assert_common_freshness_parser_defaults_to_required_sources() -> None:
    args = app.build_parser().parse_args(["ops", "assert-common-freshness"])

    assert args.command == "ops"
    assert args.ops_command == "assert-common-freshness"
    assert args.sources == [Source.FDR, Source.FRED, Source.ECOS, Source.KRX]
    assert args.end is None
    assert args.max_run_age_hours == 30
    assert args.daily_max_lag_days == 2
    assert args.macro_max_lag_days == 45
    assert args.series is None
    assert args.handler == app._handle_ops_assert_common_freshness


def test_ops_assert_common_freshness_parser_supports_overrides() -> None:
    args = app.build_parser().parse_args(
        [
            "ops",
            "assert-common-freshness",
            "--sources",
            "fdr,krx",
            "--end",
            "2026-06-13",
            "--max-run-age-hours",
            "48",
            "--daily-max-lag-days",
            "3",
            "--macro-max-lag-days",
            "60",
            "--series",
            "market_kospi,market_kosdaq",
        ]
    )

    assert args.sources == [Source.FDR, Source.KRX]
    assert args.end == date(2026, 6, 13)
    assert args.max_run_age_hours == 48
    assert args.daily_max_lag_days == 3
    assert args.macro_max_lag_days == 60
    assert args.series == "market_kospi,market_kosdaq"


@pytest.mark.parametrize("value", ["-0.1", "1.1", "nan", "Infinity", "abc"])
def test_common_readiness_report_parser_rejects_invalid_threshold(value: str) -> None:
    with pytest.raises(SystemExit):
        app.build_parser().parse_args(
            [
                "common",
                "readiness-report",
                "--start",
                "2026-06-01",
                "--end",
                "2026-06-08",
                "--required-coverage-ratio",
                value,
            ]
        )


def test_handle_common_seed_catalog_calls_storage_and_seed(monkeypatch) -> None:
    calls: dict[str, object] = {}

    class FakeStorage:
        def __init__(self, dsn: str) -> None:
            calls["dsn"] = dsn
            calls["storage"] = self
            self.init_schema_called = False

        def init_schema(self) -> None:
            self.init_schema_called = True

    def fake_seed(storage: FakeStorage):
        calls["seed_storage"] = storage
        return SimpleNamespace(
            series_upsert=UpsertResult(updated=8),
            catalog_upsert=UpsertResult(updated=12),
        )

    monkeypatch.setattr(app, "get_settings", lambda: SimpleNamespace(db_dsn="postgresql://test"))
    monkeypatch.setattr(
        "krx_collector.infra.db_postgres.repositories.PostgresStorage",
        FakeStorage,
    )
    monkeypatch.setattr(
        "krx_collector.service.default_common_feature_catalog.seed_common_feature_catalog",
        fake_seed,
    )

    args = app.build_parser().parse_args(["common", "seed-catalog", "--init-schema"])
    app._handle_common_seed_catalog(args)

    storage = calls["storage"]
    assert calls["dsn"] == "postgresql://test"
    assert calls["seed_storage"] is storage
    assert storage.init_schema_called is True


def test_handle_ops_assert_common_freshness_calls_service(monkeypatch) -> None:
    calls: dict[str, object] = {}

    class FakeStorage:
        def __init__(self, dsn: str) -> None:
            calls["dsn"] = dsn
            calls["storage"] = self

    def fake_assert_common_freshness(**kwargs):
        calls["assert_kwargs"] = kwargs
        return CommonFreshnessAssertResult(
            sources=kwargs["sources"],
            end=kwargs["end"],
            checked_series=4,
        )

    monkeypatch.setattr(app, "get_settings", lambda: SimpleNamespace(db_dsn="postgresql://test"))
    monkeypatch.setattr(
        "krx_collector.infra.db_postgres.repositories.PostgresStorage",
        FakeStorage,
    )
    monkeypatch.setattr(
        "krx_collector.service.freshness.assert_common_freshness",
        fake_assert_common_freshness,
    )

    args = app.build_parser().parse_args(
        [
            "ops",
            "assert-common-freshness",
            "--sources",
            "fdr,krx",
            "--end",
            "2026-06-13",
            "--series",
            "market_kospi,market_kosdaq",
        ]
    )
    app._handle_ops_assert_common_freshness(args)

    assert calls["dsn"] == "postgresql://test"
    assert calls["assert_kwargs"]["storage"] is calls["storage"]
    assert calls["assert_kwargs"]["sources"] == [Source.FDR, Source.KRX]
    assert calls["assert_kwargs"]["end"] == date(2026, 6, 13)
    assert calls["assert_kwargs"]["series_ids"] == ["market_kospi", "market_kosdaq"]


def test_handle_ops_assert_common_freshness_exits_on_failure(monkeypatch) -> None:
    class FakeStorage:
        def __init__(self, dsn: str) -> None:
            self.dsn = dsn

    def fake_assert_common_freshness(**kwargs):
        return CommonFreshnessAssertResult(
            sources=kwargs["sources"],
            end=kwargs["end"],
            checked_series=1,
            violations=[
                CommonFreshnessViolation(
                    source=Source.KRX,
                    check="latest_observation",
                    message="stale",
                    series_id="market_kospi",
                )
            ],
        )

    monkeypatch.setattr(app, "get_settings", lambda: SimpleNamespace(db_dsn="postgresql://test"))
    monkeypatch.setattr(
        "krx_collector.infra.db_postgres.repositories.PostgresStorage",
        FakeStorage,
    )
    monkeypatch.setattr(
        "krx_collector.service.freshness.assert_common_freshness",
        fake_assert_common_freshness,
    )

    args = app.build_parser().parse_args(
        ["ops", "assert-common-freshness", "--sources", "krx"]
    )

    with pytest.raises(SystemExit) as exc_info:
        app._handle_ops_assert_common_freshness(args)

    assert exc_info.value.code == 2


def test_handle_common_sync_calls_service_with_providers(monkeypatch) -> None:
    calls: dict[str, object] = {}
    fake_providers = [object(), object()]

    class FakeStorage:
        def __init__(self, dsn: str) -> None:
            calls["dsn"] = dsn
            calls["storage"] = self
            self.init_schema_called = False

        def init_schema(self) -> None:
            self.init_schema_called = True

    def fake_build_providers(sources: list[object]) -> list[object]:
        calls["provider_sources"] = sources
        return fake_providers

    def fake_sync_common_features(**kwargs):
        calls["sync_kwargs"] = kwargs
        return CommonFeatureSyncResult(
            series_processed=2,
            requests_attempted=2,
            rows_upserted=2,
        )

    monkeypatch.setattr(app, "get_settings", lambda: SimpleNamespace(db_dsn="postgresql://test"))
    monkeypatch.setattr(app, "_build_common_feature_providers", fake_build_providers)
    monkeypatch.setattr(
        "krx_collector.infra.db_postgres.repositories.PostgresStorage",
        FakeStorage,
    )
    monkeypatch.setattr(
        "krx_collector.service.sync_common_features.sync_common_features",
        fake_sync_common_features,
    )

    args = app.build_parser().parse_args(
        [
            "common",
            "sync",
            "--sources",
            "pykrx,fdr",
            "--series",
            "market_kospi,global_sp500",
            "--start",
            "2026-06-01",
            "--end",
            "2026-06-08",
            "--force",
            "--rate-limit-seconds",
            "0.5",
            "--include-inactive",
            "--init-schema",
        ]
    )
    app._handle_common_sync(args)

    storage = calls["storage"]
    sync_kwargs = calls["sync_kwargs"]
    assert calls["dsn"] == "postgresql://test"
    assert storage.init_schema_called is True
    assert calls["provider_sources"] == [Source.PYKRX, Source.FDR]
    assert sync_kwargs["providers"] == fake_providers
    assert sync_kwargs["storage"] is storage
    assert sync_kwargs["start"] == date(2026, 6, 1)
    assert sync_kwargs["end"] == date(2026, 6, 8)
    assert sync_kwargs["sources"] == [Source.PYKRX, Source.FDR]
    assert sync_kwargs["series_ids"] == ["market_kospi", "global_sp500"]
    assert sync_kwargs["active_only"] is False
    assert sync_kwargs["force"] is True
    assert sync_kwargs["rate_limit_seconds"] == 0.5


def test_handle_common_sync_rejects_include_inactive_without_series() -> None:
    args = app.build_parser().parse_args(
        [
            "common",
            "sync",
            "--sources",
            "ecos",
            "--start",
            "2026-06-01",
            "--end",
            "2026-06-08",
            "--include-inactive",
        ]
    )

    with pytest.raises(SystemExit, match="--include-inactive requires"):
        app._handle_common_sync(args)


def test_build_common_feature_providers_supports_ecos() -> None:
    providers = app._build_common_feature_providers([Source.ECOS])

    assert len(providers) == 1
    assert providers[0].source() == Source.ECOS


def test_build_common_feature_providers_supports_fred() -> None:
    providers = app._build_common_feature_providers([Source.FRED])

    assert len(providers) == 1
    assert providers[0].source() == Source.FRED


def test_build_common_feature_providers_supports_krx(monkeypatch) -> None:
    class FakeKrxProvider:
        def source(self) -> Source:
            return Source.KRX

    monkeypatch.setattr(
        "krx_collector.adapters.common_features_krx.KrxCommonFeatureProvider",
        FakeKrxProvider,
    )

    providers = app._build_common_feature_providers([Source.KRX])

    assert len(providers) == 1
    assert providers[0].source() == Source.KRX


def test_common_sync_parser_supports_krx_source() -> None:
    args = app.build_parser().parse_args(
        [
            "common",
            "sync",
            "--sources",
            "krx",
            "--series",
            "market_kospi_krx",
            "--start",
            "2026-06-01",
            "--end",
            "2026-06-08",
            "--include-inactive",
        ]
    )

    assert args.sources == [Source.KRX]


def test_handle_common_build_daily_calls_service(monkeypatch) -> None:
    calls: dict[str, object] = {}

    class FakeStorage:
        def __init__(self, dsn: str) -> None:
            calls["dsn"] = dsn
            calls["storage"] = self
            self.init_schema_called = False

        def init_schema(self) -> None:
            self.init_schema_called = True

    def fake_build_common_feature_daily_facts(**kwargs):
        calls["build_kwargs"] = kwargs
        return CommonFeatureBuildResult(
            features_processed=2,
            feature_dates_processed=5,
            facts_built=10,
            null_facts=1,
            facts_upserted=10,
        )

    monkeypatch.setattr(app, "get_settings", lambda: SimpleNamespace(db_dsn="postgresql://test"))
    monkeypatch.setattr(
        "krx_collector.infra.db_postgres.repositories.PostgresStorage",
        FakeStorage,
    )
    monkeypatch.setattr(
        (
            "krx_collector.service.build_common_feature_daily_facts."
            "build_common_feature_daily_facts"
        ),
        fake_build_common_feature_daily_facts,
    )

    args = app.build_parser().parse_args(
        [
            "common",
            "build-daily",
            "--feature-codes",
            "market_kospi_close,global_sp500_ret_1d",
            "--start",
            "2026-06-01",
            "--end",
            "2026-06-08",
            "--include-inactive",
            "--init-schema",
        ]
    )
    app._handle_common_build_daily(args)

    storage = calls["storage"]
    build_kwargs = calls["build_kwargs"]
    assert calls["dsn"] == "postgresql://test"
    assert storage.init_schema_called is True
    assert build_kwargs["storage"] is storage
    assert build_kwargs["start"] == date(2026, 6, 1)
    assert build_kwargs["end"] == date(2026, 6, 8)
    assert build_kwargs["feature_codes"] == [
        "market_kospi_close",
        "global_sp500_ret_1d",
    ]
    assert build_kwargs["active_only"] is False


def test_handle_common_build_daily_rejects_include_inactive_without_feature_codes() -> None:
    args = app.build_parser().parse_args(
        [
            "common",
            "build-daily",
            "--start",
            "2026-06-01",
            "--end",
            "2026-06-08",
            "--include-inactive",
        ]
    )

    with pytest.raises(SystemExit, match="--include-inactive requires"):
        app._handle_common_build_daily(args)


def test_handle_common_coverage_report_calls_service(monkeypatch) -> None:
    calls: dict[str, object] = {}

    class FakeStorage:
        def __init__(self, dsn: str) -> None:
            calls["dsn"] = dsn
            calls["storage"] = self

    def fake_build_common_feature_coverage_report(**kwargs):
        calls["coverage_kwargs"] = kwargs
        return CommonFeatureCoverageReport(
            target_count=4,
            rows=[
                CommonFeatureCoverageRow(
                    feature_code="global_vix_level",
                    feature_name_kr="VIX",
                    target_count=4,
                    fact_count=4,
                    non_null_count=3,
                    null_count=1,
                    missing_count=0,
                    coverage_ratio=Decimal("0.7500"),
                    pit_violation_count=0,
                )
            ],
        )

    monkeypatch.setattr(app, "get_settings", lambda: SimpleNamespace(db_dsn="postgresql://test"))
    monkeypatch.setattr(
        "krx_collector.infra.db_postgres.repositories.PostgresStorage",
        FakeStorage,
    )
    monkeypatch.setattr(
        (
            "krx_collector.service.report_common_feature_coverage."
            "build_common_feature_coverage_report"
        ),
        fake_build_common_feature_coverage_report,
    )

    args = app.build_parser().parse_args(
        [
            "common",
            "coverage-report",
            "--feature-codes",
            "global_vix_level,fx_usdkrw_level",
            "--start",
            "2026-06-01",
            "--end",
            "2026-06-08",
            "--include-inactive",
        ]
    )
    app._handle_common_coverage_report(args)

    storage = calls["storage"]
    coverage_kwargs = calls["coverage_kwargs"]
    assert calls["dsn"] == "postgresql://test"
    assert coverage_kwargs["storage"] is storage
    assert coverage_kwargs["start"] == date(2026, 6, 1)
    assert coverage_kwargs["end"] == date(2026, 6, 8)
    assert coverage_kwargs["feature_codes"] == ["global_vix_level", "fx_usdkrw_level"]
    assert coverage_kwargs["active_only"] is False


def test_handle_common_coverage_report_rejects_include_inactive_without_feature_codes() -> None:
    args = app.build_parser().parse_args(
        [
            "common",
            "coverage-report",
            "--start",
            "2026-06-01",
            "--end",
            "2026-06-08",
            "--include-inactive",
        ]
    )

    with pytest.raises(SystemExit, match="--include-inactive requires"):
        app._handle_common_coverage_report(args)


def test_handle_common_readiness_report_calls_service(monkeypatch) -> None:
    calls: dict[str, object] = {}

    class FakeStorage:
        def __init__(self, dsn: str) -> None:
            calls["dsn"] = dsn
            calls["storage"] = self

    def fake_build_common_feature_readiness_report(**kwargs):
        calls["readiness_kwargs"] = kwargs
        return CommonFeatureReadinessReport(
            target_count=4,
            rows=[
                CommonFeatureReadinessRow(
                    feature_code="rate_kr_gov3y_level",
                    feature_name_kr="국고채 3년",
                    target_count=4,
                    fact_count=4,
                    non_null_count=4,
                    null_count=0,
                    missing_count=0,
                    coverage_ratio=Decimal("1.0000"),
                    pit_violation_count=0,
                    required_coverage_ratio=Decimal("1.0000"),
                    ready=True,
                )
            ],
        )

    monkeypatch.setattr(app, "get_settings", lambda: SimpleNamespace(db_dsn="postgresql://test"))
    monkeypatch.setattr(
        "krx_collector.infra.db_postgres.repositories.PostgresStorage",
        FakeStorage,
    )
    monkeypatch.setattr(
        (
            "krx_collector.service.report_common_feature_readiness."
            "build_common_feature_readiness_report"
        ),
        fake_build_common_feature_readiness_report,
    )

    args = app.build_parser().parse_args(
        [
            "common",
            "readiness-report",
            "--feature-codes",
            "rate_kr_gov3y_level,macro_cpi_level",
            "--start",
            "2026-06-01",
            "--end",
            "2026-06-08",
            "--required-coverage-ratio",
            "1.0",
            "--include-inactive",
        ]
    )
    app._handle_common_readiness_report(args)

    storage = calls["storage"]
    readiness_kwargs = calls["readiness_kwargs"]
    assert calls["dsn"] == "postgresql://test"
    assert readiness_kwargs["storage"] is storage
    assert readiness_kwargs["start"] == date(2026, 6, 1)
    assert readiness_kwargs["end"] == date(2026, 6, 8)
    assert readiness_kwargs["feature_codes"] == ["rate_kr_gov3y_level", "macro_cpi_level"]
    assert readiness_kwargs["active_only"] is False
    assert readiness_kwargs["required_coverage_ratio"] == Decimal("1.0000")


def test_handle_common_readiness_report_rejects_include_inactive_without_feature_codes() -> None:
    args = app.build_parser().parse_args(
        [
            "common",
            "readiness-report",
            "--start",
            "2026-06-01",
            "--end",
            "2026-06-08",
            "--include-inactive",
        ]
    )

    with pytest.raises(SystemExit, match="--include-inactive requires"):
        app._handle_common_readiness_report(args)


def test_handle_common_readiness_report_can_fail_on_not_ready(monkeypatch) -> None:
    class FakeStorage:
        def __init__(self, dsn: str) -> None:
            self.dsn = dsn

    def fake_build_common_feature_readiness_report(**kwargs):
        return CommonFeatureReadinessReport(
            target_count=4,
            rows=[
                CommonFeatureReadinessRow(
                    feature_code="global_vix_level",
                    feature_name_kr="VIX",
                    target_count=4,
                    fact_count=4,
                    non_null_count=3,
                    null_count=1,
                    missing_count=0,
                    coverage_ratio=Decimal("0.7500"),
                    pit_violation_count=0,
                    required_coverage_ratio=Decimal("1.0000"),
                    ready=False,
                    blockers=("coverage 0.7500 below required 1.0000",),
                )
            ],
        )

    monkeypatch.setattr(app, "get_settings", lambda: SimpleNamespace(db_dsn="postgresql://test"))
    monkeypatch.setattr(
        "krx_collector.infra.db_postgres.repositories.PostgresStorage",
        FakeStorage,
    )
    monkeypatch.setattr(
        (
            "krx_collector.service.report_common_feature_readiness."
            "build_common_feature_readiness_report"
        ),
        fake_build_common_feature_readiness_report,
    )

    args = app.build_parser().parse_args(
        [
            "common",
            "readiness-report",
            "--start",
            "2026-06-01",
            "--end",
            "2026-06-08",
            "--fail-on-not-ready",
        ]
    )

    with pytest.raises(SystemExit) as exc_info:
        app._handle_common_readiness_report(args)

    assert exc_info.value.code == 2
