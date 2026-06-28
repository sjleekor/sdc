from datetime import date
from types import SimpleNamespace

import pytest

from krx_collector.cli import app
from krx_collector.domain.enums import Source
from krx_collector.domain.models import (
    CommonFeatureSyncResult,
    UpsertResult,
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


def test_db_sync_remote_parser_supports_ssh_compression_flags() -> None:
    args = app.build_parser().parse_args(["db", "sync-remote", "--ssh-compression"])

    assert args.command == "db"
    assert args.db_command == "sync-remote"
    assert args.ssh_compression is True
    assert args.handler == app._handle_db_sync_remote

    args = app.build_parser().parse_args(["db", "sync-remote", "--no-ssh-compression"])

    assert args.ssh_compression is False


def test_db_sync_remote_parser_leaves_ssh_compression_unset_by_default() -> None:
    args = app.build_parser().parse_args(["db", "sync-remote"])

    assert args.ssh_compression is None


def test_profile_all_parser_supports_role_filter() -> None:
    args = app.build_parser().parse_args(
        ["profile", "all", "--role", "raw", "--weight", "full,light"]
    )

    assert args.command == "profile"
    assert args.profile_command == "all"
    assert args.role == "raw"
    assert args.weight == "full,light"
    assert args.handler == app._handle_profile_all


def test_profile_all_handler_filters_by_raw_role(monkeypatch) -> None:
    captured = {}

    def fake_run_profile_specs(args, specs):
        captured["tables"] = [spec.table for spec in specs]

    monkeypatch.setattr(app, "_run_profile_specs", fake_run_profile_specs)
    args = app.build_parser().parse_args(["profile", "all", "--role", "raw"])

    args.handler(args)

    assert "daily_ohlcv" in captured["tables"]
    assert "krx_security_flow_raw" in captured["tables"]
    assert "common_feature_observation_raw" in captured["tables"]
    assert "stock_metric_fact" not in captured["tables"]
    assert "common_feature_daily_fact" not in captured["tables"]
    assert "ingestion_runs" not in captured["tables"]


def test_profile_all_handler_rejects_unknown_role() -> None:
    args = app.build_parser().parse_args(["profile", "all", "--role", "mystery"])

    with pytest.raises(SystemExit) as exc:
        args.handler(args)

    assert exc.value.code == 1


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
