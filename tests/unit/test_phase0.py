import os

from krx_collector.domain.enums import RunType, Source
from krx_collector.infra.config.settings import Settings


def test_settings_compute_dsn_and_opendart_key() -> None:
    settings = Settings(
        db_dsn="",
        db_host="db-host",
        db_port=15432,
        db_name="krx_db",
        db_user="collector",
        db_password="secret",
        opendart_api_key="test-opendart-key",
    )

    assert settings.db_dsn == "postgresql://collector:secret@db-host:15432/krx_db"
    assert settings.opendart_api_key == "test-opendart-key"
    assert settings.opendart_api_keys == ("test-opendart-key",)


def test_settings_normalize_multiple_opendart_keys() -> None:
    settings = Settings(
        opendart_api_key=" key_b ",
        opendart_api_keys_raw=" key_a , key_b ,, key_c , ",
    )

    assert settings.opendart_api_key == "key_b"
    assert settings.opendart_api_keys == ("key_a", "key_b", "key_c")


def test_settings_accept_comma_separated_opendart_keys_from_env(monkeypatch) -> None:
    monkeypatch.setenv("OPENDART_API_KEYS", " key_a , key_b ,, key_c , ")
    monkeypatch.setenv("OPENDART_API_KEY", " key_b ")

    settings = Settings(_env_file=None)

    assert settings.opendart_api_key == "key_b"
    assert settings.opendart_api_keys == ("key_a", "key_b", "key_c")


def test_settings_allow_empty_multiple_opendart_keys() -> None:
    settings = Settings(
        opendart_api_key="",
        opendart_api_keys_raw=" , , ",
    )

    assert settings.opendart_api_keys == ()


def test_settings_exports_krx_credentials_to_environment(monkeypatch) -> None:
    monkeypatch.delenv("KRX_ID", raising=False)
    monkeypatch.delenv("KRX_PW", raising=False)
    settings = Settings(_env_file=None, krx_id="krx-user", krx_pw="krx-pass")

    settings.export_krx_credentials_to_environment()

    assert os.environ["KRX_ID"] == "krx-user"
    assert os.environ["KRX_PW"] == "krx-pass"


def test_future_sources_and_run_types_are_declared() -> None:
    assert Source.OPENDART.value == "OPENDART"
    assert Source.KRX.value == "KRX"
    assert RunType.DART_CORP_SYNC.value == "dart_corp_sync"
    assert RunType.DART_FINANCIAL_SYNC.value == "dart_financial_sync"
    assert RunType.KRX_FLOW_SYNC.value == "krx_flow_sync"
