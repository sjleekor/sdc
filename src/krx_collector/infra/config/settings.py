"""Application settings loaded from environment / ``.env`` file.

Uses ``pydantic-settings`` for typed, validated configuration.  The
``Settings`` class reads from environment variables (and ``.env`` in dev)
and exposes a computed ``db_dsn`` that falls back to individual DB_*
components when ``DB_DSN`` is not provided.

Usage::

    from krx_collector.infra.config.settings import get_settings

    settings = get_settings()
    print(settings.db_dsn)
"""

from __future__ import annotations

import os
from enum import StrEnum
from functools import lru_cache
from pathlib import Path
from typing import Any

from pydantic import Field, PrivateAttr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

DEFAULT_KRX_MDC_TIMEOUT_SECONDS = 20.0


class RunMode(StrEnum):
    """Application run mode."""

    DEV = "dev"
    PROD = "prod"


class LogFormat(StrEnum):
    """Log output format."""

    PLAIN = "plain"
    JSON = "json"


class UniverseSourceDefault(StrEnum):
    """Default universe data source."""

    FDR = "fdr"
    PYKRX = "pykrx"


class Settings(BaseSettings):
    """Central application configuration.

    Reads from environment variables (prefix-free) and an optional ``.env``
    file located in the project root.

    Attributes:
        db_dsn: Full PostgreSQL DSN.  If empty, computed from DB_HOST etc.
        db_host: Database host (used if ``db_dsn`` is empty).
        db_port: Database port.
        db_name: Database name.
        db_user: Database user.
        db_password: Database password.
        log_level: Python logging level name.
        log_format: Output format (plain text or JSON).
        log_dir: Optional directory for rotating file logs.
        run_mode: dev or prod.
        universe_source_default: Default source for universe sync.
        opendart_api_key: Optional OpenDART API key for future DART-based
            ingestion features.
        rate_limit_seconds: Delay between API calls (seconds).
        long_rest_interval: Number of API requests between long rests
            (0 disables long rests).
        long_rest_seconds: Duration of each long rest, in seconds.
        remote_db_info_path: Path to the remote DB metadata file.
        remote_db_batch_size: Batch size for remote-to-local DB sync.
        remote_db_host_override: Optional hostname override for the remote DB.
        remote_db_ssh_host: Optional SSH host for local port forwarding.
        remote_db_ssh_local_port: Optional fixed local port for SSH tunnel.
        krx_mdc_timeout_seconds: HTTP timeout for KRX MDC requests.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        populate_by_name=True,
    )

    # Database
    db_dsn: str = ""
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "krx_data"
    db_user: str = "krx_user"
    db_password: str = ""

    # Logging
    log_level: str = "INFO"
    log_format: LogFormat = LogFormat.PLAIN
    log_dir: Path | None = None

    # Runtime
    run_mode: RunMode = RunMode.DEV

    # Universe
    universe_source_default: UniverseSourceDefault = UniverseSourceDefault.FDR

    # KRX / pykrx authentication
    krx_id: str = ""
    krx_pw: str = ""
    krx_mdc_timeout_seconds: float = DEFAULT_KRX_MDC_TIMEOUT_SECONDS

    # OpenDART
    opendart_api_key: str = ""
    opendart_api_keys_raw: str = Field(default="", validation_alias="OPENDART_API_KEYS")
    _opendart_api_keys: tuple[str, ...] = PrivateAttr(default=())

    # Rate limiting
    rate_limit_seconds: float = 0.2
    long_rest_interval: int = 100
    long_rest_seconds: float = 10.0

    # Remote DB sync
    remote_db_info_path: Path = Path("/Users/whishaw/wss_p/stock_data_collector_secrets/db_info")
    remote_db_batch_size: int = 50000
    remote_db_host_override: str | None = None
    remote_db_ssh_host: str | None = None
    remote_db_ssh_local_port: int | None = None

    @property
    def opendart_api_keys(self) -> tuple[str, ...]:
        """Normalized OpenDART key list from OPENDART_API_KEYS and OPENDART_API_KEY."""
        return self._opendart_api_keys

    @field_validator("krx_mdc_timeout_seconds", mode="before")
    @classmethod
    def _parse_krx_mdc_timeout_seconds(cls, value: Any) -> float:
        """Accept timeout values as seconds, with an optional ``s`` suffix."""
        if value is None or value == "":
            return DEFAULT_KRX_MDC_TIMEOUT_SECONDS
        if isinstance(value, str):
            normalized = value.strip().lower()
            for suffix in ("seconds", "second", "secs", "sec", "s"):
                if normalized.endswith(suffix):
                    normalized = normalized[: -len(suffix)].strip()
                    break
            value = normalized
        try:
            seconds = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "KRX_MDC_TIMEOUT_SECONDS must be a positive number of seconds"
            ) from exc
        if seconds <= 0:
            raise ValueError("KRX_MDC_TIMEOUT_SECONDS must be greater than zero")
        return seconds

    @model_validator(mode="after")
    def _compute_dsn(self) -> Settings:
        """Build ``db_dsn`` from individual components if not set directly."""
        if not self.db_dsn:
            self.db_dsn = (
                f"postgresql://{self.db_user}:{self.db_password}"
                f"@{self.db_host}:{self.db_port}/{self.db_name}"
            )

        ordered_keys: list[str] = []
        seen_keys: set[str] = set()

        for key in self.opendart_api_keys_raw.split(","):
            normalized = key.strip()
            if normalized and normalized not in seen_keys:
                ordered_keys.append(normalized)
                seen_keys.add(normalized)

        legacy_key = self.opendart_api_key.strip()
        if legacy_key and legacy_key not in seen_keys:
            ordered_keys.append(legacy_key)

        self.opendart_api_key = legacy_key
        self._opendart_api_keys = tuple(ordered_keys)
        return self

    def export_krx_credentials_to_environment(self) -> None:
        """Expose .env-loaded KRX credentials for pykrx's import-time auth hook."""
        if self.krx_id and not os.environ.get("KRX_ID"):
            os.environ["KRX_ID"] = self.krx_id
        if self.krx_pw and not os.environ.get("KRX_PW"):
            os.environ["KRX_PW"] = self.krx_pw


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the singleton ``Settings`` instance (cached)."""
    return Settings()


def configure_krx_credentials_from_settings() -> None:
    """Load KRX credentials from settings before importing pykrx modules."""
    get_settings().export_krx_credentials_to_environment()
