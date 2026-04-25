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

from enum import StrEnum
from functools import lru_cache
from pathlib import Path

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


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
    opendart_api_key: str = ""
    opendart_api_keys_raw: str = Field(default="", validation_alias="OPENDART_API_KEYS")
    opendart_api_keys: tuple[str, ...] = ()

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
        self.opendart_api_keys = tuple(ordered_keys)
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the singleton ``Settings`` instance (cached)."""
    return Settings()
