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

from pydantic import model_validator
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
        rate_limit_seconds: Delay between API calls (seconds).
        long_rest_interval: Number of API requests between long rests
            (0 disables long rests).
        long_rest_seconds: Duration of each long rest, in seconds.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
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

    # Rate limiting
    rate_limit_seconds: float = 0.2
    long_rest_interval: int = 100
    long_rest_seconds: float = 10.0

    @model_validator(mode="after")
    def _compute_dsn(self) -> Settings:
        """Build ``db_dsn`` from individual components if not set directly."""
        if not self.db_dsn:
            self.db_dsn = (
                f"postgresql://{self.db_user}:{self.db_password}"
                f"@{self.db_host}:{self.db_port}/{self.db_name}"
            )
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the singleton ``Settings`` instance (cached)."""
    return Settings()
