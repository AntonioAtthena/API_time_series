"""
Application configuration loaded from environment variables / .env file.

All fields have safe defaults so the app starts with zero configuration —
just run `uvicorn app.main:app` and a local SQLite database is created
automatically.  Override via a .env file or environment variables when
deploying to a server.
"""

from typing import Any

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings resolved from environment variables.

    Attributes:
        database_url: Async database connection string.  Defaults to a local
            SQLite file (financial.db) so no external database is needed.
        api_keys: Set of valid bearer tokens for the X-API-Key header.
        env: Runtime environment label; controls debug behaviour.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )

    database_url: str = "sqlite+aiosqlite:///./financial.db"
    api_keys: list[str] = ["dev-api-key-change-me"]
    env: str = "development"

    # Rate limiting — requests per minute per client IP.
    # Set to 0 to disable rate limiting entirely.
    rate_limit_per_minute: int = 60
    rate_limit_upload_per_minute: int = 10

    # LGPD (Lei nº 13.709/2018) — Brazilian data protection compliance.
    # These values are surfaced verbatim by the GET /privacidade endpoint so
    # that clients and auditors can inspect the data-handling posture via the API.
    #
    # lgpd_responsavel:        Legal name of the data controller (controlador).
    # lgpd_encarregado_email:  Contact address for the DPO (encarregado), required
    #                          by Art. 41 LGPD.  Must be set before commercial launch.
    # lgpd_retencao_logs_dias: How many days access logs (which contain IP addresses,
    #                          personal data under Art. 5 I LGPD) are retained by
    #                          the infrastructure layer before deletion.
    lgpd_responsavel: str = "Atthena Financial API"
    lgpd_encarregado_email: str = ""
    lgpd_retencao_logs_dias: int = 90

    @field_validator("api_keys", mode="before")
    @classmethod
    def parse_api_keys(cls, v: Any) -> list[str]:
        """Accept either a JSON array or a comma-separated string.

        Args:
            v: Raw value from the environment variable.

        Returns:
            List of non-empty, stripped API key strings.
        """
        if isinstance(v, str):
            return [k.strip() for k in v.split(",") if k.strip()]
        return v


# Module-level singleton — import this everywhere instead of re-instantiating.
settings = Settings()
