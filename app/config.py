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
