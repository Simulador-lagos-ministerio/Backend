from __future__ import annotations

from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Centralized application settings loaded from environment (.env).
    """

    # Project root directory
    PROJECT_ROOT: Path = Path(__file__).resolve().parents[2]

    model_config = SettingsConfigDict(
        env_file=str(PROJECT_ROOT / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Environment
    ENV: str = Field(default="development", description="development|test|production")
    DEBUG: bool = Field(default=False)

    # Database (single Postgres/PostGIS for everything)
    POSTGIS_URL: str = Field(
        default="postgresql+psycopg2://postgres:postgres@localhost:5432/postgres"
    )

    # CORS
    CORS_ORIGINS: str = Field(default="http://localhost:5173")

    # JWT (Access token)
    JWT_SECRET_KEY: str = Field(default="CHANGE_ME_IN_PROD")
    JWT_ALGORITHM: str = Field(default="HS256")
    JWT_EXPIRE_MINUTES: int = Field(default=30)

    # Refresh tokens
    REFRESH_TOKEN_EXPIRE_DAYS: int = Field(default=30)

    # DB init behavior: in production you should use Alembic migrations.
    # This is a safe default: do NOT auto-create tables in production.
    DB_INIT_ON_STARTUP: bool = Field(default=True)

    # S3 / MinIO
    S3_ENDPOINT_URL: Optional[str] = Field(default="http://localhost:9000")
    S3_ACCESS_KEY: str = Field(default="minioadmin")
    S3_SECRET_KEY: str = Field(default="minioadmin")
    S3_BUCKET: str = Field(default="lakes")
    S3_REGION: str = Field(default="us-east-1")


settings = Settings()
