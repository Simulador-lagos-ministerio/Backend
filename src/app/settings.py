"""App settings and environment configuration."""
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict
from pyparsing import Optional

BASE_DIR = Path(__file__).resolve().parents[2]  # .../Backend
DEFAULT_SQLITE_PATH = BASE_DIR / "database.db"

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    env: str = "dev"

    sqlite_url: str = "sqlite:///./database.db"
    postgis_url: str = "postgresql+psycopg2://maps_user:maps_pass@localhost:5433/maps"

    s3_endpoint: str = "http://localhost:9000"
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin"
    s3_bucket: str = "maps"
    s3_region: str = "us-east-1"

    jwt_secret_key: str = "dev-secret"         # override via .env
    jwt_algorithm: str = "HS256"
    jwt_expire_minutes: int = 60 * 24 * 60     # 60 days

    cors_origins: str = "http://localhost:5173,http://localhost:3000"         # "http://...,..."

settings = Settings()
