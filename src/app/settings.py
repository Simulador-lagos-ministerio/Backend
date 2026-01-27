"""App settings and environment configuration."""
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parents[2]  # .../Backend
DEFAULT_SQLITE_PATH = BASE_DIR / "database.db"

class Settings(BaseSettings):
    # Load .env from the repo root, not from the current working directory.
    model_config = SettingsConfigDict(env_file=str(BASE_DIR / ".env"), extra="ignore")

    env: str = "dev"

    # Absolute sqlite path to avoid CWD-dependent DB creation.
    sqlite_url: str = f"sqlite:///{DEFAULT_SQLITE_PATH.as_posix()}"

    # All fields can be overridden via environment (.env).
    postgis_url: str = "postgresql+psycopg2://maps_user:maps_pass@localhost:5433/maps"

    s3_endpoint: str = "http://localhost:9000"
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin"
    s3_bucket: str = "maps"
    s3_region: str = "us-east-1"

settings = Settings()
