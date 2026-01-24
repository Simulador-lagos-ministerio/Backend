from pydantic_settings import BaseSettings, SettingsConfigDict

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

settings = Settings()
