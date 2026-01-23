from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    env: str = "dev"

    sqlite_url: str = "sqlite:///./database.db"
    postgis_url: str

    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    s3_bucket: str = "maps"
    s3_region: str = "us-east-1"

settings = Settings()
