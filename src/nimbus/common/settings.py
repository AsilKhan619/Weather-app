from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Postgres
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "nimbus"
    postgres_user: str = "nimbus"
    postgres_password: str = "nimbus_dev_only"
    postgres_readonly_user: str = "nimbus_ro"
    postgres_readonly_password: str = "nimbus_ro_dev_only"

    # Kafka
    kafka_bootstrap_servers: str = "localhost:29092"

    # LLM (Anthropic) — off by default; the data platform must run without a key.
    llm_enabled: bool = False
    anthropic_api_key: str | None = None
    nimbus_agent_model: str = "claude-sonnet-5"
    nimbus_briefing_model: str = "claude-haiku-4-5-20251001"

    # App
    nimbus_env: str = "dev"
    log_level: str = "INFO"

    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql+psycopg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def postgres_readonly_dsn(self) -> str:
        return (
            f"postgresql+psycopg://{self.postgres_readonly_user}:{self.postgres_readonly_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()
