from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=(".env", "../.env", "../../FinData/.env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "findata"
    clickhouse_password: str = "findata"
    clickhouse_database: str = "findata"
    clickhouse_table: str = "bars_1m"
    cors_origins: str = "http://localhost:5173,http://127.0.0.1:5173"
    sse_poll_interval_sec: float = 5.0

    @property
    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]


settings = Settings()
