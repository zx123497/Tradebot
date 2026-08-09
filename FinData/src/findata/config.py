"""Environment-backed settings for FinData apps."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

FIN_DATA_DIR = Path(__file__).resolve().parent.parent.parent
DEFAULT_FINNHUB_WS_URL = "wss://ws.finnhub.io?token={token}"


@dataclass(frozen=True)
class Settings:
    finnhub_api_key: str | None
    finnhub_ws_url_template: str
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_bars_topic: str
    kafka_bars_5m_topic: str
    kafka_bars_volume_topic: str
    kafka_bars_dollar_topic: str
    kafka_consumer_group: str
    kafka_trades_consumer_group: str
    gics_sector: str
    subscribe_symbols: str | None
    symbol_limit: int | None
    clickhouse_host: str
    clickhouse_port: int
    clickhouse_user: str
    clickhouse_password: str
    clickhouse_database: str
    clickhouse_table: str
    clickhouse_trades_table: str
    include_notional: bool
    mock_finnhub_host: str
    mock_finnhub_port: int
    mock_replay_delay_sec: float
    mock_subscribe_quiet_sec: float
    openlineage_enabled: bool

    @property
    def using_mock_finnhub(self) -> bool:
        template = self.finnhub_ws_url_template
        return "{token}" not in template and not template.startswith(
            "wss://ws.finnhub.io"
        )

    def resolve_finnhub_ws_url(self) -> str:
        api_key = self.finnhub_api_key or "test-token"
        if "{token}" in self.finnhub_ws_url_template:
            return self.finnhub_ws_url_template.format(token=api_key)
        return self.finnhub_ws_url_template


def load_settings(env_file: Path | None = None) -> Settings:
    load_dotenv(env_file or (FIN_DATA_DIR / ".env"))

    symbol_limit_raw = os.getenv("SYMBOL_LIMIT")
    return Settings(
        finnhub_api_key=os.environ.get("FINNHUB_API_KEY"),
        finnhub_ws_url_template=os.getenv("FINNHUB_WS_URL", DEFAULT_FINNHUB_WS_URL),
        kafka_bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9094"
        ),
        kafka_topic=os.getenv("KAFKA_TOPIC", "sp500.trades"),
        kafka_bars_topic=os.getenv("KAFKA_BARS_TOPIC", "sp500.bars.1m"),
        kafka_bars_5m_topic=os.getenv("KAFKA_BARS_5M_TOPIC", "sp500.bars.5m"),
        kafka_bars_volume_topic=os.getenv(
            "KAFKA_BARS_VOLUME_TOPIC", "sp500.bars.volume"
        ),
        kafka_bars_dollar_topic=os.getenv(
            "KAFKA_BARS_DOLLAR_TOPIC", "sp500.bars.dollar"
        ),
        kafka_consumer_group=os.getenv(
            "KAFKA_CONSUMER_GROUP", "sp500-bars-consumer"
        ),
        kafka_trades_consumer_group=os.getenv(
            "KAFKA_TRADES_CONSUMER_GROUP", "sp500-trades-consumer"
        ),
        gics_sector=os.getenv("GICS_SECTOR", "Information Technology"),
        subscribe_symbols=os.getenv("SUBSCRIBE_SYMBOLS"),
        symbol_limit=int(symbol_limit_raw) if symbol_limit_raw else None,
        clickhouse_host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        clickhouse_port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        clickhouse_user=os.getenv("CLICKHOUSE_USER", "findata"),
        clickhouse_password=os.getenv("CLICKHOUSE_PASSWORD", "findata"),
        clickhouse_database=os.getenv("CLICKHOUSE_DATABASE", "findata"),
        clickhouse_table=os.getenv("CLICKHOUSE_TABLE", "bars_1m"),
        clickhouse_trades_table=os.getenv("CLICKHOUSE_TRADES_TABLE", "trades"),
        include_notional=os.getenv("INCLUDE_NOTIONAL", "false").lower()
        in ("1", "true", "yes"),
        mock_finnhub_host=os.getenv("MOCK_FINNHUB_HOST", "0.0.0.0"),
        mock_finnhub_port=int(os.getenv("MOCK_FINNHUB_PORT", "8765")),
        mock_replay_delay_sec=float(
            os.getenv("MOCK_FINNHUB_REPLAY_DELAY_SEC", "0.05")
        ),
        mock_subscribe_quiet_sec=float(
            os.getenv("MOCK_FINNHUB_SUBSCRIBE_QUIET_SEC", "0.5")
        ),
        openlineage_enabled=os.getenv("OPENLINEAGE_ENABLED", "false").lower()
        in ("1", "true", "yes"),
    )
