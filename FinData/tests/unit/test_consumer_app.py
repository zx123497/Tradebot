from fakes import FakeBarConsumer, FakeBarStore

from findata.config import Settings
from findata.consumer_app import handle_bar, run_consumer
from findata.lineage import OpenLineageEmitter


def _settings() -> Settings:
    return Settings(
        finnhub_api_key=None,
        finnhub_ws_url_template="ws://localhost:8765",
        kafka_bootstrap_servers="localhost:9094",
        kafka_topic="sp500.trades",
        kafka_bars_topic="sp500.bars.1m",
        kafka_bars_5m_topic="sp500.bars.5m",
        kafka_bars_volume_topic="sp500.bars.volume",
        kafka_bars_dollar_topic="sp500.bars.dollar",
        kafka_consumer_group="test-group",
        kafka_trades_consumer_group="test-trades-group",
        gics_sector="Information Technology",
        subscribe_symbols=None,
        symbol_limit=None,
        clickhouse_host="localhost",
        clickhouse_port=8123,
        clickhouse_user="findata",
        clickhouse_password="findata",
        clickhouse_database="findata",
        clickhouse_table="bars_1m",
        clickhouse_trades_table="trades",
        include_notional=False,
        mock_finnhub_host="0.0.0.0",
        mock_finnhub_port=8765,
        mock_replay_delay_sec=0.0,
        mock_subscribe_quiet_sec=0.0,
        openlineage_enabled=False,
    )


def test_handle_bar_inserts_once():
    store = FakeBarStore()
    bar = {
        "symbol": "AAPL",
        "window_start": "2026-01-01T00:00:00.000",
        "open_price": 100.0,
        "high_price": 105.0,
        "low_price": 99.0,
        "close_price": 102.0,
        "volume": 50.0,
        "trade_count": 4,
        "vwap": 102.5,
    }
    handle_bar(bar, store, log=False)
    assert store.bars == [bar]


def test_run_consumer_polls_and_closes():
    store = FakeBarStore()
    bar = {
        "symbol": "AAPL",
        "window_start": "2026-01-01T00:00:00.000",
        "open_price": 100.0,
        "high_price": 105.0,
        "low_price": 99.0,
        "close_price": 102.0,
        "volume": 50.0,
        "trade_count": 4,
        "vwap": 102.5,
    }
    consumer = FakeBarConsumer([bar])
    run_consumer(
        _settings(),
        consumer,
        store,
        lineage=OpenLineageEmitter("test", enabled=False),
    )
    assert store.bars == [bar]
    assert consumer.closed is True
