"""Kafka bars → ClickHouse consumer application."""

from __future__ import annotations

from findata.clickhouse_store import BarClickHouseStore
from findata.config import Settings, load_settings
from findata.kafka_consumer import TradeKafkaConsumer
from findata.lineage import OpenLineageEmitter, clickhouse_dataset, kafka_dataset
from findata.ports import BarConsumer, BarStore


def handle_bar(bar: dict, store: BarStore, *, log: bool = True) -> None:
    if log:
        print(
            f"[{bar['symbol']}] {bar['window_start']} "
            f"O={bar['open_price']} H={bar['high_price']} "
            f"L={bar['low_price']} C={bar['close_price']} "
            f"V={bar['volume']} trades={bar['trade_count']} "
            f"vwap={bar['vwap']}"
        )
    store.insert_bar(bar)


def run_consumer(
    settings: Settings,
    consumer: BarConsumer,
    store: BarStore,
    *,
    lineage: OpenLineageEmitter | None = None,
) -> None:
    topic = settings.kafka_bars_topic
    bootstrap = settings.kafka_bootstrap_servers
    lineage = lineage or OpenLineageEmitter(
        "bars_clickhouse_consumer", enabled=settings.openlineage_enabled
    )
    inputs = [kafka_dataset(topic, bootstrap)]
    outputs = [
        clickhouse_dataset(
            settings.clickhouse_database,
            settings.clickhouse_table,
            settings.clickhouse_host,
            settings.clickhouse_port,
        )
    ]
    lineage.start(inputs=inputs, outputs=outputs)

    def on_bar(bar: dict) -> None:
        handle_bar(bar, store)

    try:
        consumer.poll(on_bar)
    except KeyboardInterrupt:
        print("\nStopping consumer...")
        lineage.complete(inputs=inputs, outputs=outputs)
    except Exception as exc:
        lineage.fail(error=str(exc), inputs=inputs, outputs=outputs)
        raise
    finally:
        consumer.close()


def main() -> None:
    settings = load_settings()
    store = BarClickHouseStore(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
        table=settings.clickhouse_table,
    )
    consumer = TradeKafkaConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=settings.kafka_bars_topic,
        group_id=settings.kafka_consumer_group,
    )
    run_consumer(settings, consumer, store)
