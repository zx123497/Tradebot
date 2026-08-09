"""Kafka trades → ClickHouse ticks consumer (TTL table)."""

from __future__ import annotations

from findata.clickhouse_store import TradeClickHouseStore
from findata.config import load_settings
from findata.kafka_consumer import TradeKafkaConsumer
from findata.lineage import OpenLineageEmitter, clickhouse_dataset, kafka_dataset


def main() -> None:
    settings = load_settings()
    store = TradeClickHouseStore(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
        table=settings.clickhouse_trades_table,
    )
    consumer = TradeKafkaConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=settings.kafka_topic,
        group_id=settings.kafka_trades_consumer_group,
    )
    lineage = OpenLineageEmitter(
        "trades_clickhouse_consumer", enabled=settings.openlineage_enabled
    )
    inputs = [kafka_dataset(settings.kafka_topic, settings.kafka_bootstrap_servers)]
    outputs = [
        clickhouse_dataset(
            settings.clickhouse_database,
            settings.clickhouse_trades_table,
            settings.clickhouse_host,
            settings.clickhouse_port,
        )
    ]
    lineage.start(inputs=inputs, outputs=outputs)

    def on_trade(trade: dict) -> None:
        # Bar consumer deserializer returns dict; trade topic uses trade fields.
        if "window_start" in trade:
            return
        print(
            f"[tick] {trade.get('symbol')} "
            f"px={trade.get('price')} vol={trade.get('volume')} "
            f"ts={trade.get('timestamp_ms')}"
        )
        store.insert_trade(trade)

    try:
        consumer.poll(on_trade)
    except KeyboardInterrupt:
        print("\nStopping trades consumer...")
        lineage.complete(inputs=inputs, outputs=outputs)
    except Exception as exc:
        lineage.fail(error=str(exc), inputs=inputs, outputs=outputs)
        raise
    finally:
        consumer.close()
