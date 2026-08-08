import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

from clickhouse_store import BarClickHouseStore
from kafka_consumer import TradeKafkaConsumer
from lineage import OpenLineageEmitter, clickhouse_dataset, kafka_dataset

FIN_DATA_DIR = Path(__file__).resolve().parent.parent


def run_consumer() -> None:
    load_dotenv(FIN_DATA_DIR / ".env")

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
    topic = os.getenv("KAFKA_BARS_TOPIC", "sp500.bars.1m")
    group_id = os.getenv("KAFKA_CONSUMER_GROUP", "sp500-bars-consumer")

    ch_host = os.getenv("CLICKHOUSE_HOST", "localhost")
    ch_port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    ch_database = os.getenv("CLICKHOUSE_DATABASE", "findata")
    ch_table = os.getenv("CLICKHOUSE_TABLE", "bars_1m")

    store = BarClickHouseStore(
        host=ch_host,
        port=ch_port,
        username=os.getenv("CLICKHOUSE_USER", "findata"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "findata"),
        database=ch_database,
        table=ch_table,
    )

    consumer = TradeKafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        group_id=group_id,
    )

    lineage = OpenLineageEmitter("bars_clickhouse_consumer")
    inputs = [kafka_dataset(topic, bootstrap_servers)]
    outputs = [clickhouse_dataset(ch_database, ch_table, ch_host, ch_port)]
    lineage.start(inputs=inputs, outputs=outputs)

    def handle_bar(bar: dict) -> None:
        print(
            f"[{bar['symbol']}] {bar['window_start']} "
            f"O={bar['open_price']} H={bar['high_price']} "
            f"L={bar['low_price']} C={bar['close_price']} "
            f"V={bar['volume']} trades={bar['trade_count']} "
            f"vwap={bar['vwap']}"
        )
        store.insert_bar(bar)

    try:
        consumer.poll(handle_bar)
    except KeyboardInterrupt:
        print("\nStopping consumer...")
        lineage.complete(inputs=inputs, outputs=outputs)
    except Exception as exc:
        lineage.fail(error=str(exc), inputs=inputs, outputs=outputs)
        raise
    finally:
        consumer.close()


if __name__ == "__main__":
    run_consumer()
