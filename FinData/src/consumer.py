import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

from clickhouse_store import BarClickHouseStore
from kafka_consumer import TradeKafkaConsumer

FIN_DATA_DIR = Path(__file__).resolve().parent.parent


def run_consumer() -> None:
    load_dotenv(FIN_DATA_DIR / ".env")

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
    topic = os.getenv("KAFKA_BARS_TOPIC", "sp500.bars.1m")
    group_id = os.getenv("KAFKA_CONSUMER_GROUP", "sp500-bars-consumer")

    store = BarClickHouseStore(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "findata"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "findata"),
        database=os.getenv("CLICKHOUSE_DATABASE", "findata"),
        table=os.getenv("CLICKHOUSE_TABLE", "bars_1m"),
    )

    consumer = TradeKafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        group_id=group_id,
    )

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
    finally:
        consumer.close()


if __name__ == "__main__":
    run_consumer()
