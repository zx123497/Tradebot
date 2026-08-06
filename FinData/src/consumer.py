import os
from pathlib import Path

from dotenv import load_dotenv

from kafka_consumer import TradeKafkaConsumer

FIN_DATA_DIR = Path(__file__).resolve().parent.parent


def run_consumer() -> None:
    load_dotenv(FIN_DATA_DIR / ".env")

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "sp500.trades")
    group_id = os.getenv("KAFKA_CONSUMER_GROUP", "sp500-consumer")

    consumer = TradeKafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        group_id=group_id,
    )

    try:
        consumer.poll_and_print()
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    run_consumer()
