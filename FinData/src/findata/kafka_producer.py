from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.serializer import DefaultSerializer

from findata.json_serde import JsonBytesSerializer
from findata.models import finnhub_trade_to_payload


class TradeKafkaProducer:
    def __init__(self, bootstrap_servers: str, topic: str):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(","),
            key_serializer=DefaultSerializer(),
            value_serializer=JsonBytesSerializer(),
            acks="all",
            retries=3,
        )

    def publish_trade(self, trade: dict) -> None:
        payload = finnhub_trade_to_payload(trade)
        self.producer.send(self.topic, key=payload["symbol"], value=payload)

    def flush(self) -> None:
        self.producer.flush()

    def close(self) -> None:
        self.flush()
        self.producer.close()
