from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.serializer import DefaultSerializer, JsonSerializer


class TradeKafkaProducer:
    def __init__(self, bootstrap_servers: str, topic: str):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(","),
            key_serializer=DefaultSerializer(),
            value_serializer=JsonSerializer(),
            acks="all",
            retries=3,
        )

    def publish_trade(self, trade: dict) -> None:
        symbol = trade["s"]
        payload = {
            "symbol": symbol,
            "price": trade["p"],
            "timestamp_ms": trade["t"],
            "volume": trade["v"],
            "received_at": datetime.now(timezone.utc).isoformat(),
        }
        if "c" in trade:
            payload["conditions"] = trade["c"]

        self.producer.send(self.topic, key=symbol, value=payload)

    def flush(self) -> None:
        self.producer.flush()

    def close(self) -> None:
        self.flush()
        self.producer.close()
