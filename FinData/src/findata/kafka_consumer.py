from collections.abc import Callable
from datetime import datetime

from kafka import KafkaConsumer
from kafka.serializer import DefaultSerializer

from findata.json_serde import JsonBytesDeserializer


class TradeKafkaConsumer:
    """Kafka consumer for bar (or trade) JSON topics."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str = "sp500-consumer",
    ):
        self.topic = topic
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers.split(","),
            group_id=group_id,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            key_deserializer=DefaultSerializer(),
            value_deserializer=JsonBytesDeserializer(),
        )

    def poll(self, on_bar: Callable[[dict], None]) -> None:
        print(f"Listening on topic '{self.topic}'...")
        for message in self.consumer:
            if message.value is None:
                continue
            on_bar(message.value)

    def poll_and_print(self) -> None:
        def print_trade(trade: dict) -> None:
            trade_time = datetime.fromtimestamp(trade["timestamp_ms"] / 1000)
            print(
                f"[{trade['symbol']}] price={trade['price']} "
                f"volume={trade['volume']} trade_time={trade_time} "
                f"received_at={trade['received_at']}"
            )

        self.poll(print_trade)

    def close(self) -> None:
        self.consumer.close()
