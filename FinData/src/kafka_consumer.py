from collections.abc import Callable
from datetime import datetime

from kafka import KafkaConsumer
from kafka.serializer import DefaultSerializer, JsonSerializer


class TradeKafkaConsumer:
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
            value_deserializer=JsonSerializer(),
        )

    def poll(self, on_trade: Callable[[dict], None]) -> None:
        print(f"Listening on topic '{self.topic}'...")
        for message in self.consumer:
            on_trade(message.value)

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
