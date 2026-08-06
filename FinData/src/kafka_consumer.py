import json
from datetime import datetime

from kafka import KafkaConsumer


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
            key_deserializer=lambda key: key.decode("utf-8") if key else None,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        )

    def poll_and_print(self) -> None:
        print(f"Listening on topic '{self.topic}'...")
        for message in self.consumer:
            trade = message.value
            trade_time = datetime.fromtimestamp(trade["timestamp_ms"] / 1000)
            print(
                f"[{trade['symbol']}] price={trade['price']} "
                f"volume={trade['volume']} trade_time={trade_time} "
                f"received_at={trade['received_at']}"
            )

    def close(self) -> None:
        self.consumer.close()
