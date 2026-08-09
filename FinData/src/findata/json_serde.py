"""JSON Kafka serde that actually returns bytes.

kafka-python 3.x ships a JsonSerializer that calls super().serialize() on the
abstract Serializer base (which is a no-op), so every value becomes null.
"""

from __future__ import annotations

import json
from typing import Any

from kafka.serializer import Deserializer, Serializer


class JsonBytesSerializer(Serializer):
    def serialize(self, topic: str, headers: Any, data: Any) -> bytes | None:
        if data is None:
            return None
        return json.dumps(data).encode("utf-8")


class JsonBytesDeserializer(Deserializer):
    def deserialize(self, topic: str, headers: Any, data: bytes | None) -> Any:
        if data is None:
            return None
        return json.loads(data.decode("utf-8"))
