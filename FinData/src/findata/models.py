"""Trade / bar message helpers."""

from __future__ import annotations

from datetime import datetime, timezone


def finnhub_trade_to_payload(trade: dict, *, received_at: datetime | None = None) -> dict:
    """Map Finnhub trade fields (s/p/t/v) to Kafka trade JSON."""
    received = received_at or datetime.now(timezone.utc)
    payload = {
        "symbol": trade["s"],
        "price": trade["p"],
        "timestamp_ms": trade["t"],
        "volume": trade["v"],
        "received_at": received.isoformat(),
    }
    if "c" in trade:
        payload["conditions"] = trade["c"]
    return payload


def parse_finnhub_trade_message(raw: str | dict) -> list[dict]:
    """Return Finnhub trade dicts from a WS message; empty if not a trade event."""
    import json

    msg = json.loads(raw) if isinstance(raw, str) else raw
    if msg.get("type") != "trade":
        return []
    return list(msg.get("data") or [])
