from datetime import datetime, timezone

from findata.models import finnhub_trade_to_payload, parse_finnhub_trade_message


def test_finnhub_trade_to_payload():
    received = datetime(2026, 1, 1, tzinfo=timezone.utc)
    trade = {"s": "AAPL", "p": 100.0, "t": 1_700_000_000_000, "v": 10.0, "c": ["1"]}
    payload = finnhub_trade_to_payload(trade, received_at=received)
    assert payload == {
        "symbol": "AAPL",
        "price": 100.0,
        "timestamp_ms": 1_700_000_000_000,
        "volume": 10.0,
        "received_at": received.isoformat(),
        "conditions": ["1"],
    }


def test_parse_finnhub_trade_message_ignores_non_trade():
    assert parse_finnhub_trade_message('{"type":"ping"}') == []


def test_parse_finnhub_trade_message_extracts_data():
    raw = {
        "type": "trade",
        "data": [{"s": "AAPL", "p": 1.0, "t": 2, "v": 3.0}],
    }
    assert parse_finnhub_trade_message(raw) == raw["data"]
