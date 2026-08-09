import json

from fakes import FakeTradePublisher

from findata.producer_app import handle_trade_message


def test_handle_trade_message_publishes_and_flushes():
    publisher = FakeTradePublisher()
    raw = json.dumps(
        {
            "type": "trade",
            "data": [
                {"s": "AAPL", "p": 100.0, "t": 1000, "v": 10.0},
                {"s": "AAPL", "p": 101.0, "t": 2000, "v": 5.0},
            ],
        }
    )
    assert handle_trade_message(raw, publisher) == 2
    assert len(publisher.published) == 2
    assert publisher.published[0]["s"] == "AAPL"
    assert publisher.flush_count == 1


def test_handle_trade_message_ignores_non_trade():
    publisher = FakeTradePublisher()
    assert handle_trade_message('{"type":"ping"}', publisher) == 0
    assert publisher.published == []
    assert publisher.flush_count == 0
