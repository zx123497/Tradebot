from findata.json_serde import JsonBytesDeserializer, JsonBytesSerializer


def test_json_bytes_round_trip():
    ser = JsonBytesSerializer()
    de = JsonBytesDeserializer()
    payload = {"symbol": "AAPL", "price": 100.5, "timestamp_ms": 1, "volume": 2.0}
    raw = ser.serialize("sp500.trades", None, payload)
    assert isinstance(raw, bytes)
    assert raw is not None
    assert de.deserialize("sp500.trades", None, raw) == payload


def test_json_serializer_none_stays_none():
    ser = JsonBytesSerializer()
    assert ser.serialize("t", None, None) is None


def test_json_deserializer_none_stays_none():
    de = JsonBytesDeserializer()
    assert de.deserialize("t", None, None) is None
