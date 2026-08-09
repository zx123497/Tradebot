from fakes import FakeFixtureClient

from findata.fixtures import (
    FIXTURE_SYMBOL,
    FIXTURE_TICKS,
    WATERMARK_ADVANCE_TICK,
    build_fixture_rows,
    expected_bar,
    seed_trades,
    unique_window_start_ms,
)


def test_expected_bar_ohlcv_vwap():
    bar = expected_bar()
    assert bar["symbol"] == FIXTURE_SYMBOL
    assert bar["open"] == 100.0
    assert bar["high"] == 105.0
    assert bar["low"] == 99.0
    assert bar["close"] == 102.0
    assert bar["volume"] == 50.0
    assert bar["trade_count"] == 4
    assert bar["vwap"] == 102.5


def test_unique_window_start_ms_increases_with_time():
    a = unique_window_start_ms(1000.0)
    b = unique_window_start_ms(1001.0)
    assert b - a == 120_000


def test_build_fixture_rows_includes_watermark_tick():
    rows = build_fixture_rows(1_000_000)
    assert len(rows) == len(FIXTURE_TICKS) + 1
    assert rows[-1][3] == 1_000_000 + WATERMARK_ADVANCE_TICK[0]


def test_seed_trades_truncates_and_inserts():
    client = FakeFixtureClient()
    window = seed_trades(client, clear_existing=True, window_start_ms=42_000)
    assert window == 42_000
    assert any("TRUNCATE" in c for c in client.commands)
    assert len(client.inserts) == 1
    table, rows, _cols = client.inserts[0]
    assert table == "trades"
    assert len(rows) == 5
