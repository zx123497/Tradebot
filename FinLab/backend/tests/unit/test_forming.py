"""Unit tests for forming-bar helpers (no ClickHouse)."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app.forming import (
    aggregate_trades_to_bar,
    bar_interval,
    clickhouse_window_expr,
    merge_closed_and_forming,
    supports_forming,
    window_start_for,
)
from app.schemas import Bar


def test_supports_forming():
    assert supports_forming("1m")
    assert supports_forming("5m")
    assert not supports_forming("volume")
    assert not supports_forming("dollar")


def test_window_start_1m():
    ts = datetime(2026, 8, 12, 14, 5, 42, 123000, tzinfo=timezone.utc)
    assert window_start_for(ts, "1m") == datetime(
        2026, 8, 12, 14, 5, 0, tzinfo=timezone.utc
    )


def test_window_start_5m():
    ts = datetime(2026, 8, 12, 14, 7, 42, tzinfo=timezone.utc)
    assert window_start_for(ts, "5m") == datetime(
        2026, 8, 12, 14, 5, 0, tzinfo=timezone.utc
    )
    ts2 = datetime(2026, 8, 12, 14, 10, 1, tzinfo=timezone.utc)
    assert window_start_for(ts2, "5m") == datetime(
        2026, 8, 12, 14, 10, 0, tzinfo=timezone.utc
    )


def test_window_start_rejects_volume():
    with pytest.raises(ValueError):
        window_start_for(datetime.now(timezone.utc), "volume")


def test_bar_interval_and_ch_expr():
    assert bar_interval("1m").total_seconds() == 60
    assert bar_interval("5m").total_seconds() == 300
    assert "toStartOfMinute" in clickhouse_window_expr("1m")
    assert "INTERVAL 5 MINUTE" in clickhouse_window_expr("5m")


def test_aggregate_trades_to_bar_ohlcv_vwap():
    win = datetime(2026, 8, 12, 14, 5, 0, tzinfo=timezone.utc)
    trades = [
        (100.0, 10.0, datetime(2026, 8, 12, 14, 5, 1, tzinfo=timezone.utc)),
        (102.0, 5.0, datetime(2026, 8, 12, 14, 5, 10, tzinfo=timezone.utc)),
        (101.0, 5.0, datetime(2026, 8, 12, 14, 5, 20, tzinfo=timezone.utc)),
        (99.0, 10.0, datetime(2026, 8, 12, 14, 5, 30, tzinfo=timezone.utc)),
    ]
    bar = aggregate_trades_to_bar("AAPL", win, trades)
    assert bar is not None
    assert bar.open == 100.0
    assert bar.high == 102.0
    assert bar.low == 99.0
    assert bar.close == 99.0
    assert bar.volume == 30.0
    assert bar.trade_count == 4
    # (100*10 + 102*5 + 101*5 + 99*10) / 30 = 3005/30
    assert bar.vwap == pytest.approx(3005 / 30)
    assert bar.is_partial is True
    assert bar.source == "forming"


def test_aggregate_empty():
    win = datetime(2026, 8, 12, 14, 5, 0, tzinfo=timezone.utc)
    assert aggregate_trades_to_bar("AAPL", win, []) is None


def _closed(symbol: str, start: datetime, close: float) -> Bar:
    return Bar(
        symbol=symbol,
        window_start=start,
        open=close,
        high=close,
        low=close,
        close=close,
        volume=1,
        trade_count=1,
        vwap=close,
        is_partial=False,
        source="flink",
    )


def _forming(symbol: str, start: datetime, close: float) -> Bar:
    return Bar(
        symbol=symbol,
        window_start=start,
        open=close,
        high=close,
        low=close,
        close=close,
        volume=2,
        trade_count=2,
        vwap=close,
        is_partial=True,
        source="forming",
    )


def test_merge_appends_forming_when_not_closed():
    t0 = datetime(2026, 8, 12, 14, 4, 0, tzinfo=timezone.utc)
    t1 = datetime(2026, 8, 12, 14, 5, 0, tzinfo=timezone.utc)
    closed = [_closed("AAPL", t0, 100.0)]
    forming = [_forming("AAPL", t1, 101.0)]
    merged = merge_closed_and_forming(closed, forming)
    assert len(merged) == 2
    assert merged[0].window_start == t0
    assert merged[0].is_partial is False
    assert merged[1].window_start == t1
    assert merged[1].is_partial is True
    assert merged[1].close == 101.0


def test_merge_prefers_closed_over_forming_same_window():
    t1 = datetime(2026, 8, 12, 14, 5, 0, tzinfo=timezone.utc)
    closed = [_closed("AAPL", t1, 100.0)]
    forming = [_forming("AAPL", t1, 999.0)]
    merged = merge_closed_and_forming(closed, forming)
    assert len(merged) == 1
    assert merged[0].close == 100.0
    assert merged[0].is_partial is False
    assert merged[0].source == "flink"
