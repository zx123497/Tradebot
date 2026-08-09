"""Deterministic trade fixtures for pipeline tests."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Protocol

# One symbol, one completed 1-minute window of known OHLCV.
FIXTURE_SYMBOL = "AAPL"
# (offset_ms_within_minute, price, volume)
FIXTURE_TICKS = [
    (0, 100.0, 10.0),
    (10_000, 105.0, 20.0),
    (20_000, 99.0, 5.0),
    (50_000, 102.0, 15.0),
]
# Extra tick in the *next* minute so Flink's event-time watermark advances.
WATERMARK_ADVANCE_TICK = (70_000, 102.0, 1.0)

TRADE_COLUMN_NAMES = [
    "symbol",
    "price",
    "volume",
    "timestamp_ms",
    "trade_time",
    "received_at",
    "conditions",
]


class FixtureClient(Protocol):
    def command(self, sql: str) -> Any: ...

    def insert(
        self,
        table: str,
        rows: list,
        column_names: list[str],
    ) -> Any: ...


def expected_bar() -> dict:
    prices = [p for _, p, _ in FIXTURE_TICKS]
    volumes = [v for _, _, v in FIXTURE_TICKS]
    total_vol = sum(volumes)
    return {
        "symbol": FIXTURE_SYMBOL,
        "open": prices[0],
        "high": max(prices),
        "low": min(prices),
        "close": prices[-1],
        "volume": total_vol,
        "trade_count": len(FIXTURE_TICKS),
        "vwap": sum(p * v for p, v in zip(prices, volumes)) / total_vol,
    }


def unique_window_start_ms(now_s: float | None = None) -> int:
    """Unique 1m window start always ahead of prior Flink watermarks.

    Spaces windows by 2 minutes of event time per wall-clock second.
    """
    return int(now_s if now_s is not None else time.time()) * 120_000


def build_fixture_rows(
    window_start_ms: int,
    *,
    received_at: datetime | None = None,
) -> list[list]:
    now = received_at or datetime.now(timezone.utc)
    rows = []
    for offset_ms, price, volume in [*FIXTURE_TICKS, WATERMARK_ADVANCE_TICK]:
        ts_ms = window_start_ms + offset_ms
        trade_time = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        rows.append(
            [
                FIXTURE_SYMBOL,
                price,
                volume,
                ts_ms,
                trade_time,
                now,
                [],
            ]
        )
    return rows


def seed_trades(
    client: FixtureClient,
    *,
    clear_existing: bool = True,
    window_start_ms: int | None = None,
) -> int:
    """Insert fixture ticks into findata.trades. Returns window_start_ms."""
    window_start_ms = (
        window_start_ms if window_start_ms is not None else unique_window_start_ms()
    )

    if clear_existing:
        client.command("TRUNCATE TABLE IF EXISTS trades")
        client.command("TRUNCATE TABLE IF EXISTS bars_1m")

    rows = build_fixture_rows(window_start_ms)
    client.insert("trades", rows, column_names=TRADE_COLUMN_NAMES)
    print(
        f"Seeded {len(rows)} trades for {FIXTURE_SYMBOL} "
        f"window_start_ms={window_start_ms}",
        flush=True,
    )
    return window_start_ms
