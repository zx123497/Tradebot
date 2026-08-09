"""Shared fakes for unit tests."""

from __future__ import annotations

from collections.abc import Callable


class FakeTradePublisher:
    def __init__(self) -> None:
        self.published: list[dict] = []
        self.flush_count = 0
        self.closed = False

    def publish_trade(self, trade: dict) -> None:
        self.published.append(trade)

    def flush(self) -> None:
        self.flush_count += 1

    def close(self) -> None:
        self.closed = True


class FakeBarStore:
    def __init__(self) -> None:
        self.bars: list[dict] = []

    def insert_bar(self, bar: dict) -> None:
        self.bars.append(bar)


class FakeBarConsumer:
    def __init__(self, bars: list[dict]):
        self._bars = bars
        self.closed = False

    def poll(self, on_bar: Callable[[dict], None]) -> None:
        for bar in self._bars:
            on_bar(bar)

    def close(self) -> None:
        self.closed = True


class FakeFixtureClient:
    def __init__(self) -> None:
        self.commands: list[str] = []
        self.inserts: list[tuple] = []

    def command(self, sql: str):
        self.commands.append(sql)

    def insert(self, table: str, rows: list, column_names: list[str]):
        self.inserts.append((table, rows, column_names))
