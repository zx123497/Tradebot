"""Protocol ports for FinData I/O boundaries."""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol


class TradePublisher(Protocol):
    def publish_trade(self, trade: dict) -> None: ...

    def flush(self) -> None: ...

    def close(self) -> None: ...


class BarConsumer(Protocol):
    def poll(self, on_bar: Callable[[dict], None]) -> None: ...

    def close(self) -> None: ...


class BarStore(Protocol):
    def insert_bar(self, bar: dict) -> None: ...


class TradeSource(Protocol):
    def load_trades(self, symbols: set[str] | None = None) -> list[dict]: ...
