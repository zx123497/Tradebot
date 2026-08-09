"""Mock Finnhub WebSocket that replays trades from a TradeSource."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable

import websockets
from websockets.asyncio.server import ServerConnection, serve

from findata.clickhouse_store import ClickHouseTradeSource
from findata.config import Settings, load_settings
from findata.ports import TradeSource


class ListTradeSource:
    """In-memory TradeSource for unit tests and local demos."""

    def __init__(self, trades: list[dict]):
        self._trades = trades

    def load_trades(self, symbols: set[str] | None = None) -> list[dict]:
        if not symbols:
            return list(self._trades)
        return [t for t in self._trades if t["s"] in symbols]


def make_client_handler(
    trade_source: TradeSource,
    *,
    subscribe_quiet_sec: float = 0.5,
    replay_delay_sec: float = 0.05,
) -> Callable[[ServerConnection], object]:
    async def handle_client(websocket: ServerConnection) -> None:
        print(f"Client connected: {websocket.remote_address}")
        subscribed: set[str] = set()
        replayed = False
        replay_task: asyncio.Task | None = None

        async def replay_after_quiet_period() -> None:
            nonlocal replayed
            await asyncio.sleep(subscribe_quiet_sec)
            if replayed:
                return
            trades = trade_source.load_trades(subscribed)
            if not trades:
                print(f"No trades for {sorted(subscribed)}")
                return
            print(f"Replaying {len(trades)} trades for {sorted(subscribed)}...")
            for trade in trades:
                await websocket.send(json.dumps({"type": "trade", "data": [trade]}))
                await asyncio.sleep(replay_delay_sec)
            replayed = True
            print("Replay complete")

        try:
            async for raw in websocket:
                msg = json.loads(raw)
                msg_type = msg.get("type")

                if msg_type == "subscribe":
                    symbol = msg.get("symbol")
                    if not symbol:
                        continue
                    subscribed.add(symbol)
                    print(f"Subscribed: {symbol} (total={len(subscribed)})")
                    if replay_task and not replay_task.done():
                        replay_task.cancel()
                    if not replayed:
                        replay_task = asyncio.create_task(replay_after_quiet_period())

                elif msg_type == "unsubscribe":
                    symbol = msg.get("symbol")
                    if symbol in subscribed:
                        subscribed.remove(symbol)

        except websockets.exceptions.ConnectionClosed:
            print(f"Client disconnected: {websocket.remote_address}")
        finally:
            if replay_task and not replay_task.done():
                replay_task.cancel()

    return handle_client


async def run_mock_server(
    trade_source: TradeSource,
    *,
    host: str = "0.0.0.0",
    port: int = 8765,
    subscribe_quiet_sec: float = 0.5,
    replay_delay_sec: float = 0.05,
) -> None:
    handler = make_client_handler(
        trade_source,
        subscribe_quiet_sec=subscribe_quiet_sec,
        replay_delay_sec=replay_delay_sec,
    )
    print(f"Mock Finnhub WebSocket listening on ws://{host}:{port}")
    async with serve(handler, host, port):
        await asyncio.Future()


def main(settings: Settings | None = None) -> None:
    import clickhouse_connect

    settings = settings or load_settings()
    client = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
    )
    source = ClickHouseTradeSource(client)
    asyncio.run(
        run_mock_server(
            source,
            host=settings.mock_finnhub_host,
            port=settings.mock_finnhub_port,
            subscribe_quiet_sec=settings.mock_subscribe_quiet_sec,
            replay_delay_sec=settings.mock_replay_delay_sec,
        )
    )
