import asyncio
import json

import pytest

from findata.mock_finnhub import ListTradeSource, make_client_handler


class FakeWebSocket:
    def __init__(self):
        self.remote_address = ("127.0.0.1", 12345)
        self.sent: list[str] = []
        self._messages: asyncio.Queue[str] = asyncio.Queue()

    async def send(self, data: str) -> None:
        self.sent.append(data)

    def __aiter__(self):
        return self

    async def __anext__(self) -> str:
        msg = await self._messages.get()
        if msg is None:
            raise StopAsyncIteration
        return msg

    async def push(self, msg: str | None) -> None:
        await self._messages.put(msg)


@pytest.mark.asyncio
async def test_mock_replay_order_from_trade_source():
    trades = [
        {"s": "AAPL", "p": 100.0, "t": 1, "v": 10.0},
        {"s": "AAPL", "p": 105.0, "t": 2, "v": 20.0},
        {"s": "MSFT", "p": 1.0, "t": 3, "v": 1.0},
    ]
    source = ListTradeSource(trades)
    handler = make_client_handler(
        source, subscribe_quiet_sec=0.01, replay_delay_sec=0.0
    )
    ws = FakeWebSocket()
    task = asyncio.create_task(handler(ws))

    await ws.push(json.dumps({"type": "subscribe", "symbol": "AAPL"}))
    await asyncio.sleep(0.05)
    await ws.push(None)
    await asyncio.wait_for(task, timeout=2.0)

    assert len(ws.sent) == 2
    first = json.loads(ws.sent[0])
    second = json.loads(ws.sent[1])
    assert first["type"] == "trade"
    assert first["data"][0]["p"] == 100.0
    assert second["data"][0]["p"] == 105.0
