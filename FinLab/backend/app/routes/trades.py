from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator

from fastapi import APIRouter, Query, Security
from fastapi.responses import StreamingResponse

from app.auth import require_cf_access
from app.bar_types import as_utc
from app.clickhouse import get_client
from app.config import settings
from app.schemas import TradeTick, TradesResponse

router = APIRouter(
    prefix="/api",
    tags=["trades"],
    dependencies=[Security(require_cf_access)],
)


def _row_to_trade(row: tuple) -> TradeTick:
    return TradeTick(
        symbol=row[0],
        price=float(row[1]),
        volume=float(row[2]),
        timestamp=as_utc(row[3]),
    )


@router.get("/trades", response_model=TradesResponse)
async def list_trades(
    symbol: str | None = Query(None),
    limit: int = Query(100, ge=1, le=1000),
) -> TradesResponse:
    client = await get_client()
    params: dict = {"limit": limit}
    where = []
    if symbol:
        where.append("symbol = {symbol:String}")
        params["symbol"] = symbol.upper()
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    result = await client.query(
        f"""
        SELECT symbol, price, volume, timestamp
        FROM {settings.clickhouse_trades_table}
        {where_sql}
        ORDER BY timestamp DESC
        LIMIT {{limit:UInt32}}
        """,
        parameters=params,
    )
    trades = [_row_to_trade(row) for row in result.result_rows]
    trades.reverse()  # oldest → newest for UI
    return TradesResponse(trades=trades)


async def _fetch_trades_since(
    symbols: list[str] | None,
    since: datetime,
) -> list[TradeTick]:
    client = await get_client()
    params: dict = {"since": since}
    where = ["timestamp > {since:DateTime64(3, 'UTC')}"]
    if symbols:
        where.append("symbol IN {symbols:Array(String)}")
        params["symbols"] = symbols
    result = await client.query(
        f"""
        SELECT symbol, price, volume, timestamp
        FROM {settings.clickhouse_trades_table}
        WHERE {' AND '.join(where)}
        ORDER BY timestamp ASC
        LIMIT 500
        """,
        parameters=params,
    )
    return [_row_to_trade(row) for row in result.result_rows]


async def _trade_event_stream(symbols: list[str] | None) -> AsyncIterator[str]:
    last_seen = datetime.now(timezone.utc) - timedelta(seconds=30)
    yield f"event: hello\ndata: {json.dumps({'status': 'connected'})}\n\n"
    while True:
        try:
            trades = await _fetch_trades_since(symbols, last_seen)
            for trade in trades:
                ts = as_utc(trade.timestamp)
                if ts > last_seen:
                    last_seen = ts
                yield f"event: trade\ndata: {json.dumps(trade.model_dump(mode='json'))}\n\n"
        except Exception as exc:  # noqa: BLE001
            yield f"event: error\ndata: {json.dumps({'error': str(exc)})}\n\n"
        await asyncio.sleep(settings.sse_trades_poll_interval_sec)


@router.get("/trades/stream")
async def stream_trades(
    symbols: str | None = Query(
        None, description="Comma-separated symbols; omit for all"
    ),
) -> StreamingResponse:
    symbol_list = (
        [s.strip().upper() for s in symbols.split(",") if s.strip()]
        if symbols
        else None
    )
    return StreamingResponse(
        _trade_event_stream(symbol_list),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
