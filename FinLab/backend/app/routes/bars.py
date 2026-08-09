from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from app.clickhouse import get_client
from app.config import settings
from app.schemas import Bar, BarsResponse, SymbolSummary

router = APIRouter(prefix="/api", tags=["bars"])


def _as_utc(value: datetime) -> datetime:
    """Normalize ClickHouse/query datetimes to aware UTC for safe comparisons."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _row_to_bar(row: tuple) -> Bar:
    return Bar(
        symbol=row[0],
        window_start=_as_utc(row[1]),
        open=float(row[2]),
        high=float(row[3]),
        low=float(row[4]),
        close=float(row[5]),
        volume=float(row[6]),
        trade_count=int(row[7]),
        vwap=float(row[8]),
    )


@router.get("/symbols", response_model=list[SymbolSummary])
async def list_symbols() -> list[SymbolSummary]:
    client = await get_client()
    result = await client.query(
        f"""
        SELECT
            symbol,
            window_start,
            open,
            high,
            low,
            close,
            volume,
            trade_count,
            vwap,
            prev_close
        FROM (
            SELECT
                symbol,
                window_start,
                open,
                high,
                low,
                close,
                volume,
                trade_count,
                vwap,
                lagInFrame(toNullable(close), 1) OVER (
                    PARTITION BY symbol
                    ORDER BY window_start
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS prev_close,
                row_number() OVER (
                    PARTITION BY symbol
                    ORDER BY window_start DESC
                ) AS rn
            FROM {settings.clickhouse_table}
        )
        WHERE rn = 1
        ORDER BY symbol
        """
    )
    summaries: list[SymbolSummary] = []
    for row in result.result_rows:
        close = float(row[5])
        prev_close = float(row[9]) if row[9] is not None else None
        change = (close - prev_close) if prev_close is not None else None
        change_pct = (
            (change / prev_close) * 100.0
            if prev_close not in (None, 0) and change is not None
            else None
        )
        summaries.append(
            SymbolSummary(
                symbol=row[0],
                window_start=_as_utc(row[1]),
                open=float(row[2]),
                high=float(row[3]),
                low=float(row[4]),
                close=close,
                volume=float(row[6]),
                trade_count=int(row[7]),
                vwap=float(row[8]),
                prev_close=prev_close,
                change=change,
                change_pct=change_pct,
            )
        )
    return summaries


@router.get("/bars", response_model=BarsResponse)
async def get_bars(
    symbol: str = Query(..., min_length=1),
    from_ts: datetime | None = Query(None, alias="from"),
    to_ts: datetime | None = Query(None, alias="to"),
    limit: int = Query(240, ge=1, le=5000),
) -> BarsResponse:
    client = await get_client()
    if from_ts is None:
        from_ts = datetime.now(timezone.utc) - timedelta(hours=4)

    params: dict = {"symbol": symbol, "from_ts": from_ts, "limit": limit}
    where = [
        "symbol = {symbol:String}",
        "window_start >= {from_ts:DateTime64(3, 'UTC')}",
    ]
    if to_ts is not None:
        where.append("window_start <= {to_ts:DateTime64(3, 'UTC')}")
        params["to_ts"] = to_ts

    result = await client.query(
        f"""
        SELECT symbol, window_start, open, high, low, close, volume, trade_count, vwap
        FROM {settings.clickhouse_table}
        WHERE {' AND '.join(where)}
        ORDER BY window_start ASC
        LIMIT {{limit:UInt32}}
        """,
        parameters=params,
    )
    if result.result_rows is None:
        raise HTTPException(status_code=500, detail="ClickHouse query failed")
    bars = [_row_to_bar(row) for row in result.result_rows]
    return BarsResponse(symbol=symbol, bars=bars)


async def _fetch_bars_since(
    symbols: list[str] | None,
    since: datetime,
) -> list[Bar]:
    client = await get_client()
    params: dict = {"since": since}
    where = ["window_start > {since:DateTime64(3, 'UTC')}"]
    if symbols:
        where.append("symbol IN {symbols:Array(String)}")
        params["symbols"] = symbols
    result = await client.query(
        f"""
        SELECT symbol, window_start, open, high, low, close, volume, trade_count, vwap
        FROM {settings.clickhouse_table}
        WHERE {' AND '.join(where)}
        ORDER BY window_start ASC
        LIMIT 500
        """,
        parameters=params,
    )
    return [_row_to_bar(row) for row in result.result_rows]


async def _bar_event_stream(symbols: list[str] | None) -> AsyncIterator[str]:
    last_seen = datetime.now(timezone.utc) - timedelta(minutes=2)
    yield f"event: hello\ndata: {json.dumps({'status': 'connected'})}\n\n"
    while True:
        try:
            bars = await _fetch_bars_since(symbols, last_seen)
            for bar in bars:
                window_start = _as_utc(bar.window_start)
                if window_start > last_seen:
                    last_seen = window_start
                payload = bar.model_dump(mode="json")
                yield f"event: bar\ndata: {json.dumps(payload)}\n\n"
        except Exception as exc:  # noqa: BLE001 — keep SSE alive
            yield f"event: error\ndata: {json.dumps({'error': str(exc)})}\n\n"
        await asyncio.sleep(settings.sse_poll_interval_sec)


@router.get("/bars/stream")
async def stream_bars(
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
        _bar_event_stream(symbol_list),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
