from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator

from fastapi import APIRouter, HTTPException, Query, Security
from fastapi.responses import StreamingResponse

from app.auth import require_cf_access
from app.bar_types import VALID_BAR_TYPES, as_utc, resolve_bar_table
from app.clickhouse import get_client
from app.config import settings
from app.forming import (
    clickhouse_window_expr,
    merge_closed_and_forming,
    supports_forming,
    window_start_for,
)
from app.schemas import Bar, BarsResponse, SymbolSummary

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api",
    tags=["bars"],
    dependencies=[Security(require_cf_access)],
)


def _row_to_bar(row: tuple, *, is_partial: bool = False, source: str = "flink") -> Bar:
    return Bar(
        symbol=row[0],
        window_start=as_utc(row[1]),
        open=float(row[2]),
        high=float(row[3]),
        low=float(row[4]),
        close=float(row[5]),
        volume=float(row[6]),
        trade_count=int(row[7]),
        vwap=float(row[8]),
        notional=float(row[9]) if len(row) > 9 and row[9] is not None else None,
        is_partial=is_partial,
        source=source,
    )


def _table_or_400(bar_type: str) -> str:
    try:
        return resolve_bar_table(bar_type)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/symbols", response_model=list[SymbolSummary])
async def list_symbols(
    bar_type: str = Query("1m", description=f"One of: {', '.join(VALID_BAR_TYPES)}"),
) -> list[SymbolSummary]:
    table = _table_or_400(bar_type)
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
            FROM {table}
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
                window_start=as_utc(row[1]),
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


async def _query_closed_bars(
    symbol: str,
    bar_type: str,
    *,
    from_ts: datetime | None,
    to_ts: datetime | None,
    limit: int,
) -> list[Bar]:
    table = resolve_bar_table(bar_type)
    client = await get_client()
    if from_ts is None:
        # Volume/dollar bars may span longer — still default to last day of history
        hours = 24 if bar_type in ("volume", "dollar") else 4
        from_ts = datetime.now(timezone.utc) - timedelta(hours=hours)

    params: dict = {"symbol": symbol, "from_ts": from_ts, "limit": limit}
    where = [
        "symbol = {symbol:String}",
        "window_start >= {from_ts:DateTime64(3, 'UTC')}",
    ]
    if to_ts is not None:
        where.append("window_start <= {to_ts:DateTime64(3, 'UTC')}")
        params["to_ts"] = to_ts

    select_cols = "symbol, window_start, open, high, low, close, volume, trade_count, vwap"
    if bar_type in ("volume", "dollar"):
        select_cols += ", notional"

    result = await client.query(
        f"""
        SELECT {select_cols}
        FROM {table}
        WHERE {' AND '.join(where)}
        ORDER BY window_start ASC
        LIMIT {{limit:UInt32}}
        """,
        parameters=params,
    )
    if result.result_rows is None:
        raise HTTPException(status_code=500, detail="ClickHouse query failed")
    return [_row_to_bar(row) for row in result.result_rows]


@router.get("/bars", response_model=BarsResponse)
async def get_bars(
    symbol: str = Query(..., min_length=1),
    bar_type: str = Query("1m", description=f"One of: {', '.join(VALID_BAR_TYPES)}"),
    from_ts: datetime | None = Query(None, alias="from"),
    to_ts: datetime | None = Query(None, alias="to"),
    limit: int = Query(240, ge=1, le=5000),
) -> BarsResponse:
    """Closed Flink bars only (strategy / historical analysis route)."""
    _table_or_400(bar_type)
    bars = await _query_closed_bars(
        symbol.upper(), bar_type, from_ts=from_ts, to_ts=to_ts, limit=limit
    )
    return BarsResponse(symbol=symbol.upper(), bar_type=bar_type, bars=bars, live=False)


async def _fetch_bars_since(
    table: str,
    symbols: list[str] | None,
    since: datetime,
    *,
    with_notional: bool,
) -> list[Bar]:
    client = await get_client()
    params: dict = {"since": since}
    where = ["window_start > {since:DateTime64(3, 'UTC')}"]
    if symbols:
        where.append("symbol IN {symbols:Array(String)}")
        params["symbols"] = symbols
    select_cols = "symbol, window_start, open, high, low, close, volume, trade_count, vwap"
    if with_notional:
        select_cols += ", notional"
    result = await client.query(
        f"""
        SELECT {select_cols}
        FROM {table}
        WHERE {' AND '.join(where)}
        ORDER BY window_start ASC
        LIMIT 500
        """,
        parameters=params,
    )
    return [_row_to_bar(row) for row in result.result_rows]


async def _bar_event_stream(
    symbols: list[str] | None,
    bar_type: str,
) -> AsyncIterator[str]:
    table = resolve_bar_table(bar_type)
    with_notional = bar_type in ("volume", "dollar")
    last_seen = datetime.now(timezone.utc) - timedelta(minutes=2)
    yield f"event: hello\ndata: {json.dumps({'status': 'connected', 'bar_type': bar_type})}\n\n"
    while True:
        try:
            bars = await _fetch_bars_since(
                table, symbols, last_seen, with_notional=with_notional
            )
            for bar in bars:
                window_start = as_utc(bar.window_start)
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
    bar_type: str = Query("1m", description=f"One of: {', '.join(VALID_BAR_TYPES)}"),
) -> StreamingResponse:
    """SSE of newly closed Flink bars (analysis route)."""
    _table_or_400(bar_type)
    symbol_list = (
        [s.strip().upper() for s in symbols.split(",") if s.strip()]
        if symbols
        else None
    )
    return StreamingResponse(
        _bar_event_stream(symbol_list, bar_type),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


async def _fetch_forming_bars(
    bar_type: str,
    symbols: list[str] | None,
    *,
    now: datetime | None = None,
) -> list[Bar]:
    """Aggregate open-window OHLCV from raw trades for 1m/5m.

    Skips symbols where Flink has already written a closed bar for the window.
    """
    if not supports_forming(bar_type):
        return []

    now = now or datetime.now(timezone.utc)
    win_start = window_start_for(now, bar_type)
    win_expr = clickhouse_window_expr(bar_type, "timestamp")
    closed_table = resolve_bar_table(bar_type)
    client = await get_client()
    params: dict = {"win_start": win_start}
    where = [
        "timestamp >= {win_start:DateTime64(3, 'UTC')}",
        f"{win_expr} = {{win_start:DateTime64(3, 'UTC')}}",
        f"""symbol NOT IN (
            SELECT symbol FROM {closed_table}
            WHERE window_start = {{win_start:DateTime64(3, 'UTC')}}
        )""",
    ]
    if symbols:
        where.append("symbol IN {symbols:Array(String)}")
        params["symbols"] = symbols

    result = await client.query(
        f"""
        SELECT
            symbol,
            {win_expr} AS window_start,
            argMin(price, timestamp) AS open,
            max(price) AS high,
            min(price) AS low,
            argMax(price, timestamp) AS close,
            sum(volume) AS volume,
            toUInt32(count()) AS trade_count,
            if(
                sum(volume) = 0,
                argMax(price, timestamp),
                sum(price * volume) / sum(volume)
            ) AS vwap
        FROM {settings.clickhouse_trades_table}
        WHERE {' AND '.join(where)}
        GROUP BY symbol, window_start
        ORDER BY symbol
        LIMIT 500
        """,
        parameters=params,
    )
    return [
        _row_to_bar(row, is_partial=True, source="forming")
        for row in result.result_rows
    ]


def _bar_fingerprint(bar: Bar) -> tuple:
    return (
        bar.symbol,
        as_utc(bar.window_start).isoformat(),
        bar.open,
        bar.high,
        bar.low,
        bar.close,
        bar.volume,
        bar.trade_count,
        bar.vwap,
        bar.is_partial,
        bar.source,
    )


@router.get("/bars/live", response_model=BarsResponse)
async def get_live_bars(
    symbol: str = Query(..., min_length=1),
    bar_type: str = Query("1m", description=f"One of: {', '.join(VALID_BAR_TYPES)}"),
    from_ts: datetime | None = Query(None, alias="from"),
    to_ts: datetime | None = Query(None, alias="to"),
    limit: int = Query(240, ge=1, le=5000),
) -> BarsResponse:
    """UI route: closed Flink bars plus a forming candle for the open 1m/5m window.

    Volume/dollar bar types have no forming candle (threshold bars stay Flink-only).
    """
    _table_or_400(bar_type)
    symbol_u = symbol.upper()
    closed = await _query_closed_bars(
        symbol_u, bar_type, from_ts=from_ts, to_ts=to_ts, limit=limit
    )
    forming = await _fetch_forming_bars(bar_type, [symbol_u])
    bars = merge_closed_and_forming(closed, forming)
    return BarsResponse(
        symbol=symbol_u,
        bar_type=bar_type,
        bars=bars,
        live=supports_forming(bar_type),
    )


async def _live_bar_event_stream(
    symbols: list[str] | None,
    bar_type: str,
) -> AsyncIterator[str]:
    """SSE: forming-bar updates + newly closed Flink bars for the UI."""
    table = resolve_bar_table(bar_type)
    with_notional = bar_type in ("volume", "dollar")
    forming_enabled = supports_forming(bar_type)
    last_closed_seen = datetime.now(timezone.utc) - timedelta(minutes=2)
    # (symbol, window_start_iso) → last emitted fingerprint
    last_forming: dict[tuple[str, str], tuple] = {}
    yield (
        "event: hello\n"
        f"data: {json.dumps({'status': 'connected', 'bar_type': bar_type, 'live': forming_enabled})}\n\n"
    )
    while True:
        try:
            # Closed Flink bars (authoritative when a window finishes)
            closed = await _fetch_bars_since(
                table, symbols, last_closed_seen, with_notional=with_notional
            )
            closed_keys: set[tuple[str, str]] = set()
            for bar in closed:
                window_start = as_utc(bar.window_start)
                if window_start > last_closed_seen:
                    last_closed_seen = window_start
                key = (bar.symbol, window_start.isoformat())
                closed_keys.add(key)
                last_forming.pop(key, None)
                payload = bar.model_dump(mode="json")
                yield f"event: bar\ndata: {json.dumps(payload)}\n\n"

            if forming_enabled:
                forming = await _fetch_forming_bars(bar_type, symbols)
                for bar in forming:
                    window_start = as_utc(bar.window_start)
                    key = (bar.symbol, window_start.isoformat())
                    if key in closed_keys:
                        continue
                    fp = _bar_fingerprint(bar)
                    if last_forming.get(key) == fp:
                        continue
                    last_forming[key] = fp
                    payload = bar.model_dump(mode="json")
                    yield f"event: bar\ndata: {json.dumps(payload)}\n\n"
        except Exception:  # noqa: BLE001 — keep SSE alive
            logger.exception("Unhandled error in live bars SSE stream")
            yield (
                f"event: error\ndata: "
                f"{json.dumps({'error': 'An internal error occurred.'})}\n\n"
            )
        await asyncio.sleep(settings.sse_live_poll_interval_sec)


@router.get("/bars/live/stream")
async def stream_live_bars(
    symbols: str | None = Query(
        None, description="Comma-separated symbols; omit for all"
    ),
    bar_type: str = Query("1m", description=f"One of: {', '.join(VALID_BAR_TYPES)}"),
) -> StreamingResponse:
    """SSE for the UI: forming open-window bars (1m/5m) plus closed Flink bars."""
    _table_or_400(bar_type)
    symbol_list = (
        [s.strip().upper() for s in symbols.split(",") if s.strip()]
        if symbols
        else None
    )
    return StreamingResponse(
        _live_bar_event_stream(symbol_list, bar_type),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
