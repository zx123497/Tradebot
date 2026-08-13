"""Forming (in-progress) OHLCV bars from raw trades for the live UI route.

Flink emits closed event-time bars only after the window watermark advances.
The UI needs a provisional candle for the open 1m/5m window; that is built
here from ``findata.trades`` and tagged ``is_partial=True``.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.schemas import Bar

# Time bars that can be formed from wall-clock trade aggregation.
FORMING_BAR_TYPES: frozenset[str] = frozenset({"1m", "5m"})

_BAR_INTERVAL: dict[str, timedelta] = {
    "1m": timedelta(minutes=1),
    "5m": timedelta(minutes=5),
}


def supports_forming(bar_type: str) -> bool:
    return bar_type.strip().lower() in FORMING_BAR_TYPES


def bar_interval(bar_type: str) -> timedelta:
    key = bar_type.strip().lower()
    if key not in _BAR_INTERVAL:
        raise ValueError(f"Forming bars not supported for bar_type '{bar_type}'")
    return _BAR_INTERVAL[key]


def window_start_for(ts: datetime, bar_type: str) -> datetime:
    """Floor ``ts`` to the open time-bar window (UTC)."""
    ts = ts.astimezone(timezone.utc) if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    key = bar_type.strip().lower()
    if key == "1m":
        return ts.replace(second=0, microsecond=0)
    if key == "5m":
        minute = ts.minute - (ts.minute % 5)
        return ts.replace(minute=minute, second=0, microsecond=0)
    raise ValueError(f"Forming bars not supported for bar_type '{bar_type}'")


def clickhouse_window_expr(bar_type: str, column: str = "timestamp") -> str:
    """ClickHouse expression that floors a timestamp column to the bar window."""
    key = bar_type.strip().lower()
    if key == "1m":
        return f"toStartOfMinute({column})"
    if key == "5m":
        return f"toStartOfInterval({column}, INTERVAL 5 MINUTE)"
    raise ValueError(f"Forming bars not supported for bar_type '{bar_type}'")


def merge_closed_and_forming(closed: list[Bar], forming: list[Bar]) -> list[Bar]:
    """Append forming bars when their window is not already closed by Flink.

    Closed (non-partial) bars always win for a given ``(symbol, window_start)``.
    """
    by_key: dict[tuple[str, datetime], Bar] = {}
    for bar in closed:
        start = bar.window_start
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        key = (bar.symbol, start)
        by_key[key] = bar.model_copy(
            update={
                "window_start": start,
                "is_partial": False,
                "source": bar.source or "flink",
            }
        )

    for bar in forming:
        start = bar.window_start
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        key = (bar.symbol, start)
        if key in by_key and not by_key[key].is_partial:
            continue
        by_key[key] = bar.model_copy(
            update={
                "window_start": start,
                "is_partial": True,
                "source": "forming",
            }
        )

    return sorted(by_key.values(), key=lambda b: (b.symbol, b.window_start))


def aggregate_trades_to_bar(
    symbol: str,
    window_start: datetime,
    trades: list[tuple[float, float, datetime]],
) -> Bar | None:
    """Build one OHLCV bar from ``(price, volume, timestamp)`` trades.

    Used by unit tests; production path aggregates in ClickHouse.
    """
    if not trades:
        return None
    ordered = sorted(trades, key=lambda t: t[2])
    prices = [p for p, _, _ in ordered]
    volumes = [v for _, v, _ in ordered]
    total_vol = sum(volumes)
    notional = sum(p * v for p, v, _ in ordered)
    vwap = (notional / total_vol) if total_vol else prices[-1]
    return Bar(
        symbol=symbol,
        window_start=window_start,
        open=prices[0],
        high=max(prices),
        low=min(prices),
        close=prices[-1],
        volume=total_vol,
        trade_count=len(ordered),
        vwap=vwap,
        is_partial=True,
        source="forming",
    )
