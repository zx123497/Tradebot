"""Shared helpers for FinLab ClickHouse market-data routes."""

from __future__ import annotations

from datetime import datetime, timezone

BAR_TYPE_TABLES: dict[str, str] = {
    "1m": "bars_1m",
    "5m": "bars_5m",
    "volume": "bars_volume",
    "dollar": "bars_dollar",
}

VALID_BAR_TYPES = tuple(BAR_TYPE_TABLES.keys())


def resolve_bar_table(bar_type: str) -> str:
    key = bar_type.strip().lower()
    if key not in BAR_TYPE_TABLES:
        raise ValueError(
            f"Invalid bar_type '{bar_type}'. Expected one of: {', '.join(VALID_BAR_TYPES)}"
        )
    return BAR_TYPE_TABLES[key]


def as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
