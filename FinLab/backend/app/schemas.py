from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class Bar(BaseModel):
    symbol: str
    window_start: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    trade_count: int
    vwap: float


class SymbolSummary(BaseModel):
    symbol: str
    window_start: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    trade_count: int
    vwap: float
    prev_close: float | None = None
    change: float | None = None
    change_pct: float | None = None


class HealthResponse(BaseModel):
    status: str
    clickhouse: bool


class BarsResponse(BaseModel):
    symbol: str
    bars: list[Bar] = Field(default_factory=list)
