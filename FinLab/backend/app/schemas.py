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
    notional: float | None = None


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
    bar_type: str
    bars: list[Bar] = Field(default_factory=list)


class TradeTick(BaseModel):
    symbol: str
    price: float
    volume: float
    timestamp: datetime


class TradesResponse(BaseModel):
    trades: list[TradeTick] = Field(default_factory=list)
