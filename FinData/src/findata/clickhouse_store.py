from datetime import datetime, timezone

import clickhouse_connect


def _parse_timestamp(value: str | datetime | int | float) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        # treat as epoch millis if large, else seconds
        ms = value if value > 1e12 else value * 1000
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


class BarClickHouseStore:
    """Inserts OHLCV bars; includes notional when present (volume/dollar bars)."""

    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        database: str,
        table: str = "bars_1m",
        *,
        include_notional: bool = False,
    ):
        self.table = table
        self.include_notional = include_notional
        self.client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
        )

    def insert_bar(self, bar: dict) -> None:
        row = [
            bar["symbol"],
            _parse_timestamp(bar["window_start"]),
            bar["open_price"],
            bar["high_price"],
            bar["low_price"],
            bar["close_price"],
            bar["volume"],
            int(bar["trade_count"]),
            bar["vwap"],
        ]
        columns = [
            "symbol",
            "window_start",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "trade_count",
            "vwap",
        ]
        if self.include_notional:
            notional = bar.get("notional")
            if notional is None:
                notional = float(bar["vwap"]) * float(bar["volume"])
            row.append(float(notional))
            columns.append("notional")

        self.client.insert(self.table, [row], column_names=columns)


class TradeClickHouseStore:
    """Writes tick rows into findata.trades (TTL 30d schema)."""

    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        database: str,
        table: str = "trades",
    ):
        self.table = table
        self.client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
        )

    def insert_trade(self, trade: dict) -> None:
        ts = trade.get("timestamp")
        if ts is None and "timestamp_ms" in trade:
            ts = datetime.fromtimestamp(trade["timestamp_ms"] / 1000, tz=timezone.utc)
        else:
            ts = _parse_timestamp(ts)
        row = [
            trade["symbol"],
            float(trade["price"]),
            float(trade["volume"]),
            ts,
        ]
        self.client.insert(
            self.table,
            [row],
            column_names=["symbol", "price", "volume", "timestamp"],
        )


class ClickHouseTradeSource:
    """TradeSource adapter that reads Finnhub-shaped ticks from ClickHouse."""

    def __init__(self, client):
        self.client = client

    def load_trades(self, symbols: set[str] | None = None) -> list[dict]:
        if symbols:
            symbol_list = ", ".join(f"'{s}'" for s in sorted(symbols))
            query = (
                "SELECT symbol, price, volume, toUnixTimestamp64Milli(timestamp) "
                f"FROM trades WHERE symbol IN ({symbol_list}) "
                "ORDER BY timestamp ASC"
            )
        else:
            query = (
                "SELECT symbol, price, volume, toUnixTimestamp64Milli(timestamp) "
                "FROM trades ORDER BY timestamp ASC"
            )
        result = self.client.query(query)
        trades = []
        for symbol, price, volume, timestamp_ms in result.result_rows:
            trades.append(
                {
                    "s": symbol,
                    "p": float(price),
                    "v": float(volume),
                    "t": int(timestamp_ms),
                }
            )
        return trades
