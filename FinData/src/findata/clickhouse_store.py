from datetime import datetime, timezone

import clickhouse_connect


def _parse_timestamp(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class BarClickHouseStore:
    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        database: str,
        table: str = "bars_1m",
    ):
        self.table = table
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
        self.client.insert(
            self.table,
            [row],
            column_names=[
                "symbol",
                "window_start",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "trade_count",
                "vwap",
            ],
        )


class TradeClickHouseStore:
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
        row = [
            trade["symbol"],
            trade["price"],
            trade["volume"],
            trade["timestamp_ms"],
            datetime.fromtimestamp(trade["timestamp_ms"] / 1000, tz=timezone.utc),
            datetime.fromisoformat(trade["received_at"]),
            trade.get("conditions", []),
        ]
        self.client.insert(
            self.table,
            [row],
            column_names=[
                "symbol",
                "price",
                "volume",
                "timestamp_ms",
                "trade_time",
                "received_at",
                "conditions",
            ],
        )


class ClickHouseTradeSource:
    """TradeSource adapter that reads Finnhub-shaped ticks from ClickHouse."""

    def __init__(self, client):
        self.client = client

    def load_trades(self, symbols: set[str] | None = None) -> list[dict]:
        if symbols:
            symbol_list = ", ".join(f"'{s}'" for s in sorted(symbols))
            query = (
                "SELECT symbol, price, volume, timestamp_ms "
                f"FROM trades WHERE symbol IN ({symbol_list}) "
                "ORDER BY timestamp_ms ASC"
            )
        else:
            query = (
                "SELECT symbol, price, volume, timestamp_ms "
                "FROM trades ORDER BY timestamp_ms ASC"
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
