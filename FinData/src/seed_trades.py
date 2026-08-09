"""CLI entry: seed ClickHouse trades fixture."""

from __future__ import annotations

import clickhouse_connect

from findata.config import load_settings
from findata.fixtures import expected_bar, seed_trades


def main() -> None:
    settings = load_settings()
    client = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
    )
    seed_trades(client, clear_existing=True)
    print("Expected bar:", expected_bar())


if __name__ == "__main__":
    main()
