from __future__ import annotations

from clickhouse_connect import get_async_client
from clickhouse_connect.driver.asyncclient import AsyncClient

from app.config import settings

_client: AsyncClient | None = None


async def connect() -> AsyncClient:
    global _client
    if _client is None:
        _client = await get_async_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_database,
        )
    return _client


async def close() -> None:
    global _client
    if _client is not None:
        await _client.close()
        _client = None


async def get_client() -> AsyncClient:
    return await connect()


async def ping() -> bool:
    client = await get_client()
    return bool(await client.command("SELECT 1"))
