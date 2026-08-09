from __future__ import annotations

from contextlib import asynccontextmanager
from collections.abc import AsyncIterator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app import clickhouse
from app.config import settings
from app.routes import bars, health


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    await clickhouse.connect()
    try:
        yield
    finally:
        await clickhouse.close()


app = FastAPI(title="FinLab API", version="0.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origin_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router)
app.include_router(bars.router)


@app.get("/")
async def root() -> dict[str, str]:
    return {"service": "finlab-api", "docs": "/docs"}
