from fastapi import APIRouter

from app.clickhouse import ping
from app.schemas import HealthResponse

router = APIRouter(prefix="/api", tags=["health"])


@router.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    ok = False
    try:
        ok = ping()
    except Exception:  # noqa: BLE001
        ok = False
    return HealthResponse(
        status="ok" if ok else "degraded",
        clickhouse=ok,
    )
