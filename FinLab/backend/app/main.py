from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.routes import bars, health

app = FastAPI(title="FinLab API", version="0.1.0")

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
def root() -> dict[str, str]:
    return {"service": "finlab-api", "docs": "/docs"}
