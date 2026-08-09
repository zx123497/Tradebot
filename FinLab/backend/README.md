# FinLab backend

FastAPI service that reads ClickHouse `findata.bars_1m` for the FinLab UI.

```bash
cp .env.example .env   # optional; defaults match FinData
make lab-api           # from repo root → http://localhost:8000/docs
```

Runs `fastapi dev app/main.py` inside `FinLab/backend`.

Endpoints: `/api/health`, `/api/symbols`, `/api/bars`, `/api/bars/stream` (SSE).
