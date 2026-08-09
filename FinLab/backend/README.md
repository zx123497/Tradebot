# FinLab backend

FastAPI service that reads ClickHouse bar tables + live trades for the FinLab UI.

```bash
cp .env.example .env   # optional; defaults match FinData
make lab-api           # from repo root → http://localhost:8000/docs
```

Runs `fastapi dev app/main.py` inside `FinLab/backend`.

Endpoints:

- `GET /api/health`
- `GET /api/symbols?bar_type=1m|5m|volume|dollar`
- `GET /api/bars?symbol=…&bar_type=…`
- `GET /api/bars/stream?bar_type=…` (SSE)
- `GET /api/trades?symbol=…`
- `GET /api/trades/stream` (SSE)
