# FinLab backend

FastAPI service that reads ClickHouse bar tables + live trades for the FinLab UI.

```bash
cp .env.example .env   # optional; defaults match FinData
make lab-api           # from repo root → http://localhost:8000/docs
```

Runs `fastapi dev app/main.py` inside `FinLab/backend`.

### Docker

```bash
# from repo root (FinData stack must be up for ClickHouse)
docker build -f FinLab/backend/Dockerfile -t finlab-api .
make lab-up            # API :8000 + UI :3000
```

Endpoints:

- `GET /api/health`
- `GET /api/symbols?bar_type=1m|5m|volume|dollar`
- `GET /api/bars?symbol=…&bar_type=…`
- `GET /api/bars/stream?bar_type=…` (SSE)
- `GET /api/trades?symbol=…`
- `GET /api/trades/stream` (SSE)
