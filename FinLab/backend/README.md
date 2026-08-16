# FinLab backend

FastAPI service that reads ClickHouse bar tables + live trades for the FinLab UI.

```bash
cp .env.example .env   # optional; defaults match FinData
make lab-api           # from repo root → http://localhost:8000/docs
```

Cloudflare Access (`POLICY_AUD`, `TEAM_DOMAIN`) lives in GitHub secrets, not in this repo. Leave them unset locally to skip JWT checks.

Runs `fastapi dev app/main.py` inside `FinLab/backend`.

### Docker

```bash
# from FinLab/ (FinData stack must be up for ClickHouse)
docker build -t finlab-api ./backend
make lab-up            # API :8000 + UI :3000
```

Endpoints:

- `GET /api/health`
- `GET /api/symbols?bar_type=1m|5m|volume|dollar`
- `GET /api/bars?symbol=…&bar_type=…`
- `GET /api/bars/stream?bar_type=…` (SSE)
- `GET /api/trades?symbol=…`
- `GET /api/trades/stream` (SSE)
