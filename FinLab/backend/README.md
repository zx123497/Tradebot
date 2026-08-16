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
- `GET /api/bars?symbol=…&bar_type=…` — closed Flink bars (analysis)
- `GET /api/bars/stream?bar_type=…` (SSE) — newly closed Flink bars
- `GET /api/bars/live?symbol=…&bar_type=…` — UI: closed bars + forming open 1m/5m candle
- `GET /api/bars/live/stream?bar_type=…` (SSE) — UI: forming updates + closed bars
- `GET /api/trades?symbol=…`
- `GET /api/trades/stream` (SSE)

### Dual bar routes

Flink tumbling windows only emit a bar after the window watermark advances, so the
open minute never appears on `/api/bars`. The UI uses `/api/bars/live*`, which
aggregates the current 1m/5m window from `findata.trades` and tags it
`is_partial=true` / `source=forming`. When Flink writes the closed bar, it
replaces the forming candle. Volume/dollar bars stay Flink-only (no forming).
