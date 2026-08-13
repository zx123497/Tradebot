# Tradebot

Real-time S&P 500 market data pipeline: stream live trades from Finnhub, aggregate them into 1-minute OHLCV bars with Apache Flink, and store the bars in ClickHouse.

```
Finnhub WebSocket
       │
       ▼
 Python producer ──► Kafka: sp500.trades ──► ClickHouse: trades (TTL 30d)
                           │
                           ▼
                   Flink DataStreamJob
                   ├── 1m / 5m time bars
                   ├── volume bars (~50k shares)
                   └── dollar bars (~$5M notional)
                           │
                           ▼
                   Kafka: sp500.bars.{1m,5m,volume,dollar}
                           │
                           ▼
 Python consumers ──► ClickHouse: bars_* (no TTL)
```

By default the producer filters to the **Information Technology** GICS sector.

## Stack

| Component | Role |
|-----------|------|
| [Finnhub](https://finnhub.io/) | Live US equity trade WebSocket |
| Apache Kafka | Message bus for raw trades and aggregated bars |
| Apache Flink 2.3 | Event-time 1-minute tumbling window aggregation |
| ClickHouse | Analytical store for OHLCV bars |
| Python 3.11 + `uv` | Producer and consumer |

## Project layout

```
tradebot/
├── FinData/                    # Pipeline (Kafka / Flink / ClickHouse)
├── FinLab/
│   ├── backend/                # FastAPI → ClickHouse bars_1m
│   └── frontend/               # Vite + shadcn realtime dashboard
├── findata-flink/              # Java Flink job
├── Makefile
└── pyproject.toml
```

## Prerequisites

- Docker Desktop (or Docker Engine + Compose)
- [uv](https://docs.astral.sh/uv/) (Python 3.11)
- A [Finnhub API key](https://finnhub.io/register)

## Setup

1. **Install Python deps**

```bash
uv sync --extra dev
```

2. **Configure environment**

```bash
cp FinData/.env.example FinData/.env
```

Edit `FinData/.env` and set `FINNHUB_API_KEY`. Other defaults work for local Docker:

| Variable | Default | Notes |
|----------|---------|-------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9094` | Host → Kafka external listener |
| `KAFKA_TOPIC` | `sp500.trades` | Raw trades |
| `KAFKA_BARS_TOPIC` | `sp500.bars.1m` | Aggregated bars |
| `GICS_SECTOR` | `Information Technology` | Symbol filter |
| `SYMBOL_LIMIT` | _(unset)_ | Cap subscriptions (Finnhub free tier ~30–50) |
| `CLICKHOUSE_*` | `findata` / `findata` | DB credentials |

## Run

### 1. Start everything (recommended for home lab / VM)

Ensure `FinData/.env` has `FINNHUB_API_KEY`, then:

```bash
make up
# equivalent: cd FinData && docker compose up -d --build
```

This starts Kafka, ClickHouse, Flink, **producer**, and **consumer**.

```bash
make logs-producer   # follow producer logs
make logs-consumer   # follow consumer logs
```

### 2. Infra only + local Python apps

```bash
make infra           # Docker services without producer/consumer
make producer        # host process via uv
make consumer
```

Flink UI: http://localhost:8081

Inside Docker, producer/consumer use `kafka:9092` and `clickhouse:8123`. Host runs still use `localhost` values from `.env`.

## Query ClickHouse

```bash
docker exec -it findata-clickhouse-1 clickhouse-client \
  --user findata --password findata \
  --query "SELECT symbol, window_start, open, high, low, close, volume, trade_count, vwap
           FROM findata.bars_1m
           ORDER BY window_start DESC
           LIMIT 20"
```

Or via HTTP:

```bash
echo "SELECT count() FROM findata.bars_1m" | \
  curl 'http://localhost:8123/?user=findata&password=findata' --data-binary @-
```

## Flink job

`findata-flink` builds a fat JAR and runs inside Docker:

- **Source:** Kafka `sp500.trades` (JSON trades)
- **Window:** 1-minute tumbling, event time from `timestamp_ms`, 5s out-of-orderness
- **Aggregate:** open / high / low / close / volume / trade_count / VWAP per symbol
- **Sink:** Kafka `sp500.bars.1m` (JSON matching the Python consumer)

Rebuild and resubmit after Java changes:

```bash
cd FinData && docker compose up -d --build jobmanager taskmanager flink-job-submitter
```

Check job status:

```bash
curl -s http://localhost:8081/jobs/overview | python3 -m json.tool
```

## Ports

| Service | Port |
|---------|------|
| Kafka (host) | 9094 |
| ClickHouse HTTP | 8123 |
| ClickHouse native | 9000 |
| Flink UI | 8081 |

## Tests

```bash
make test                 # unit tests (no Docker)
make test-integration     # e2e (infra + consumer must be up)
```

Unit tests cover JSON serde, Finnhub message handling, consumer bar writes, fixtures, and mock WS replay via Protocol fakes.

## Integration test

Replay fixture trades from ClickHouse through a mock Finnhub WebSocket and assert Flink bars land in `bars_1m`.

```bash
# Infra + consumer (stop the live Finnhub producer if running)
make infra
cd FinData && docker compose up -d consumer

make test-integration
```

What it covers:

1. **Seed** deterministic AAPL ticks into `findata.trades` (unique event-time window)
2. **Mock Finnhub WS** reads those rows and emits Finnhub trade messages
3. **Producer** connects via `FINNHUB_WS_URL=ws://localhost:8765` and `SUBSCRIBE_SYMBOLS=AAPL`
4. **Flink** aggregates → `sp500.bars.1m`
5. **Consumer** writes → `findata.bars_1m`
6. Assert OHLCV / VWAP / trade_count match the fixture

Helpers:

```bash
make seed-trades      # seed only
make mock-finnhub     # mock WS only (ws://localhost:8765)
```

Or via Compose profile:

```bash
cd FinData && docker compose --profile test up -d mock-finnhub
```

## FinLab dashboard

Realtime watchlist + multi-bar candlesticks (`1m` / `5m` / `volume` / `dollar`) and a live trade tape over ClickHouse.

```bash
# 1. Pipeline data available (all bar consumers)
make infra
cd FinData && docker compose up -d consumer consumer-bars-5m consumer-bars-volume consumer-bars-dollar trades-consumer

# 2a. Local (host) — API :8000, Vite :5173
make lab-api
make lab-ui

# 2b. Docker — API :8000, nginx UI :3000 (joins findata_default for ClickHouse)
make lab-up
```

Dockerfiles: `FinLab/backend/Dockerfile`, `FinLab/frontend/Dockerfile` (compose: `FinLab/docker-compose.yml`).

API routes: `GET /api/health`, `/api/symbols?bar_type=…`, `/api/bars` + `/api/bars/stream` (closed Flink bars), `/api/bars/live` + `/api/bars/live/stream` (UI: forming open 1m/5m from trades + closed bars), `/api/trades`, `/api/trades/stream` (SSE).
Copy `FinLab/backend/.env.example` if you need non-default ClickHouse settings.

The dashboard chart uses the **live** routes so the current minute/5m candle updates
as trades arrive. Flink closed bars remain the source of truth for strategy analysis.

## Notes

- **Schema changes:** `findata.trades` / `bars_*` are created from `FinData/clickhouse/init.sql` only on first ClickHouse boot. After changing the schema, recreate the volume: `cd FinData && docker compose down -v && docker compose up -d`.
- Finnhub free tier limits concurrent WebSocket subscriptions; use `SYMBOL_LIMIT` if you hit rate limits.
- Raw ticks land on Kafka + ClickHouse `trades` (30-day TTL). Aggregated bars have **no** TTL.
- Flink volume-bar threshold defaults to **50,000** shares; dollar-bar threshold to **$5,000,000** notional (`FLINK_VOLUME_BAR_THRESHOLD` / `FLINK_DOLLAR_BAR_THRESHOLD`).
- Inside Docker, Flink connects to Kafka at `kafka:9092`. From the host, use `localhost:9094`.
- OpenLineage emitters remain in the codebase but are **disabled** (`OPENLINEAGE_ENABLED=false`) unless you run your own Marquez backend.
