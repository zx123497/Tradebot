# Tradebot

Real-time S&P 500 market data pipeline: stream live trades from Finnhub, aggregate them into 1-minute OHLCV bars with Apache Flink, and store the bars in ClickHouse.

```
Finnhub WebSocket
       │
       ▼
 Python producer ──► Kafka: sp500.trades
                           │
                           ▼
                   Flink DataStreamJob
                   (1-minute OHLCV + VWAP)
                           │
                           ▼
                   Kafka: sp500.bars.1m
                           │
                           ▼
 Python consumer ──► ClickHouse: findata.bars_1m
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
├── FinData/
│   ├── docker-compose.yml      # Kafka, ClickHouse, Flink, producer, consumer
│   ├── clickhouse/init.sql     # Schema (trades + bars_1m)
│   ├── sp500.csv               # S&P 500 symbol universe
│   ├── .env.example
│   └── src/
│       ├── main.py             # Finnhub → Kafka producer
│       ├── consumer.py         # Kafka bars → ClickHouse
│       ├── kafka_producer.py
│       ├── kafka_consumer.py
│       ├── clickhouse_store.py
│       └── lineage.py          # Optional OpenLineage emitter (disabled by default)
├── findata-flink/              # Java Flink job
│   ├── Dockerfile
│   ├── submit-job.sh
│   └── src/main/java/findata/
│       └── DataStreamJob.java
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
uv sync
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

## Notes

- Finnhub free tier limits concurrent WebSocket subscriptions; use `SYMBOL_LIMIT` if you hit rate limits.
- Raw trades stay on Kafka; ClickHouse stores aggregated bars only.
- Inside Docker, Flink connects to Kafka at `kafka:9092`. From the host, use `localhost:9094`.
- OpenLineage emitters remain in the codebase but are **disabled** (`OPENLINEAGE_ENABLED=false`) unless you run your own Marquez backend.
