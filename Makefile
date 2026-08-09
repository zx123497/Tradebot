# Full stack (infra + Flink + producer + consumer)
up:
	cd FinData && docker compose up -d --build --remove-orphans

# Infra only (Kafka, ClickHouse, Flink) — no producer/consumer
infra:
	cd FinData && docker compose up -d --build \
		kafka kafka-init clickhouse \
		jobmanager taskmanager flink-job-submitter

# Local (host) runs — use when services are up via `make infra`
producer:
	uv run python FinData/src/main.py

consumer:
	uv run python FinData/src/consumer.py

logs-producer:
	cd FinData && docker compose logs -f producer

logs-consumer:
	cd FinData && docker compose logs -f consumer

flink-ui:
	@echo "Flink UI: http://localhost:8081"

# Seed ClickHouse trades fixture
seed-trades:
	uv run python FinData/src/seed_trades.py

# Mock Finnhub WS (reads from ClickHouse trades). Host: ws://localhost:8765
mock-finnhub:
	uv run python FinData/src/mock_finnhub_ws.py

# Unit tests (no infra)
test:
	uv run pytest FinData/tests/unit -q

# Full pipeline integration test (infra + consumer must already be running)
test-integration:
	uv run pytest FinData/tests/integration -m integration -q

down:
	cd FinData && docker compose down
