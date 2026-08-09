# Full stack (infra + Flink + producer + consumer)
up:
	cd FinData && docker compose up -d --build

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

down:
	cd FinData && docker compose down
