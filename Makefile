# Full stack (infra + Flink + producer + consumer)
up:
	cd FinData && docker compose up -d --build

# Infra only (Kafka, ClickHouse, Flink, Marquez) — no producer/consumer
infra:
	cd FinData && docker compose up -d --build \
		kafka kafka-init clickhouse \
		jobmanager taskmanager flink-job-submitter \
		marquez-db marquez marquez-web

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

marquez-ui:
	@echo "Marquez UI: http://localhost:3000"
	@echo "Marquez API: http://localhost:5002"

down:
	cd FinData && docker compose down
