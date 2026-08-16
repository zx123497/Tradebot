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

trades-consumer:
	uv run python FinData/src/trades_consumer.py

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

# FinLab dashboard (ClickHouse must be up with bars data)
lab-api:
	cd FinLab/backend && uv run fastapi dev app/main.py --port 8000

lab-ui:
	cd FinLab/frontend && npm run dev

# Dockerized FinLab (requires FinData stack for ClickHouse / findata_default network)
lab-up:
	cd FinLab && docker compose up -d --build

lab-down:
	cd FinLab && docker compose down

# Push gitignored local .env credentials to GitHub Actions secrets
push-secrets:
	bash scripts/push-github-secrets.sh

# VM: pull Docker Hub images (no local build).
# Requires DOCKERHUB_USERNAME. IMAGE_TAG defaults to latest.
up-prod:
	@test -n "$(DOCKERHUB_USERNAME)" || (echo "Set DOCKERHUB_USERNAME" >&2; exit 1)
	cd FinData && docker compose -f docker-compose.yml -f docker-compose.prod.yml pull
	cd FinData && docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --no-build --remove-orphans

lab-up-prod:
	@test -n "$(DOCKERHUB_USERNAME)" || (echo "Set DOCKERHUB_USERNAME" >&2; exit 1)
	cd FinLab && docker compose -f docker-compose.yml -f docker-compose.prod.yml pull
	cd FinLab && docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --no-build

vm-deploy:
	bash scripts/vm-deploy.sh

down:
	cd FinData && docker compose down
