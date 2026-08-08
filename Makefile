producer:
	uv run python FinData/src/main.py

consumer:
	uv run python FinData/src/consumer.py

infra:
	cd FinData && docker compose up -d --build

flink-ui:
	@echo "Flink UI: http://localhost:8081"
