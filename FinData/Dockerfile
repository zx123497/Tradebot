FROM python:3.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Install deps + findata package from the repo lockfile.
COPY pyproject.toml uv.lock ./
COPY FinData/src/findata ./FinData/src/findata
RUN uv sync --frozen --no-dev

COPY FinData/ ./FinData/

WORKDIR /app/FinData/src
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/FinData/src

# Default; overridden per-service in compose.
CMD ["uv", "run", "--no-sync", "python", "main.py"]
