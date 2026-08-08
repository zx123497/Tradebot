FROM python:3.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Install deps from the repo lockfile, then copy FinData sources.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY FinData/ ./FinData/

WORKDIR /app/FinData/src
ENV PYTHONUNBUFFERED=1

# Default; overridden per-service in compose.
CMD ["uv", "run", "--no-sync", "python", "main.py"]
