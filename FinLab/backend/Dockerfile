# Build from repo root:
#   docker build -f FinLab/backend/Dockerfile -t finlab-api .
FROM python:3.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Workspace lockfile pins FinLab/backend deps
COPY pyproject.toml uv.lock ./
COPY FinLab/backend/pyproject.toml FinLab/backend/README.md ./FinLab/backend/
COPY FinLab/backend/app ./FinLab/backend/app

RUN uv sync --frozen --package backend --no-dev

WORKDIR /app/FinLab/backend
ENV PYTHONUNBUFFERED=1

EXPOSE 8000
CMD ["uv", "run", "--no-sync", "fastapi", "run", "app/main.py", "--host", "0.0.0.0", "--port", "8000"]
