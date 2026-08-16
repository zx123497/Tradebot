#!/usr/bin/env bash
# Pull FinData + FinLab images from Docker Hub and recreate containers.
# Run on the VM (or via the GitHub Actions SSH deploy job).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export IMAGE_TAG="${IMAGE_TAG:-latest}"

if [[ -z "${DOCKERHUB_USERNAME:-}" ]]; then
  echo "Set DOCKERHUB_USERNAME (Docker Hub namespace)" >&2
  exit 1
fi

echo "Deploying ${DOCKERHUB_USERNAME}/* IMAGE_TAG=${IMAGE_TAG}"

cd "$ROOT/FinData"
docker compose -f docker-compose.yml -f docker-compose.prod.yml pull
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --no-build --remove-orphans

cd "$ROOT/FinLab"
docker compose -f docker-compose.yml -f docker-compose.prod.yml pull
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --no-build
