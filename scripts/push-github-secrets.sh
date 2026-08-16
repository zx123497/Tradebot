#!/usr/bin/env bash
# Push local gitignored credentials to GitHub Actions secrets.
# Values are piped to `gh` and never printed.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if ! command -v gh >/dev/null 2>&1; then
  echo "gh CLI is required: https://cli.github.com/" >&2
  exit 1
fi

if ! gh auth status >/dev/null 2>&1; then
  echo "Not logged in to GitHub. Run: gh auth login" >&2
  exit 1
fi

dotenv_get() {
  local file="$1" name="$2" line=""
  [[ -f "$file" ]] || return 0
  line="$(grep -E "^${name}=" "$file" | tail -n 1 || true)"
  if [[ -z "$line" ]]; then
    line="$(grep -E "^# ${name}=" "$file" | tail -n 1 || true)"
    line="${line#\# }"
  fi
  [[ -n "$line" ]] || return 0
  printf '%s' "${line#*=}"
}

push_secret() {
  local name="$1" value="$2"
  if [[ -z "$value" ]]; then
    echo "skip ${name} (empty / not in local .env)"
    return 0
  fi
  printf '%s' "$value" | gh secret set "$name"
  echo "set ${name}"
}

FINNHUB_API_KEY="$(dotenv_get "$ROOT/FinData/.env" FINNHUB_API_KEY)"
CLICKHOUSE_PASSWORD="$(dotenv_get "$ROOT/FinData/.env" CLICKHOUSE_PASSWORD)"
POLICY_AUD="$(dotenv_get "$ROOT/FinLab/backend/.env" POLICY_AUD)"
TEAM_DOMAIN="$(dotenv_get "$ROOT/FinLab/backend/.env" TEAM_DOMAIN)"
DOCKERHUB_USERNAME="$(dotenv_get "$ROOT/FinData/.env" DOCKERHUB_USERNAME)"
if [[ -z "$DOCKERHUB_USERNAME" ]]; then
  DOCKERHUB_USERNAME="$(dotenv_get "$ROOT/FinLab/backend/.env" DOCKERHUB_USERNAME)"
fi
DOCKERHUB_TOKEN="$(dotenv_get "$ROOT/FinData/.env" DOCKERHUB_TOKEN)"
if [[ -z "$DOCKERHUB_TOKEN" ]]; then
  DOCKERHUB_TOKEN="$(dotenv_get "$ROOT/FinLab/backend/.env" DOCKERHUB_TOKEN)"
fi

push_secret FINNHUB_API_KEY "$FINNHUB_API_KEY"
push_secret CLICKHOUSE_PASSWORD "$CLICKHOUSE_PASSWORD"
push_secret POLICY_AUD "$POLICY_AUD"
push_secret TEAM_DOMAIN "$TEAM_DOMAIN"
push_secret DOCKERHUB_USERNAME "$DOCKERHUB_USERNAME"
push_secret DOCKERHUB_TOKEN "$DOCKERHUB_TOKEN"

echo "Done"
