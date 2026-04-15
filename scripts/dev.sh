#!/usr/bin/env bash
# Local development: docker runs infra only, app runs on host via uv.
set -euo pipefail

cd "$(dirname "$0")/.."

echo "Starting postgres + minio..."
docker compose --profile local up -d --wait db minio minio-init

echo "Syncing dependencies..."
uv sync

echo "Running migrations..."
uv run alembic upgrade head

echo "Starting uvicorn on :8000 (reload mode)..."
exec uv run uvicorn main:app --host 0.0.0.0 --port 8000 --reload
