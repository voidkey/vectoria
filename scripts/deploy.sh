#!/usr/bin/env bash
# Production deploy: pull the latest image from Docker Hub and roll the app.
# Expects the image to already be built and pushed via scripts/build-push.sh.
set -euo pipefail

cd "$(dirname "$0")/.."

ENV_FILE="${ENV_FILE:-.env.prod}"
COMPOSE=(docker compose -f compose.yaml -f compose.prod.yaml --env-file "$ENV_FILE")

if [ ! -f "$ENV_FILE" ]; then
    echo "Error: $ENV_FILE not found. Copy .env.example and fill in production values."
    exit 1
fi

echo "Pulling latest code (for compose file updates)..."
git pull --ff-only

echo "Pulling image from Docker Hub..."
"${COMPOSE[@]}" pull app worker

echo "Starting infra (db)..."
"${COMPOSE[@]}" up -d --wait db

# Run migrations BEFORE starting the app so it never serves requests on a
# stale schema. `run --rm` spins up a one-shot container sharing compose
# network and env, then exits.
echo "Running database migrations..."
"${COMPOSE[@]}" run --rm app alembic upgrade head

echo "Rolling app + worker containers..."
"${COMPOSE[@]}" up -d --wait app worker

echo "Deploy complete. Tail logs: ${COMPOSE[*]} logs -f app worker"
