#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

PORT=8001
LOG_FILE="uvicorn.log"

# Kill existing process on the port
PID=$(lsof -ti :"$PORT" 2>/dev/null || true)
if [ -n "$PID" ]; then
    echo "Stopping previous process (PID: $PID)..."
    kill "$PID"
    sleep 1
fi

# Pull latest code
echo "Pulling latest code..."
git pull

# Sync dependencies
echo "Syncing dependencies..."
uv sync --frozen

# Install Playwright system deps; skip browser download if already present
echo "Ensuring Playwright Chromium is installed..."
uv run playwright install-deps chromium
uv run playwright install chromium || true

# Run DB migrations
echo "Running database migrations..."
uv run alembic upgrade head

# Start the app in background
echo "Starting uvicorn on port $PORT..."
nohup uv run uvicorn main:app --host 0.0.0.0 --port "$PORT" > "$LOG_FILE" 2>&1 &
echo "Started (PID: $!), logs: $LOG_FILE"
