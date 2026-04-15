#!/usr/bin/env bash
# Host-mode deploy: run the app directly via uv on the host (no docker for the app).
# Assumes postgres + minio are reachable (either from compose.yaml or external).
set -euo pipefail

cd "$(dirname "$0")/.."

PORT="${PORT:-8001}"
LOG_DIR="logs"
LOG_FILE="$LOG_DIR/uvicorn-$(date +%Y%m%d-%H%M%S).log"

mkdir -p "$LOG_DIR"
find "$LOG_DIR" -maxdepth 1 -name 'uvicorn-*.log' -mtime +14 -delete 2>/dev/null || true

PID=$(lsof -ti :"$PORT" 2>/dev/null || true)
if [ -n "$PID" ]; then
    echo "Stopping previous process (PID: $PID)..."
    kill "$PID"
    for _ in $(seq 1 20); do
        kill -0 "$PID" 2>/dev/null || break
        sleep 0.5
    done
    if kill -0 "$PID" 2>/dev/null; then
        echo "Process $PID did not exit within 10s; sending SIGKILL..."
        kill -9 "$PID" || true
        sleep 1
    fi
fi

echo "Pulling latest code..."
git pull --ff-only

echo "Syncing dependencies..."
uv sync --frozen

echo "Ensuring Playwright Chromium is installed..."
uv run playwright install-deps chromium
uv run playwright install chromium || echo "WARN: playwright browser install failed; URL parsing for WeChat may break" >&2

echo "Running database migrations..."
uv run alembic upgrade head

echo "Starting uvicorn on port $PORT..."
nohup uv run uvicorn main:app --host 0.0.0.0 --port "$PORT" > "$LOG_FILE" 2>&1 &
echo "Started (PID: $!), logs: $LOG_FILE"
