#!/usr/bin/env bash
# Host-mode deploy: run the app directly via uv on the host (no docker for the app).
# Assumes postgres + minio are reachable (either from compose.yaml or external).
set -euo pipefail

cd "$(dirname "$0")/.."

PORT="${PORT:-8001}"
LOG_DIR="logs"
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
API_LOG="$LOG_DIR/uvicorn-${TIMESTAMP}.log"
WORKER_LOG="$LOG_DIR/worker-${TIMESTAMP}.log"
WORKER_PID_FILE="$LOG_DIR/worker.pid"

mkdir -p "$LOG_DIR"
find "$LOG_DIR" -maxdepth 1 -name 'uvicorn-*.log' -mtime +14 -delete 2>/dev/null || true
find "$LOG_DIR" -maxdepth 1 -name 'worker-*.log' -mtime +14 -delete 2>/dev/null || true

stop_process() {
    local pid="$1" label="$2"
    if ! kill -0 "$pid" 2>/dev/null; then return; fi
    echo "Stopping $label (PID: $pid)..."
    kill "$pid"
    for _ in $(seq 1 20); do
        kill -0 "$pid" 2>/dev/null || return
        sleep 0.5
    done
    echo "Process $pid did not exit within 10s; sending SIGKILL..."
    kill -9 "$pid" 2>/dev/null || true
    sleep 1
}

PID=$(lsof -ti :"$PORT" 2>/dev/null || true)
[ -n "$PID" ] && stop_process "$PID" "API"

if [ -f "$WORKER_PID_FILE" ]; then
    stop_process "$(cat "$WORKER_PID_FILE")" "worker"
    rm -f "$WORKER_PID_FILE"
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
nohup uv run uvicorn main:app --host 0.0.0.0 --port "$PORT" > "$API_LOG" 2>&1 &
echo "API started (PID: $!), logs: $API_LOG"

# Worker memory limit (virtual address space). If a handler allocates beyond
# this, it gets MemoryError instead of triggering the OOM killer on the API.
WORKER_MEM_LIMIT_KB="${WORKER_MEM_LIMIT_KB:-4194304}"  # default 4GB

echo "Starting task worker (mem limit: ${WORKER_MEM_LIMIT_KB}KB)..."
nohup bash -c "ulimit -v $WORKER_MEM_LIMIT_KB; exec uv run python -m worker" > "$WORKER_LOG" 2>&1 &
echo $! > "$WORKER_PID_FILE"
echo "Worker started (PID: $!), logs: $WORKER_LOG"
