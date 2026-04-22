"""Entry point: uv run python -m worker"""
from config import get_settings
from infra.metrics import WORKER_RSS_LIMIT_BYTES, start_metrics_server
from worker.runner import main

_cfg = get_settings()

# Expose prometheus metrics on a dedicated port before the main loop blocks.
# Workers have no FastAPI app, so we run the stdlib HTTP server from
# prometheus_client directly. K8s ServiceMonitor scrapes <pod-ip>:<port>/metrics.
start_metrics_server(_cfg.worker_metrics_port)

# Export the configured RSS self-kill threshold as a gauge so the
# ``near-limit`` alert can compare current RSS against it.
# 0 = disabled (the alert rule treats 0 as "no threshold, skip").
WORKER_RSS_LIMIT_BYTES.set(_cfg.worker_rss_limit_bytes)

main()
