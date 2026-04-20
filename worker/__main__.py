"""Entry point: uv run python -m worker"""
from config import get_settings
from infra.metrics import start_metrics_server
from worker.runner import main

# Expose prometheus metrics on a dedicated port before the main loop blocks.
# Workers have no FastAPI app, so we run the stdlib HTTP server from
# prometheus_client directly. K8s ServiceMonitor scrapes <pod-ip>:<port>/metrics.
start_metrics_server(get_settings().worker_metrics_port)

main()
