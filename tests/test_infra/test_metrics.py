"""Smoke tests for the Prometheus metrics wiring.

Scope:
  - the infra.metrics module exports the expected metric objects
  - all vectoria_* metrics carry the expected label names (guards against
    accidental rename that would silently break dashboards)
  - the FastAPI /metrics endpoint is reachable and returns Prometheus text
"""
import pytest

from infra import metrics


_EXPECTED_LABELS = {
    "WORKER_RSS_BYTES": (),
    "WORKER_RSS_KILLS": (),
    "WORKER_TASKS_INFLIGHT": (),
    "TASK_DURATION_SECONDS": ("task_type", "status"),
    "TASK_TOTAL": ("task_type", "status"),
    "QUEUE_DEPTH": ("task_type",),
    "QUEUE_OLDEST_AGE_SECONDS": ("task_type",),
    "EXTERNAL_API_CALLS": ("api", "status"),
    "EXTERNAL_API_DURATION_SECONDS": ("api",),
    "CIRCUIT_STATE": ("name",),
    "CIRCUIT_TRANSITIONS": ("name", "to_state"),
    "PARSE_DURATION_SECONDS": ("engine", "status"),
}


@pytest.mark.parametrize("name, expected_labels", list(_EXPECTED_LABELS.items()))
def test_metric_exported_with_expected_labels(name, expected_labels):
    metric = getattr(metrics, name, None)
    assert metric is not None, f"infra.metrics does not export {name}"
    assert tuple(metric._labelnames) == expected_labels, (
        f"{name} labels changed: got {metric._labelnames}, "
        f"expected {expected_labels} — dashboards will break"
    )


def test_metric_names_follow_vectoria_prefix():
    """All custom metrics must be prefixed to namespace them in shared Prom."""
    for attr_name in _EXPECTED_LABELS:
        metric = getattr(metrics, attr_name)
        assert metric._name.startswith("vectoria_"), (
            f"{attr_name} ({metric._name}) must start with vectoria_"
        )


def test_start_metrics_server_is_idempotent(monkeypatch):
    """Calling twice must not raise — avoids crash loops if entry point
    is invoked more than once (e.g. pytest collecting worker code).
    """
    from infra import metrics as m

    calls = []

    def _fake_start(port):
        calls.append(port)

    monkeypatch.setattr(m, "start_http_server", _fake_start)
    monkeypatch.setattr(m, "_metrics_server_started", False)

    m.start_metrics_server(9999)
    m.start_metrics_server(9999)
    assert calls == [9999], "start_metrics_server must bind only once"


async def test_metrics_endpoint_returns_prometheus_text(client):
    """The FastAPI /metrics endpoint is reachable and returns exposition-format
    text. This verifies the Instrumentator wire-up end to end.
    """
    resp = await client.get("/metrics")
    assert resp.status_code == 200
    # Prometheus exposition format content-type; allow version suffix.
    assert "text/plain" in resp.headers.get("content-type", "")
    body = resp.text
    # The instrumentator's built-in HTTP metrics are registered at app
    # startup and appear even without prior traffic:
    assert "http_requests_total" in body or "http_request_duration" in body
