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


def _parse_count(engine: str, status: str) -> float:
    """Read the _count sample for a (engine, status) labelset from the
    live Histogram — public `collect()` API, not the private _count attr."""
    for family in metrics.PARSE_DURATION_SECONDS.collect():
        for sample in family.samples:
            if (
                sample.name.endswith("_count")
                and sample.labels.get("engine") == engine
                and sample.labels.get("status") == status
            ):
                return sample.value
    return 0.0


class _StubCircuitOpen(Exception):
    """Stand-in for infra.circuit_breaker.CircuitOpenError — the real one
    pulls in the circuit_breaker module graph, which isn't needed here."""


async def test_observe_parse_classifies_status(monkeypatch):
    """observe_parse must route each exception type to the right `status`
    label. Dashboards and the ParseErrorRate / ParseTimeoutSpike alerts
    all key off this classification — getting it wrong silently breaks
    both without any test signal.

    monkeypatch.setitem restores ``sys.modules`` at teardown; without it
    the stub leaks and later tests that hit the real circuit_breaker
    (via conftest._reset_breakers_for_tests) fail with ImportError.
    """
    import sys
    import types
    stub_module = types.ModuleType("infra.circuit_breaker")
    stub_module.CircuitOpenError = _StubCircuitOpen
    monkeypatch.setitem(sys.modules, "infra.circuit_breaker", stub_module)

    engine = "test-classify"
    before = {s: _parse_count(engine, s) for s in ("ok", "error", "timeout", "circuit_open")}

    # ok path
    async with metrics.observe_parse(engine):
        pass

    # timeout path (builtin TimeoutError covers asyncio & concurrent.futures on 3.11+)
    with pytest.raises(TimeoutError):
        async with metrics.observe_parse(engine):
            raise TimeoutError("simulated pool timeout")

    # circuit-open path
    with pytest.raises(_StubCircuitOpen):
        async with metrics.observe_parse(engine):
            raise _StubCircuitOpen("circuit tripped")

    # generic error path
    with pytest.raises(ValueError):
        async with metrics.observe_parse(engine):
            raise ValueError("boom")

    after = {s: _parse_count(engine, s) for s in ("ok", "error", "timeout", "circuit_open")}
    delta = {s: after[s] - before[s] for s in before}
    assert delta == {"ok": 1, "timeout": 1, "circuit_open": 1, "error": 1}, (
        f"observe_parse mis-classified outcomes: {delta}"
    )
