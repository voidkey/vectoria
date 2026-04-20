"""Circuit breaker state-machine tests.

Covers the three transitions that matter operationally:
  CLOSED → OPEN  after N consecutive failures
  OPEN   → HALF_OPEN  after reset_timeout elapses
  HALF_OPEN → CLOSED  on successful probe
  HALF_OPEN → OPEN    on failed probe
And the safety invariant: only one probe in HALF_OPEN at a time.
"""
from __future__ import annotations

import asyncio

import httpx
import pytest

from infra.circuit_breaker import (
    CircuitBreaker,
    CircuitOpenError,
    State,
    _http_server_error,
    _reset_breakers_for_tests,
    get_breaker,
)


class _Boom(Exception):
    pass


async def _fail() -> None:
    raise _Boom("nope")


async def _ok() -> str:
    return "ok"


async def test_starts_closed_and_passes_through_successes():
    b = CircuitBreaker(name="t", failure_threshold=3)
    for _ in range(10):
        assert await b.call(_ok) == "ok"
    assert b.current_state() is State.CLOSED


async def test_opens_after_threshold_consecutive_failures():
    b = CircuitBreaker(name="t", failure_threshold=3)
    for _ in range(3):
        with pytest.raises(_Boom):
            await b.call(_fail)
    assert b.current_state() is State.OPEN


async def test_open_state_fails_fast_without_invoking_fn():
    b = CircuitBreaker(name="t", failure_threshold=2)
    for _ in range(2):
        with pytest.raises(_Boom):
            await b.call(_fail)
    assert b.current_state() is State.OPEN

    calls = 0

    async def _spy() -> str:
        nonlocal calls
        calls += 1
        return "x"

    with pytest.raises(CircuitOpenError):
        await b.call(_spy)
    assert calls == 0, "OPEN breaker must not invoke the wrapped callable"


async def test_success_resets_failure_counter_before_threshold():
    """A single success in CLOSED wipes earlier failures — only truly
    consecutive streaks trip the breaker.
    """
    b = CircuitBreaker(name="t", failure_threshold=3)
    for _ in range(2):
        with pytest.raises(_Boom):
            await b.call(_fail)
    # One success resets
    assert await b.call(_ok) == "ok"
    # Now 2 more failures should not open
    for _ in range(2):
        with pytest.raises(_Boom):
            await b.call(_fail)
    assert b.current_state() is State.CLOSED


async def test_half_open_after_reset_timeout_then_probe_success_closes():
    b = CircuitBreaker(name="t", failure_threshold=2, reset_timeout=0.05)
    for _ in range(2):
        with pytest.raises(_Boom):
            await b.call(_fail)
    assert b.current_state() is State.OPEN

    await asyncio.sleep(0.06)  # exceed reset_timeout

    # Next call is the probe; it succeeds, circuit closes.
    assert await b.call(_ok) == "ok"
    assert b.current_state() is State.CLOSED


async def test_half_open_probe_failure_reopens():
    b = CircuitBreaker(name="t", failure_threshold=2, reset_timeout=0.05)
    for _ in range(2):
        with pytest.raises(_Boom):
            await b.call(_fail)
    await asyncio.sleep(0.06)

    with pytest.raises(_Boom):
        await b.call(_fail)
    assert b.current_state() is State.OPEN


async def test_half_open_only_allows_one_concurrent_probe():
    """Second caller in HALF_OPEN sees circuit-open error — not two
    simultaneous hits at a recovering dependency.
    """
    b = CircuitBreaker(name="t", failure_threshold=2, reset_timeout=0.05)
    for _ in range(2):
        with pytest.raises(_Boom):
            await b.call(_fail)
    await asyncio.sleep(0.06)

    started = asyncio.Event()
    release = asyncio.Event()

    async def _slow_probe() -> str:
        started.set()
        await release.wait()
        return "probe"

    probe_task = asyncio.create_task(b.call(_slow_probe))
    await started.wait()

    # Second caller lands while probe is in flight.
    with pytest.raises(CircuitOpenError):
        await b.call(_ok)

    release.set()
    assert await probe_task == "probe"


async def test_failure_predicate_skips_client_errors():
    """4xx from dependency shouldn't open the circuit — it means our
    request was bad, not that the service is down.
    """
    b = CircuitBreaker(
        name="t",
        failure_threshold=2,
        is_failure=_http_server_error,
    )

    def _mk_4xx():
        req = httpx.Request("POST", "http://x")
        resp = httpx.Response(status_code=400, request=req)
        return httpx.HTTPStatusError("400", request=req, response=resp)

    async def _raise_4xx():
        raise _mk_4xx()

    for _ in range(5):
        with pytest.raises(httpx.HTTPStatusError):
            await b.call(_raise_4xx)
    assert b.current_state() is State.CLOSED, "4xx must not open the breaker"


async def test_failure_predicate_counts_5xx_and_timeout():
    b = CircuitBreaker(
        name="t",
        failure_threshold=3,
        is_failure=_http_server_error,
    )

    def _mk_5xx():
        req = httpx.Request("POST", "http://x")
        resp = httpx.Response(status_code=502, request=req)
        return httpx.HTTPStatusError("502", request=req, response=resp)

    async def _raise_5xx():
        raise _mk_5xx()

    async def _raise_timeout():
        raise httpx.ReadTimeout("slow")

    with pytest.raises(httpx.HTTPStatusError):
        await b.call(_raise_5xx)
    with pytest.raises(httpx.HTTPStatusError):
        await b.call(_raise_5xx)
    with pytest.raises(httpx.ReadTimeout):
        await b.call(_raise_timeout)
    assert b.current_state() is State.OPEN


async def test_get_breaker_returns_singleton_per_name():
    _reset_breakers_for_tests()
    a = get_breaker("mineru")
    b = get_breaker("mineru")
    assert a is b, "get_breaker must return the same instance for the same name"


async def test_get_breaker_rejects_unknown_name():
    with pytest.raises(KeyError):
        get_breaker("definitely-not-a-real-service")


async def test_cancelled_error_does_not_count_as_failure():
    """Task cancellation (client disconnect, worker shutdown) is about
    the caller, not the dependency's health. Must not tick the failure
    counter — otherwise a redeploy that cancels in-flight requests can
    spuriously open every circuit.
    """
    b = CircuitBreaker(name="t", failure_threshold=2)

    async def _cancelled():
        raise asyncio.CancelledError()

    # Three cancellations in a row; threshold is 2 — if counted as
    # failures, state would be OPEN. It must stay CLOSED.
    for _ in range(3):
        with pytest.raises(asyncio.CancelledError):
            await b.call(_cancelled)
    assert b.current_state() is State.CLOSED


async def test_state_changes_are_reflected_in_prometheus_metrics():
    """State transitions must update CIRCUIT_STATE and CIRCUIT_TRANSITIONS.

    Guards against silent dashboard breakage: if a label name ever
    drifts (e.g. ``to_state`` → ``new_state``), the expected samples
    disappear and this test fails — before ops notice gaps in graphs.
    """
    from prometheus_client import REGISTRY

    # Unique name: CIRCUIT_TRANSITIONS is a cumulative Counter with no
    # per-test reset, so we want fresh label sets each run.
    name = "metric_wiring_probe"
    b = CircuitBreaker(
        name=name, failure_threshold=2, reset_timeout=0.05,
    )

    # __post_init__ sets CIRCUIT_STATE = 0 (CLOSED)
    state_val = REGISTRY.get_sample_value(
        "vectoria_circuit_state", {"name": name},
    )
    assert state_val == 0.0, "gauge must read 0 (CLOSED) at init"

    # Drive into OPEN
    for _ in range(2):
        with pytest.raises(_Boom):
            await b.call(_fail)

    state_val = REGISTRY.get_sample_value(
        "vectoria_circuit_state", {"name": name},
    )
    assert state_val == 2.0, "gauge must read 2 (OPEN) after threshold failures"

    transitions_to_open = REGISTRY.get_sample_value(
        "vectoria_circuit_transitions_total",
        {"name": name, "to_state": "open"},
    )
    assert transitions_to_open == 1.0, "must count exactly one CLOSED→OPEN"

    # Probe through HALF_OPEN → CLOSED
    await asyncio.sleep(0.06)
    assert await b.call(_ok) == "ok"

    state_val = REGISTRY.get_sample_value(
        "vectoria_circuit_state", {"name": name},
    )
    assert state_val == 0.0, "gauge must read 0 (CLOSED) after successful probe"

    transitions_to_half = REGISTRY.get_sample_value(
        "vectoria_circuit_transitions_total",
        {"name": name, "to_state": "half_open"},
    )
    transitions_to_closed = REGISTRY.get_sample_value(
        "vectoria_circuit_transitions_total",
        {"name": name, "to_state": "closed"},
    )
    assert transitions_to_half == 1.0
    assert transitions_to_closed == 1.0
