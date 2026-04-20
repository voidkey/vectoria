"""Async circuit breaker for external service calls.

Why
---
MinerU, the Vision LLM, and the embedding API are all out-of-process
HTTP dependencies. When any of them degrades (500s, timeouts, network
partition) naive retries turn a local problem into worker starvation:
each in-flight task holds its slot for the full timeout before failing,
and the ingest queue backs up behind the dead dependency.

A circuit breaker fails fast once a dependency is proven unhealthy:

    CLOSED  ── failure_threshold consecutive failures ──►  OPEN
      ▲                                                      │
      │                                             reset_timeout
      │                                                      │
      └────── probe success ─── HALF_OPEN  ◄─────────────────┘
                                   │
                                   └──── probe failure ──►  OPEN

Design notes
------------
* **Consecutive failures**, not failure-rate-in-window. Simpler to reason
  about and matches what operators expect from the name.
* Only one concurrent probe in ``HALF_OPEN`` — a stampede of probes
  against a recovering dependency is what kills it again.
* ``failure_predicate`` lets callers decide what counts. MinerU's 4xx
  responses (client error) should NOT open the circuit; its 5xx and
  timeouts should.
* Metric observations (``EXTERNAL_API_*`` + ``CIRCUIT_*``) are wired
  inside so call sites don't need to remember to instrument.
"""
from __future__ import annotations

import asyncio
import logging
import time
from asyncio import CancelledError
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable, TypeVar

from infra.metrics import (
    CIRCUIT_STATE,
    CIRCUIT_TRANSITIONS,
    EXTERNAL_API_CALLS,
    EXTERNAL_API_DURATION_SECONDS,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


class State(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


_STATE_TO_GAUGE = {State.CLOSED: 0, State.HALF_OPEN: 1, State.OPEN: 2}


class CircuitOpenError(RuntimeError):
    """Raised instead of invoking the wrapped callable when the circuit is open.

    Callers should catch this to apply a fallback (return empty, try a
    different engine, enqueue for retry, ...) rather than treating it as
    a generic exception.
    """


def _default_is_failure(exc: BaseException) -> bool:  # noqa: ARG001
    """Default predicate: every exception counts as a failure."""
    return True


@dataclass
class CircuitBreaker:
    """Async circuit breaker with metric side-effects.

    Construct once per external dependency (module-level singleton is
    fine) and share across callers. ``call()`` is the only public
    entrypoint.
    """

    name: str
    failure_threshold: int = 5
    reset_timeout: float = 300.0
    # Only exceptions for which this predicate returns True count as
    # failures. Client-side errors (malformed input, 4xx) should not
    # open the circuit — they don't indicate the dependency is down.
    is_failure: Callable[[BaseException], bool] = field(
        default=_default_is_failure,
    )
    # Internal state ------------------------------------------------------
    _state: State = field(default=State.CLOSED, init=False)
    _consecutive_failures: int = field(default=0, init=False)
    _opened_at_monotonic: float = field(default=0.0, init=False)
    _probe_inflight: bool = field(default=False, init=False)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)

    def __post_init__(self) -> None:
        CIRCUIT_STATE.labels(name=self.name).set(_STATE_TO_GAUGE[State.CLOSED])

    # -------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------

    async def call(
        self,
        fn: Callable[..., Awaitable[T]],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """Invoke ``await fn(*args, **kwargs)`` through the breaker.

        Raises ``CircuitOpenError`` if the breaker is open (or if it's
        HALF_OPEN with a probe already in flight). Any other exception
        from ``fn`` is re-raised unchanged; if ``is_failure`` returns
        True for it, the failure counter is advanced.
        """
        is_probe = await self._enter_call()

        t_start = time.monotonic()
        try:
            result = await fn(*args, **kwargs)
        except CancelledError:
            # Cancellation reflects the caller (request disconnect,
            # worker shutdown) — not the dependency's health. Release
            # the probe permit so a future call can re-probe, but don't
            # tick the failure counter or mark the outcome.
            if is_probe:
                async with self._lock:
                    self._probe_inflight = False
            raise
        except BaseException as exc:  # noqa: BLE001 — we re-raise below
            elapsed = time.monotonic() - t_start
            await self._record_outcome(failed=self.is_failure(exc), is_probe=is_probe)
            self._observe(elapsed, status="error")
            raise
        else:
            elapsed = time.monotonic() - t_start
            await self._record_outcome(failed=False, is_probe=is_probe)
            self._observe(elapsed, status="ok")
            return result

    def current_state(self) -> State:
        """Read-only snapshot of the state. Mostly for tests/debug."""
        return self._state

    # -------------------------------------------------------------------
    # Internals
    # -------------------------------------------------------------------

    async def _enter_call(self) -> bool:
        """Decide whether to let this call through. Returns ``True`` if
        the caller is the HALF_OPEN probe so ``_record_outcome`` can
        release the probe permit.
        """
        now = time.monotonic()
        async with self._lock:
            # Maybe transition OPEN → HALF_OPEN if reset_timeout elapsed.
            if (
                self._state is State.OPEN
                and now - self._opened_at_monotonic >= self.reset_timeout
            ):
                self._transition_to(State.HALF_OPEN)

            if self._state is State.OPEN:
                self._fail_fast_observe()
                raise CircuitOpenError(f"{self.name} circuit open")

            if self._state is State.HALF_OPEN:
                if self._probe_inflight:
                    # Someone else is already testing the water.
                    self._fail_fast_observe()
                    raise CircuitOpenError(
                        f"{self.name} circuit open (probe in flight)",
                    )
                self._probe_inflight = True
                return True

            return False

    async def _record_outcome(self, *, failed: bool, is_probe: bool) -> None:
        async with self._lock:
            if is_probe:
                self._probe_inflight = False

            if failed:
                self._consecutive_failures += 1
                if self._state is State.HALF_OPEN:
                    # Probe failed → back to OPEN; restart the clock.
                    self._open()
                elif (
                    self._state is State.CLOSED
                    and self._consecutive_failures >= self.failure_threshold
                ):
                    self._open()
            else:
                self._consecutive_failures = 0
                if self._state is State.HALF_OPEN:
                    self._transition_to(State.CLOSED)

    # --- State transition helpers (caller must hold _lock) -------------

    def _open(self) -> None:
        self._opened_at_monotonic = time.monotonic()
        self._transition_to(State.OPEN)

    def _transition_to(self, new: State) -> None:
        if new is self._state:
            return
        logger.info(
            "Circuit %s: %s → %s",
            self.name, self._state.value, new.value,
        )
        self._state = new
        CIRCUIT_STATE.labels(name=self.name).set(_STATE_TO_GAUGE[new])
        CIRCUIT_TRANSITIONS.labels(name=self.name, to_state=new.value).inc()

    # --- Metrics helpers -----------------------------------------------

    def _observe(self, elapsed: float, *, status: str) -> None:
        EXTERNAL_API_CALLS.labels(api=self.name, status=status).inc()
        EXTERNAL_API_DURATION_SECONDS.labels(api=self.name).observe(elapsed)

    def _fail_fast_observe(self) -> None:
        """Record a call that the breaker rejected without invoking ``fn``."""
        EXTERNAL_API_CALLS.labels(api=self.name, status="circuit_open").inc()


# ---------------------------------------------------------------------------
# Module-level singletons for each external dependency.
# Construction is cheap (no I/O) so we build them at import time.
# Thresholds live in config so ops can tune without code changes.
# ---------------------------------------------------------------------------


def _http_server_error(exc: BaseException) -> bool:
    """Breaker predicate for HTTP-based dependencies: count 5xx + transport
    errors, ignore 4xx (client error — fixing ourselves, not the service).

    Tolerant of malformed exceptions (e.g. tests that build HTTPStatusError
    with a MagicMock response): if we can't read the status code, count
    the exception as a real failure — safer than silently ignoring it.
    """
    # Imported lazily to avoid httpx/openai import cost for callers that
    # only want the breaker class.
    import httpx

    if isinstance(exc, httpx.HTTPStatusError):
        status = getattr(exc.response, "status_code", None)
        return not isinstance(status, int) or status >= 500
    if isinstance(exc, (httpx.TransportError, httpx.TimeoutException)):
        return True
    # OpenAI SDK maps 5xx / connection / timeout to specific types.
    try:
        from openai import (
            APIConnectionError,
            APIStatusError,
            APITimeoutError,
        )
    except ImportError:
        pass
    else:
        if isinstance(exc, APIStatusError):
            status = getattr(exc, "status_code", None)
            return not isinstance(status, int) or status >= 500
        if isinstance(exc, (APIConnectionError, APITimeoutError)):
            return True
    # Anything else: play safe and count as failure. Callers can pass a
    # narrower predicate if they disagree.
    return True


_breakers: dict[str, CircuitBreaker] = {}


def get_breaker(name: str) -> CircuitBreaker:
    """Return (creating on first access) the named breaker.

    Thresholds are read from settings on first construction so tests can
    monkeypatch config before importing dependent modules. Call sites use
    this instead of holding module-level breaker references directly, so
    tests can also reset the registry between cases.
    """
    if name in _breakers:
        return _breakers[name]

    from config import get_settings

    cfg = get_settings()
    spec = {
        "mineru": (
            cfg.mineru_breaker_threshold,
            cfg.mineru_breaker_reset_timeout,
            _http_server_error,
        ),
        "vision": (
            cfg.vision_breaker_threshold,
            cfg.vision_breaker_reset_timeout,
            _http_server_error,
        ),
        "embedding": (
            cfg.embedding_breaker_threshold,
            cfg.embedding_breaker_reset_timeout,
            _http_server_error,
        ),
    }
    if name not in spec:
        raise KeyError(f"unknown breaker: {name}")

    threshold, reset, predicate = spec[name]
    breaker = CircuitBreaker(
        name=name,
        failure_threshold=threshold,
        reset_timeout=reset,
        is_failure=predicate,
    )
    _breakers[name] = breaker
    return breaker


def _reset_breakers_for_tests() -> None:
    """Clear the breaker registry. Tests only."""
    _breakers.clear()
