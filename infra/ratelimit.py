"""Distributed rate limiting via Redis.

Why
---
When multiple workers fan out image downloads to third-party CDNs in
W3 (小红书, 微博, 微信公众号, X, …) each worker cannot limit in
isolation — the platforms only see the aggregate traffic from our pod
fleet and ban the whole service. This module puts the token bucket in
Redis so all workers consume from the same bucket atomically, via a
Lua script the ``limits`` library runs on the server.

Usage
-----
    from infra.ratelimit import acquire

    # in some image fetcher
    if await acquire("xhscdn.com", rate=3, per_seconds=1):
        await httpx_client.get(url)
    else:
        # token bucket empty — defer, fall back, or drop per caller's strategy
        ...

Behaviour under Redis outage
----------------------------
Fail-open with a per-process local fallback bucket. Before W5-6 this
module returned ``False`` on Redis errors (fail-closed) and callers
silently dropped the image — "protecting us from being banned" was
nominal but the actual effect was content loss during transient
Redis blips. The new behaviour: on Redis error we flip to a local
``MemoryStorage`` bucket with the same rate, so the worker still
limits itself; aggregate protection degrades from "shared bucket
across N workers" to "N separate buckets each with the full rate",
but in practice Redis outages are short and the extra traffic stays
well under CDN ban thresholds. Image loss is the bigger real-world
cost and this trade avoids it.

The fallback is sticky for the duration of the outage — we don't
flip back to Redis on every call because the `limits` RedisStorage
doesn't expose a health probe; instead we retry Redis opportunistically
(once every ~60 s). Operators watching
``RATELIMIT_CHECKS_TOTAL{result="local_fallback"}`` can tell when
the service is degraded.

Testing
-------
``_reset_for_tests()`` clears the module-level singleton so tests can
swap in the in-memory backend via ``_set_storage_for_tests(...)``.
"""
from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

from limits import RateLimitItemPerSecond
from limits.aio.storage import MemoryStorage, RedisStorage, Storage
from limits.aio.strategies import MovingWindowRateLimiter

from infra.metrics import RATELIMIT_CHECKS_TOTAL

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

_storage: Storage | None = None
_limiter: MovingWindowRateLimiter | None = None

# Local per-process fallback, built lazily on the first Redis error.
# Shared across all keys inside this process. Using ``MemoryStorage``
# from the same ``limits`` library so the semantics match (moving-
# window strategy, same RateLimitItem type).
_fallback_limiter: MovingWindowRateLimiter | None = None

# Retry Redis at most every _REDIS_RETRY_SECONDS after an outage so
# we're not hammering a down Redis on every acquire. When the timer
# elapses we attempt one real limiter call; if it works, subsequent
# acquires route to Redis again.
_REDIS_RETRY_SECONDS = 60.0
_last_redis_error_ts: float = 0.0


def _get_fallback_limiter() -> MovingWindowRateLimiter:
    global _fallback_limiter  # noqa: PLW0603
    if _fallback_limiter is None:
        _fallback_limiter = MovingWindowRateLimiter(MemoryStorage())
    return _fallback_limiter


async def _get_limiter() -> MovingWindowRateLimiter:
    """Lazily build the module-level (storage, limiter) pair.

    Storage construction in ``limits`` is cheap (no connection yet);
    the first ``hit()`` call actually opens the Redis connection. That
    means import time stays side-effect free and the process can start
    even if Redis is briefly unreachable.
    """
    global _storage, _limiter  # noqa: PLW0603
    if _limiter is not None:
        return _limiter
    if _storage is None:
        from config import get_settings
        url = get_settings().redis_url.get_secret_value()
        _storage = RedisStorage(url)
    _limiter = MovingWindowRateLimiter(_storage)
    return _limiter


async def acquire(
    key: str,
    *,
    rate: int,
    per_seconds: int = 1,
    metric_label: str | None = None,
) -> bool:
    """Consume one token for ``key``. Returns True if allowed.

    ``key`` is the bucket identifier — typically a bare domain like
    ``"xhscdn.com"`` or a composite like ``"xhscdn.com:user-42"`` if you
    need per-user-per-domain pacing. Different keys use independent
    buckets; same key across workers shares the same bucket.

    ``metric_label`` overrides the Prometheus label used on
    ``RATELIMIT_CHECKS_TOTAL``. Inbound limiters key buckets per
    principal (per-API-key) for correct accounting but need a *low*-
    cardinality label (the bucket name) for Prometheus, otherwise
    /metrics grows one time series per distinct caller. Outbound
    domain-based callers leave this None — their key is already low
    cardinality.

    On Redis failure, degrades to a per-process in-memory token bucket
    with the same rate (see module docstring). Callers treat the
    return value as "is this request permitted now"; they don't need
    to distinguish shared-Redis vs local-fallback — the bucket shape
    is the same, only the blast radius of "who shares the bucket"
    changes. Outages surface via
    ``RATELIMIT_CHECKS_TOTAL{result="local_fallback"}``.
    """
    global _last_redis_error_ts  # noqa: PLW0603
    item = RateLimitItemPerSecond(rate, per_seconds)
    label = metric_label if metric_label is not None else key

    now = time.monotonic()
    in_outage = (
        _last_redis_error_ts > 0
        and (now - _last_redis_error_ts) < _REDIS_RETRY_SECONDS
    )
    if not in_outage:
        limiter = await _get_limiter()
        try:
            allowed = await limiter.hit(item, key)
        except Exception:  # noqa: BLE001
            logger.warning(
                "rate limit Redis check failed for key=%s; falling back "
                "to local bucket for ~%ds",
                key, int(_REDIS_RETRY_SECONDS),
            )
            _last_redis_error_ts = now
            # fall through to local fallback below
        else:
            # Successful Redis hit — clear any prior outage marker.
            if _last_redis_error_ts:
                logger.info("rate limit Redis recovered; resuming shared bucket")
                _last_redis_error_ts = 0.0
            RATELIMIT_CHECKS_TOTAL.labels(
                key=label, result="allowed" if allowed else "blocked",
            ).inc()
            return allowed

    # Outage path: serve from the in-memory fallback.
    fallback = _get_fallback_limiter()
    try:
        allowed = await fallback.hit(item, key)
    except Exception:  # noqa: BLE001 — last resort; don't propagate
        logger.exception(
            "local fallback rate limiter failed for key=%s; allowing", key,
        )
        RATELIMIT_CHECKS_TOTAL.labels(key=label, result="error").inc()
        return True
    RATELIMIT_CHECKS_TOTAL.labels(key=label, result="local_fallback").inc()
    return allowed


async def get_window_stats(
    key: str, *, rate: int, per_seconds: int = 1,
) -> tuple[int, int]:
    """Return ``(reset_unix_ts, remaining)`` for ``key`` without consuming
    a token.

    Used by the inbound limiter to populate ``X-RateLimit-Reset`` and
    ``X-RateLimit-Remaining`` response headers (GitHub/Stripe convention)
    so well-behaved clients can self-pace before hitting 429.

    Falls back to the local bucket on Redis error, same shape as
    :func:`acquire` — pacing degrades but never blocks the request path.
    """
    global _last_redis_error_ts  # noqa: PLW0603
    item = RateLimitItemPerSecond(rate, per_seconds)

    now = time.monotonic()
    in_outage = (
        _last_redis_error_ts > 0
        and (now - _last_redis_error_ts) < _REDIS_RETRY_SECONDS
    )
    if not in_outage:
        limiter = await _get_limiter()
        try:
            reset, remaining = await limiter.get_window_stats(item, key)
        except Exception:  # noqa: BLE001
            _last_redis_error_ts = now
            # fall through to local fallback
        else:
            return int(reset), int(remaining)

    fallback = _get_fallback_limiter()
    try:
        reset, remaining = await fallback.get_window_stats(item, key)
    except Exception:  # noqa: BLE001 — never fail the caller
        # Sane defaults: assume bucket is empty and resets at end of window.
        return int(time.time() + per_seconds), 0
    return int(reset), int(remaining)


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

def _reset_for_tests() -> None:
    """Clear the singleton pair so the next ``acquire`` rebuilds it.

    Intended for pytest fixtures only. Production code must not call
    this — dropping the limiter mid-flight would reset every bucket.
    """
    global _storage, _limiter, _fallback_limiter, _last_redis_error_ts  # noqa: PLW0603
    _storage = None
    _limiter = None
    _fallback_limiter = None
    _last_redis_error_ts = 0.0


def _set_storage_for_tests(storage: Storage) -> None:
    """Replace the backing storage with a test double (typically
    ``MemoryStorage()``). Pair with ``_reset_for_tests`` in fixture
    teardown.
    """
    global _storage, _limiter  # noqa: PLW0603
    _storage = storage
    _limiter = MovingWindowRateLimiter(storage)


# Re-exported so tests can ``from infra.ratelimit import MemoryStorage``
# without pulling in the ``limits`` namespace directly.
__all__ = ["acquire", "MemoryStorage"]
