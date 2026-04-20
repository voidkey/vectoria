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
Fail-closed: ``acquire`` returns False and logs a warning. The rate
limiter's job is to protect *us* from banning ourselves on third-party
services; during a Redis outage, allowing the request through would
let a stampede past the normal protection. Callers that prefer
fail-open behaviour can catch the False return and proceed anyway.

Testing
-------
``_reset_for_tests()`` clears the module-level singleton so tests can
swap in the in-memory backend via ``_set_storage_for_tests(...)``.
"""
from __future__ import annotations

import logging
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


async def acquire(key: str, *, rate: int, per_seconds: int = 1) -> bool:
    """Consume one token for ``key``. Returns True if allowed.

    ``key`` is the bucket identifier — typically a bare domain like
    ``"xhscdn.com"`` or a composite like ``"xhscdn.com:user-42"`` if you
    need per-user-per-domain pacing. Different keys use independent
    buckets; same key across workers shares the same bucket.

    On Redis failure, logs a warning and returns False so callers
    don't accidentally stampede during an outage. The caller's code
    path should treat False as "try again later".
    """
    limiter = await _get_limiter()
    item = RateLimitItemPerSecond(rate, per_seconds)
    try:
        allowed = await limiter.hit(item, key)
    except Exception:  # noqa: BLE001 — telemetry path, must never propagate
        logger.warning(
            "rate limit check failed for key=%s rate=%d/%ds; failing closed",
            key, rate, per_seconds,
        )
        RATELIMIT_CHECKS_TOTAL.labels(key=key, result="error").inc()
        return False

    RATELIMIT_CHECKS_TOTAL.labels(
        key=key, result="allowed" if allowed else "blocked",
    ).inc()
    return allowed


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

def _reset_for_tests() -> None:
    """Clear the singleton pair so the next ``acquire`` rebuilds it.

    Intended for pytest fixtures only. Production code must not call
    this — dropping the limiter mid-flight would reset every bucket.
    """
    global _storage, _limiter  # noqa: PLW0603
    _storage = None
    _limiter = None


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
