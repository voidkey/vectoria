"""Per-domain anti-bot cooldown, shared across the fleet via Redis.

When a domain anti-bots us (a terminal ``AntiBotBlockedError``), we set a
short-lived flag so subsequent URLs to that domain fail fast instead of each
re-probing the hostile site and escalating the ban. The flag is a plain
Redis key with a TTL — "half-open" is implicit (the next request after the
TTL probes naturally). All operations FAIL OPEN: a Redis outage degrades to
today's behaviour (no cross-fleet cooldown), never blocks the fetch path.

Outage handling mirrors ``infra.ratelimit``: bounded socket timeouts so a
network partition can't hang the hot fetch path, plus a sticky 60s backoff
so a down Redis isn't probed (or logged) on every fetch.
"""
from __future__ import annotations

import logging
import time

import redis.asyncio as aioredis

from config import get_settings
from infra.metrics import URL_BLOCK_COOLDOWN_TOTAL

logger = logging.getLogger(__name__)

_KEY_TEMPLATE = "urlblock:{host}"

# Bound per-call latency: on a partition the connect/read fails fast instead
# of hanging the URL fetch up to the OS default timeout.
_SOCKET_TIMEOUT = 2.0

# After a Redis error, skip Redis for this long rather than retrying (and
# re-logging) on every fetch. Mirrors infra.ratelimit._REDIS_RETRY_SECONDS.
_REDIS_RETRY_SECONDS = 60.0
_last_redis_error_ts: float = 0.0

_client: "aioredis.Redis | None" = None


def _get_client() -> "aioredis.Redis":
    global _client  # noqa: PLW0603
    if _client is None:
        url = get_settings().redis_url.get_secret_value()
        _client = aioredis.from_url(
            url,
            decode_responses=True,
            socket_connect_timeout=_SOCKET_TIMEOUT,
            socket_timeout=_SOCKET_TIMEOUT,
        )
    return _client


def _in_outage() -> bool:
    return (
        _last_redis_error_ts > 0
        and (time.monotonic() - _last_redis_error_ts) < _REDIS_RETRY_SECONDS
    )


def _note_error() -> None:
    global _last_redis_error_ts  # noqa: PLW0603
    if not _in_outage():  # log once per outage window, not per fetch
        # Called from within an ``except`` block, so exc_info captures the
        # triggering error (timeout vs refused vs auth) for diagnosis.
        logger.warning(
            "block-cooldown Redis unavailable; fail-open for ~%ds",
            int(_REDIS_RETRY_SECONDS),
            exc_info=True,
        )
    _last_redis_error_ts = time.monotonic()


def _note_ok() -> None:
    global _last_redis_error_ts  # noqa: PLW0603
    if _last_redis_error_ts:
        logger.info("block-cooldown Redis recovered")
        _last_redis_error_ts = 0.0


async def is_blocked(host: str) -> bool:
    """True if ``host`` is in anti-bot cooldown. Fail-open (False on error)."""
    if not host or _in_outage():
        return False
    try:
        present = await _get_client().exists(_KEY_TEMPLATE.format(host=host))
    except Exception:  # noqa: BLE001 — fail open, never block fetching
        _note_error()
        return False
    _note_ok()
    if present:
        URL_BLOCK_COOLDOWN_TOTAL.labels(action="shortcircuit").inc()
        return True
    return False


async def mark_blocked(host: str) -> None:
    """Put ``host`` into cooldown for ``url_block_cooldown_seconds``. Fail-open."""
    if not host or _in_outage():
        return
    ttl = get_settings().url_block_cooldown_seconds
    try:
        await _get_client().set(_KEY_TEMPLATE.format(host=host), "1", ex=ttl)
    except Exception:  # noqa: BLE001 — fail open
        _note_error()
        return
    _note_ok()
    URL_BLOCK_COOLDOWN_TOTAL.labels(action="marked").inc()


def _reset_for_tests() -> None:
    global _client, _last_redis_error_ts  # noqa: PLW0603
    _client = None
    _last_redis_error_ts = 0.0


def _set_client_for_tests(client: "aioredis.Redis") -> None:
    global _client  # noqa: PLW0603
    _client = client
