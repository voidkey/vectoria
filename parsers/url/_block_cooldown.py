"""Per-domain anti-bot cooldown, shared across the fleet via Redis.

When a domain anti-bots us (a terminal ``AntiBotBlockedError``), we set a
short-lived flag so subsequent URLs to that domain fail fast instead of each
re-probing the hostile site and escalating the ban. The flag is a plain
Redis key with a TTL — "half-open" is implicit (the next request after the
TTL probes naturally). All operations FAIL OPEN: a Redis outage degrades to
today's behaviour (no cross-fleet cooldown), never blocks the fetch path.
"""
from __future__ import annotations

import logging

import redis.asyncio as aioredis

from config import get_settings
from infra.metrics import URL_BLOCK_COOLDOWN_TOTAL

logger = logging.getLogger(__name__)

_KEY = "urlblock:{host}"
_client: "aioredis.Redis | None" = None


def _get_client() -> "aioredis.Redis":
    global _client  # noqa: PLW0603
    if _client is None:
        url = get_settings().redis_url.get_secret_value()
        _client = aioredis.from_url(url, decode_responses=True)
    return _client


async def is_blocked(host: str) -> bool:
    """True if ``host`` is in anti-bot cooldown. Fail-open (False on error)."""
    if not host:
        return False
    try:
        present = await _get_client().exists(_KEY.format(host=host))
    except Exception:  # noqa: BLE001 — fail open, never block fetching
        logger.warning("block-cooldown check failed for %s; fail-open", host, exc_info=True)
        return False
    if present:
        URL_BLOCK_COOLDOWN_TOTAL.labels(action="shortcircuit").inc()
        return True
    return False


async def mark_blocked(host: str) -> None:
    """Put ``host`` into cooldown for ``url_block_cooldown_seconds``. Fail-open."""
    if not host:
        return
    ttl = get_settings().url_block_cooldown_seconds
    try:
        await _get_client().set(_KEY.format(host=host), "1", ex=ttl)
        URL_BLOCK_COOLDOWN_TOTAL.labels(action="marked").inc()
    except Exception:  # noqa: BLE001 — fail open
        logger.warning("block-cooldown mark failed for %s; fail-open", host, exc_info=True)


def _reset_for_tests() -> None:
    global _client  # noqa: PLW0603
    _client = None


def _set_client_for_tests(client) -> None:
    global _client  # noqa: PLW0603
    _client = client
