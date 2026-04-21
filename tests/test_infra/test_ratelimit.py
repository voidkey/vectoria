"""Rate limiter contract tests.

The whole point of this module is that ``acquire`` returns True exactly
``rate`` times inside any ``per_seconds`` window *across all callers*.
If that breaks, W3 platform fetchers will stampede their targets and
get our IP banned. Tests run against the in-memory backend (same code
path as Redis for token accounting; no network required in CI).
"""
import asyncio

import pytest
from limits.aio.storage import MemoryStorage

from infra import ratelimit


@pytest.fixture(autouse=True)
def _memory_backend():
    """Reset the module singleton and wire an in-memory storage for
    each test. Production uses Redis; the accounting strategy
    (``MovingWindowRateLimiter``) is identical across backends.
    """
    ratelimit._reset_for_tests()
    ratelimit._set_storage_for_tests(MemoryStorage())
    yield
    ratelimit._reset_for_tests()


async def test_allows_up_to_rate_then_blocks():
    # 3 tokens per 1-second window; 4th call blocked.
    for _ in range(3):
        assert await ratelimit.acquire("domain-a", rate=3, per_seconds=1)
    assert await ratelimit.acquire("domain-a", rate=3, per_seconds=1) is False


async def test_different_keys_have_independent_buckets():
    # Exhausting domain-a must not affect domain-b.
    for _ in range(3):
        await ratelimit.acquire("domain-a", rate=3, per_seconds=1)
    assert await ratelimit.acquire("domain-a", rate=3, per_seconds=1) is False

    # domain-b still has a full bucket.
    assert await ratelimit.acquire("domain-b", rate=3, per_seconds=1)


async def test_concurrent_acquires_share_a_single_bucket():
    """Atomic accounting across concurrent awaiters — only ``rate`` of
    ``N`` parallel callers may succeed. If this test starts flapping,
    we lost atomicity (Lua script or single-instance guarantee).
    """
    n_parallel = 10
    rate = 3

    async def _try():
        return await ratelimit.acquire("concurrent", rate=rate, per_seconds=1)

    results = await asyncio.gather(*(_try() for _ in range(n_parallel)))
    allowed = sum(1 for r in results if r)
    blocked = sum(1 for r in results if not r)

    assert allowed == rate, f"expected exactly {rate} allowed, got {allowed}"
    assert blocked == n_parallel - rate


async def test_bucket_refills_after_window_elapses():
    # Exhaust the bucket.
    for _ in range(2):
        await ratelimit.acquire("refill-test", rate=2, per_seconds=1)
    assert await ratelimit.acquire("refill-test", rate=2, per_seconds=1) is False

    # Moving window: after 1.1 s the earliest tokens fall out of scope.
    await asyncio.sleep(1.1)
    assert await ratelimit.acquire("refill-test", rate=2, per_seconds=1)


async def test_failure_of_backend_falls_back_to_local_bucket(monkeypatch):
    """W5-6 change: when Redis throws, acquire flips to an in-memory
    local bucket instead of silently dropping the caller's request.

    Rationale: the old fail-closed behaviour "protected us from being
    banned" in theory, but in practice callers treat False as "skip
    this image", so a Redis blip meant image loss during ingest.
    Fail-open with local rate-limiting keeps per-worker pacing and
    accepts a small degradation in cross-worker coordination.
    """

    class _ExplodingStorage(MemoryStorage):
        async def acquire_entry(self, *a, **kw):  # type: ignore[override]
            raise ConnectionError("simulated redis outage")

    ratelimit._reset_for_tests()
    ratelimit._set_storage_for_tests(_ExplodingStorage())

    # Must not raise. First call triggers the Redis failure and falls
    # back to the local bucket — the local bucket has room, so True.
    assert await ratelimit.acquire("any", rate=10, per_seconds=1) is True

    # Subsequent calls within the retry window go straight to local
    # without hitting Redis again (checked via metric).
    from prometheus_client import REGISTRY

    def _read(result: str) -> float:
        v = REGISTRY.get_sample_value(
            "vectoria_ratelimit_checks_total",
            {"key": "any", "result": result},
        )
        return float(v or 0)

    before_fallback = _read("local_fallback")
    await ratelimit.acquire("any", rate=10, per_seconds=1)
    after_fallback = _read("local_fallback")
    assert after_fallback > before_fallback, (
        "local_fallback metric should increment on outage-path acquires"
    )


async def test_metric_increments_by_result():
    from prometheus_client import REGISTRY

    def _read(result: str, key: str = "metric-test") -> float:
        v = REGISTRY.get_sample_value(
            "vectoria_ratelimit_checks_total",
            {"key": key, "result": result},
        )
        return float(v or 0)

    before_allowed = _read("allowed")
    before_blocked = _read("blocked")

    for _ in range(2):
        await ratelimit.acquire("metric-test", rate=2, per_seconds=1)
    await ratelimit.acquire("metric-test", rate=2, per_seconds=1)  # blocked

    assert _read("allowed") == before_allowed + 2
    assert _read("blocked") == before_blocked + 1
