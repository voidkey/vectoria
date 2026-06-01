import pytest

from parsers.url import _block_cooldown as bc


class _FakeRedis:
    def __init__(self):
        self.store = {}
        self.set_calls = []

    async def exists(self, key):
        return 1 if key in self.store else 0

    async def set(self, key, value, ex=None):
        self.store[key] = value
        self.set_calls.append((key, value, ex))


class _RaisingRedis:
    def __init__(self):
        self.exists_calls = 0

    async def exists(self, key):
        self.exists_calls += 1
        raise RuntimeError("redis down")

    async def set(self, *a, **k):
        raise RuntimeError("redis down")


@pytest.fixture(autouse=True)
def _reset():
    bc._reset_for_tests()
    yield
    bc._reset_for_tests()


@pytest.mark.asyncio
async def test_unset_host_not_blocked():
    bc._set_client_for_tests(_FakeRedis())
    assert await bc.is_blocked("example.com") is False


@pytest.mark.asyncio
async def test_mark_then_blocked():
    fake = _FakeRedis()
    bc._set_client_for_tests(fake)
    await bc.mark_blocked("example.com")
    assert await bc.is_blocked("example.com") is True
    key, _val, ex = fake.set_calls[0]
    assert key == "urlblock:example.com"
    assert ex == 900


@pytest.mark.asyncio
async def test_empty_host_is_noop():
    fake = _FakeRedis()
    bc._set_client_for_tests(fake)
    assert await bc.is_blocked("") is False
    await bc.mark_blocked("")
    assert fake.set_calls == []


@pytest.mark.asyncio
async def test_fail_open_on_redis_error():
    bc._set_client_for_tests(_RaisingRedis())
    assert await bc.is_blocked("example.com") is False
    await bc.mark_blocked("example.com")  # must not raise


@pytest.mark.asyncio
async def test_mark_blocked_fail_open_independently():
    """mark_blocked's OWN error path (not piggybacking on a prior is_blocked
    outage) must fail open and mark the outage."""
    bc._set_client_for_tests(_RaisingRedis())
    await bc.mark_blocked("example.com")  # first call, must not raise
    assert bc._in_outage() is True  # its error path tripped the backoff


@pytest.mark.asyncio
async def test_outage_backoff_skips_redis_within_window():
    """After a Redis error, calls within the backoff window must NOT hit Redis
    again (no per-fetch reconnect / log spam)."""
    raising = _RaisingRedis()
    bc._set_client_for_tests(raising)
    assert await bc.is_blocked("example.com") is False
    assert raising.exists_calls == 1  # first call attempted Redis and failed
    # Subsequent calls within the 60s window short-circuit before Redis.
    assert await bc.is_blocked("example.com") is False
    assert await bc.is_blocked("other.com") is False
    assert raising.exists_calls == 1  # no further Redis calls during outage
    # mark_blocked also skips Redis during the outage (no raise, no-op).
    await bc.mark_blocked("example.com")


@pytest.mark.asyncio
async def test_metric_increments_on_mark_and_hit():
    from infra.metrics import URL_BLOCK_COOLDOWN_TOTAL

    bc._set_client_for_tests(_FakeRedis())
    marked = URL_BLOCK_COOLDOWN_TOTAL.labels(action="marked")._value.get()
    short = URL_BLOCK_COOLDOWN_TOTAL.labels(action="shortcircuit")._value.get()

    await bc.mark_blocked("example.com")
    assert await bc.is_blocked("example.com") is True

    assert URL_BLOCK_COOLDOWN_TOTAL.labels(action="marked")._value.get() == marked + 1
    assert URL_BLOCK_COOLDOWN_TOTAL.labels(action="shortcircuit")._value.get() == short + 1
