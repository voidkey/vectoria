"""download_images × distributed rate limiter integration.

Guards the contract that W3 platform extractors will rely on:
  * Each image fetch goes through ``infra.ratelimit.acquire`` with a
    per-domain (rate, per_seconds) pair.
  * A URL that fails the rate gate is *skipped*, not retried forever,
    so a single flooded CDN can't stall the whole batch.
  * Different hostnames have independent buckets.

All tests use ``limits`` MemoryStorage so they run without Redis.
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from limits.aio.storage import MemoryStorage

from infra import ratelimit
from parsers.url._handlers import download_images, _rate_for_host


@pytest.fixture(autouse=True)
def _fresh_limiter():
    ratelimit._reset_for_tests()
    ratelimit._set_storage_for_tests(MemoryStorage())
    yield
    ratelimit._reset_for_tests()


def _fake_client_ctx(resp):
    """Build an AsyncClient context-manager double that returns ``resp``
    from every ``.get()``.
    """
    client = MagicMock()
    client.get = AsyncMock(return_value=resp)
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)
    return client


# ---------------------------------------------------------------------------
# Rate limiter tuning
# ---------------------------------------------------------------------------

def test_rate_map_covers_target_platforms():
    """The built-in rate table must cover the W3 target CDNs so the
    default 10/s doesn't silently over-pace a protected platform.
    """
    # Exact + subdomain match should both resolve to the platform entry.
    assert _rate_for_host("mmbiz.qpic.cn") == (10, 1)
    assert _rate_for_host("sns-img-qc.xhscdn.com") == (3, 1)
    assert _rate_for_host("pbs.twimg.com") == (2, 1)


def test_rate_map_fallback_for_unknown_host():
    # Must never return 0 — that would block everything.
    rate, per = _rate_for_host("unknown.example.com")
    assert rate > 0 and per > 0


# ---------------------------------------------------------------------------
# download_images with limiter
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_skips_image_when_rate_bucket_exhausted():
    """If the bucket can't grant a token within retry budget, the image
    is skipped — not retried forever, and not fetched anyway.
    """
    # Force the rate limiter to always return False for this test by
    # monkeypatching ``acquire`` itself (simulates a Redis returning
    # "blocked" indefinitely).
    async def _always_block(*a, **kw):
        return False

    resp = MagicMock(status_code=200, content=b"x")
    client_ctx = _fake_client_ctx(resp)

    with (
        patch("parsers.url._handlers.rl_acquire", new=_always_block),
        patch("parsers.url._handlers.httpx.AsyncClient", return_value=client_ctx),
    ):
        result = await download_images(["https://xhscdn.com/img.jpg"])

    assert result == {}
    client_ctx.get.assert_not_called(), "blocked URL must not hit the network"


@pytest.mark.asyncio
async def test_limiter_called_per_image_with_host_key():
    """Verify the limiter sees one key per unique hostname."""
    recorded: list[str] = []

    async def _record(key, **kw):
        recorded.append(key)
        return True  # allow all for this test

    resp = MagicMock(status_code=200, content=b"x")
    client_ctx = _fake_client_ctx(resp)

    with (
        patch("parsers.url._handlers.rl_acquire", new=_record),
        patch("parsers.url._handlers.httpx.AsyncClient", return_value=client_ctx),
    ):
        await download_images([
            "https://mmbiz.qpic.cn/a.jpg",
            "https://xhscdn.com/b.jpg",
            "https://mmbiz.qpic.cn/c.jpg",   # same host as #1
        ])

    assert recorded == ["mmbiz.qpic.cn", "xhscdn.com", "mmbiz.qpic.cn"]


@pytest.mark.asyncio
async def test_rate_table_used_when_gating():
    """The limiter must be called with rate=(rate, per_seconds) matching
    the CDN's entry in ``_DOMAIN_RATES``.
    """
    recorded: list[tuple[int, int]] = []

    async def _spy(key, *, rate, per_seconds):
        recorded.append((rate, per_seconds))
        return True

    resp = MagicMock(status_code=200, content=b"x")
    client_ctx = _fake_client_ctx(resp)

    with (
        patch("parsers.url._handlers.rl_acquire", new=_spy),
        patch("parsers.url._handlers.httpx.AsyncClient", return_value=client_ctx),
    ):
        await download_images([
            "https://mmbiz.qpic.cn/a.jpg",   # wechat → (10, 1)
            "https://xhscdn.com/b.jpg",      # xhs → (3, 1)
            "https://pbs.twimg.com/c.jpg",   # twitter → (2, 1)
        ])

    assert recorded == [(10, 1), (3, 1), (2, 1)]


@pytest.mark.asyncio
async def test_http_exception_does_not_break_batch():
    """A failing URL shouldn't bring down the others — ensures we catch
    httpx exceptions per-URL rather than propagating.
    """
    async def _allow_all(*a, **kw):
        return True

    ok_resp = MagicMock(status_code=200, content=b"img")
    client = MagicMock()
    # First call raises, second returns success.
    client.get = AsyncMock(side_effect=[ConnectionError("dns dead"), ok_resp])
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)

    with (
        patch("parsers.url._handlers.rl_acquire", new=_allow_all),
        patch("parsers.url._handlers.httpx.AsyncClient", return_value=client),
    ):
        result = await download_images([
            "https://broken.example.com/a.jpg",
            "https://working.example.com/b.jpg",
        ])

    assert list(result.keys()) == ["https://working.example.com/b.jpg"]


@pytest.mark.asyncio
async def test_cap_at_20_urls():
    """The existing 20-URL hard cap survives the async refactor."""
    async def _allow_all(*a, **kw):
        return True

    ok_resp = MagicMock(status_code=200, content=b"img")
    client_ctx = _fake_client_ctx(ok_resp)

    with (
        patch("parsers.url._handlers.rl_acquire", new=_allow_all),
        patch("parsers.url._handlers.httpx.AsyncClient", return_value=client_ctx),
    ):
        result = await download_images([
            f"https://example.com/{i}.jpg" for i in range(50)
        ])

    # Exactly 20 succeed; the other 30 are trimmed.
    assert len(result) == 20
    assert client_ctx.get.await_count == 20
