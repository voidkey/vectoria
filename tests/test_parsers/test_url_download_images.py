"""download_images × distributed rate limiter integration.

Guards the contract that W3 platform extractors will rely on:
  * Each image fetch goes through ``infra.ratelimit.acquire`` with a
    per-domain (rate, per_seconds) pair.
  * A URL that fails the rate gate is *skipped*, not retried forever,
    so a single flooded CDN can't stall the whole batch.
  * Different hostnames have independent buckets.

All tests use ``limits`` MemoryStorage so they run without Redis.
"""
import asyncio
from collections import Counter
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
    """Build an AsyncClient context-manager double and a matching
    ``fetch_capped`` stub.  After migrating to the capped factory,
    ``download_images`` uses ``make_async_client`` + ``fetch_capped``
    from ``_http.py``.  Tests that need to stub out responses must
    patch both at ``parsers.url._http.*`` (the import site).
    """
    client = MagicMock()
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)
    # Attach the legacy .get mock so callers can still assert on it.
    client.get = AsyncMock(return_value=resp)
    return client


def _make_fetch_capped_stub(resp):
    """Return an async callable that emulates ``fetch_capped`` returning
    ``(resp, resp.content)``.  When ``resp.status_code != 200`` or
    ``resp.content`` is falsy the return value is still yielded —
    callers reproduce the original ``if resp.status_code == 200`` guard.
    """
    async def _stub(client, url, **kw):
        return resp, resp.content
    return _stub


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


def test_rate_map_covers_qpic_variants():
    """Real traffic surfaced mmecoa.qpic.cn in early 2024 — a newer
    WeChat CDN variant that the original ``mmbiz.qpic.cn`` entry
    didn't match. The broadened ``qpic.cn`` suffix now catches both,
    and any future ``*.qpic.cn`` host WeChat rolls out.
    """
    for host in (
        "mmbiz.qpic.cn",
        "mmecoa.qpic.cn",
        "mmedia-static.qpic.cn",  # hypothetical future
    ):
        assert _rate_for_host(host) == (10, 1), host


def test_rate_map_covers_xiaohongshu_static_assets():
    """picasso-static.xiaohongshu.com carries UI assets for xhs notes
    (cover images, logos). It should be rate-limited as tight as
    xhscdn.com since it's the same platform's infra.
    """
    for host in (
        "xiaohongshu.com",
        "picasso-static.xiaohongshu.com",
        "www.xiaohongshu.com",
    ):
        assert _rate_for_host(host) == (3, 1), host


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
    fetch_mock = AsyncMock(side_effect=_make_fetch_capped_stub(resp))

    with (
        patch("parsers.url._handlers.rl_acquire", new=_always_block),
        patch("parsers.url._http.make_async_client", return_value=client_ctx),
        patch("parsers.url._http.fetch_capped", new=fetch_mock),
    ):
        result = await download_images(["https://xhscdn.com/img.jpg"])

    assert result == {}
    fetch_mock.assert_not_called(), "blocked URL must not hit the network"


@pytest.mark.asyncio
async def test_limiter_called_per_image_with_host_key():
    """Verify the limiter sees one key per unique hostname.

    Fetches run concurrently, so assert the *multiset* of keys, not their
    order — call order is no longer part of the contract.
    """
    recorded: list[str] = []

    async def _record(key, **kw):
        recorded.append(key)
        return True  # allow all for this test

    resp = MagicMock(status_code=200, content=b"x")
    client_ctx = _fake_client_ctx(resp)

    with (
        patch("parsers.url._handlers.rl_acquire", new=_record),
        patch("parsers.url._http.make_async_client", return_value=client_ctx),
        patch("parsers.url._http.fetch_capped", new=_make_fetch_capped_stub(resp)),
    ):
        await download_images([
            "https://mmbiz.qpic.cn/a.jpg",
            "https://xhscdn.com/b.jpg",
            "https://mmbiz.qpic.cn/c.jpg",   # same host as #1
        ])

    assert Counter(recorded) == Counter(
        ["mmbiz.qpic.cn", "xhscdn.com", "mmbiz.qpic.cn"]
    )


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
        patch("parsers.url._http.make_async_client", return_value=client_ctx),
        patch("parsers.url._http.fetch_capped", new=_make_fetch_capped_stub(resp)),
    ):
        await download_images([
            "https://mmbiz.qpic.cn/a.jpg",   # wechat → (10, 1)
            "https://xhscdn.com/b.jpg",      # xhs → (3, 1)
            "https://pbs.twimg.com/c.jpg",   # twitter → (2, 1)
        ])

    # Concurrent fetch ⇒ order-independent; assert the multiset of rate pairs.
    assert Counter(recorded) == Counter([(10, 1), (3, 1), (2, 1)])


@pytest.mark.asyncio
async def test_http_exception_does_not_break_batch():
    """A failing URL shouldn't bring down the others — ensures we catch
    httpx exceptions per-URL rather than propagating.

    Since W5-1, download_images also runs an SSRF re-check per URL;
    this test stubs that to no-op so we're testing the httpx-error
    path specifically, not the SSRF path.
    """
    async def _allow_all(*a, **kw):
        return True
    async def _noop_ssrf(*a, **kw):
        return None

    ok_resp = MagicMock(status_code=200, content=b"img")
    client = MagicMock()
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)

    # Key the failure to the URL, not call order: fetches run concurrently so
    # "which call is first" is nondeterministic.
    async def _fetch_broken_fails(c, url, **kw):
        if "broken" in url:
            raise ConnectionError("dns dead")
        return ok_resp, ok_resp.content

    with (
        patch("parsers.url._handlers.rl_acquire", new=_allow_all),
        patch("parsers.url._http.make_async_client", return_value=client),
        patch("parsers.url._http.fetch_capped", side_effect=_fetch_broken_fails),
        patch("api.url_validation.reresolve_and_check_ssrf", new=_noop_ssrf),
    ):
        result = await download_images([
            "https://broken.example.com/a.jpg",
            "https://working.example.com/b.jpg",
        ])

    assert list(result.keys()) == ["https://working.example.com/b.jpg"]


@pytest.mark.asyncio
async def test_unexpected_ssrf_error_skips_only_that_image():
    """A non-AppError raised in the pre-fetch stage (SSRF re-check / rate
    gate) must skip just that image, not abort the whole concurrent batch.

    Regression for the best-effort guard: under gather(), an unguarded raise
    here would propagate and sink sibling fetches.
    """
    async def _allow_all(*a, **kw):
        return True

    ok_resp = MagicMock(status_code=200, content=b"img")
    client_ctx = _fake_client_ctx(ok_resp)

    async def _ssrf_blows_up_on_broken(url, *a, **kw):
        if "broken" in url:
            raise RuntimeError("resolver exploded")  # NOT an AppError
        return None

    with (
        patch("parsers.url._handlers.rl_acquire", new=_allow_all),
        patch("parsers.url._http.make_async_client", return_value=client_ctx),
        patch("parsers.url._http.fetch_capped", new=_make_fetch_capped_stub(ok_resp)),
        patch("api.url_validation.reresolve_and_check_ssrf",
              new=_ssrf_blows_up_on_broken),
    ):
        result = await download_images([
            "https://broken.example.com/a.jpg",
            "https://working.example.com/b.jpg",
        ])

    assert list(result.keys()) == ["https://working.example.com/b.jpg"]


@pytest.mark.asyncio
async def test_downloads_run_concurrently_and_bounded():
    """Regression for the perf fix: images fetch in parallel (bounded by the
    semaphore), not one at a time.

    Serial download of many slow-CDN images ran a single task for minutes,
    starving the worker heartbeat. Track peak simultaneous in-flight fetches:
    serial peaks at 1; concurrent must overlap yet stay within the cap.
    """
    from parsers.url._handlers import _IMAGE_DOWNLOAD_CONCURRENCY

    async def _allow_all(*a, **kw):
        return True

    async def _noop_ssrf(*a, **kw):
        return None

    ok_resp = MagicMock(status_code=200, content=b"img")
    client = MagicMock()
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)

    inflight = 0
    peak = 0

    async def _slow_fetch(c, url, **kw):
        nonlocal inflight, peak
        inflight += 1
        peak = max(peak, inflight)
        await asyncio.sleep(0.02)
        inflight -= 1
        return ok_resp, ok_resp.content

    with (
        patch("parsers.url._handlers.rl_acquire", new=_allow_all),
        patch("parsers.url._http.make_async_client", return_value=client),
        patch("parsers.url._http.fetch_capped", side_effect=_slow_fetch),
        patch("api.url_validation.reresolve_and_check_ssrf", new=_noop_ssrf),
    ):
        result = await download_images(
            [f"https://example.com/{i}.jpg" for i in range(10)]
        )

    assert len(result) == 10
    assert peak > 1, "fetches must overlap — serial execution regressed"
    assert peak <= _IMAGE_DOWNLOAD_CONCURRENCY, "semaphore bound exceeded"


@pytest.mark.asyncio
async def test_cap_at_20_urls(monkeypatch):
    """The hard cap survives the async refactor; pin cap=20 for the test."""
    class _Stub:
        url_image_cap = 20
    monkeypatch.setattr(
        "parsers.url._handlers.get_settings", lambda: _Stub(), raising=False
    )

    async def _allow_all(*a, **kw):
        return True

    ok_resp = MagicMock(status_code=200, content=b"img")
    client_ctx = _fake_client_ctx(ok_resp)
    fetch_mock = AsyncMock(side_effect=_make_fetch_capped_stub(ok_resp))

    with (
        patch("parsers.url._handlers.rl_acquire", new=_allow_all),
        patch("parsers.url._http.make_async_client", return_value=client_ctx),
        patch("parsers.url._http.fetch_capped", new=fetch_mock),
    ):
        result = await download_images([
            f"https://example.com/{i}.jpg" for i in range(50)
        ])

    # Exactly 20 succeed; the other 30 are trimmed.
    assert len(result) == 20
    assert fetch_mock.await_count == 20
