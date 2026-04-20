"""Browser pool contract tests — no real Chromium in CI.

The pool is the main perf win of W3-e. If singleton semantics regress,
every worker task would re-launch Chromium and parse latency balloons
by 2-4 s. These tests mock playwright to assert:

  * get_browser() launches exactly once and caches the result
  * concurrent awaiters share the same instance (lock works)
  * parse_session yields a context and closes it on scope exit
  * close happens even when the body raises
  * block_heavy=True registers a route handler
"""
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from parsers.url import _browser


@pytest.fixture(autouse=True)
def _fresh_pool():
    """Each test starts with a cold pool — singleton would otherwise
    carry mock state across tests.
    """
    _browser._reset_for_tests()
    yield
    _browser._reset_for_tests()


def _make_playwright_mock():
    """Build a mocked async_playwright()-like object with the
    minimum surface the module uses.
    """
    browser = MagicMock(name="Browser")
    browser.is_connected = MagicMock(return_value=True)
    browser.close = AsyncMock()
    browser.new_context = AsyncMock(
        return_value=MagicMock(
            close=AsyncMock(),
            new_page=AsyncMock(),
            route=AsyncMock(),
        ),
    )

    chromium = MagicMock()
    chromium.launch = AsyncMock(return_value=browser)

    playwright = MagicMock(chromium=chromium, stop=AsyncMock())

    ctx_mgr = MagicMock()
    ctx_mgr.start = AsyncMock(return_value=playwright)

    return browser, chromium, playwright, ctx_mgr


@pytest.mark.asyncio
async def test_get_browser_launches_once_and_caches():
    """First call launches; second call returns the same instance
    without touching chromium.launch a second time.
    """
    browser, chromium, playwright, ctx_mgr = _make_playwright_mock()
    with patch("playwright.async_api.async_playwright", return_value=ctx_mgr):
        b1 = await _browser.get_browser()
        b2 = await _browser.get_browser()

    assert b1 is b2
    assert chromium.launch.await_count == 1


@pytest.mark.asyncio
async def test_concurrent_get_browser_calls_share_one_launch():
    """Lock semantics: 10 coroutines awaiting get_browser before the
    first launch finishes must all receive the same browser and
    chromium.launch must run exactly once.
    """
    browser, chromium, playwright, ctx_mgr = _make_playwright_mock()

    # Make launch take a tick so the race is observable.
    async def _slow_launch(**kw):
        await asyncio.sleep(0.01)
        return browser
    chromium.launch = AsyncMock(side_effect=_slow_launch)

    with patch("playwright.async_api.async_playwright", return_value=ctx_mgr):
        results = await asyncio.gather(
            *(_browser.get_browser() for _ in range(10))
        )

    assert all(r is browser for r in results)
    assert chromium.launch.await_count == 1, (
        f"launch called {chromium.launch.await_count} times; "
        "the singleton lock regressed"
    )


@pytest.mark.asyncio
async def test_shutdown_closes_browser_and_playwright():
    browser, chromium, playwright, ctx_mgr = _make_playwright_mock()
    with patch("playwright.async_api.async_playwright", return_value=ctx_mgr):
        await _browser.get_browser()
        await _browser.shutdown()

    browser.close.assert_awaited_once()
    playwright.stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_shutdown_is_idempotent():
    """Calling shutdown twice must not raise — we call it from signal
    handlers that may run multiple times.
    """
    browser, chromium, playwright, ctx_mgr = _make_playwright_mock()
    with patch("playwright.async_api.async_playwright", return_value=ctx_mgr):
        await _browser.get_browser()
        await _browser.shutdown()
        await _browser.shutdown()  # must not raise


@pytest.mark.asyncio
async def test_parse_session_creates_and_closes_context():
    browser, chromium, playwright, ctx_mgr = _make_playwright_mock()
    context = browser.new_context.return_value

    with patch("playwright.async_api.async_playwright", return_value=ctx_mgr):
        async with _browser.parse_session(user_agent="UA/1") as ctx:
            assert ctx is context

    browser.new_context.assert_awaited_once_with(user_agent="UA/1")
    context.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_parse_session_closes_on_exception():
    browser, chromium, playwright, ctx_mgr = _make_playwright_mock()
    context = browser.new_context.return_value

    with patch("playwright.async_api.async_playwright", return_value=ctx_mgr):
        with pytest.raises(RuntimeError, match="boom"):
            async with _browser.parse_session():
                raise RuntimeError("boom")

    context.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_parse_session_blocks_heavy_resources_by_default():
    browser, chromium, playwright, ctx_mgr = _make_playwright_mock()
    context = browser.new_context.return_value

    with patch("playwright.async_api.async_playwright", return_value=ctx_mgr):
        async with _browser.parse_session():
            pass

    context.route.assert_awaited_once()
    # First arg is the match pattern, second is the handler.
    pattern, _handler = context.route.await_args.args
    assert pattern == "**/*"


@pytest.mark.asyncio
async def test_parse_session_can_skip_resource_blocking():
    """block_heavy=False is used by the WeChat slow path so image
    requests are allowed through (some article variants need them
    for hydration).
    """
    browser, chromium, playwright, ctx_mgr = _make_playwright_mock()
    context = browser.new_context.return_value

    with patch("playwright.async_api.async_playwright", return_value=ctx_mgr):
        async with _browser.parse_session(block_heavy=False):
            pass

    context.route.assert_not_called()
