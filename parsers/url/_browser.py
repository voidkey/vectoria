"""Shared Chromium browser for the worker process.

Why
---
Launching Chromium is the slowest step in any page-rendering parse —
2-4 s before ``page.goto()`` even starts. A worker that processes N
URL tasks and launches Chromium for each pays that cost N times. The
module-level singleton here launches once per process and each parse
only creates/disposes a lightweight context (cookie/storage isolated).

Lifecycle
---------
* First caller wins the lazy ``get_browser()`` race under an
  asyncio.Lock. Subsequent callers reuse the same instance.
* A context-manager helper ``parse_session()`` yields a fresh context
  with image/font/media requests blocked, then closes it on exit.
  We only need DOM for extraction — not pixels — so blocking those
  resource types saves bandwidth + CPU and tightens the p95 of
  ``wait_until=domcontentloaded``.
* ``shutdown()`` is idempotent. Worker process exit would kill
  Chromium anyway, but calling it from the SIGTERM handler lets us
  close cleanly and flush any pending IO on the CDP channel.

Tests
-----
Unit tests patch ``async_playwright`` and assert the singleton
contract. Real end-to-end playwright exercise is deferred to vtest
smoke runs — headless Chromium in CI is slow and brittle.
"""
from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, AsyncIterator

if TYPE_CHECKING:
    from playwright.async_api import Browser, BrowserContext, Playwright

logger = logging.getLogger(__name__)

_playwright: "Playwright | None" = None
_browser: "Browser | None" = None
_launch_lock = asyncio.Lock()


async def get_browser() -> "Browser":
    """Return the process-wide Chromium instance, launching on first call."""
    global _playwright, _browser  # noqa: PLW0603
    if _browser is not None and _browser.is_connected():
        return _browser
    async with _launch_lock:
        # Second check under lock to avoid a double-launch race — first
        # winner launches, subsequent awaiters return the result.
        if _browser is not None and _browser.is_connected():
            return _browser

        from playwright.async_api import async_playwright

        _playwright = await async_playwright().start()
        _browser = await _playwright.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox"],
        )
        logger.info("Chromium launched (pid visible via playwright)")
    return _browser


async def shutdown() -> None:
    """Close the browser and stop playwright cleanly. Idempotent."""
    global _playwright, _browser  # noqa: PLW0603
    if _browser is not None:
        try:
            await _browser.close()
        except Exception:
            logger.exception("error closing browser on shutdown")
    if _playwright is not None:
        try:
            await _playwright.stop()
        except Exception:
            logger.exception("error stopping playwright on shutdown")
    _browser = None
    _playwright = None


_BLOCKED_RESOURCE_TYPES = {"image", "font", "media"}


async def _block_heavy_resources(route):  # type: ignore[no-untyped-def]
    """Request router: deny requests for pixels / fonts / video so the
    page finishes loading on DOM only. Stylesheets are kept because
    some SPAs gate DOM mutations on CSS-driven layout.
    """
    if route.request.resource_type in _BLOCKED_RESOURCE_TYPES:
        try:
            await route.abort()
            return
        except Exception:
            pass
    try:
        await route.continue_()
    except Exception:
        pass


@asynccontextmanager
async def parse_session(
    *, user_agent: str | None = None,
    block_heavy: bool = True,
) -> "AsyncIterator[BrowserContext]":
    """Yield a fresh BrowserContext from the pooled browser.

    Close is guaranteed on scope exit (success or exception), so
    callers don't need a try/finally. Cookies/storage are isolated to
    this context — two concurrent calls don't see each other's state.
    """
    browser = await get_browser()
    context = await browser.new_context(user_agent=user_agent)
    if block_heavy:
        await context.route("**/*", _block_heavy_resources)
    try:
        yield context
    finally:
        try:
            await context.close()
        except Exception:
            logger.exception("error closing browser context")


def _reset_for_tests() -> None:
    """Clear singleton state. Tests only."""
    global _playwright, _browser  # noqa: PLW0603
    _playwright = None
    _browser = None
