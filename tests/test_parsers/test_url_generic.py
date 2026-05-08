import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from contextlib import contextmanager
from parsers.url._generic import GenericHandler
from parsers.base import ParseResult


@contextmanager
def _patch_async_httpx(*, html: str | None = None, url: str = "",
                      side_effect: Exception | None = None):
    """Patch ``httpx.AsyncClient`` returned by ``_generic.py``'s
    ``async with httpx.AsyncClient(...) as client`` block.

    Either provide HTML (and optional final URL) for success, or
    pass ``side_effect=SomeException`` to make the mocked ``.get``
    raise (mirroring network failures).
    """
    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    if side_effect is not None:
        mock_client.get = AsyncMock(side_effect=side_effect)
    else:
        mock_resp = MagicMock()
        mock_resp.text = html or ""
        mock_resp.url = url
        mock_resp.raise_for_status = MagicMock()
        mock_client.get = AsyncMock(return_value=mock_resp)
    with patch("parsers.url._generic.httpx.AsyncClient",
               return_value=mock_client):
        yield


def test_handler_match_always_true():
    h = GenericHandler()
    assert h.match("https://example.com/anything")
    assert h.match("https://unknown.org/page")


def test_handler_download_headers_always_none():
    h = GenericHandler()
    assert h.download_headers("https://example.com") is None


@pytest.mark.asyncio
async def test_handler_parse_with_httpx():
    html = "<html><head><title>Test</title></head><body><p>Content</p></body></html>"
    long_content = "Extracted content. " * 25

    with _patch_async_httpx(html=html, url="https://example.com/page"), \
         patch("parsers.url._handlers.trafilatura.extract", return_value=long_content):
        h = GenericHandler()
        result = await h.parse("https://example.com/page")

    assert "Extracted content" in result.content
    assert result.title == "Test"


@pytest.mark.asyncio
async def test_handler_falls_back_to_playwright_on_js_challenge():
    pw_result = ParseResult(content="Real SPA content " * 25, title="SPA Page")

    with _patch_async_httpx(
             html="<html><body>Just a moment...</body></html>",
             url="https://example.com/spa",
         ), \
         patch("parsers.url._handlers.trafilatura.extract", return_value="Just a moment..."), \
         patch.object(GenericHandler, "_parse_with_playwright", new_callable=AsyncMock,
                      return_value=pw_result):
        h = GenericHandler()
        result = await h.parse("https://example.com/spa")

    assert result.content == pw_result.content


@pytest.mark.asyncio
async def test_handler_returns_empty_on_total_failure():
    # httpx fails → empty result → needs_browser_fallback → playwright fallback
    # Playwright import raises → returns empty
    with _patch_async_httpx(side_effect=Exception("connection failed")), \
         patch.dict("sys.modules", {"playwright.async_api": None}):
        h = GenericHandler()
        result = await h.parse("https://bad-url.example")

    assert result.content == ""


@pytest.mark.asyncio
async def test_playwright_recovers_from_navigation_race():
    """Real-world failure: when a page does a client-side navigation
    (ad-tracker redirect, SPA route change) right after
    ``goto(domcontentloaded)`` returns, the first ``page.title()``
    inside the poll loop races with that navigation and playwright
    raises ``Execution context was destroyed``. The handler must
    swallow that error and keep polling, not let it bubble out and
    burn all three worker retries.
    """
    from contextlib import asynccontextmanager
    from playwright.async_api import Error as PlaywrightError

    page = MagicMock()
    page.goto = AsyncMock()
    page.wait_for_timeout = AsyncMock()
    page.wait_for_load_state = AsyncMock()
    page.url = "https://example.com/article"
    # First two title() calls die mid-navigation; third succeeds.
    page.title = AsyncMock(side_effect=[
        PlaywrightError(
            "Page.title: Execution context was destroyed, "
            "most likely because of a navigation"
        ),
        PlaywrightError(
            "Page.title: Execution context was destroyed, "
            "most likely because of a navigation"
        ),
        "Article Title",
    ])
    page.content = AsyncMock(return_value="<html><body>real content</body></html>")

    ctx = MagicMock()
    ctx.new_page = AsyncMock(return_value=page)

    @asynccontextmanager
    async def fake_session(**_kwargs):
        yield ctx

    # httpx fails outright so ``needs_browser_fallback`` triggers the
    # playwright path; trafilatura then extracts the article body from
    # the post-recovery ``page.content()``.
    long_content = "Real article body. " * 25

    with _patch_async_httpx(side_effect=Exception("connection failed")), \
         patch("parsers.url._handlers.trafilatura.extract", return_value=long_content), \
         patch("parsers.url._browser.parse_session", fake_session):
        h = GenericHandler()
        result = await h.parse("https://example.com/article")

    # Loop should have retried past the two navigation errors and finally
    # captured title + content on the third tick.
    assert page.title.await_count == 3
    assert result.title == "Article Title"
    assert "Real article body" in result.content


@pytest.mark.asyncio
async def test_generic_allow_image_only_stays_false():
    """Generic handler is HTML-scraped — stay strict."""
    html = "<html><head><title>Test</title></head><body><p>Content</p></body></html>"
    long_content = "Extracted content. " * 25

    with _patch_async_httpx(html=html, url="https://example.com/post"), \
         patch("parsers.url._handlers.trafilatura.extract", return_value=long_content):
        handler = GenericHandler()
        result = await handler.parse("https://example.com/post")

    assert result.allow_image_only is False
