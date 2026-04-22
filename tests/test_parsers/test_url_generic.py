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
