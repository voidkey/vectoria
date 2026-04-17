import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from parsers.url._generic import GenericHandler
from parsers.base import ParseResult


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

    mock_resp = MagicMock()
    mock_resp.text = html
    mock_resp.url = "https://example.com/page"
    mock_resp.raise_for_status = MagicMock()

    with patch("parsers.url._generic.httpx.get", return_value=mock_resp), \
         patch("parsers.url._handlers.trafilatura.extract", return_value=long_content):
        h = GenericHandler()
        result = await h.parse("https://example.com/page")

    assert "Extracted content" in result.content
    assert result.title == "Test"


@pytest.mark.asyncio
async def test_handler_falls_back_to_playwright_on_js_challenge():
    mock_resp = MagicMock()
    mock_resp.text = "<html><body>Just a moment...</body></html>"
    mock_resp.url = "https://example.com/spa"
    mock_resp.raise_for_status = MagicMock()

    pw_result = ParseResult(content="Real SPA content " * 25, images={}, title="SPA Page")

    with patch("parsers.url._generic.httpx.get", return_value=mock_resp), \
         patch("parsers.url._handlers.trafilatura.extract", return_value="Just a moment..."), \
         patch.object(GenericHandler, "_parse_with_playwright", new_callable=AsyncMock,
                      return_value=pw_result):
        h = GenericHandler()
        result = await h.parse("https://example.com/spa")

    assert result.content == pw_result.content


@pytest.mark.asyncio
async def test_handler_returns_empty_on_total_failure():
    with patch("parsers.url._generic.httpx.get", side_effect=Exception("connection failed")), \
         patch("parsers.url._generic.async_playwright", None):
        h = GenericHandler()
        result = await h.parse("https://bad-url.example")

    assert result.content == ""
