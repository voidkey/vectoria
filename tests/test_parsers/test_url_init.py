import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from contextlib import contextmanager
from parsers.url import (
    UrlParser,
    download_images,
    get_wechat_headers,
)
from parsers.base import ParseResult


def _public_dns():
    """Return a getaddrinfo result that passes SSRF re-check — public IP."""
    return [(2, 1, 6, "", ("93.184.216.34", 0))]


@contextmanager
def _async_httpx(module: str, *, html: str, url: str = ""):
    """Patch the ``httpx.AsyncClient`` referenced from a given module."""
    mock_resp = MagicMock()
    mock_resp.text = html
    mock_resp.url = url
    mock_resp.raise_for_status = MagicMock()
    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mock_client.get = AsyncMock(return_value=mock_resp)
    with patch(f"{module}.httpx.AsyncClient", return_value=mock_client):
        yield


def test_url_parser_engine_name():
    assert UrlParser.engine_name == "url"


def test_url_parser_supported_types():
    assert "url" in UrlParser.supported_types


def test_public_reexports():
    """Symbols that external code imports must be accessible."""
    assert callable(download_images)
    assert callable(get_wechat_headers)


@pytest.mark.asyncio
async def test_dispatch_wechat_to_wechat_handler():
    fake_html = """
    <html><body>
    <h2 id="activity-name"><span class="js_title_inner">测试标题</span></h2>
    <div id="js_content"><p>测试内容</p></div>
    </body></html>
    """

    with _async_httpx("parsers.url._wechat", html=fake_html), \
         patch("parsers.url._handlers.trafilatura.extract", return_value="测试内容"), \
         patch("api.url_validation.socket.getaddrinfo", return_value=_public_dns()):
        parser = UrlParser()
        result = await parser.parse("https://mp.weixin.qq.com/s/test123")

    assert result.title == "测试标题"
    assert "测试内容" in result.content


@pytest.mark.asyncio
async def test_dispatch_generic_url():
    html = "<html><head><title>Generic</title></head><body><p>Content</p></body></html>"
    long_content = "Generic content. " * 25

    with _async_httpx("parsers.url._generic", html=html, url="https://example.com/page"), \
         patch("parsers.url._handlers.trafilatura.extract", return_value=long_content), \
         patch("api.url_validation.socket.getaddrinfo", return_value=_public_dns()):
        parser = UrlParser()
        result = await parser.parse("https://example.com/page")

    assert "Generic content" in result.content
