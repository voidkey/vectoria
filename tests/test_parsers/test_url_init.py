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
    """Patch ``parsers.url._http.make_async_client`` / ``fetch_capped``
    so handlers that route through the capped factory return canned HTML.

    The ``module`` argument is kept for backward-compat call-site
    readability but is no longer used as a patch target — after Task 7
    every handler imports from ``parsers.url._http``.
    """
    import parsers.url._http as _http_mod

    html_bytes = html.encode("utf-8") if isinstance(html, str) else html
    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)

    mock_resp = MagicMock()
    mock_resp.encoding = "utf-8"
    mock_resp.url = url

    async def _fake_fetch(client, fetch_url, **kw):
        return mock_resp, html_bytes

    with patch.object(_http_mod, "make_async_client", return_value=mock_client), \
         patch.object(_http_mod, "fetch_capped", side_effect=_fake_fetch):
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


def test_feishu_handler_registered_before_blacklist():
    from parsers.url import find_handler
    from parsers.url._feishu import FeishuHandler

    h = find_handler("https://whobotai.feishu.cn/docx/ABC")
    assert isinstance(h, FeishuHandler)


def test_feishu_handler_does_not_match_sheets():
    from parsers.url import find_handler
    from parsers.url._feishu import FeishuHandler

    h = find_handler("https://whobotai.feishu.cn/sheets/ABC")
    assert not isinstance(h, FeishuHandler)
