import pytest
from unittest.mock import patch, MagicMock
from parsers.url import (
    UrlParser,
    download_images,
    get_wechat_headers,
)
from parsers.base import ParseResult


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
    mock_resp = MagicMock()
    mock_resp.text = fake_html
    mock_resp.raise_for_status = MagicMock()

    with patch("parsers.url._wechat.httpx.get", return_value=mock_resp), \
         patch("parsers.url._handlers.trafilatura.extract", return_value="测试内容"):
        parser = UrlParser()
        result = await parser.parse("https://mp.weixin.qq.com/s/test123")

    assert result.title == "测试标题"
    assert "测试内容" in result.content


@pytest.mark.asyncio
async def test_dispatch_generic_url():
    html = "<html><head><title>Generic</title></head><body><p>Content</p></body></html>"
    long_content = "Generic content. " * 25

    mock_resp = MagicMock()
    mock_resp.text = html
    mock_resp.url = "https://example.com/page"
    mock_resp.raise_for_status = MagicMock()

    with patch("parsers.url._generic.httpx.get", return_value=mock_resp), \
         patch("parsers.url._handlers.trafilatura.extract", return_value=long_content):
        parser = UrlParser()
        result = await parser.parse("https://example.com/page")

    assert "Generic content" in result.content
