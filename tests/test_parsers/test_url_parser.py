import pytest
from unittest.mock import patch, AsyncMock
from parsers.url_parser import UrlParser
from parsers.base import ParseResult


@pytest.mark.asyncio
async def test_parse_url_with_trafilatura():
    with patch("parsers.url_parser.trafilatura.fetch_url", return_value="<html><body><h1>Title</h1><p>Body</p></body></html>"), \
         patch("parsers.url_parser.trafilatura.extract", return_value="Title\n\nBody"):

        parser = UrlParser()
        result = await parser.parse("https://example.com")

    assert "Body" in result.content
    assert result.title != ""


@pytest.mark.asyncio
async def test_parse_url_returns_empty_on_failure():
    with patch("parsers.url_parser.trafilatura.fetch_url", return_value=None):
        parser = UrlParser()
        result = await parser.parse("https://bad-url.example")

    assert result.content == ""


def test_engine_name():
    assert UrlParser.engine_name == "url"


def test_supported_types():
    assert "url" in UrlParser.supported_types


@pytest.mark.asyncio
async def test_parse_wechat_extracts_image_urls():
    """WeChat path should return image_urls (not images bytes) and clean title."""
    fake_html = """
    <html><head><title>文章标题-微信公众号</title></head><body>
    <h2 id="activity-name">干净的标题</h2>
    <div id="js_content">
        <p>正文内容</p>
        <img data-src="https://mmbiz.qpic.cn/img1.jpg" src="" />
        <img data-src="https://mmbiz.qpic.cn/img2.jpg" src="" />
    </div>
    </body></html>
    """

    mock_page = AsyncMock()
    mock_page.goto = AsyncMock()
    mock_page.evaluate = AsyncMock(side_effect=[
        None,  # scroll
        "干净的标题",  # title
        fake_html.split('<div id="js_content">')[1].split('</div>')[0],  # content HTML
        ["https://mmbiz.qpic.cn/img1.jpg", "https://mmbiz.qpic.cn/img2.jpg"],  # image URLs
    ])
    mock_page.wait_for_timeout = AsyncMock()
    mock_page.content = AsyncMock(return_value=fake_html)

    mock_browser = AsyncMock()
    mock_browser.new_page = AsyncMock(return_value=mock_page)
    mock_browser.close = AsyncMock()

    mock_playwright = AsyncMock()
    mock_playwright.chromium.launch = AsyncMock(return_value=mock_browser)

    mock_cm = AsyncMock()
    mock_cm.__aenter__ = AsyncMock(return_value=mock_playwright)
    mock_cm.__aexit__ = AsyncMock(return_value=False)

    with patch("parsers.url_parser.trafilatura.extract", return_value="正文内容"):
        with patch("parsers.url_parser.async_playwright", return_value=mock_cm):
            parser = UrlParser()
            result = await parser.parse("https://mp.weixin.qq.com/s/abc123")

    assert result.title == "干净的标题"
    assert result.image_urls == ["https://mmbiz.qpic.cn/img1.jpg", "https://mmbiz.qpic.cn/img2.jpg"]
    assert result.images == {}
    assert "正文内容" in result.content


@pytest.mark.asyncio
async def test_parse_non_wechat_returns_image_urls():
    """Non-WeChat URLs should also return image_urls instead of downloading images."""
    html_with_images = '<html><head><title>Test Page</title></head><body>' \
        '<img src="https://example.com/photo.jpg" />' \
        '<img src="https://example.com/banner.png" />' \
        '<p>Hello world</p></body></html>'

    with patch("parsers.url_parser.trafilatura.fetch_url", return_value=html_with_images), \
         patch("parsers.url_parser.trafilatura.extract", return_value="Hello world"):
        parser = UrlParser()
        result = await parser.parse("https://example.com/article")

    assert result.image_urls == [
        "https://example.com/photo.jpg",
        "https://example.com/banner.png",
    ]
    assert result.images == {}
