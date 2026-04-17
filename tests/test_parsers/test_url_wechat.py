import pytest
from unittest.mock import patch, MagicMock, AsyncMock
import lxml.html

from parsers.url._wechat import (
    WechatHandler,
    is_wechat_url,
    get_wechat_headers,
    extract_datasrc_urls,
    extract_wechat_title,
)
from parsers.base import ParseResult


def test_is_wechat_url_true():
    assert is_wechat_url("https://mp.weixin.qq.com/s/abc123")


def test_is_wechat_url_false():
    assert not is_wechat_url("https://example.com/article")


def test_get_wechat_headers_wechat_url():
    headers = get_wechat_headers("https://mp.weixin.qq.com/s/abc123")
    assert headers is not None
    assert headers["Referer"] == "https://mp.weixin.qq.com/"
    assert "MicroMessenger" in headers["User-Agent"]


def test_get_wechat_headers_non_wechat_url():
    assert get_wechat_headers("https://example.com/article") is None


def test_extract_datasrc_urls():
    doc = lxml.html.fromstring(
        '<div><img data-src="https://a.com/1.jpg"/>'
        '<img data-src="https://a.com/2.jpg"/>'
        '<img src="https://a.com/no-datasrc.jpg"/></div>'
    )
    urls = extract_datasrc_urls(doc)
    assert urls == ["https://a.com/1.jpg", "https://a.com/2.jpg"]


def test_extract_datasrc_urls_deduplicates():
    doc = lxml.html.fromstring(
        '<div><img data-src="https://a.com/1.jpg"/>'
        '<img data-src="https://a.com/1.jpg"/></div>'
    )
    assert extract_datasrc_urls(doc) == ["https://a.com/1.jpg"]


def test_extract_wechat_title_with_inner_span():
    doc = lxml.html.fromstring(
        '<html><body><h2 id="activity-name">'
        '<span class="js_title_inner">标题文字</span></h2></body></html>'
    )
    assert extract_wechat_title(doc) == "标题文字"


def test_extract_wechat_title_plain_text():
    doc = lxml.html.fromstring(
        '<html><body><h2 id="activity-name">纯文本标题</h2></body></html>'
    )
    assert extract_wechat_title(doc) == "纯文本标题"


def test_handler_match():
    h = WechatHandler()
    assert h.match("https://mp.weixin.qq.com/s/abc123")
    assert h.match("https://weixin.qq.com/some/page")
    assert not h.match("https://example.com/article")


def test_handler_download_headers():
    h = WechatHandler()
    headers = h.download_headers("https://mp.weixin.qq.com/s/abc123")
    assert headers is not None
    assert "MicroMessenger" in headers["User-Agent"]
    assert h.download_headers("https://example.com") is None


@pytest.mark.asyncio
async def test_handler_parse_regular_article():
    fake_html = """
    <html><head><title>文章标题-微信公众号</title></head><body>
    <h2 id="activity-name"><span class="js_title_inner">干净的标题</span></h2>
    <div id="js_content" style="visibility: hidden;">
        <p>正文内容</p>
        <img data-src="https://mmbiz.qpic.cn/img1.jpg" src="" />
        <img data-src="https://mmbiz.qpic.cn/img2.jpg" src="" />
    </div>
    </body></html>
    """
    mock_resp = MagicMock()
    mock_resp.text = fake_html
    mock_resp.raise_for_status = MagicMock()

    with patch("parsers.url._wechat.httpx.get", return_value=mock_resp), \
         patch("parsers.url._wechat.extract_with_trafilatura", return_value="正文内容"):
        h = WechatHandler()
        result = await h.parse("https://mp.weixin.qq.com/s/abc123")

    assert result.title == "干净的标题"
    assert result.image_urls == ["https://mmbiz.qpic.cn/img1.jpg", "https://mmbiz.qpic.cn/img2.jpg"]
    assert "正文内容" in result.content


@pytest.mark.asyncio
async def test_handler_parse_image_message():
    fake_html = """
    <html><body>
    <div id="js_image_content">
        <h1 class="rich_media_title">图片消息标题</h1>
        <div id="js_image_desc">
            <p>图片描述</p>
            <img data-src="https://mmbiz.qpic.cn/desc1.jpg" src="" />
        </div>
    </div>
    </body></html>
    """
    mock_resp = MagicMock()
    mock_resp.text = fake_html
    mock_resp.raise_for_status = MagicMock()

    with patch("parsers.url._wechat.httpx.get", return_value=mock_resp), \
         patch("parsers.url._wechat.extract_with_trafilatura", return_value="图片描述"):
        h = WechatHandler()
        result = await h.parse("https://mp.weixin.qq.com/s/imgmsg456")

    assert result.title == "图片消息标题"
    assert "https://mmbiz.qpic.cn/desc1.jpg" in result.image_urls
    assert "图片描述" in result.content


@pytest.mark.asyncio
async def test_handler_falls_back_to_playwright_on_empty():
    empty_html = "<html><body><p>no article</p></body></html>"
    mock_resp = MagicMock()
    mock_resp.text = empty_html
    mock_resp.raise_for_status = MagicMock()

    pw_result = ParseResult(content="playwright content", images={}, title="PW Title")

    with patch("parsers.url._wechat.httpx.get", return_value=mock_resp), \
         patch.object(WechatHandler, "_parse_with_playwright", new_callable=AsyncMock,
                      return_value=pw_result):
        h = WechatHandler()
        result = await h.parse("https://mp.weixin.qq.com/s/fallback123")

    assert result.content == "playwright content"
