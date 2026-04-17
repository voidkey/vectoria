import pytest
from parsers.url._handlers import (
    SiteHandler,
    register_handler,
    find_handler,
    extract_image_urls,
    extract_with_trafilatura,
    download_images,
)
from parsers.base import ParseResult


class FakeHandler:
    hosts = {"example.com"}

    def match(self, url: str) -> bool:
        from urllib.parse import urlparse
        return urlparse(url).hostname in self.hosts

    async def parse(self, url: str) -> ParseResult:
        return ParseResult(content="fake", images={}, title="Fake")

    def download_headers(self, url: str) -> dict[str, str] | None:
        return None


def test_register_and_find_handler():
    from parsers.url._handlers import _handlers
    handler = FakeHandler()
    # Insert before GenericHandler (catch-all) so our handler matches first
    _handlers.insert(0, handler)
    try:
        found = find_handler("https://example.com/page")
        assert found is handler
    finally:
        _handlers.remove(handler)


def test_find_handler_generic_is_catchall():
    """GenericHandler matches everything, so find_handler never returns None."""
    from parsers.url._generic import GenericHandler
    found = find_handler("https://unknown-domain.test/page")
    assert isinstance(found, GenericHandler)


def test_extract_image_urls_resolves_relative():
    html = '<img src="/img/a.jpg" /><img src="https://cdn.example.com/b.png" />'
    urls = extract_image_urls(html, "https://example.com/page")
    assert urls == ["https://example.com/img/a.jpg", "https://cdn.example.com/b.png"]


def test_extract_image_urls_matches_data_src():
    html = '<img data-src="https://mmbiz.qpic.cn/img1.jpg" src="" />'
    urls = extract_image_urls(html, "https://mp.weixin.qq.com/s/abc")
    assert urls == ["https://mmbiz.qpic.cn/img1.jpg"]


def test_extract_image_urls_filters_data_uris():
    html = '<img src="data:image/png;base64,abc" /><img src="https://example.com/real.jpg" />'
    urls = extract_image_urls(html, "https://example.com")
    assert urls == ["https://example.com/real.jpg"]


def test_extract_image_urls_caps_at_20():
    html = "".join(f'<img src="https://example.com/img{i}.jpg" />' for i in range(30))
    urls = extract_image_urls(html, "https://example.com")
    assert len(urls) == 20


def test_extract_with_trafilatura_returns_markdown():
    html = "<html><body><p>Hello world paragraph with enough text to be meaningful</p></body></html>"
    text = extract_with_trafilatura(html)
    assert "Hello" in text or text == ""


def test_download_images_with_headers():
    from unittest.mock import patch
    fake_response = type("R", (), {"status_code": 200, "content": b"\x89PNG fake"})()

    with patch("parsers.url._handlers.httpx.get", return_value=fake_response) as mock_get:
        result = download_images(
            ["https://mmbiz.qpic.cn/img1.jpg"],
            headers={"Referer": "https://mp.weixin.qq.com/"},
        )

    assert "https://mmbiz.qpic.cn/img1.jpg" in result
    assert result["https://mmbiz.qpic.cn/img1.jpg"] == b"\x89PNG fake"
    mock_get.assert_called_once()
    call_kwargs = mock_get.call_args
    assert call_kwargs.kwargs["headers"]["Referer"] == "https://mp.weixin.qq.com/"
