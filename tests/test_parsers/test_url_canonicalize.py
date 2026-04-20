"""``canonicalize_via`` + WeChat image-URL canonicalization.

The platform extractor pattern for W3 rests on two claims guarded here:

  * Every handler MAY implement ``canonicalize_image_url(url)``; the
    absence of the method is not an error — callers use
    ``canonicalize_via(handler, url)`` which no-ops when missing.

  * ``download_images_for_url`` automatically threads the matching
    handler's canonicalization into ``download_images``. One call site
    line → correct headers + correct URL variant, no per-platform
    ceremony in worker/API code.
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from limits.aio.storage import MemoryStorage

from infra import ratelimit
from parsers.url import download_images_for_url
from parsers.url._handlers import canonicalize_via, find_handler
from parsers.url._wechat import (
    WechatHandler,
    canonicalize_wechat_image_url,
)


@pytest.fixture(autouse=True)
def _fresh_limiter():
    ratelimit._reset_for_tests()
    ratelimit._set_storage_for_tests(MemoryStorage())
    yield
    ratelimit._reset_for_tests()


# ---------------------------------------------------------------------------
# canonicalize_via: handler-absent / no-op cases
# ---------------------------------------------------------------------------

def test_canonicalize_via_returns_original_when_no_handler():
    assert canonicalize_via(None, "https://example.com/a.jpg") \
        == "https://example.com/a.jpg"


def test_canonicalize_via_noops_when_handler_lacks_method():
    class _Dummy:
        def match(self, _): return True
        async def parse(self, _): pass
        def download_headers(self, _): return None

    assert canonicalize_via(_Dummy(), "https://example.com/a.jpg") \
        == "https://example.com/a.jpg"


def test_canonicalize_via_swallows_exceptions():
    """A buggy canonicalize_image_url must never bring down the batch."""
    class _Exploder:
        def match(self, _): return True
        async def parse(self, _): pass
        def download_headers(self, _): return None
        def canonicalize_image_url(self, url):
            raise RuntimeError("boom")

    assert canonicalize_via(_Exploder(), "https://x.com/a.jpg") \
        == "https://x.com/a.jpg"


# ---------------------------------------------------------------------------
# WeChat canonicalization rules
# ---------------------------------------------------------------------------

def test_wechat_adds_wx_fmt_jpeg():
    out = canonicalize_wechat_image_url(
        "https://mmbiz.qpic.cn/mmbiz_jpg/abc/640",
    )
    assert "wx_fmt=jpeg" in out
    # Structural — must stay on the same host/path.
    assert out.startswith("https://mmbiz.qpic.cn/mmbiz_jpg/abc/640")


def test_wechat_preserves_existing_wx_fmt():
    url = "https://mmbiz.qpic.cn/mmbiz_jpg/abc/640?wx_fmt=png"
    assert canonicalize_wechat_image_url(url) == url


def test_wechat_handler_exposes_canonicalize_image_url():
    """The Protocol contract — handlers that want per-URL rewriting
    expose it as an instance method, checked via duck typing.
    """
    h = WechatHandler()
    assert hasattr(h, "canonicalize_image_url")
    out = h.canonicalize_image_url("https://mmbiz.qpic.cn/foo/640")
    assert "wx_fmt=jpeg" in out


def test_wechat_canonicalize_is_noop_for_other_hosts():
    """Only mmbiz.qpic.cn is rewritten. The article host
    (mp.weixin.qq.com) is not an image CDN; neither are CDNs belonging
    to other platforms.
    """
    assert canonicalize_wechat_image_url("https://mp.weixin.qq.com/a") \
        == "https://mp.weixin.qq.com/a"
    assert canonicalize_wechat_image_url("https://xhscdn.com/img.jpg") \
        == "https://xhscdn.com/img.jpg"


def test_wechat_canonicalize_preserves_other_query_params():
    """Existing query params (besides wx_fmt) must survive."""
    url = "https://mmbiz.qpic.cn/mmbiz_jpg/abc/640?tp=webp&wxfrom=5"
    out = canonicalize_wechat_image_url(url)
    assert "tp=webp" in out
    assert "wxfrom=5" in out
    assert "wx_fmt=jpeg" in out


# ---------------------------------------------------------------------------
# download_images_for_url integrates handler headers + canonicalization
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_download_images_for_url_applies_wechat_canonicalize():
    """End-to-end: article from mp.weixin.qq.com + image from
    mmbiz.qpic.cn → fetch URL must carry ``wx_fmt=jpeg`` even though
    the caller passed the plain URL.
    """
    fetched_urls: list[str] = []

    ok_resp = MagicMock(status_code=200, content=b"IMG")
    client = MagicMock()

    async def _get(u, **kw):
        fetched_urls.append(u)
        return ok_resp

    client.get = _get
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)

    with patch("parsers.url._handlers.httpx.AsyncClient", return_value=client):
        result = await download_images_for_url(
            "https://mp.weixin.qq.com/s/abc123",
            ["https://mmbiz.qpic.cn/mmbiz_jpg/xxx/640"],
        )

    assert len(fetched_urls) == 1
    assert "wx_fmt=jpeg" in fetched_urls[0], (
        f"fetch URL should be canonicalized; got {fetched_urls[0]}"
    )
    # But the *returned dict* keys by the ORIGINAL URL (markdown match).
    assert "https://mmbiz.qpic.cn/mmbiz_jpg/xxx/640" in result


@pytest.mark.asyncio
async def test_download_images_for_url_uses_handler_headers():
    """For a WeChat article, the handler's Referer + UA must be sent."""
    captured_kwargs: dict = {}

    ok_resp = MagicMock(status_code=200, content=b"x")
    client = MagicMock()
    client.get = AsyncMock(return_value=ok_resp)
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)

    def _capture(*args, **kwargs):
        captured_kwargs.update(kwargs)
        return client

    with patch("parsers.url._handlers.httpx.AsyncClient", side_effect=_capture):
        await download_images_for_url(
            "https://mp.weixin.qq.com/s/xxx",
            ["https://mmbiz.qpic.cn/pic/640"],
        )

    hdrs = captured_kwargs.get("headers") or {}
    assert hdrs.get("Referer") == "https://mp.weixin.qq.com/"
    assert "MicroMessenger" in (hdrs.get("User-Agent") or "")


@pytest.mark.asyncio
async def test_download_images_for_url_with_generic_source_no_canonicalize():
    """Non-platform URLs get their images fetched as-is. Generic
    handler has no canonicalize_image_url, so the fetch URL must
    equal the original.
    """
    fetched_urls: list[str] = []
    ok_resp = MagicMock(status_code=200, content=b"x")
    client = MagicMock()

    async def _get(u, **kw):
        fetched_urls.append(u)
        return ok_resp
    client.get = _get
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)

    with patch("parsers.url._handlers.httpx.AsyncClient", return_value=client):
        await download_images_for_url(
            "https://example.com/blog/post",
            ["https://example.com/fig.jpg"],
        )

    assert fetched_urls == ["https://example.com/fig.jpg"]
