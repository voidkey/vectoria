"""Xiaohongshu handler contract tests.

Parse/playwright behaviour isn't covered here — selectors drift with
the real site and unit tests against a frozen fixture give false
confidence. What we can guard without a live environment:

  * match() claims xhs and xhslink domains, rejects lookalikes
  * canonicalize rewrites webp→jpg only on xhs CDN hosts
  * download_headers sets Referer to the note URL
  * Handler is registered before the Generic catch-all
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from limits.aio.storage import MemoryStorage

from infra import ratelimit
from parsers.url import download_images_for_url, find_handler
from parsers.url._xhs import (
    XhsHandler,
    canonicalize_xhs_image_url,
    get_xhs_headers,
    is_xhs_url,
)


@pytest.fixture(autouse=True)
def _fresh_limiter():
    ratelimit._reset_for_tests()
    ratelimit._set_storage_for_tests(MemoryStorage())
    yield
    ratelimit._reset_for_tests()


# ---------------------------------------------------------------------------
# match() domain coverage
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("url", [
    "https://www.xiaohongshu.com/explore/abc123",
    "https://xiaohongshu.com/discovery/item/xyz",
    "https://m.xiaohongshu.com/explore/abc",
    "https://xhslink.com/abc",   # shortener
    "http://xiaohongshu.com/foo",
])
def test_match_claims_xhs_urls(url):
    assert is_xhs_url(url)
    assert XhsHandler().match(url)


@pytest.mark.parametrize("url", [
    "https://example.com/xiaohongshu",        # lookalike path
    "https://xiaohongshu.fake.com/page",      # subdomain attack
    "https://xhscdn.com/img.jpg",             # image CDN is NOT the article
    "https://weibo.com/u/1",
])
def test_match_rejects_non_xhs(url):
    assert not is_xhs_url(url)
    assert not XhsHandler().match(url)


# ---------------------------------------------------------------------------
# canonicalize_image_url
# ---------------------------------------------------------------------------

def test_canonicalize_swaps_webp_for_jpg():
    url = (
        "https://sns-img-bd.xhscdn.com/abc123"
        "?imageView2/2/w/1080/format/webp"
    )
    out = canonicalize_xhs_image_url(url)
    assert "format/jpg" in out
    assert "format/webp" not in out


def test_canonicalize_preserves_quality_width_and_mode():
    """Only format/webp is rewritten; the rest of the Qiniu directive
    must survive or signed URLs break.
    """
    url = (
        "https://sns-img-bd.xhscdn.com/abc"
        "?imageView2/2/w/1080/q/75/format/webp|imageMogr2/strip"
    )
    out = canonicalize_xhs_image_url(url)
    assert "imageView2/2/w/1080/q/75/format/jpg" in out
    assert "imageMogr2/strip" in out


def test_canonicalize_noop_when_no_format_directive():
    url = "https://ci.xiaohongshu.com/notes-pre/abc"
    assert canonicalize_xhs_image_url(url) == url


def test_canonicalize_noop_for_non_xhs_cdn():
    """Must not touch URLs from other CDNs (mmbiz, pbs.twimg, etc.)."""
    assert canonicalize_xhs_image_url("https://mmbiz.qpic.cn/img") \
        == "https://mmbiz.qpic.cn/img"


def test_canonicalize_matches_all_known_xhs_cdn_subdomains():
    """Multiple subdomains serve xhs images; all must hit the rewrite."""
    for host in ("sns-img-bd.xhscdn.com", "sns-img-qc.xhscdn.com",
                 "ci.xiaohongshu.com"):
        url = f"https://{host}/foo?imageView2/format/webp"
        assert "format/jpg" in canonicalize_xhs_image_url(url), host


# ---------------------------------------------------------------------------
# download_headers
# ---------------------------------------------------------------------------

def test_download_headers_sets_referer_to_source():
    hdrs = get_xhs_headers("https://www.xiaohongshu.com/explore/abc")
    assert hdrs is not None
    assert hdrs["Referer"] == "https://www.xiaohongshu.com/explore/abc"
    assert "Mobile" in hdrs["User-Agent"]


def test_download_headers_none_for_non_xhs():
    assert get_xhs_headers("https://example.com/") is None


# ---------------------------------------------------------------------------
# Registration — handler dispatch order
# ---------------------------------------------------------------------------

def test_handler_registered_in_url_package():
    """find_handler() must pick XhsHandler for xhs URLs, not the
    Generic catch-all. Dispatch order in parsers/url/__init__.py puts
    platform handlers before Generic; this guards that ordering.
    """
    h = find_handler("https://www.xiaohongshu.com/explore/xyz")
    assert h is not None
    assert type(h).__name__ == "XhsHandler"


# ---------------------------------------------------------------------------
# Integration with download_images_for_url
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_download_images_for_url_applies_xhs_canonicalize():
    """Article URL points to xiaohongshu.com → handler resolves →
    image URL at xhscdn.com gets its format rewritten before fetch.
    """
    fetched: list[str] = []
    ok = MagicMock(status_code=200, content=b"x")
    client = MagicMock()

    async def _get(u, **kw):
        fetched.append(u)
        return ok
    client.get = _get
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)

    with patch("parsers.url._handlers.httpx.AsyncClient", return_value=client):
        result = await download_images_for_url(
            "https://www.xiaohongshu.com/explore/xyz",
            ["https://sns-img-bd.xhscdn.com/abc?imageView2/2/w/1080/format/webp"],
        )

    assert len(fetched) == 1
    assert "format/jpg" in fetched[0]
    assert "format/webp" not in fetched[0]
    # Dict keyed by original so markdown references still match.
    original = "https://sns-img-bd.xhscdn.com/abc?imageView2/2/w/1080/format/webp"
    assert original in result


@pytest.mark.asyncio
async def test_download_images_for_url_sends_xhs_referer():
    captured: dict = {}
    ok = MagicMock(status_code=200, content=b"x")
    client = MagicMock()
    client.get = AsyncMock(return_value=ok)
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)

    def _capture(*a, **kw):
        captured.update(kw)
        return client

    with patch("parsers.url._handlers.httpx.AsyncClient", side_effect=_capture):
        await download_images_for_url(
            "https://www.xiaohongshu.com/explore/xyz",
            ["https://ci.xiaohongshu.com/img.jpg"],
        )

    hdrs = captured.get("headers") or {}
    assert hdrs.get("Referer") == "https://www.xiaohongshu.com/explore/xyz"


# ---------------------------------------------------------------------------
# allow_image_only opt-in
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_xhs_success_sets_allow_image_only_true():
    """Structured-source handler opts into image_only rescue."""
    mock_page = AsyncMock()
    mock_page.goto = AsyncMock()
    mock_page.wait_for_timeout = AsyncMock()
    mock_page.evaluate = AsyncMock(return_value={
        "title": "note title",
        "body": "hello world",
        "imgs": ["https://sns-webpic-qc.xhscdn.com/a.jpg"],
    })

    mock_ctx = AsyncMock()
    mock_ctx.new_page = AsyncMock(return_value=mock_page)
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_ctx)
    mock_ctx.__aexit__ = AsyncMock(return_value=None)

    with patch("parsers.url._browser.parse_session", return_value=mock_ctx):
        h = XhsHandler()
        result = await h.parse("https://www.xiaohongshu.com/explore/abc")

    assert result.allow_image_only is True
