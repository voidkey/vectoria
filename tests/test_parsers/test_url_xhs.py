"""Xiaohongshu handler contract tests.

In addition to the domain-match / canonicalize / headers contracts,
this module covers the HTML→fields extractor (``extract_xhs_from_html``)
with hand-crafted DOM snippets that mirror what we observed live:

  * "new" video-note layout — empty og:*, .author-desc holds the body,
    <title> holds the title, .new-note-view-container wraps everything
  * "legacy" image-note layout — #detail-title / #detail-desc / og:*
    Still present on some accounts as of fixture capture

Selectors will drift again. The defence here is layered fallbacks
(SSR meta tags first, then post-hydration DOM nodes) plus an
integration check on real URLs at deploy time, not unit-test purity.
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from limits.aio.storage import MemoryStorage

from infra import ratelimit
from parsers.url import download_images_for_url, find_handler
from parsers.base import AntiBotBlockedError
from parsers.url._blacklist import UnparseableUrlError
from parsers.url._xhs import (
    XhsHandler,
    canonicalize_xhs_image_url,
    extract_xhs_from_html,
    get_xhs_headers,
    is_xhs_url,
    is_xhs_video_note,
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
# extract_xhs_from_html — HTML→fields extractor
# ---------------------------------------------------------------------------
# Hand-crafted DOM snippets below mirror the live structures we
# captured on 2026-05-25 from xhslink.com short links. See module
# docstring for the layered-fallback rationale.

_NEW_VIDEO_NOTE = """
<html>
  <head>
    <title>世界上最好的贵人就是执行力超强的自己 - 小红书</title>
    <meta name="description" content="#胡楚靓 #行动大于一切 @胡楚靓 @热点薯">
  </head>
  <body>
    <div class="new-note-view-container">
      <div class="author-username">胡言楚语靓</div>
      <div class="author-desc-wrapper">
        <div class="reds-text fs15 author-desc">
          <i class="author-desc-trigger">展开</i>
          世界上最好的贵人就是执行力超强的自己 @胡楚靓 #胡楚靓
        </div>
      </div>
      <div class="comment-text-box">
        <div class="reds-text fs14 line-clamp-2 comment-text-box exp1">
          这是评论区里某条评论 不该被当成正文
        </div>
      </div>
      <img src="https://sns-webpic-qc.xhscdn.com/note0.jpg?imageView2/2/w/1080/format/jpg" />
      <img data-src="https://ci.xiaohongshu.com/note1.jpg" />
      <img src="https://sns-avatar-qc.xhscdn.com/avatar.jpg" />
      <img src="https://example.com/external.png" />
    </div>
  </body>
</html>
"""

_LEGACY_IMAGE_NOTE = """
<html>
  <head>
    <title>旧版图文笔记 - 小红书</title>
    <meta property="og:title" content="旧版图文笔记标题">
    <meta property="og:description" content="旧版图文 og 描述">
  </head>
  <body>
    <h1 id="detail-title">旧版图文笔记</h1>
    <div id="detail-desc">这是旧版图文笔记的正文段落。</div>
    <img src="https://ci.xiaohongshu.com/legacy.jpg" />
  </body>
</html>
"""


def test_extract_title_strips_xhs_suffix():
    out = extract_xhs_from_html(_NEW_VIDEO_NOTE, "https://www.xiaohongshu.com/x", 12)
    assert out["title"] == "世界上最好的贵人就是执行力超强的自己"


def test_extract_body_uses_meta_description_when_present():
    """SSR-rendered <meta name='description'> is the most stable source —
    it survives hydration races and DOM refactors. Prefer it when present.
    """
    out = extract_xhs_from_html(_NEW_VIDEO_NOTE, "https://www.xiaohongshu.com/x", 12)
    assert "#胡楚靓" in out["body"]
    assert "#行动大于一切" in out["body"]
    # Comment text must NOT leak in
    assert "这是评论区里某条评论" not in out["body"]


def test_extract_body_falls_back_to_author_desc_when_meta_missing():
    """When SSR meta is empty (some account types) the .author-desc node
    on the new video DOM holds the same content."""
    html = _NEW_VIDEO_NOTE.replace(
        '<meta name="description" content="#胡楚靓 #行动大于一切 @胡楚靓 @热点薯">',
        "",
    )
    out = extract_xhs_from_html(html, "https://www.xiaohongshu.com/x", 12)
    assert "世界上最好的贵人就是执行力超强的自己" in out["body"]
    # The "展开" UI control must be stripped, not folded into body
    assert not out["body"].startswith("展开")
    # Comment text from sibling div must NOT leak in
    assert "这是评论区里某条评论" not in out["body"]


def test_extract_supports_legacy_image_note_selectors():
    """Old image-note layouts (#detail-title / #detail-desc) still appear
    on some accounts; keep them as a fallback."""
    out = extract_xhs_from_html(_LEGACY_IMAGE_NOTE, "https://www.xiaohongshu.com/x", 12)
    assert out["title"] == "旧版图文笔记"
    assert "这是旧版图文笔记的正文段落" in out["body"]


def test_extract_title_falls_back_when_title_tag_missing():
    """If <title> is absent, og:title is next; then url netloc as last
    resort (handler-level)."""
    html = """
    <html><head>
      <meta property="og:title" content="只有 og:title 的笔记">
    </head><body></body></html>
    """
    out = extract_xhs_from_html(html, "https://www.xiaohongshu.com/x", 12)
    assert out["title"] == "只有 og:title 的笔记"


def test_extract_imgs_filters_avatars_and_non_xhs_hosts():
    out = extract_xhs_from_html(_NEW_VIDEO_NOTE, "https://www.xiaohongshu.com/x", 12)
    imgs = out["imgs"]
    # xhs CDN images kept
    assert any("note0.jpg" in u for u in imgs)
    assert any("note1.jpg" in u for u in imgs)
    # avatars / external hosts dropped
    assert not any("avatar" in u for u in imgs)
    assert not any("example.com" in u for u in imgs)


def test_extract_imgs_dedupes_and_caps():
    cap = 2
    html = """
    <html><body>
      <img src="https://ci.xiaohongshu.com/a.jpg">
      <img src="https://ci.xiaohongshu.com/a.jpg">
      <img src="https://ci.xiaohongshu.com/b.jpg">
      <img src="https://ci.xiaohongshu.com/c.jpg">
    </body></html>
    """
    out = extract_xhs_from_html(html, "https://www.xiaohongshu.com/x", cap)
    assert len(out["imgs"]) == cap
    assert out["imgs"][0].endswith("a.jpg")
    assert out["imgs"][1].endswith("b.jpg")


def test_extract_handles_empty_dom():
    out = extract_xhs_from_html(
        "<html><head></head><body></body></html>",
        "https://www.xiaohongshu.com/x", 12,
    )
    assert out["title"] == ""
    assert out["body"] == ""
    assert out["imgs"] == []


# ---------------------------------------------------------------------------
# Video-note detection — skip until we have a real video pipeline
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("url", [
    # Live samples captured 2026-05-25 from xhslink.com share redirects.
    "https://www.xiaohongshu.com/discovery/item/abc?type=video&xsec_token=foo",
    "https://www.xiaohongshu.com/discovery/item/abc?app_platform=ios&type=video&xsec_source=app_share",
])
def test_is_xhs_video_note_detects_type_video(url):
    assert is_xhs_video_note(url)


@pytest.mark.parametrize("url", [
    # Image notes either omit ``type`` or set it to something other than ``video``
    "https://www.xiaohongshu.com/discovery/item/abc?xsec_token=foo",
    "https://www.xiaohongshu.com/discovery/item/abc?type=normal&xsec_token=foo",
    "https://www.xiaohongshu.com/explore/abc",
    # ``xsec_source`` contains the substring "video" — must not false-match
    "https://www.xiaohongshu.com/discovery/item/abc?xsec_source=videofeed",
])
def test_is_xhs_video_note_rejects_non_video(url):
    assert not is_xhs_video_note(url)


# ---------------------------------------------------------------------------
# allow_image_only opt-in (image notes only)
# ---------------------------------------------------------------------------

def _xhs_browser_mock(final_url: str, html: str):
    """Wire up a parse_session mock that yields a page whose ``url`` is
    ``final_url`` (post-redirect) and whose ``content()`` returns ``html``.
    """
    mock_page = AsyncMock()
    mock_page.goto = AsyncMock()
    mock_page.wait_for_timeout = AsyncMock()
    mock_page.content = AsyncMock(return_value=html)
    # ``page.url`` is a property in the real API but AsyncMock treats
    # attribute access as a sync value, which is what we want here.
    mock_page.url = final_url

    mock_ctx = AsyncMock()
    mock_ctx.new_page = AsyncMock(return_value=mock_page)
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_ctx)
    mock_ctx.__aexit__ = AsyncMock(return_value=None)
    return mock_ctx


@pytest.mark.asyncio
async def test_xhs_image_note_returns_parse_result():
    """Image-note final URL (no type=video) still flows through extraction."""
    mock_ctx = _xhs_browser_mock(
        final_url="https://www.xiaohongshu.com/discovery/item/abc?xsec_token=t",
        html=_NEW_VIDEO_NOTE,  # the DOM here is fine; what matters is the URL
    )
    with patch("parsers.url._browser.parse_session", return_value=mock_ctx):
        result = await XhsHandler().parse("https://www.xiaohongshu.com/explore/abc")

    assert result.allow_image_only is True
    assert result.title == "世界上最好的贵人就是执行力超强的自己"
    assert "#胡楚靓" in result.content
    assert any("xhscdn.com" in u or "xiaohongshu.com" in u
               for u in (result.image_urls or []))


@pytest.mark.asyncio
async def test_xhs_video_note_raises_unparseable():
    """Video notes have only hashtag-heavy captions — skip until a real
    video pipeline (ASR + frame OCR) lands. UnparseableUrlError →
    worker marks status=failed, image pipeline not enqueued, no retry.
    """
    mock_ctx = _xhs_browser_mock(
        final_url="https://www.xiaohongshu.com/discovery/item/abc?type=video&xsec_token=t",
        html=_NEW_VIDEO_NOTE,
    )
    with patch("parsers.url._browser.parse_session", return_value=mock_ctx):
        with pytest.raises(UnparseableUrlError) as exc:
            await XhsHandler().parse("http://xhslink.com/o/abc")
    # Message must mention the reason — surfaces in documents.error_msg.
    msg = str(exc.value).lower()
    assert "video" in msg


# ---------------------------------------------------------------------------
# Block-page detection
# ---------------------------------------------------------------------------

_VERIFICATION_PAGE_HTML = """
<html>
  <head>
    <title>安全验证</title>
  </head>
  <body>
    <div class="verify-container">
      <p>请完成下方验证</p>
      <div id="captcha"></div>
    </div>
  </body>
</html>
"""


@pytest.mark.asyncio
async def test_xhs_verification_page_raises_antibot_blocked():
    """When the post-render page is an anti-bot verification wall
    (title '安全验证' / body '请完成下方验证'), AntiBotBlockedError is
    raised instead of storing the verification page as content.
    AntiBotBlockedError is a PermanentParseError → worker short-circuits,
    no retry, no dead-task noise.
    """
    mock_ctx = _xhs_browser_mock(
        final_url="https://www.xiaohongshu.com/explore/abc",
        html=_VERIFICATION_PAGE_HTML,
    )
    with patch("parsers.url._browser.parse_session", return_value=mock_ctx):
        with pytest.raises(AntiBotBlockedError) as exc:
            await XhsHandler().parse("https://www.xiaohongshu.com/explore/abc")
    assert "安全验证" in str(exc.value) or "anti-bot" in str(exc.value).lower()
