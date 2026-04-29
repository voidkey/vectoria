"""BlacklistHandler — short-circuit known-unparseable URL patterns
*before* the catch-all GenericHandler burns 30s of playwright startup
on something we already know will fail (anti-bot, JS-only video
players, login walls).

Guards against regressions where a future refactor either:
  * registers BlacklistHandler in the wrong order (after Generic — dead code)
  * silently swallows the rejection into an empty ParseResult (caller
    can't distinguish "rejected by policy" from "page genuinely empty")
"""
from __future__ import annotations

import pytest

from parsers.url._blacklist import BlacklistHandler, UnparseableUrlError


def test_match_bilibili_video():
    h = BlacklistHandler()
    assert h.match("https://www.bilibili.com/video/BV1FN4y1q7eN")
    assert h.match("https://bilibili.com/video/BV1abc")
    # subdomain still matches via endswith
    assert h.match("https://m.bilibili.com/video/BV1xyz")


def test_match_bilibili_short_link():
    h = BlacklistHandler()
    assert h.match("https://b23.tv/abc123")


def test_match_other_video_platforms():
    h = BlacklistHandler()
    assert h.match("https://www.douyin.com/video/7123456789")
    assert h.match("https://v.douyin.com/abc")
    assert h.match("https://www.tiktok.com/@user/video/7123")
    assert h.match("https://www.iqiyi.com/v_xyz123.html")
    assert h.match("https://v.youku.com/v_show/id_abc.html")
    assert h.match("https://www.ixigua.com/7123456")


def test_match_larksuite_overseas():
    """Overseas Lark (larksuite.com) shares feishu's login-wall behavior
    but the FeishuHandler is scoped to ``*.feishu.cn``. Without a
    blacklist entry, larksuite URLs fall through to GenericHandler and
    burn 30s of playwright before failing on the login redirect.
    """
    h = BlacklistHandler()
    assert h.match("https://example.larksuite.com/docx/abc")
    assert h.match("https://larksuite.com/anything")
    # subdomain still matches
    assert h.match("https://www.larksuite.com/wiki/xyz")


def test_does_not_match_legitimate_urls():
    """The blacklist must be narrow — false positives would block
    legitimate sources. Bilibili's main page (not /video/), wechat
    articles, and arbitrary blogs all pass through to the right
    downstream handler."""
    h = BlacklistHandler()
    # bilibili main / search / user pages are not video-player pages
    assert not h.match("https://www.bilibili.com/")
    assert not h.match("https://www.bilibili.com/search?keyword=foo")
    # Other platforms entirely
    assert not h.match("https://mp.weixin.qq.com/s/abcdef")
    assert not h.match("https://example.com/article/123")
    assert not h.match("https://x.com/handle/status/1")


def test_does_not_match_subpaths_outside_pattern():
    """douyin.com/discover should be allowed; only /video/ pattern is
    blacklisted. Path regex is anchored, host alone isn't enough."""
    h = BlacklistHandler()
    assert not h.match("https://www.douyin.com/discover")


@pytest.mark.asyncio
async def test_parse_raises_with_reason():
    """match() returning True means parse() must raise UnparseableUrlError
    with a human-readable reason — that string lands in
    documents.error_msg so users / operators see *why* the URL was
    rejected without having to grep code or alerts.
    """
    h = BlacklistHandler()
    with pytest.raises(UnparseableUrlError) as excinfo:
        await h.parse("https://www.bilibili.com/video/BV1FN4y1q7eN")
    msg = str(excinfo.value)
    assert "bilibili" in msg.lower()
    assert "anti-bot" in msg or "not supported" in msg


def test_download_headers_returns_none():
    """The blacklist handler never fetches anything — image download
    helpers shouldn't even reach here, but if they did, returning
    None means "no special headers" and the caller skips."""
    h = BlacklistHandler()
    assert h.download_headers("https://www.bilibili.com/video/BV1") is None


def test_handler_registration_order_blacklist_before_generic():
    """End-to-end registration check: importing the package wires
    handlers in priority order. BlacklistHandler must come before
    GenericHandler — otherwise the catch-all swallows everything and
    the blacklist becomes dead code.
    """
    from parsers.url import find_handler
    from parsers.url._blacklist import BlacklistHandler as _BH
    from parsers.url._generic import GenericHandler as _GH

    h = find_handler("https://www.bilibili.com/video/BV1FN4y1q7eN")
    assert isinstance(h, _BH), (
        f"expected BlacklistHandler to claim bilibili video URLs, "
        f"got {type(h).__name__}"
    )
    # Sanity: a non-blacklisted URL still routes to a real handler
    # (wechat / generic / etc.) — blacklist isn't accidentally a
    # broader-than-intended catch.
    h_other = find_handler("https://example.com/article")
    assert not isinstance(h_other, _BH)
    assert isinstance(h_other, _GH)
