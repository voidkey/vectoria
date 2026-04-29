"""Feishu docx handler tests — mock playwright, no real network."""
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from parsers.base import ParseResult
from parsers.url._feishu import (
    FeishuHandler,
    download_images_in_context,
    extract_feishu_image_urls,
    is_feishu_docx_url,
    replace_image_urls_with_names,
    sniff_image_mime,
)


def test_is_feishu_docx_url_docx_path():
    assert is_feishu_docx_url("https://whobotai.feishu.cn/docx/ON7udn213ozGYPx8USXcRtJunFc")


def test_is_feishu_docx_url_docx_with_query():
    assert is_feishu_docx_url(
        "https://whobotai.feishu.cn/docx/ON7udn213ozGYPx8USXcRtJunFc?ignore_wx_jump=1"
    )


def test_is_feishu_docx_url_wiki_path():
    assert is_feishu_docx_url("https://example.feishu.cn/wiki/ABCdef123")


def test_is_feishu_docx_url_rejects_sheets():
    assert not is_feishu_docx_url("https://example.feishu.cn/sheets/abc")


def test_is_feishu_docx_url_rejects_drive():
    assert not is_feishu_docx_url("https://example.feishu.cn/drive/folder/abc")


def test_is_feishu_docx_url_rejects_larksuite():
    # Overseas variant out of scope for this handler
    assert not is_feishu_docx_url("https://example.larksuite.com/docx/abc")


def test_is_feishu_docx_url_rejects_other_host():
    assert not is_feishu_docx_url("https://example.com/docx/abc")


def test_extract_feishu_image_urls_from_internal_drive_stream():
    html = """
    <div class="docx-content">
      <p>正文一段</p>
      <img src="https://internal-api-drive-stream.feishu.cn/space/api/box/stream/download/v2/cover/A/?mount_point=docx_image"/>
      <p>正文二段</p>
      <img src="https://internal-api-drive-stream.feishu.cn/space/api/box/stream/download/v2/cover/B/?mount_point=docx_image"/>
    </div>
    """
    urls = extract_feishu_image_urls(html)
    assert urls == [
        "https://internal-api-drive-stream.feishu.cn/space/api/box/stream/download/v2/cover/A/?mount_point=docx_image",
        "https://internal-api-drive-stream.feishu.cn/space/api/box/stream/download/v2/cover/B/?mount_point=docx_image",
    ]


def test_extract_feishu_image_urls_dedup_keeps_first():
    html = """
    <img src="https://internal-api-drive-stream.feishu.cn/x/A/?p=1"/>
    <img src="https://internal-api-drive-stream.feishu.cn/x/A/?p=1"/>
    """
    urls = extract_feishu_image_urls(html)
    assert urls == ["https://internal-api-drive-stream.feishu.cn/x/A/?p=1"]


def test_extract_feishu_image_urls_skips_data_uri():
    html = '<img src="data:image/png;base64,abc"/>' \
           '<img src="https://internal-api-drive-stream.feishu.cn/x/B/?"/>'
    urls = extract_feishu_image_urls(html)
    assert urls == ["https://internal-api-drive-stream.feishu.cn/x/B/?"]


def test_extract_feishu_image_urls_skips_unrelated_hosts():
    """Avatars, emoji, share-card thumbnails on s1-imfile.feishucdn.com or
    sf-static.feishucdn.com aren't doc images — keep only main-content
    drive-stream URLs to avoid polluting the doc with chrome assets.
    """
    html = """
    <img src="https://s1-imfile.feishucdn.com/avatar/1.png"/>
    <img src="https://internal-api-drive-stream.feishu.cn/x/REAL/?"/>
    <img src="https://sf-static.feishucdn.com/icon.png"/>
    """
    urls = extract_feishu_image_urls(html)
    assert urls == ["https://internal-api-drive-stream.feishu.cn/x/REAL/?"]


def test_replace_image_urls_with_names_two_images():
    md = (
        "Header\n\n"
        "![](https://internal-api-drive-stream.feishu.cn/x/A/?p=1)\n\n"
        "Middle\n\n"
        "![](https://internal-api-drive-stream.feishu.cn/x/B/?p=2)\n"
    )
    urls = [
        "https://internal-api-drive-stream.feishu.cn/x/A/?p=1",
        "https://internal-api-drive-stream.feishu.cn/x/B/?p=2",
    ]
    names = ["image_0001.jpg", "image_0002.jpg"]
    out = replace_image_urls_with_names(md, urls, names)
    assert "![](image_0001.jpg)" in out
    assert "![](image_0002.jpg)" in out
    assert "internal-api-drive-stream" not in out


def test_replace_image_urls_with_names_preserves_alt_text():
    md = "![描述](https://internal-api-drive-stream.feishu.cn/x/A/?p=1)"
    out = replace_image_urls_with_names(
        md,
        ["https://internal-api-drive-stream.feishu.cn/x/A/?p=1"],
        ["image_0001.jpg"],
    )
    assert out == "![描述](image_0001.jpg)"


def test_replace_image_urls_with_names_url_not_in_md_no_op():
    """If a URL was extracted from raw HTML but trafilatura dropped it
    from the markdown (rare — happens for figure captions), skip the
    replacement quietly. Don't emit a stray placeholder.
    """
    md = "Body without that image"
    out = replace_image_urls_with_names(
        md,
        ["https://internal-api-drive-stream.feishu.cn/x/A/?"],
        ["image_0001.jpg"],
    )
    assert out == "Body without that image"


@pytest.mark.asyncio
async def test_download_images_in_context_returns_bytes_per_url():
    """One request per URL, results keyed by URL, Referer set to doc URL."""
    resp_a = MagicMock(); resp_a.ok = True; resp_a.status = 200
    resp_a.body = AsyncMock(return_value=b"AAA")
    resp_b = MagicMock(); resp_b.ok = True; resp_b.status = 200
    resp_b.body = AsyncMock(return_value=b"BBB")

    request = MagicMock()
    request.get = AsyncMock(side_effect=[resp_a, resp_b])
    ctx = MagicMock(); ctx.request = request

    out = await download_images_in_context(
        ctx,
        ["https://x/A", "https://x/B"],
        doc_url="https://whobotai.feishu.cn/docx/Z",
    )

    assert out == {"https://x/A": b"AAA", "https://x/B": b"BBB"}
    # Both calls receive a Referer pointing back to the doc
    for call in request.get.await_args_list:
        assert call.kwargs["headers"]["Referer"] == "https://whobotai.feishu.cn/docx/Z"


@pytest.mark.asyncio
async def test_download_images_in_context_skips_non_200():
    resp = MagicMock(); resp.ok = False; resp.status = 401
    resp.body = AsyncMock(return_value=b"err")

    request = MagicMock(); request.get = AsyncMock(return_value=resp)
    ctx = MagicMock(); ctx.request = request

    out = await download_images_in_context(
        ctx, ["https://x/A"], doc_url="https://whobotai.feishu.cn/docx/Z",
    )
    assert out == {}


@pytest.mark.asyncio
async def test_download_images_in_context_swallows_exceptions():
    request = MagicMock(); request.get = AsyncMock(side_effect=RuntimeError("net"))
    ctx = MagicMock(); ctx.request = request

    out = await download_images_in_context(
        ctx, ["https://x/A"], doc_url="https://whobotai.feishu.cn/docx/Z",
    )
    assert out == {}


@pytest.mark.asyncio
async def test_download_images_in_context_skips_empty_body():
    """200 OK with empty bytes (rare CDN rate-limit signal) is dropped."""
    resp = MagicMock(); resp.ok = True; resp.status = 200
    resp.body = AsyncMock(return_value=b"")

    request = MagicMock(); request.get = AsyncMock(return_value=resp)
    ctx = MagicMock(); ctx.request = request

    out = await download_images_in_context(
        ctx, ["https://x/A"], doc_url="https://whobotai.feishu.cn/docx/Z",
    )
    assert out == {}


def test_sniff_image_mime_png():
    assert sniff_image_mime(b"\x89PNG\r\n\x1a\nrest") == "image/png"


def test_sniff_image_mime_jpeg():
    assert sniff_image_mime(b"\xff\xd8\xff\xe0\x00\x10JFIF") == "image/jpeg"


def test_sniff_image_mime_webp():
    assert sniff_image_mime(b"RIFF\x00\x00\x00\x00WEBPVP8 ") == "image/webp"


def test_sniff_image_mime_gif():
    assert sniff_image_mime(b"GIF89a\x00\x00") == "image/gif"


def test_sniff_image_mime_unknown_falls_back_to_jpeg():
    assert sniff_image_mime(b"\x00\x00\x00\x00") == "image/jpeg"


def test_handler_match_docx():
    h = FeishuHandler()
    assert h.match("https://x.feishu.cn/docx/abc")


def test_handler_match_wiki():
    h = FeishuHandler()
    assert h.match("https://x.feishu.cn/wiki/abc")


def test_handler_match_other_path_false():
    h = FeishuHandler()
    assert not h.match("https://x.feishu.cn/sheets/abc")


def test_handler_match_other_host_false():
    h = FeishuHandler()
    assert not h.match("https://example.com/docx/abc")


def test_handler_download_headers_returns_none():
    """Bare httpx fetches will 401 anyway; we don't promise headers
    that would make them work, so the protocol's None branch fires."""
    h = FeishuHandler()
    assert h.download_headers("https://x.feishu.cn/docx/abc") is None


def _make_ctx_mock(*, page_html: str, page_url: str, image_payloads: dict[str, bytes]):
    """Build a minimal BrowserContext mock that returns canned HTML on
    ``page.content()``, canned URL on ``page.url``, and per-URL bytes via
    ``context.request.get(url).body()``.
    """
    page = MagicMock()
    page.goto = AsyncMock()
    page.wait_for_timeout = AsyncMock()
    page.evaluate = AsyncMock(return_value=None)
    page.content = AsyncMock(return_value=page_html)
    type(page).url = page_url  # ``page.url`` is a property in real PW

    async def _request_get(url, headers=None):
        resp = MagicMock()
        if url in image_payloads:
            resp.ok = True
            resp.status = 200
            resp.body = AsyncMock(return_value=image_payloads[url])
        else:
            resp.ok = False
            resp.status = 404
            resp.body = AsyncMock(return_value=b"")
            resp.dispose = AsyncMock()
        return resp

    request = MagicMock(); request.get = _request_get
    ctx = MagicMock(); ctx.request = request
    ctx.new_page = AsyncMock(return_value=page)
    return ctx, page


def _patch_parse_session(ctx_mock):
    @asynccontextmanager
    async def _fake_session(**kw):
        yield ctx_mock
    return patch("parsers.url._feishu.parse_session", _fake_session)


@pytest.mark.asyncio
async def test_parse_returns_inline_image_refs():
    img_a_url = "https://internal-api-drive-stream.feishu.cn/x/A/?p=1"
    img_b_url = "https://internal-api-drive-stream.feishu.cn/x/B/?p=2"
    page_html = f"""
    <html><head><title>WhobotAI 文档</title></head><body>
      <div class="docx-content">
        <h1>标题</h1><p>正文段落一</p>
        <img src="{img_a_url}"/>
        <p>正文段落二</p>
        <img src="{img_b_url}"/>
      </div>
    </body></html>
    """
    ctx, page = _make_ctx_mock(
        page_html=page_html,
        page_url="https://whobotai.feishu.cn/docx/ABC",
        image_payloads={
            img_a_url: b"\x89PNG\r\n\x1a\nA",
            img_b_url: b"\xff\xd8\xff\xe0B",
        },
    )

    h = FeishuHandler()
    with _patch_parse_session(ctx):
        result = await h.parse("https://whobotai.feishu.cn/docx/ABC")

    assert isinstance(result, ParseResult)
    assert result.title == "WhobotAI 文档"
    assert "正文段落一" in result.content
    # image_urls path is NOT used; refs go inline so worker takes the
    # has_inline_images branch.
    assert result.image_urls in (None, [])
    assert len(result.image_refs) == 2

    refs = sorted(result.image_refs, key=lambda r: r.name)
    assert refs[0].name == "image_0001.png"
    assert refs[0].mime == "image/png"
    assert refs[0].materialize() == b"\x89PNG\r\n\x1a\nA"
    assert refs[1].name == "image_0002.jpg"
    assert refs[1].mime == "image/jpeg"

    # Markdown should reference the placeholder names, not the original
    # URLs — required for image_metadata.extract_metadata_into_refs.
    assert "image_0001.png" in result.content
    assert "image_0002.jpg" in result.content
    assert "internal-api-drive-stream" not in result.content


@pytest.mark.asyncio
async def test_parse_login_redirect_raises_permanent_parse_error():
    """Non-public docs redirect to accounts.feishu.cn after page.goto.
    The handler raises PermanentParseError so the worker fails the doc
    immediately without 3× retry — login won't be added between attempts.
    """
    from parsers.base import PermanentParseError

    ctx, page = _make_ctx_mock(
        page_html="<html><body>login form</body></html>",
        page_url="https://accounts.feishu.cn/accounts/page/login?app_id=2&redirect_uri=...",
        image_payloads={},
    )
    h = FeishuHandler()
    with _patch_parse_session(ctx):
        with pytest.raises(PermanentParseError):
            await h.parse("https://whobotai.feishu.cn/docx/PRIVATE")


@pytest.mark.asyncio
async def test_parse_zero_images_still_returns_text():
    page_html = """
    <html><head><title>纯文本文档</title></head><body>
      <div class="docx-content"><p>只有正文，无图。</p></div>
    </body></html>
    """
    ctx, page = _make_ctx_mock(
        page_html=page_html,
        page_url="https://whobotai.feishu.cn/docx/TEXT",
        image_payloads={},
    )
    h = FeishuHandler()
    with _patch_parse_session(ctx):
        result = await h.parse("https://whobotai.feishu.cn/docx/TEXT")

    assert "只有正文" in result.content
    assert result.image_refs == []


@pytest.mark.asyncio
async def test_parse_caps_at_url_image_cap(monkeypatch):
    """Doc with > cap images: only ``cap`` refs returned, truncation
    metric incremented exactly once.
    """
    from infra import metrics
    monkeypatch.setattr("parsers.url._feishu.get_settings",
                        lambda: type("S", (), {"url_image_cap": 2})())

    img_urls = [
        f"https://internal-api-drive-stream.feishu.cn/x/{i}/?p=1" for i in "ABC"
    ]
    page_html = "<html><body><div class='docx-content'>" + "".join(
        f'<img src="{u}"/>' for u in img_urls
    ) + "</div></body></html>"
    ctx, page = _make_ctx_mock(
        page_html=page_html,
        page_url="https://whobotai.feishu.cn/docx/MANY",
        image_payloads={u: b"\xff\xd8\xff" for u in img_urls},
    )

    counter = metrics.URL_IMAGES_TRUNCATED_TOTAL.labels(handler="feishu")
    before = counter._value.get()
    h = FeishuHandler()
    with _patch_parse_session(ctx):
        result = await h.parse("https://whobotai.feishu.cn/docx/MANY")

    assert len(result.image_refs) == 2
    assert counter._value.get() == before + 1
