"""Feishu docx handler tests — mock playwright, no real network."""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from parsers.url._feishu import extract_feishu_image_urls, is_feishu_docx_url
from parsers.url._feishu import replace_image_urls_with_names
from parsers.url._feishu import download_images_in_context


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
