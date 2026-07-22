import io
import json
import zipfile

import pytest

from parsers.capture._media import (
    cap_items,
    catalog_assets,
    dedupe_srcset_variants,
    lottie_json_from_bytes,
    lottie_manifest_entry,
    make_video_response_handler,
    merge_video_manifest,
    rasterize_svgs,
    render_lottie_previews,
    video_descriptors,
)


# ── Phase 8 lottie helpers ──────────────────────────────────────────────────
def _lottie(nm="Anim", w=200, h=100, fr=30, ip=0, op=60, layers=None):
    return {"v": "5.7.4", "ip": ip, "op": op, "w": w, "h": h, "fr": fr, "nm": nm,
            "layers": layers if layers is not None else [{"ty": 4}, {"ty": 4}]}


def _dotlottie(anim: dict, path="animations/data.json") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("manifest.json", json.dumps({"animations": [{"id": "data"}]}))
        zf.writestr(path, json.dumps(anim))
    return buf.getvalue()


def test_lottie_json_from_plain_json():
    anim = _lottie()
    data = json.dumps(anim).encode()
    out = lottie_json_from_bytes("https://x/a.json", data)
    assert out is not None
    body, parsed = out
    assert parsed["nm"] == "Anim"
    assert json.loads(body)["w"] == 200


def test_lottie_json_from_dotlottie_zip_v1():
    zip_bytes = _dotlottie(_lottie(nm="Zipped"), path="animations/data.json")
    assert zip_bytes[:4] == b"PK\x03\x04"
    out = lottie_json_from_bytes("https://x/a.lottie", zip_bytes)
    assert out is not None
    _body, parsed = out
    assert parsed["nm"] == "Zipped"


def test_lottie_json_from_dotlottie_zip_v2_a_path():
    zip_bytes = _dotlottie(_lottie(nm="V2"), path="a/data.json")
    out = lottie_json_from_bytes("https://x/a.lottie", zip_bytes)
    assert out is not None
    assert out[1]["nm"] == "V2"


def test_lottie_json_rejects_invalid_structure():
    # Missing the core lottie keys (v/ip/op/layers/w/h/fr) → not a lottie.
    data = json.dumps({"hello": "world", "layers": []}).encode()
    assert lottie_json_from_bytes("https://x/a.json", data) is None


def test_lottie_json_rejects_non_json():
    assert lottie_json_from_bytes("https://x/a.json", b"not json at all") is None


def test_lottie_json_dotlottie_without_animation_json_returns_none():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("manifest.json", "{}")
    assert lottie_json_from_bytes("https://x/a.lottie", buf.getvalue()) is None


def test_lottie_json_rejects_zip_bomb_member_over_uncompressed_cap():
    # A crafted dotLottie whose animation member decompresses far past the cap must
    # be skipped BEFORE zf.read() (guarding a zip-decompression bomb): the compressed
    # body is small, but ZipInfo.file_size (central directory) exceeds max_uncompressed.
    big_anim = _lottie(layers=[{"ty": 4, "pad": "x" * 200_000}])  # highly compressible
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("manifest.json", json.dumps({"animations": [{"id": "data"}]}))
        zf.writestr("animations/data.json", json.dumps(big_anim))
    zip_bytes = buf.getvalue()
    # Compressed body is tiny, but the member's declared uncompressed size is huge.
    assert len(zip_bytes) < 4096
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        assert zf.getinfo("animations/data.json").file_size > 4096
    # Cap set below the member's uncompressed size -> rejected (no member valid),
    # and the guard fires from ZipInfo.file_size without decompressing.
    assert lottie_json_from_bytes(
        "https://x/a.lottie", zip_bytes, max_uncompressed=4096) is None
    # A normal small lottie still parses with the same tiny cap.
    small = _dotlottie(_lottie(nm="Small"), path="animations/data.json")
    out = lottie_json_from_bytes("https://x/a.lottie", small, max_uncompressed=4096)
    assert out is not None and out[1]["nm"] == "Small"


def test_lottie_manifest_entry_shape():
    entry = lottie_manifest_entry("animation-0.json", "https://x/a.json",
                                  _lottie(nm="Hero", w=400, h=300, fr=30, ip=0, op=60))
    assert entry["file"] == "assets/lottie/animation-0.json"
    assert "url" not in entry           # reference entry has no url — 1:1 key set
    assert entry["name"] == "Hero"
    assert entry["width"] == 400 and entry["height"] == 300
    assert entry["frameRate"] == 30
    assert entry["duration"] == 2.0     # (60-0)/30
    assert entry["layers"] == 2
    assert set(entry) == {"file", "name", "width", "height",
                          "duration", "frameRate", "layers"}


def test_dedupe_keeps_highest_width_variant():
    items = [
        {"url": "https://x.com/_next/image?url=a&w=640&q=75", "type": "Image", "contexts": ["img[srcset]"]},
        {"url": "https://x.com/_next/image?url=a&w=1920&q=75", "type": "Image", "contexts": ["img[src]"]},
        {"url": "https://x.com/_next/image?url=a&w=1080&q=75", "type": "Image", "contexts": ["img[srcset]"]},
    ]
    out = dedupe_srcset_variants(items)
    assert len(out) == 1
    assert out[0]["url"] == "https://x.com/_next/image?url=a&w=1920&q=75"


def test_dedupe_merges_contexts_and_boolean_signals():
    items = [
        {"url": "https://x.com/_next/image?url=a&w=640", "type": "Image",
         "contexts": ["img[srcset]"], "inBanner": True},
        {"url": "https://x.com/_next/image?url=a&w=1920", "type": "Image",
         "contexts": ["img[src]"], "matchesTitleBrand": True},
    ]
    out = dedupe_srcset_variants(items)
    assert len(out) == 1
    assert set(out[0]["contexts"]) == {"img[srcset]", "img[src]"}
    assert out[0]["inBanner"] is True
    assert out[0]["matchesTitleBrand"] is True


def test_dedupe_does_not_false_positive_on_w_in_param_name():
    # ?show= / ?flow= contain the substring "w=" but have no real w param — they
    # must NOT be treated as srcset variants (which would strip q= and collapse them).
    items = [
        {"url": "https://cdn.com/a.jpg?show=1&q=90", "type": "Image", "contexts": ["img[src]"]},
        {"url": "https://cdn.com/a.jpg?show=1&q=40", "type": "Image", "contexts": ["img[src]"]},
    ]
    out = dedupe_srcset_variants(items)
    assert len(out) == 2  # distinct q= → distinct images, not collapsed
    assert {i["url"] for i in out} == {
        "https://cdn.com/a.jpg?show=1&q=90", "https://cdn.com/a.jpg?show=1&q=40"}


def test_dedupe_leaves_distinct_urls_untouched_and_ordered():
    items = [
        {"url": "https://x.com/a.png", "type": "Image", "contexts": ["img[src]"]},
        {"url": "https://x.com/b.png", "type": "Image", "contexts": ["img[src]"]},
    ]
    out = dedupe_srcset_variants(items)
    assert [i["url"] for i in out] == ["https://x.com/a.png", "https://x.com/b.png"]


def test_dedupe_does_not_mutate_input_contexts():
    items = [
        {"url": "https://x.com/_next/image?url=a&w=640", "type": "Image", "contexts": ["c1"]},
        {"url": "https://x.com/_next/image?url=a&w=1920", "type": "Image", "contexts": ["c2"]},
    ]
    dedupe_srcset_variants(items)
    assert items[0]["contexts"] == ["c1"]  # original list untouched


def test_cap_items_truncates_and_flags():
    items = [{"url": f"u{i}"} for i in range(5)]
    kept, truncated = cap_items(items, 3)
    assert len(kept) == 3
    assert truncated is True


def test_cap_items_no_truncation_when_under_cap():
    items = [{"url": "u0"}, {"url": "u1"}]
    kept, truncated = cap_items(items, 10)
    assert kept == items
    assert truncated is False


class _FakePage:
    """Minimal stand-in for a Playwright page: page.evaluate returns a canned value
    keyed by which script constant it was handed."""

    def __init__(self, catalog, videos):
        self._catalog = catalog
        self._videos = videos
        self.calls = []

    async def evaluate(self, script):
        self.calls.append(script)
        # Route by a marker unique to each script.
        if "assetMap" in script:
            return self._catalog
        if "nearestCaption" in script:
            return self._videos
        raise AssertionError("unexpected script")


@pytest.mark.asyncio
async def test_catalog_assets_dedupes_and_caps():
    raw = [
        {"url": "https://x.com/_next/image?url=a&w=640", "type": "Image", "contexts": ["img[srcset]"]},
        {"url": "https://x.com/_next/image?url=a&w=1920", "type": "Image", "contexts": ["img[src]"]},
        {"url": "https://x.com/b.png", "type": "Image", "contexts": ["img[src]"]},
        {"url": "https://x.com/c.png", "type": "Background", "contexts": ["css url()"]},
    ]
    page = _FakePage(catalog=raw, videos=[])
    out = await catalog_assets(page, cap=2)
    # 4 raw -> 3 after srcset dedup -> capped to 2
    assert len(out) == 2
    assert out[0]["url"] == "https://x.com/_next/image?url=a&w=1920"


@pytest.mark.asyncio
async def test_catalog_assets_handles_empty():
    page = _FakePage(catalog=None, videos=None)
    assert await catalog_assets(page, cap=50) == []


@pytest.mark.asyncio
async def test_video_descriptors_caps():
    vids = [{"src": f"https://x.com/v{i}.mp4"} for i in range(5)]
    page = _FakePage(catalog=[], videos=vids)
    out = await video_descriptors(page, cap=3)
    assert len(out) == 3
    assert out[0]["src"] == "https://x.com/v0.mp4"


# ---- merge_video_manifest ----

def test_merge_video_manifest_shapes_entries_verbatim_keys():
    dom = [{"src": "https://x.com/hero.mp4", "width": 1280, "height": 720,
            "sourceWidth": 1920, "sourceHeight": 1080, "heading": "Watch",
            "caption": "A demo", "ariaLabel": "hero video", "filename": "hero.mp4"}]
    out = merge_video_manifest(set(), dom, cap=6)
    assert len(out) == 1
    e = out[0]
    # Reference manifest keys + the two internal control keys (stripped at serialize).
    assert set(e) == {"url", "filename", "width", "height", "sourceWidth",
                      "sourceHeight", "heading", "caption", "ariaLabel",
                      "_source", "_download"}
    assert e["url"] == "https://x.com/hero.mp4"
    assert e["filename"] == "hero.mp4"
    assert e["_source"] == "dom"
    assert e["width"] == 1280 and e["height"] == 720
    assert e["sourceWidth"] == 1920 and e["sourceHeight"] == 1080
    assert e["heading"] == "Watch" and e["caption"] == "A demo"
    assert e["ariaLabel"] == "hero video"
    assert e["_download"] is True     # direct .mp4 ext


def test_merge_video_manifest_dedups_url_in_both_network_and_dom():
    # DOM (rich) wins over the network-only thin entry for the same URL.
    dom = [{"src": "https://x.com/clip.mp4", "width": 800, "height": 450}]
    net = {"https://x.com/clip.mp4", "https://x.com/other.webm"}
    out = merge_video_manifest(net, dom, cap=6)
    urls = [e["url"] for e in out]
    assert urls.count("https://x.com/clip.mp4") == 1
    clip = next(e for e in out if e["url"] == "https://x.com/clip.mp4")
    assert clip["_source"] == "dom"          # DOM entry kept, not overwritten by network
    assert clip["width"] == 800
    other = next(e for e in out if e["url"] == "https://x.com/other.webm")
    assert other["_source"] == "network"
    assert other["width"] == 0               # thin network entry
    # Network-only filename is derived from the URL path.
    assert other["filename"] == "other.webm"


def test_merge_video_manifest_marks_hls_dash_blob_not_downloadable():
    net = {
        "https://x.com/stream.m3u8",   # HLS
        "https://x.com/stream.mpd",    # DASH
        "blob:https://x.com/abc",      # blob
        "data:video/mp4;base64,AAAA",  # data
        "https://x.com/real.mov",      # direct ext
    }
    out = merge_video_manifest(net, [], cap=10)
    by_url = {e["url"]: e for e in out}
    assert by_url["https://x.com/stream.m3u8"]["_download"] is False
    assert by_url["https://x.com/stream.mpd"]["_download"] is False
    assert by_url["blob:https://x.com/abc"]["_download"] is False
    assert by_url["data:video/mp4;base64,AAAA"]["_download"] is False
    assert by_url["https://x.com/real.mov"]["_download"] is True


def test_merge_video_manifest_caps_total():
    dom = [{"src": f"https://x.com/v{i}.mp4"} for i in range(4)]
    net = {f"https://x.com/n{i}.mp4" for i in range(10)}
    out = merge_video_manifest(net, dom, cap=6)
    assert len(out) == 6
    # DOM entries are kept first (richer), so all 4 DOM urls survive the cap.
    dom_urls = {f"https://x.com/v{i}.mp4" for i in range(4)}
    assert dom_urls <= {e["url"] for e in out}


def test_merge_video_manifest_empty():
    assert merge_video_manifest(set(), [], cap=6) == []


# ---- make_video_response_handler / _looks_like_video_response ----
#
# The response handler is the hottest, trickiest code in the phase: it decides
# which network responses count as downloadable videos, entirely from a URL +
# headers pair, and MUST NOT raise into Playwright's event loop. The integration
# harness only ever emits well-formed video/* responses, so these direct unit
# tests drive each discrimination branch with a hand-built fake response.


class _FakeResponse:
    """Minimal stand-in for a Playwright Response: exposes ``.url`` and ``.headers``
    (a plain dict). ``raise_on_headers`` makes ``.headers`` access blow up, to prove
    the handler swallows a malformed response object."""

    def __init__(self, url, headers=None, *, raise_on_headers=False):
        self.url = url
        self._headers = headers or {}
        self._raise_on_headers = raise_on_headers

    @property
    def headers(self):
        if self._raise_on_headers:
            raise RuntimeError("boom: headers unavailable")
        return self._headers


def test_handler_adds_direct_mp4_with_large_content_length():
    discovered: set = set()
    handler = make_video_response_handler(discovered)
    handler(_FakeResponse(
        "https://x.com/hero.mp4",
        {"content-type": "video/mp4", "content-length": "5000000"}))
    assert discovered == {"https://x.com/hero.mp4"}


def test_handler_adds_direct_mp4_with_unknown_content_length():
    # No content-length header at all → clen defaults to 0 → not treated as tiny.
    discovered: set = set()
    handler = make_video_response_handler(discovered)
    handler(_FakeResponse("https://x.com/hero.mp4", {"content-type": "video/mp4"}))
    assert discovered == {"https://x.com/hero.mp4"}


def test_handler_adds_video_content_type_on_extensionless_url():
    discovered: set = set()
    handler = make_video_response_handler(discovered)
    handler(_FakeResponse(
        "https://x.com/stream/segment",
        {"content-type": "video/webm", "content-length": "900000"}))
    assert discovered == {"https://x.com/stream/segment"}


def test_handler_skips_non_video_content_type():
    discovered: set = set()
    handler = make_video_response_handler(discovered)
    handler(_FakeResponse("https://x.com/pic", {"content-type": "image/png",
                                                "content-length": "9000"}))
    handler(_FakeResponse("https://x.com/page", {"content-type": "text/html",
                                                 "content-length": "9000"}))
    assert discovered == set()


def test_handler_skips_tiny_body():
    # 0 < content-length < 100 → likely error page / tracking beacon, skip.
    discovered: set = set()
    handler = make_video_response_handler(discovered)
    handler(_FakeResponse(
        "https://x.com/tiny.mp4",
        {"content-type": "video/mp4", "content-length": "42"}))
    assert discovered == set()


def test_handler_skips_non_http_url():
    discovered: set = set()
    handler = make_video_response_handler(discovered)
    handler(_FakeResponse("blob:https://x.com/abc",
                          {"content-type": "video/mp4", "content-length": "5000000"}))
    handler(_FakeResponse("data:video/mp4;base64,AAAA",
                          {"content-type": "video/mp4"}))
    assert discovered == set()


def test_handler_swallows_headers_access_raising():
    discovered: set = set()
    handler = make_video_response_handler(discovered)
    # A response whose .headers raises must not bubble out; when the content-type
    # is thus unknown, the URL ext still classifies it — .mp4 → added; note the
    # handler itself must never raise.
    resp = _FakeResponse("https://x.com/clip.mp4", raise_on_headers=True)
    handler(resp)  # must not raise
    # Ext-based classification still fires (headers unreadable → treated as {}),
    # and with no readable content-length the tiny-body skip never triggers.
    assert discovered == {"https://x.com/clip.mp4"}


def test_handler_swallows_headers_raising_leaves_set_unchanged_for_non_video():
    # Same raising-headers case, but an extensionless URL → nothing to classify on
    # → set stays unchanged, and still no exception escapes.
    discovered: set = set()
    handler = make_video_response_handler(discovered)
    handler(_FakeResponse("https://x.com/opaque", raise_on_headers=True))
    assert discovered == set()


# ── Phase 8: render_lottie_previews (fake page, best-effort) ────────────────
class _FakeRenderPage:
    def __init__(self, *, set_content_raises=False, screenshot=b"PNG",
                 screenshot_raises=False):
        self._set_content_raises = set_content_raises
        self._screenshot = screenshot
        self._screenshot_raises = screenshot_raises
        self.evaluated = []

    async def set_content(self, html, **k):
        if self._set_content_raises:
            raise RuntimeError("cdn blocked")

    async def evaluate(self, script, *args):
        self.evaluated.append((script, args))

    async def wait_for_function(self, expr, **k):
        return None

    async def screenshot(self, **k):
        if self._screenshot_raises:
            raise RuntimeError("render failed")
        return self._screenshot


def _entry(name="animation-0.json", op=60, ip=0):
    return {"file": f"assets/lottie/{name}", "name": name,
            "_parsed": _lottie(op=op, ip=ip)}


@pytest.mark.asyncio
async def test_render_lottie_previews_returns_png_per_entry():
    page = _FakeRenderPage()
    out = await render_lottie_previews(page, [_entry()], max_bytes=2_000_000)
    assert out == {"animation-0.json": b"PNG"}
    # mid-frame = int((60-0)*0.3) = 18 passed to the loader
    assert page.evaluated and page.evaluated[0][1][0][1] == 18


@pytest.mark.asyncio
async def test_render_lottie_previews_degrades_when_shell_load_fails(caplog):
    import logging
    page = _FakeRenderPage(set_content_raises=True)
    with caplog.at_level(logging.INFO):
        out = await render_lottie_previews(page, [_entry()], max_bytes=2_000_000)
    assert out == {}
    assert any("lottie preview render skipped" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_render_lottie_previews_degrades_when_screenshot_fails(caplog):
    import logging
    page = _FakeRenderPage(screenshot_raises=True)
    with caplog.at_level(logging.INFO):
        out = await render_lottie_previews(page, [_entry()], max_bytes=2_000_000)
    assert out == {}
    assert any("lottie preview render skipped" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_render_lottie_previews_skips_oversized():
    page = _FakeRenderPage()
    out = await render_lottie_previews(page, [_entry()], max_bytes=10)
    assert out == {}    # JSON larger than 10 bytes -> skipped


@pytest.mark.asyncio
async def test_render_lottie_previews_empty_input_returns_empty():
    out = await render_lottie_previews(_FakeRenderPage(), [], max_bytes=2_000_000)
    assert out == {}


# ── Phase 8: rasterize_svgs (fake page, best-effort browser raster) ─────────
def _png_b64(color=(1, 2, 3)) -> str:
    import base64
    from PIL import Image
    buf = io.BytesIO()
    Image.new("RGB", (20, 20), color).save(buf, format="PNG")
    return base64.b64encode(buf.getvalue()).decode()


class _FakeRasterPage:
    def __init__(self, results):
        # results: a list returned per successive evaluate call, or an Exception
        # instance to raise on the first call.
        self._results = results
        self.calls = 0

    async def evaluate(self, script, *args):
        if isinstance(self._results, Exception):
            raise self._results
        r = self._results[self.calls] if self.calls < len(self._results) else ""
        self.calls += 1
        return r


@pytest.mark.asyncio
async def test_rasterize_svgs_returns_png_items():
    svgs = [{"outerHTML": "<svg/>", "label": "logo", "isLogo": True},
            {"outerHTML": "<svg/>", "label": "menu"}]
    page = _FakeRasterPage([_png_b64(), _png_b64((9, 9, 9))])
    out = await rasterize_svgs(page, svgs, cap=10)
    assert [name for _b, name in out] == ["logo", "menu"]
    assert all(isinstance(b, bytes) and b for b, _n in out)


@pytest.mark.asyncio
async def test_rasterize_svgs_dedups_by_basename():
    svgs = [{"outerHTML": "<svg/>", "label": "logo"},
            {"outerHTML": "<svg/>", "label": "logo"}]   # same basename
    page = _FakeRasterPage([_png_b64(), _png_b64()])
    out = await rasterize_svgs(page, svgs, cap=10)
    assert len(out) == 1


@pytest.mark.asyncio
async def test_rasterize_svgs_degrades_when_page_cannot_raster(caplog):
    import logging
    svgs = [{"outerHTML": "<svg/>", "label": "logo"}]
    page = _FakeRasterPage(RuntimeError("no canvas"))
    with caplog.at_level(logging.INFO):
        out = await rasterize_svgs(page, svgs, cap=10)
    assert out == []
    assert any("svg contact sheet skipped" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_rasterize_svgs_skips_empty_result_but_keeps_others():
    svgs = [{"outerHTML": "<svg/>", "label": "a"},
            {"outerHTML": "<svg/>", "label": "b"}]
    page = _FakeRasterPage(["", _png_b64()])   # first raster failed -> skipped
    out = await rasterize_svgs(page, svgs, cap=10)
    assert [n for _b, n in out] == ["b"]


@pytest.mark.asyncio
async def test_rasterize_svgs_respects_cap():
    svgs = [{"outerHTML": "<svg/>", "label": f"s{i}"} for i in range(5)]
    page = _FakeRasterPage([_png_b64() for _ in range(5)])
    out = await rasterize_svgs(page, svgs, cap=2)
    assert len(out) == 2
