import pytest

from parsers.capture._media import (
    cap_items,
    catalog_assets,
    dedupe_srcset_variants,
    merge_video_manifest,
    video_descriptors,
)


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
            "poster": "https://x.com/p.jpg"}]
    out = merge_video_manifest(set(), dom, cap=6)
    assert len(out) == 1
    e = out[0]
    # Exact key set — nothing more, nothing less.
    assert set(e) == {"url", "source", "width", "height", "poster", "download", "preview"}
    assert e["url"] == "https://x.com/hero.mp4"
    assert e["source"] == "dom"
    assert e["width"] == 1280
    assert e["height"] == 720
    assert e["poster"] == "https://x.com/p.jpg"
    assert e["download"] is True     # direct .mp4 ext
    assert e["preview"] is None      # filled later by orchestrator


def test_merge_video_manifest_dedups_url_in_both_network_and_dom():
    # DOM (rich) wins over the network-only thin entry for the same URL.
    dom = [{"src": "https://x.com/clip.mp4", "width": 800, "height": 450}]
    net = {"https://x.com/clip.mp4", "https://x.com/other.webm"}
    out = merge_video_manifest(net, dom, cap=6)
    urls = [e["url"] for e in out]
    assert urls.count("https://x.com/clip.mp4") == 1
    clip = next(e for e in out if e["url"] == "https://x.com/clip.mp4")
    assert clip["source"] == "dom"           # DOM entry kept, not overwritten by network
    assert clip["width"] == 800
    other = next(e for e in out if e["url"] == "https://x.com/other.webm")
    assert other["source"] == "network"
    assert other["width"] == 0               # thin network entry


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
    assert by_url["https://x.com/stream.m3u8"]["download"] is False
    assert by_url["https://x.com/stream.mpd"]["download"] is False
    assert by_url["blob:https://x.com/abc"]["download"] is False
    assert by_url["data:video/mp4;base64,AAAA"]["download"] is False
    assert by_url["https://x.com/real.mov"]["download"] is True


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
