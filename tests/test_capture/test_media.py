import pytest

from parsers.capture._media import (
    cap_items,
    catalog_assets,
    dedupe_srcset_variants,
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
