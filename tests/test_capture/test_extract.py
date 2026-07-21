import pytest
from unittest.mock import AsyncMock

from parsers.capture._extract import EXTRACT_JS, run_extract


def test_extract_js_is_string():
    assert isinstance(EXTRACT_JS, str)
    assert "getComputedStyle" in EXTRACT_JS
    assert "samples" in EXTRACT_JS
    assert "innerText" in EXTRACT_JS      # visible full-text capture


def test_extract_js_collects_phase1_tokens():
    # Phase 1: headings / svgs (with isLogo) / page geometry / section layout.
    for needle in ("headings", "svgs", "isLogo", "outerHTML",
                   "layout", "callsToAction", "assetUrls",
                   "scrollWidth", "viewBox"):
        assert needle in EXTRACT_JS, needle


def test_extract_js_collects_phase2_colors():
    # Phase 2: real top-20 ranked colors + top-48 colorStats. The extractor must
    # keep `samples` (process_colors consumes it) AND add the reference-shaped
    # `ranked`/`stats`, built via elementFromPoint grid sampling and a 1x1-canvas
    # modern-color-space resolver.
    for needle in ("samples", "ranked", "stats",
                   "elementFromPoint", "colorStats",
                   "interactiveBg", "areaBg", "textCount",
                   "getContext", "getImageData"):
        assert needle in EXTRACT_JS, needle


def test_extract_js_collects_library_fingerprints():
    # Phase 6: the motion block now carries a `fingerprints` object with the
    # window-global + DOM probes detect_libraries maps to library names.
    for needle in ("fingerprints", "__NEXT_DATA__", "#__next",
                   "svelte-", "data-framer-component-type",
                   "data-engine", "rive-canvas", "window.THREE"):
        assert needle in EXTRACT_JS, needle


@pytest.mark.asyncio
async def test_run_extract_passes_through():
    page = AsyncMock()
    page.evaluate = AsyncMock(return_value={"final_url": "https://x/final"})
    out = await run_extract(page)
    page.evaluate.assert_awaited_once()
    assert out["final_url"] == "https://x/final"
