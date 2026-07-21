import io
import json
import zipfile

import pytest
from unittest.mock import AsyncMock, patch


@pytest.mark.asyncio
async def test_build_hyperframes_zip_layout():
    doc = type("D", (), {})()
    doc.id, doc.kb_id = "d1", "kb"
    doc.profile = {
        "colors": [{"hex": "#000", "role": "background"}],
        "colors_ranked": ["#0B0B0F", "#FFFFFF", "#FF3366"],
        "color_stats": [{"hex": "#0B0B0F", "count": 40, "bgCount": 30,
                         "interactiveBg": 2, "areaBg": 6, "textCount": 1,
                         "maxArea": 900000},
                        {"hex": "#FF3366", "count": 12, "bgCount": 4,
                         "interactiveBg": 8, "areaBg": 0, "textCount": 0,
                         "maxArea": 5000}],
        "spacing": {"scale": [8, 16]},
        "fonts": {"display": {"family": "Inter", "catalog_match": {"matched": False},
                              "files": [], "stack": "Inter", "renderable": False,
                              "sample_selector": "h1", "weights": []},
                  "body": {"family": "Inter", "catalog_match": {"matched": False},
                           "files": [], "stack": "Inter", "renderable": False,
                           "sample_selector": "p", "weights": []}},
        "text": {"headline": "Hi", "tagline": "there", "ctas": ["Go"], "full_text": "body"},
        "assets": [{"kind": "logo", "storage_key": "captures/kb/d1/assets/logo.svg",
                    "format": "svg", "description": "a logo"}],
        "screenshots": [{"kind": "above_fold", "image_id": "i1", "section_index": None}],
        "css_variables": {"--brand": "#f00", "--radius": "8px"},
        "headings": [{"level": 1, "text": "Hero", "font_size": "48px",
                      "font_weight": "700", "color": "#111"}],
        "svgs": [{"label": "logo", "view_box": "0 0 24 24", "width": 24,
                  "height": 24, "is_logo": True}],
        "page": {"width": 1440, "height": 5000,
                 "viewport_width": 1280, "viewport_height": 800},
        "sections": [{"index": 0, "heading": "Hero", "type": "hero",
                      "bg_color": "#0b0b0f", "layout": "split",
                      "background_image": "https://x/bg.png",
                      "cta_texts": ["Start"], "asset_urls": ["https://x/a.png"],
                      "text": "hero body"}],
    }
    storage = AsyncMock()
    storage.get = AsyncMock(return_value=b"BYTES")
    with (
        patch("parsers.capture.export.get_storage", new=AsyncMock(return_value=storage)),
        patch("parsers.capture.export._image_keys",
              new=AsyncMock(return_value={"i1": "images/kb/d1/x.png"})),
    ):
        from parsers.capture.export import build_hyperframes_zip
        data = await build_hyperframes_zip(doc)
    zf = zipfile.ZipFile(io.BytesIO(data))
    names = set(zf.namelist())
    assert "capture/extracted/tokens.json" in names
    assert "capture/extracted/fonts.json" in names
    assert "capture/extracted/fonts-manifest.json" in names
    assert "capture/extracted/visible-text.txt" in names
    assert "capture/extracted/asset-descriptions.md" in names
    assert any(n.startswith("capture/assets/") for n in names)
    assert any(n.startswith("capture/screenshots/") for n in names)
    # tokens.json is the official hyperframes shape: build-frame reads `colors`
    # as a top-20 hex STRING list, the fonts[] array, and the REAL colorStats
    # for brand-role detection (Phase 2 — was synthetic object-colors + stats).
    tokens = json.loads(zf.read("capture/extracted/tokens.json"))
    assert tokens["colors"] == ["#0B0B0F", "#FFFFFF", "#FF3366"]  # strings, not objects
    assert all(isinstance(c, str) for c in tokens["colors"])
    assert tokens["fonts"][0]["family"] == "Inter"      # role-keyed Fonts flattened to array
    # Real per-hex stats pass through verbatim (top-48, hyperframes field names).
    assert tokens["colorStats"][0]["hex"] == "#0B0B0F"
    assert tokens["colorStats"][0]["areaBg"] == 6       # REAL count, not a coverage projection
    assert tokens["colorStats"][0]["maxArea"] == 900000
    assert tokens["colorStats"][1] == {"hex": "#FF3366", "count": 12, "bgCount": 4,
                                       "interactiveBg": 8, "areaBg": 0, "textCount": 0,
                                       "maxArea": 5000}
    assert b"a logo" in zf.read("capture/extracted/asset-descriptions.md")

    # Phase 1 — hyperframes DesignTokens parity: verbatim camelCase key names.
    assert tokens["cssVariables"] == {"--brand": "#f00", "--radius": "8px"}
    hd = tokens["headings"][0]
    assert hd == {"level": 1, "text": "Hero", "fontSize": "48px",
                  "fontWeight": "700", "color": "#111"}
    sv = tokens["svgs"][0]
    assert sv == {"label": "logo", "viewBox": "0 0 24 24", "width": 24,
                  "height": 24, "isLogo": True}
    assert tokens["page"] == {"width": 1440, "height": 5000,
                              "viewport": {"width": 1280, "height": 800}}
    sec = tokens["sections"][0]
    assert sec["type"] == "hero" and sec["heading"] == "Hero"
    assert sec["backgroundColor"] == "#0b0b0f"
    assert sec["backgroundImage"] == "https://x/bg.png"
    assert sec["callsToAction"] == ["Start"]
    assert sec["assetUrls"] == ["https://x/a.png"]
    assert sec["layout"] == "split" and sec["text"] == "hero body"


@pytest.mark.asyncio
async def test_asset_descriptions_skips_blank_description_assets():
    """M1: Phase 3 adds many svg/logo/image refs with description="" — those must
    NOT emit noise "- **kind**: " lines with nothing after the colon. Only assets
    with a real description show up; blanks are dropped."""
    doc = type("D", (), {})()
    doc.id, doc.kb_id = "d1", "kb"
    doc.profile = {
        "fonts": {}, "text": {"headline": "T"}, "screenshots": [], "spacing": {},
        "assets": [
            {"kind": "logo", "storage_key": "captures/kb/d1/assets/logo.svg",
             "format": "svg", "description": ""},          # blank -> skipped
            {"kind": "image", "storage_key": "captures/kb/d1/assets/pic.jpg",
             "format": "jpg", "description": ""},           # blank -> skipped
            {"kind": "hero", "storage_key": "captures/kb/d1/assets/hero.jpg",
             "format": "jpg", "description": "the hero shot"},  # kept
        ],
    }
    storage = AsyncMock()
    storage.get = AsyncMock(return_value=b"BYTES")
    with (
        patch("parsers.capture.export.get_storage", new=AsyncMock(return_value=storage)),
        patch("parsers.capture.export._image_keys", new=AsyncMock(return_value={})),
    ):
        from parsers.capture.export import build_hyperframes_zip
        data = await build_hyperframes_zip(doc)
    zf = zipfile.ZipFile(io.BytesIO(data))
    md = zf.read("capture/extracted/asset-descriptions.md").decode()
    assert "- **hero**: the hero shot" in md
    # No noise lines: nothing ends in a bare colon-space, and blanks are gone.
    assert "- **logo**: " not in md
    assert "- **image**: " not in md
    assert md.strip() == "- **hero**: the hero shot"


@pytest.mark.asyncio
async def test_asset_descriptions_fallback_when_all_blank():
    """When every asset has a blank description, the (no descriptions) fallback
    still applies (empty join -> fallback string)."""
    doc = type("D", (), {})()
    doc.id, doc.kb_id = "d1", "kb"
    doc.profile = {
        "fonts": {}, "text": {"headline": "T"}, "screenshots": [], "spacing": {},
        "assets": [
            {"kind": "logo", "storage_key": "captures/kb/d1/assets/logo.svg",
             "format": "svg", "description": ""},
        ],
    }
    storage = AsyncMock()
    storage.get = AsyncMock(return_value=b"<svg/>")
    with (
        patch("parsers.capture.export.get_storage", new=AsyncMock(return_value=storage)),
        patch("parsers.capture.export._image_keys", new=AsyncMock(return_value={})),
    ):
        from parsers.capture.export import build_hyperframes_zip
        data = await build_hyperframes_zip(doc)
    zf = zipfile.ZipFile(io.BytesIO(data))
    assert zf.read("capture/extracted/asset-descriptions.md").decode() == "(no descriptions)"


@pytest.mark.asyncio
async def test_build_zip_routes_downloaded_svgs_by_basename():
    """Downloaded SVGs (format=svg, storage_key under assets/svgs/) go to
    capture/assets/svgs/<basename> — keyed by the unique content-hash basename,
    not {kind}.{format} (which would collide many svgs on svg.svg)."""
    doc = type("D", (), {})()
    doc.id, doc.kb_id = "d1", "kb"
    doc.profile = {
        "fonts": {}, "text": {"headline": "T"}, "screenshots": [], "spacing": {},
        "assets": [
            {"kind": "logo", "storage_key": "captures/kb/d1/assets/svgs/logo-abc123.svg",
             "format": "svg"},
            {"kind": "svg", "storage_key": "captures/kb/d1/assets/svgs/svg-def456.svg",
             "format": "svg"},
        ],
    }
    storage = AsyncMock()
    storage.get = AsyncMock(return_value=b"<svg/>")
    with (
        patch("parsers.capture.export.get_storage", new=AsyncMock(return_value=storage)),
        patch("parsers.capture.export._image_keys", new=AsyncMock(return_value={})),
    ):
        from parsers.capture.export import build_hyperframes_zip
        data = await build_hyperframes_zip(doc)
    names = set(zipfile.ZipFile(io.BytesIO(data)).namelist())
    assert "capture/assets/svgs/logo-abc123.svg" in names
    assert "capture/assets/svgs/svg-def456.svg" in names
    # not collapsed to {kind}.{format}
    assert "capture/assets/svg.svg" not in names


@pytest.mark.asyncio
async def test_build_zip_routes_catalog_images_by_basename():
    """kind==image AssetRefs (bulk catalog images) route to
    capture/assets/<basename-of-storage_key> (derived slug), not image.<fmt>."""
    doc = type("D", (), {})()
    doc.id, doc.kb_id = "d1", "kb"
    doc.profile = {
        "fonts": {}, "text": {"headline": "T"}, "screenshots": [], "spacing": {},
        "assets": [
            {"kind": "image", "storage_key": "captures/kb/d1/assets/hero-product.jpg",
             "format": "jpg", "url": "https://x/a.jpg", "vision_status": "skipped"},
            {"kind": "image", "storage_key": "captures/kb/d1/assets/pricing-table.png",
             "format": "png", "url": "https://x/b.png", "vision_status": "skipped"},
        ],
    }
    storage = AsyncMock()
    storage.get = AsyncMock(return_value=b"BYTES")
    with (
        patch("parsers.capture.export.get_storage", new=AsyncMock(return_value=storage)),
        patch("parsers.capture.export._image_keys", new=AsyncMock(return_value={})),
    ):
        from parsers.capture.export import build_hyperframes_zip
        data = await build_hyperframes_zip(doc)
    names = set(zipfile.ZipFile(io.BytesIO(data)).namelist())
    assert "capture/assets/hero-product.jpg" in names
    assert "capture/assets/pricing-table.png" in names
    assert "capture/assets/image.jpg" not in names  # not collapsed


@pytest.mark.asyncio
async def test_build_zip_emits_fonts_css_for_captured_fonts():
    """Captured role fonts (woff2 stored under captures/.../fonts/) get a
    synthesized capture/assets/fonts/fonts.css with local @font-face src."""
    doc = type("D", (), {})()
    doc.id, doc.kb_id = "d1", "kb"
    doc.profile = {
        "text": {"headline": "T"}, "screenshots": [], "spacing": {}, "assets": [],
        "fonts": {
            "display": {"family": "Poppins", "stack": "Poppins", "renderable": False,
                        "catalog_match": {"matched": False}, "sample_selector": "h1",
                        "weights": [700],
                        "files": [{"url": "captures/kb/d1/fonts/poppins.woff2",
                                   "weight": 700, "style": "normal", "format": "woff2"}]},
            "body": {"family": "Inter", "stack": "Inter", "renderable": False,
                     "catalog_match": {"matched": False}, "sample_selector": "p",
                     "weights": [400],
                     "files": [{"url": "captures/kb/d1/fonts/inter.woff2",
                                "weight": 400, "style": "normal", "format": "woff2"}]},
        },
    }
    storage = AsyncMock()
    storage.get = AsyncMock(return_value=b"WOFF2")
    with (
        patch("parsers.capture.export.get_storage", new=AsyncMock(return_value=storage)),
        patch("parsers.capture.export._image_keys", new=AsyncMock(return_value={})),
    ):
        from parsers.capture.export import build_hyperframes_zip
        data = await build_hyperframes_zip(doc)
    zf = zipfile.ZipFile(io.BytesIO(data))
    names = set(zf.namelist())
    assert "capture/assets/fonts/fonts.css" in names
    # the woff2 members are still emitted
    assert "capture/assets/fonts/poppins.woff2" in names
    assert "capture/assets/fonts/inter.woff2" in names
    css = zf.read("capture/assets/fonts/fonts.css").decode()
    assert 'font-family: "Poppins"' in css
    assert 'font-family: "Inter"' in css
    assert 'src: url("./poppins.woff2")' in css
    assert 'src: url("./inter.woff2")' in css
    assert "font-weight: 700" in css
    assert "font-style: normal" in css
    assert css.count("@font-face") == 2


def test_fonts_css_caps_total_faces_at_30():
    from parsers.capture.export import _fonts_css
    files = [{"url": f"captures/kb/d1/fonts/f{i}.woff2", "weight": 400,
              "style": "normal", "format": "woff2"} for i in range(40)]
    fonts = {"display": {"family": "Fam", "files": files}, "body": {"family": "", "files": []}}
    css = _fonts_css(fonts)
    assert css.count("@font-face") == 30


def test_fonts_css_empty_when_no_captured_files():
    from parsers.capture.export import _fonts_css
    # renderable/catalog-matched fonts have no captured files -> no css
    assert _fonts_css({"display": {"family": "Inter", "files": []},
                       "body": {"family": "Inter", "files": []}}) == ""


def test_official_tokens_colors_fallback_when_ranked_empty():
    """Legacy/partial profiles without colors_ranked fall back to the role
    tokens' hexes so downstream never gets an empty `colors`; colorStats -> []."""
    from parsers.capture.export import _official_tokens
    tokens = _official_tokens({
        "colors": [{"hex": "#123456", "role": "background"},
                   {"hex": "#ABCDEF", "role": "accent"}],
        "fonts": {}, "text": {"headline": "T"},
    })
    assert tokens["colors"] == ["#123456", "#ABCDEF"]  # from [c["hex"] ...]
    assert all(isinstance(c, str) for c in tokens["colors"])
    assert tokens["colorStats"] == []


def test_official_tokens_no_synthetic_color_stats_helper():
    """The synthetic _color_stats projection is gone."""
    import parsers.capture.export as export_mod
    assert not hasattr(export_mod, "_color_stats")
