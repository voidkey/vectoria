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
