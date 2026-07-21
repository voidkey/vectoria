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
    # tokens.json is the official hyperframes shape: build-frame reads
    # colors[].hex, the fonts[] array, and colorStats for brand-role detection.
    tokens = json.loads(zf.read("capture/extracted/tokens.json"))
    assert tokens["colors"][0]["hex"] == "#000"
    assert tokens["fonts"][0]["family"] == "Inter"      # role-keyed Fonts flattened to array
    assert tokens["colorStats"][0]["hex"] == "#000"     # projected from role/coverage
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
