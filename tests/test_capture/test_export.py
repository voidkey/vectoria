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
