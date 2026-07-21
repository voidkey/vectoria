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
    """Fallback path (no font_files): cap the role-font faces at 30."""
    from parsers.capture.export import _fonts_css
    files = [{"url": f"captures/kb/d1/fonts/f{i}.woff2", "weight": 400,
              "style": "normal", "format": "woff2"} for i in range(40)]
    profile = {"fonts": {"display": {"family": "Fam", "files": files},
                         "body": {"family": "", "files": []}}}
    css = _fonts_css(profile)
    assert css.count("@font-face") == 30


def test_fonts_css_caps_font_files_at_30():
    """Phase 4 path: font_files faces capped at 30."""
    from parsers.capture.export import _fonts_css
    ff = [{"storage_key": f"captures/kb/d1/assets/fonts/{i:08x}.woff2",
           "family": "Fam", "weight": 400, "style": "normal"} for i in range(40)]
    css = _fonts_css({"font_files": ff})
    assert css.count("@font-face") == 30


def test_fonts_css_empty_when_no_captured_files():
    from parsers.capture.export import _fonts_css
    # renderable/catalog-matched fonts have no captured files -> no css
    assert _fonts_css({"fonts": {"display": {"family": "Inter", "files": []},
                                 "body": {"family": "Inter", "files": []}}}) == ""


@pytest.mark.asyncio
async def test_build_zip_emits_real_fonts_manifest_from_font_files():
    """Phase 4: fonts-manifest.json is the real types.ts::FontsManifest built from
    profile.font_files, and fonts.css has one @font-face per captured face."""
    doc = type("D", (), {})()
    doc.id, doc.kb_id = "d1", "kb"
    doc.profile = {
        "captured_at": "2026-07-21T00:00:00+00:00",
        "text": {"headline": "T"}, "screenshots": [], "spacing": {}, "assets": [],
        "fonts": {},
        "font_files": [
            {"file": "aaaa1111.woff2", "family": "Inter", "rawFamily": "Inter",
             "subfamily": "Regular", "postscript": "Inter-Regular", "weight": 400,
             "style": "normal", "variationAxes": [], "identified": True,
             "isIcon": False,
             "storage_key": "captures/kb/d1/assets/fonts/aaaa1111.woff2"},
            {"file": "bbbb2222.woff2", "family": "Inter", "rawFamily": "Inter",
             "subfamily": "Bold", "postscript": "Inter-Bold", "weight": 700,
             "style": "normal", "variationAxes": [], "identified": True,
             "isIcon": False,
             "storage_key": "captures/kb/d1/assets/fonts/bbbb2222.woff2"},
            {"file": "cccc3333.woff2", "family": "", "rawFamily": "",
             "subfamily": "", "postscript": "", "weight": 0, "style": "normal",
             "variationAxes": [], "identified": False, "isIcon": False,
             "storage_key": "captures/kb/d1/assets/fonts/cccc3333.woff2"},
        ],
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

    manifest = json.loads(zf.read("capture/extracted/fonts-manifest.json"))
    assert set(manifest) == {"files", "families", "unidentified", "meta"}
    assert manifest["meta"] == {"generatedAt": "2026-07-21T00:00:00+00:00",
                                "tool": "fonttools"}
    assert len(manifest["files"]) == 3
    assert manifest["unidentified"] == ["cccc3333.woff2"]
    fam = next(f for f in manifest["families"] if f["family"] == "Inter")
    assert fam["weights"] == [400, 700]
    assert fam["fileCount"] == 2
    assert fam["files"] == ["aaaa1111.woff2", "bbbb2222.woff2"]

    # fonts.css + woff2 members for the two identified faces.
    assert "capture/assets/fonts/fonts.css" in names
    assert "capture/assets/fonts/aaaa1111.woff2" in names
    assert "capture/assets/fonts/bbbb2222.woff2" in names
    css = zf.read("capture/assets/fonts/fonts.css").decode()
    assert css.count("@font-face") == 2   # unidentified (family="") skipped
    assert 'src: url("./aaaa1111.woff2")' in css
    assert "font-weight: 700" in css


@pytest.mark.asyncio
async def test_build_zip_fonts_manifest_fallback_empty_font_files():
    """Old profiles without font_files still export: an empty-but-well-formed
    manifest and the role-font fallback fonts.css."""
    doc = type("D", (), {})()
    doc.id, doc.kb_id = "d1", "kb"
    doc.profile = {
        "captured_at": "2026-07-20T00:00:00+00:00",
        "text": {"headline": "T"}, "screenshots": [], "spacing": {}, "assets": [],
        "fonts": {
            "display": {"family": "Poppins", "renderable": False,
                        "catalog_match": {"matched": False},
                        "files": [{"url": "captures/kb/d1/fonts/poppins.woff2",
                                   "weight": 700, "style": "normal"}]},
            "body": {"family": "Inter", "renderable": False,
                     "catalog_match": {"matched": False}, "files": []},
        },
        # no font_files key at all
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
    manifest = json.loads(zf.read("capture/extracted/fonts-manifest.json"))
    assert manifest == {"files": [], "families": [], "unidentified": [],
                        "meta": {"generatedAt": "2026-07-20T00:00:00+00:00",
                                 "tool": "fonttools"}}
    # Fallback fonts.css from the role-font files still works.
    css = zf.read("capture/assets/fonts/fonts.css").decode()
    assert 'font-family: "Poppins"' in css
    assert 'src: url("./poppins.woff2")' in css
    assert "capture/assets/fonts/poppins.woff2" in set(zf.namelist())


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


@pytest.mark.asyncio
async def test_build_zip_emits_animations_and_shaders_lean():
    """A profile with an animation_catalog + shaders emits the LEAN
    animations.json (summary + named + <=10 keyframed) and shaders.json."""
    catalog = {
        "webAnimations": [
            {"type": "Animation", "playState": "running",
             "keyframes": [{"opacity": 0}, {"opacity": 1}]},
            {"type": "Animation", "playState": "idle"},  # no keyframes -> dropped
        ],
        "cssDeclarations": [
            {"selector": ".a", "animation": {"name": "fade", "duration": "1s",
                                             "easing": "ease"}},
            {"selector": ".b", "animation": {"name": "fade"}},   # dup name
            {"selector": ".c", "transition": {"property": "opacity", "duration": "1s"}},
        ],
        "scrollTargets": [{"selector": "#s", "rect": {"top": 0, "height": 1, "width": 1}},
                          {"selector": "#t", "rect": {"top": 5, "height": 1, "width": 1}}],
        "cdpAnimations": [{"id": "1", "name": "n", "type": "CSSTransition"}],
        "summary": {"webAnimations": 2, "cssDeclarations": 3, "scrollTargets": 2,
                    "cdpAnimations": 1, "canvases": 4},
    }
    shaders = [{"type": "vertex", "source": "uniform mat4 modelViewMatrix;"},
               {"type": "fragment", "source": "void main(){}"}]
    doc = type("D", (), {})()
    doc.id, doc.kb_id = "d1", "kb"
    doc.profile = {
        "fonts": {}, "text": {"headline": "T"}, "screenshots": [], "spacing": {},
        "assets": [], "animation_catalog": catalog, "shaders": shaders,
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
    names = set(zf.namelist())
    assert "capture/extracted/animations.json" in names
    assert "capture/extracted/shaders.json" in names
    anims = json.loads(zf.read("capture/extracted/animations.json"))
    assert anims["summary"] == catalog["summary"]
    assert anims["namedAnimations"] == ["fade"]            # deduped
    assert anims["scrollTriggeredElements"] == 2
    assert len(anims["representativeAnimations"]) == 1     # only keyframed kept
    assert json.loads(zf.read("capture/extracted/shaders.json")) == shaders


@pytest.mark.asyncio
async def test_build_zip_omits_animations_and_shaders_when_absent():
    """Old profiles (no animation_catalog / empty shaders) omit both files."""
    doc = type("D", (), {})()
    doc.id, doc.kb_id = "d1", "kb"
    doc.profile = {
        "fonts": {}, "text": {"headline": "T"}, "screenshots": [], "spacing": {},
        "assets": [],  # no animation_catalog / shaders keys at all
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
    assert "capture/extracted/animations.json" not in names
    assert "capture/extracted/shaders.json" not in names


@pytest.mark.asyncio
async def test_build_zip_emits_video_manifest_and_routes_video_assets():
    """A profile with a video_manifest emits capture/extracted/video-manifest.json;
    kind==video bodies route to capture/assets/videos/<basename> and kind==
    video_preview frames to capture/assets/videos/previews/<basename>."""
    doc = type("D", (), {})()
    doc.id, doc.kb_id = "d1", "kb"
    doc.profile = {
        "fonts": {}, "text": {"headline": "T"}, "screenshots": [], "spacing": {},
        "video_manifest": {
            "videos": [{"url": "https://x/hero.mp4", "source": "dom", "width": 1280,
                        "height": 720, "poster": "", "download": True,
                        "preview": "assets/videos/previews/video-0-preview.png",
                        "local_key": "captures/kb/d1/assets/videos/video-0.mp4",
                        "downloaded": True}],
            "meta": {"discovered": 1, "downloaded": 1, "previews": 1},
        },
        "assets": [
            {"kind": "video", "storage_key": "captures/kb/d1/assets/videos/video-0.mp4",
             "format": "mp4", "url": "https://x/hero.mp4"},
            {"kind": "video_preview",
             "storage_key": "captures/kb/d1/assets/videos/previews/video-0-preview.png",
             "format": "png", "url": "https://x/hero.mp4"},
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
    names = set(zf.namelist())
    assert "capture/extracted/video-manifest.json" in names
    manifest = json.loads(zf.read("capture/extracted/video-manifest.json"))
    assert manifest["videos"][0]["url"] == "https://x/hero.mp4"
    assert manifest["meta"]["downloaded"] == 1
    # video body + preview routed to the videos/ tree by basename.
    assert "capture/assets/videos/video-0.mp4" in names
    assert "capture/assets/videos/previews/video-0-preview.png" in names
    # not collapsed to {kind}.{format}
    assert "capture/assets/video.mp4" not in names
    assert "capture/assets/video_preview.png" not in names


@pytest.mark.asyncio
async def test_build_zip_omits_video_manifest_when_absent():
    """Old profiles (no video_manifest) omit the JSON file."""
    doc = type("D", (), {})()
    doc.id, doc.kb_id = "d1", "kb"
    doc.profile = {
        "fonts": {}, "text": {"headline": "T"}, "screenshots": [], "spacing": {},
        "assets": [],
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
    assert "capture/extracted/video-manifest.json" not in names


@pytest.mark.asyncio
async def test_build_zip_routes_lotties_and_writes_manifest():
    """Phase 8: lottie_json/lottie_preview AssetRefs route under assets/lottie/ and
    assets/lottie/previews/; the manifest is written as the bare lotties array."""
    doc = type("D", (), {})()
    doc.id, doc.kb_id = "d1", "kb"
    doc.profile = {
        "fonts": {}, "text": {"headline": "T"}, "screenshots": [], "spacing": {},
        "lottie_manifest": {
            "lotties": [{"file": "assets/lottie/animation-0.json",
                         "url": "https://x/a.json", "name": "Hero", "width": 200,
                         "height": 100, "duration": 2.0, "frameRate": 30, "layers": 2,
                         "preview": "assets/lottie/previews/animation-0-preview.png"}],
            "meta": {"discovered": 1, "previews": 1}},
        "assets": [
            {"kind": "lottie_json",
             "storage_key": "captures/kb/d1/assets/lottie/animation-0.json",
             "format": "json"},
            {"kind": "lottie_preview",
             "storage_key": "captures/kb/d1/assets/lottie/previews/animation-0-preview.png",
             "format": "png"},
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
    names = set(zf.namelist())
    assert "capture/assets/lottie/animation-0.json" in names
    assert "capture/assets/lottie/previews/animation-0-preview.png" in names
    assert "capture/extracted/lottie-manifest.json" in names
    manifest = json.loads(zf.read("capture/extracted/lottie-manifest.json"))
    assert isinstance(manifest, list) and manifest[0]["name"] == "Hero"


@pytest.mark.asyncio
async def test_build_zip_omits_lottie_manifest_when_absent():
    """Backward-compat: an old profile without lottie_manifest omits the file."""
    doc = type("D", (), {})()
    doc.id, doc.kb_id = "d1", "kb"
    doc.profile = {"fonts": {}, "text": {"headline": "T"}, "screenshots": [],
                   "spacing": {}, "assets": []}
    storage = AsyncMock()
    storage.get = AsyncMock(return_value=b"BYTES")
    with (
        patch("parsers.capture.export.get_storage", new=AsyncMock(return_value=storage)),
        patch("parsers.capture.export._image_keys", new=AsyncMock(return_value={})),
    ):
        from parsers.capture.export import build_hyperframes_zip
        data = await build_hyperframes_zip(doc)
    names = set(zipfile.ZipFile(io.BytesIO(data)).namelist())
    assert "capture/extracted/lottie-manifest.json" not in names


@pytest.mark.asyncio
async def test_build_zip_routes_contact_sheets_by_subdir():
    """Phase 8: contact_sheet AssetRefs route to reference paths by their storage_key
    subdir — screenshots/, assets/, assets/svgs/."""
    doc = type("D", (), {})()
    doc.id, doc.kb_id = "d1", "kb"
    doc.profile = {
        "fonts": {}, "text": {"headline": "T"}, "screenshots": [], "spacing": {},
        "assets": [
            {"kind": "contact_sheet", "format": "jpg",
             "storage_key": "captures/kb/d1/screenshots/contact-sheet-1.jpg"},
            {"kind": "contact_sheet", "format": "jpg",
             "storage_key": "captures/kb/d1/assets/contact-sheet-1.jpg"},
            {"kind": "contact_sheet", "format": "jpg",
             "storage_key": "captures/kb/d1/assets/svgs/contact-sheet-1.jpg"},
        ],
    }
    storage = AsyncMock()
    storage.get = AsyncMock(return_value=b"JPEG")
    with (
        patch("parsers.capture.export.get_storage", new=AsyncMock(return_value=storage)),
        patch("parsers.capture.export._image_keys", new=AsyncMock(return_value={})),
    ):
        from parsers.capture.export import build_hyperframes_zip
        data = await build_hyperframes_zip(doc)
    names = set(zipfile.ZipFile(io.BytesIO(data)).namelist())
    assert "capture/screenshots/contact-sheet-1.jpg" in names
    assert "capture/assets/contact-sheet-1.jpg" in names
    assert "capture/assets/svgs/contact-sheet-1.jpg" in names


# ---------------------------------------------------------------------------
# Phase 9 — agent scaffolding: meta.json + color-role inference
# ---------------------------------------------------------------------------

def test_infer_color_role_thresholds():
    """Ported luminance/saturation heuristic from agentPromptGenerator.ts::
    inferColorRole. Pure function, no I/O."""
    from parsers.capture.export import infer_color_role
    assert infer_color_role("#000000") == "bg-dark"       # luminance 0
    assert infer_color_role("#FFFFFF") == "bg-light"      # luminance 1
    assert infer_color_role("#FF3366") == "accent"        # saturated, mid luminance
    assert infer_color_role("#1a1a1a") == "surface-dark"  # dark but not near-black
    assert infer_color_role("#c0c0c0") == "surface-light" # light but not near-white (lum ~0.75)
    assert infer_color_role("#888888") == "neutral"       # mid gray, low saturation
    assert infer_color_role("bad") == "color"             # unparseable -> fallback
    assert infer_color_role("#zzzzzz") == "color"         # non-hex digits -> fallback


def test_infer_color_role_non_string_does_not_raise():
    """Non-str input (None / an int slipping through the ranked-color list) must
    return "color" via the TypeError guard, never raise — a raise here would abort
    the whole build_hyperframes_zip export."""
    from parsers.capture.export import infer_color_role
    assert infer_color_role(None) == "color"
    assert infer_color_role(123) == "color"


def test_contact_sheet_rows_numeric_pagination_and_labels():
    """_contact_sheet_rows sorts pages numerically (10 AFTER 2, not lexically) and
    labels each `page N of M` in that numeric order. Pure function, no I/O."""
    from parsers.capture.export import _contact_sheet_rows
    written = {
        "capture/screenshots/contact-sheet-2.jpg",
        "capture/screenshots/contact-sheet-11.jpg",
        "capture/screenshots/contact-sheet-1.jpg",
        "capture/screenshots/contact-sheet-10.jpg",
    }
    rows = _contact_sheet_rows(written, "screenshots", "Scroll grid")
    # Numeric order: 1, 2, 10, 11 (NOT lexical 1, 10, 11, 2).
    assert rows == [
        "| `screenshots/contact-sheet-1.jpg` | Scroll grid — page 1 of 4 |",
        "| `screenshots/contact-sheet-2.jpg` | Scroll grid — page 2 of 4 |",
        "| `screenshots/contact-sheet-10.jpg` | Scroll grid — page 3 of 4 |",
        "| `screenshots/contact-sheet-11.jpg` | Scroll grid — page 4 of 4 |",
    ]


@pytest.mark.asyncio
async def test_build_zip_emits_meta_json_with_counts():
    """capture/meta.json carries the reference-shaped project metadata with
    counts derived from the assembled profile."""
    doc = type("D", (), {})()
    doc.id, doc.kb_id = "d1", "kb"
    doc.profile = {
        "url": "https://www.acme.com/launch",
        "captured_at": "2026-07-21T00:00:00+00:00",
        "capture_quality": "full",
        "colors_ranked": ["#0B0B0F", "#FFFFFF", "#FF3366"],
        "fonts": {
            "display": {"family": "Poppins", "catalog_match": {"matched": False},
                        "files": [], "stack": "Poppins", "renderable": False,
                        "sample_selector": "h1", "weights": [700]},
            "body": {"family": "Inter", "catalog_match": {"matched": False},
                     "files": [], "stack": "Inter", "renderable": False,
                     "sample_selector": "p", "weights": [400]},
        },
        "text": {"headline": "Acme Launch", "tagline": "Ship faster"},
        "spacing": {},
        "assets": [
            {"kind": "logo", "storage_key": "captures/kb/d1/assets/logo.svg",
             "format": "svg"},
            {"kind": "image", "storage_key": "captures/kb/d1/assets/hero.jpg",
             "format": "jpg"},
        ],
        "screenshots": [{"kind": "above_fold", "image_id": "i1", "section_index": None},
                        {"kind": "section", "image_id": "i2", "section_index": 0}],
        "video_manifest": {"videos": [{"url": "https://x/a.mp4"}],
                           "meta": {"discovered": 1}},
        "lottie_manifest": {"lotties": [{"file": "assets/lottie/animation-0.json"},
                                        {"file": "assets/lottie/animation-1.json"}],
                            "meta": {"discovered": 2}},
    }
    storage = AsyncMock()
    storage.get = AsyncMock(return_value=b"BYTES")
    with (
        patch("parsers.capture.export.get_storage", new=AsyncMock(return_value=storage)),
        patch("parsers.capture.export._image_keys",
              new=AsyncMock(return_value={"i1": "images/kb/d1/a.png",
                                          "i2": "images/kb/d1/b.png"})),
    ):
        from parsers.capture.export import build_hyperframes_zip
        data = await build_hyperframes_zip(doc)
    zf = zipfile.ZipFile(io.BytesIO(data))
    assert "capture/meta.json" in set(zf.namelist())
    meta = json.loads(zf.read("capture/meta.json"))
    assert meta["url"] == "https://www.acme.com/launch"
    assert meta["title"] == "Acme Launch"
    assert meta["capturedAt"] == "2026-07-21T00:00:00+00:00"
    assert meta["captureQuality"] == "full"
    assert meta["generatedBy"] == "vectoria"
    counts = meta["counts"]
    assert counts["screenshots"] == 2
    assert counts["assets"] == 2
    assert counts["fonts"] == 2       # display + body, deduped families
    assert counts["videos"] == 1
    assert counts["lotties"] == 2
    assert counts["colors"] == 3
    # No index.html is ever written (reference deliberately omits it).
    assert "capture/index.html" not in set(zf.namelist())


@pytest.mark.asyncio
async def test_build_zip_meta_json_minimal_profile_backward_compat():
    """A minimal/old profile (missing newer fields) still produces a valid
    meta.json with zeroed counts and title falling back to the hostname."""
    doc = type("D", (), {})()
    doc.id, doc.kb_id = "d1", "kb"
    doc.profile = {
        "url": "https://www.example.com/",
        "fonts": {}, "text": {}, "spacing": {}, "assets": [], "screenshots": [],
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
    meta = json.loads(zf.read("capture/meta.json"))
    assert meta["title"] == "example.com"   # hostname fallback (www. stripped)
    assert meta["captureQuality"] == "full"  # default
    assert meta["counts"] == {"screenshots": 0, "assets": 0, "fonts": 0,
                              "videos": 0, "lotties": 0, "colors": 0}


# ---------------------------------------------------------------------------
# Phase 9 — agent scaffolding: AGENTS.md / CLAUDE.md / .cursorrules
# ---------------------------------------------------------------------------

def _full_scaffold_profile():
    return {
        "url": "https://www.acme.com/launch",
        "captured_at": "2026-07-21T00:00:00+00:00",
        "capture_quality": "full",
        "colors_ranked": ["#0B0B0F", "#FFFFFF", "#FF3366"],
        "fonts": {
            "display": {"family": "Poppins", "catalog_match": {"matched": False},
                        "files": [], "stack": "Poppins", "renderable": False,
                        "sample_selector": "h1", "weights": [700]},
            "body": {"family": "Inter", "catalog_match": {"matched": False},
                     "files": [], "stack": "Inter", "renderable": False,
                     "sample_selector": "p", "weights": [400]},
        },
        "text": {"headline": "Acme Launch", "tagline": "Ship faster", "full_text": "b"},
        "spacing": {},
        "design_styles": {"typography": {}},
        "page_html_key": "captures/kb/d1/page.html",
        "shaders": [{"type": "fragment", "source": "void main(){}"}],
        "assets": [
            {"kind": "logo", "storage_key": "captures/kb/d1/assets/logo.svg",
             "format": "svg", "description": "a logo"},
            {"kind": "contact_sheet", "format": "jpg",
             "storage_key": "captures/kb/d1/screenshots/contact-sheet-1.jpg"},
            {"kind": "contact_sheet", "format": "jpg",
             "storage_key": "captures/kb/d1/screenshots/contact-sheet-2.jpg"},
            {"kind": "contact_sheet", "format": "jpg",
             "storage_key": "captures/kb/d1/assets/contact-sheet-1.jpg"},
        ],
        "screenshots": [{"kind": "above_fold", "image_id": "i1", "section_index": None}],
    }


@pytest.mark.asyncio
async def test_build_zip_emits_identical_agent_scaffolding():
    """All three agent files are written with IDENTICAL content, driven off the
    artifacts ACTUALLY present in this zip (incl. paginated contact-sheet pages),
    with a brand summary (color + inferred role, display font) and a pointer to
    the product-launch-video skill."""
    doc = type("D", (), {})()
    doc.id, doc.kb_id = "d1", "kb"
    doc.profile = _full_scaffold_profile()
    storage = AsyncMock()
    storage.get = AsyncMock(return_value=b"BYTES")
    with (
        patch("parsers.capture.export.get_storage", new=AsyncMock(return_value=storage)),
        patch("parsers.capture.export._image_keys",
              new=AsyncMock(return_value={"i1": "images/kb/d1/a.png"})),
    ):
        from parsers.capture.export import build_hyperframes_zip
        data = await build_hyperframes_zip(doc)
    zf = zipfile.ZipFile(io.BytesIO(data))
    names = set(zf.namelist())
    assert "capture/AGENTS.md" in names
    assert "capture/CLAUDE.md" in names
    assert "capture/.cursorrules" in names
    # No index.html (reference deliberately omits it).
    assert "capture/index.html" not in names
    agents = zf.read("capture/AGENTS.md").decode()
    # Identical content across all three.
    assert agents == zf.read("capture/CLAUDE.md").decode()
    assert agents == zf.read("capture/.cursorrules").decode()

    # Title + source + skill pointer.
    assert "Acme Launch" in agents
    assert "https://www.acme.com/launch" in agents
    assert "product-launch-video" in agents

    # Data-inventory table lists PRESENT artifacts.
    assert "extracted/tokens.json" in agents
    assert "extracted/design-styles.json" in agents      # present (quality full)
    assert "extracted/shaders.json" in agents             # present
    assert "extracted/page.html" in agents                # present (quality full)
    # Paginated screenshot contact sheets both listed.
    assert "screenshots/contact-sheet-1.jpg" in agents
    assert "screenshots/contact-sheet-2.jpg" in agents
    assert "assets/contact-sheet-1.jpg" in agents

    # Brand summary: a top color with its inferred role + the display font.
    assert "#0B0B0F" in agents
    from parsers.capture.export import infer_color_role
    assert infer_color_role("#0B0B0F") in agents          # role hint present
    assert "Poppins" in agents


@pytest.mark.asyncio
async def test_agent_scaffolding_omits_absent_artifacts():
    """A partial-quality profile without page.html / design-styles / shaders does
    NOT list those artifacts (table is driven off the actual written members)."""
    doc = type("D", (), {})()
    doc.id, doc.kb_id = "d1", "kb"
    doc.profile = {
        "url": "https://acme.com/",
        "capture_quality": "partial",
        "colors_ranked": ["#112233"],
        "fonts": {"display": {"family": "Inter", "files": [], "weights": [],
                              "catalog_match": {"matched": False}, "stack": "Inter",
                              "renderable": False, "sample_selector": "h1"},
                  "body": {"family": "Inter", "files": [], "weights": [],
                           "catalog_match": {"matched": False}, "stack": "Inter",
                           "renderable": False, "sample_selector": "p"}},
        "text": {"headline": "Acme"}, "spacing": {},
        "assets": [], "screenshots": [],
        # no design_styles / page_html_key / shaders
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
    agents = zf.read("capture/AGENTS.md").decode()
    assert "extracted/tokens.json" in agents          # always present
    assert "extracted/design-styles.json" not in agents
    assert "extracted/page.html" not in agents
    assert "extracted/shaders.json" not in agents
    # Still identical across the three files + carries the skill pointer.
    assert agents == zf.read("capture/CLAUDE.md").decode()
    assert agents == zf.read("capture/.cursorrules").decode()
    assert "product-launch-video" in agents
