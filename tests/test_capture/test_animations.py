import pytest
from unittest.mock import AsyncMock


def test_js_constants_contain_expected_hooks():
    from parsers.capture._animations import (
        SHADER_CAPTURE_JS, IO_CAPTURE_JS, COLLECT_ANIMATIONS_JS)
    # Shader hook wraps getContext + shaderSource into window.__capturedShaders.
    assert "__capturedShaders" in SHADER_CAPTURE_JS
    assert "getContext" in SHADER_CAPTURE_JS
    assert "shaderSource" in SHADER_CAPTURE_JS
    # IO monkey-patch records observed targets.
    assert "IntersectionObserver" in IO_CAPTURE_JS
    assert "__hf_io_targets" in IO_CAPTURE_JS
    # Collector reads the Web Animations API + CSS scan + IO targets + canvases.
    assert "getAnimations" in COLLECT_ANIMATIONS_JS
    assert "__hf_io_targets" in COLLECT_ANIMATIONS_JS
    assert "canvas" in COLLECT_ANIMATIONS_JS
    # All three must be guarded (try/catch) so a page throw can't abort capture.
    for js in (SHADER_CAPTURE_JS, IO_CAPTURE_JS, COLLECT_ANIMATIONS_JS):
        assert "try" in js and "catch" in js
        assert isinstance(js, str) and js.strip()


def test_detect_libraries_merges_and_dedups():
    from parsers.capture._animations import detect_libraries
    out = detect_libraries(["GSAP", "gsap"], [], {})
    # Raw libs are passed through as-is; dedup is case-sensitive on the token.
    assert "GSAP" in out
    assert out.count("GSAP") == 1


def test_detect_libraries_next_from_dom_fingerprint():
    from parsers.capture._animations import detect_libraries
    out = detect_libraries([], [], {"nextRoot": True})
    assert "Next.js" in out


def test_detect_libraries_three_from_shader_fingerprint():
    from parsers.capture._animations import detect_libraries
    shaders = [{"type": "vertex",
                "source": "uniform mat4 modelViewMatrix; uniform mat4 projectionMatrix;"}]
    out = detect_libraries([], shaders, {})
    assert "WebGL" in out
    assert any("Three.js" in x for x in out)


def test_detect_libraries_pixi_from_shader_fingerprint():
    from parsers.capture._animations import detect_libraries
    shaders = [{"type": "fragment",
                "source": "varying vec2 vTextureCoord; uniform sampler2D uSampler;"}]
    out = detect_libraries([], shaders, {})
    assert any("PixiJS" in x for x in out)
    # Pixi branch is exclusive of the three fingerprint.
    assert not any("Three.js" in x for x in out)


def test_detect_libraries_dom_globals_and_svelte_tailwind():
    from parsers.capture._animations import detect_libraries
    fp = {"gsap": True, "three": True, "svelte": True, "tailwind": True,
          "framerMotion": True}
    out = detect_libraries([], [], fp)
    assert "GSAP" in out
    assert any("Three.js" in x for x in out)
    assert "Svelte" in out
    assert "Tailwind CSS" in out
    assert "Framer Motion" in out


@pytest.mark.asyncio
async def test_collect_animation_catalog_attaches_cdp_and_builds_summary():
    from parsers.capture._animations import collect_animation_catalog
    canned = {
        "webAnimations": [
            {"type": "Animation", "playState": "running",
             "keyframes": [{"opacity": 0}, {"opacity": 1}]},
            {"type": "Animation", "playState": "idle"},
        ],
        "cssDeclarations": [
            {"selector": ".a", "animation": {"name": "fade", "duration": "1s",
                                             "easing": "ease"}},
        ],
        "scrollTargets": [{"selector": "#s", "rect": {"top": 0, "height": 10, "width": 20}}],
        "canvasCount": 3,
    }
    page = AsyncMock()
    page.evaluate = AsyncMock(return_value=canned)
    cdp_entries = [{"id": "1", "name": "n", "type": "CSSTransition"}]
    cat = await collect_animation_catalog(page, cdp_entries)
    assert cat["cdpAnimations"] == cdp_entries
    assert cat["webAnimations"] == canned["webAnimations"]
    assert cat["summary"] == {
        "webAnimations": 2, "cssDeclarations": 1, "scrollTargets": 1,
        "cdpAnimations": 1, "canvases": 3}


@pytest.mark.asyncio
async def test_collect_shaders_dedups_by_source():
    from parsers.capture._animations import collect_shaders
    page = AsyncMock()
    page.evaluate = AsyncMock(return_value=[
        {"type": "vertex", "source": "A"},
        {"type": "vertex", "source": "A"},
        {"type": "fragment", "source": "B"},
    ])
    out = await collect_shaders(page)
    assert [s["source"] for s in out] == ["A", "B"]


@pytest.mark.asyncio
async def test_collect_shaders_empty_on_none():
    from parsers.capture._animations import collect_shaders
    page = AsyncMock()
    page.evaluate = AsyncMock(return_value=None)
    assert await collect_shaders(page) == []


@pytest.mark.asyncio
async def test_start_cdp_animation_capture_degrades_without_real_cdp():
    """The fake page has no real CDP session — start must return (None, [])
    and never raise/hang."""
    from parsers.capture._animations import start_cdp_animation_capture
    page = AsyncMock()
    # new_cdp_session raises (no real browser) — must be swallowed.
    page.context.new_cdp_session = AsyncMock(side_effect=RuntimeError("no cdp"))
    session, entries = await start_cdp_animation_capture(page)
    assert session is None
    assert entries == []
