import pytest
from unittest.mock import AsyncMock, MagicMock


def _eval_router(raw):
    """page.evaluate stub: media-catalog scripts return []; everything else
    (run_extract / design-styles / page.html) returns the canned raw dict."""
    async def _run(script, *args, **kwargs):
        if "assetMap" in script:        # ASSET_CATALOG_JS
            return []
        if "nearestCaption" in script:  # VIDEO_DESCRIPTORS_JS
            return []
        return raw
    return _run


def _fake_page(raw):
    page = AsyncMock()
    page.goto = AsyncMock()
    page.evaluate = _eval_router(raw)
    page.screenshot = AsyncMock(return_value=b"PNG")
    page.viewport_size = {"width": 1280, "height": 800}
    page.wait_for_timeout = AsyncMock()
    return page


class _FakeDeps:
    """In-memory CaptureDeps: no browser, no S3, no DB."""
    def __init__(self, page, hydrate):
        self._page = page
        self._hydrate = hydrate
        self.storage = AsyncMock()          # .put is a no-op AsyncMock
        self.uploads = []                   # records (vision_configured, [names])

    def open_page(self):
        page = self._page
        class _CM:
            async def __aenter__(self_inner):
                return page
            async def __aexit__(self_inner, *a):
                return False
        return _CM()

    async def upload_image_refs(self, refs, *, vision_configured):
        self.uploads.append((vision_configured, [r.name for r in refs]))
        return len(refs)

    async def hydrate_image_ids(self):
        return self._hydrate


def _settings():
    return MagicMock(
        vision_base_url="", capture_render_timeout=30.0, capture_settle_ms=0,
        capture_max_screenshots=10, capture_viewport_width=1280,
        capture_viewport_height=800, capture_scroll_step_frac=0.8,
        capture_scroll_step_ms=0, capture_scroll_max_steps=0,
        capture_networkidle_timeout=0, capture_img_wait_ms=0,
        capture_section_settle_ms=0, capture_max_asset_bytes=1000,
        capture_max_screenshot_height=20000, capture_color_delta_e=10.0,
        capture_asset_catalog_cap=200, capture_video_cap=20)


@pytest.mark.asyncio
async def test_run_capture_happy_path_builds_profile_without_browser_or_db():
    raw = {
        "final_url": "https://x/final",
        "colors": {"samples": [{"color": "#0b0b0f", "area": 400000, "text": False},
                               {"color": "#ffffff", "area": 200000, "text": True}],
                   "css_vars": {}, "theme_color": None},
        "fonts": {"display": {"family": "Inter", "weight": 700, "selector": "h1"},
                  "body": {"family": "Inter", "weight": 400, "selector": "p"},
                  "face_srcs": {}},
        "spacing": {"margins": [8, 16], "paddings": [16], "radii": [8],
                    "container_max_width": 1200, "section_gaps": [96]},
        "sections": [{"index": 0, "heading": "Hero", "classNames": [], "bg": "#0b0b0f",
                      "rect": {"y": 0, "height": 600}}],
        "text": {"headline": "Hello", "tagline": "world", "ctas": ["Start"],
                 "full_text": "Hello world."},
        "assets": {"logo": None, "hero": None, "og_image": None, "favicon": None,
                   "video": None, "lottie": None},
        "motion": {"libraries": ["gsap"], "has_video_background": False, "has_canvas": True},
    }
    hydrate = {"screenshot-above_fold.png": ("s1", "images/kb/d1/a.png"),
               "screenshot-full_page.png": ("s2", "images/kb/d1/b.png")}
    deps = _FakeDeps(_fake_page(raw), hydrate)

    from parsers.capture.orchestrator import run_capture
    outcome = await run_capture("https://x", "kb", "d1", _settings(), deps)

    assert outcome.title == "Hello"
    assert outcome.has_images is True
    assert outcome.enqueue_image_analysis is False   # vision_base_url empty
    prof = outcome.profile.model_dump()
    assert prof["text"]["headline"] == "Hello"
    assert prof["motion_hints"]["libraries"] == ["gsap"]
    assert {c["role"] for c in prof["colors"]} >= {"background", "text"}
    assert len(prof["screenshots"]) == 2


@pytest.mark.asyncio
async def test_run_capture_maps_phase1_tokens_and_strips_svg_markup():
    raw = {
        "final_url": "https://x/final",
        "colors": {"samples": [{"color": "#0b0b0f", "area": 400000, "text": False}],
                   "css_vars": {"--brand": "#f00", "--radius": "8px"},
                   "theme_color": None},
        "fonts": {"display": {"family": "Inter", "weight": 700, "selector": "h1"},
                  "body": {"family": "Inter", "weight": 400, "selector": "p"},
                  "face_srcs": {}},
        "spacing": {"margins": [8], "paddings": [16], "radii": [8],
                    "container_max_width": 1200, "section_gaps": []},
        "sections": [{"index": 0, "heading": "Hero", "classNames": ["hero"],
                      "bg": "#0b0b0f", "backgroundImage": "https://x/bg.png",
                      "callsToAction": ["Start", "Learn"],
                      "assetUrls": ["https://x/a.png"], "layout": "split",
                      "text": "hero body text", "x": 0, "y": 0,
                      "width": 1280, "height": 600,
                      "rect": {"y": 0, "height": 600}}],
        "headings": [{"level": 1, "text": "Hero", "fontSize": "48px",
                      "fontWeight": "700", "color": "rgb(17, 17, 17)"}],
        "svgs": [{"label": "logo", "viewBox": "0 0 24 24", "width": 24, "height": 24,
                  "outerHTML": "<svg>...huge markup...</svg>", "isLogo": True}],
        "page": {"width": 1440, "height": 5000,
                 "viewport": {"width": 1280, "height": 800}},
        "text": {"headline": "Hello", "tagline": "world", "ctas": ["Start"],
                 "full_text": "Hello world."},
        "assets": {"logo": None, "hero": None, "og_image": None, "favicon": None,
                   "video": None, "lottie": None},
        "motion": {"libraries": [], "has_video_background": False, "has_canvas": False},
    }
    deps = _FakeDeps(_fake_page(raw), {})

    from parsers.capture.orchestrator import run_capture
    outcome = await run_capture("https://x", "kb", "d1", _settings(), deps)
    prof = outcome.profile.model_dump()

    assert prof["css_variables"] == {"--brand": "#f00", "--radius": "8px"}
    h = prof["headings"][0]
    assert h["level"] == 1 and h["text"] == "Hero"
    assert h["font_size"] == "48px" and h["font_weight"] == "700"
    assert h["color"] == "rgb(17, 17, 17)"
    s = prof["svgs"][0]
    assert s["label"] == "logo" and s["view_box"] == "0 0 24 24"
    assert s["width"] == 24 and s["height"] == 24 and s["is_logo"] is True
    # DB-bloat guard: raw SVG markup must never reach the profile.
    assert "outerHTML" not in s and "outer_html" not in s
    assert "markup" not in json_dumps(s)
    assert prof["page"] == {"width": 1440, "height": 5000,
                            "viewport_width": 1280, "viewport_height": 800}
    sec = prof["sections"][0]
    assert sec["layout"] == "split"
    assert sec["background_image"] == "https://x/bg.png"
    assert sec["cta_texts"] == ["Start", "Learn"]
    assert sec["asset_urls"] == ["https://x/a.png"]
    assert sec["text"] == "hero body text"


def json_dumps(obj):
    import json
    return json.dumps(obj)
