import pytest
from unittest.mock import AsyncMock, MagicMock


# Canned Phase-6 animation catalog (COLLECT_ANIMATIONS_JS) + shaders default.
_CANNED_ANIM = {
    "webAnimations": [{"type": "Animation", "playState": "running",
                       "keyframes": [{"opacity": 0}, {"opacity": 1}]}],
    "cssDeclarations": [{"selector": ".a",
                         "animation": {"name": "fade", "duration": "1s", "easing": "ease"}}],
    "scrollTargets": [{"selector": "#s", "rect": {"top": 0, "height": 10, "width": 20}}],
    "canvasCount": 2,
}
_CANNED_SHADERS = [{"type": "vertex", "source": "uniform mat4 modelViewMatrix;"}]


def _eval_router(raw, anim=None, shaders=None):
    """page.evaluate stub: media-catalog scripts return []; the Phase-6 animation
    collector / shader read return canned structures; everything else
    (run_extract / design-styles / page.html) returns the canned raw dict."""
    async def _run(script, *args, **kwargs):
        if "assetMap" in script:            # ASSET_CATALOG_JS
            return []
        if "nearestCaption" in script:      # VIDEO_DESCRIPTORS_JS
            return []
        if "getAnimations" in script:       # COLLECT_ANIMATIONS_JS
            return anim if anim is not None else _CANNED_ANIM
        if "__capturedShaders" in script:   # collect_shaders read
            return shaders if shaders is not None else _CANNED_SHADERS
        return raw
    return _run


def _fake_cdp_session():
    """A fake CDP session mirroring Playwright's real shape: .on is SYNC event
    registration; .send / .detach are async. Used by start_cdp_animation_capture."""
    session = MagicMock()
    session.on = MagicMock()
    session.send = AsyncMock()
    session.detach = AsyncMock()
    return session


def _fake_page(raw, anim=None, shaders=None):
    page = AsyncMock()
    page.goto = AsyncMock()
    page.add_init_script = AsyncMock()
    page.evaluate = _eval_router(raw, anim=anim, shaders=shaders)
    page.screenshot = AsyncMock(return_value=b"PNG")
    page.viewport_size = {"width": 1280, "height": 800}
    page.wait_for_timeout = AsyncMock()
    # page.on is SYNC event registration in real Playwright (a MagicMock here).
    # Record the registered response handler so tests can drive it directly.
    page._response_handlers = []

    def _on(event, handler):
        if event == "response":
            page._response_handlers.append(handler)
    page.on = MagicMock(side_effect=_on)
    # No DOM video elements by default -> preview pass finds nothing.
    page.query_selector_all = AsyncMock(return_value=[])
    # Playwright: page.context.new_cdp_session(page) is async -> CDPSession.
    page.context.new_cdp_session = AsyncMock(return_value=_fake_cdp_session())
    return page


def _emit_response(page, url, content_type="video/mp4", content_length=None):
    """Drive every registered response handler with a fake response object."""
    headers = {"content-type": content_type}
    if content_length is not None:
        headers["content-length"] = str(content_length)
    resp = MagicMock()
    resp.url = url
    resp.headers = headers
    for h in page._response_handlers:
        h(resp)


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
        capture_asset_catalog_cap=200, capture_video_cap=20,
        capture_max_svgs=30, capture_min_svg_bytes=200,
        capture_max_catalog_images=40, capture_min_image_bytes=10000,
        capture_max_videos=6, capture_max_video_downloads=3,
        capture_max_video_bytes=75 * 1024 * 1024,
        capture_video_download_budget_s=180.0,
        capture_max_lotties=10, capture_max_lottie_bytes=2 * 1024 * 1024,
        capture_max_svg_sheet=60)


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
async def test_run_capture_maps_colors_ranked_and_stats():
    """Phase 2: raw colors.ranked/colors.stats map onto the new profile fields;
    role tokens (colors) are still built from colors.samples."""
    stats = [{"hex": "#0B0B0F", "count": 40, "bgCount": 30, "interactiveBg": 2,
              "areaBg": 6, "textCount": 1, "maxArea": 900000},
             {"hex": "#FF3366", "count": 12, "bgCount": 4, "interactiveBg": 8,
              "areaBg": 0, "textCount": 0, "maxArea": 5000}]
    raw = {
        "final_url": "https://x/final",
        "colors": {"samples": [{"color": "#0b0b0f", "area": 400000, "text": False}],
                   "css_vars": {}, "theme_color": None,
                   "ranked": ["#0B0B0F", "#FFFFFF", "#FF3366"], "stats": stats},
        "fonts": {"display": {"family": "Inter", "weight": 700, "selector": "h1"},
                  "body": {"family": "Inter", "weight": 400, "selector": "p"},
                  "face_srcs": {}},
        "spacing": {"margins": [8], "paddings": [16], "radii": [8],
                    "container_max_width": 1200, "section_gaps": []},
        "sections": [], "text": {"headline": "Hi", "tagline": "", "ctas": [],
                                 "full_text": ""},
        "assets": {"logo": None, "hero": None, "og_image": None, "favicon": None,
                   "video": None, "lottie": None},
        "motion": {"libraries": [], "has_video_background": False, "has_canvas": False},
    }
    deps = _FakeDeps(_fake_page(raw), {})
    from parsers.capture.orchestrator import run_capture
    outcome = await run_capture("https://x", "kb", "d1", _settings(), deps)
    prof = outcome.profile.model_dump()
    assert prof["colors_ranked"] == ["#0B0B0F", "#FFFFFF", "#FF3366"]
    assert prof["color_stats"] == stats
    # role tokens still populated from samples
    assert any(c["role"] == "background" for c in prof["colors"])


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


@pytest.mark.asyncio
async def test_run_capture_downloads_svgs_to_assets_svgs_with_content_hash():
    logo_markup = "<svg width='120' height='40'>" + "L" * 300 + "</svg>"
    plain_markup = "<svg viewBox='0 0 24 24'>" + "P" * 300 + "</svg>"
    tiny_markup = "<svg></svg>"  # below capture_min_svg_bytes -> skipped
    raw = {
        "final_url": "https://x/final",
        "colors": {"samples": [{"color": "#0b0b0f", "area": 400000, "text": False}],
                   "css_vars": {}, "theme_color": None},
        "fonts": {"display": {"family": "Inter", "weight": 700, "selector": "h1"},
                  "body": {"family": "Inter", "weight": 400, "selector": "p"},
                  "face_srcs": {}},
        "spacing": {"margins": [8], "paddings": [16], "radii": [8],
                    "container_max_width": 1200, "section_gaps": []},
        "sections": [], "text": {"headline": "Hi", "tagline": "", "ctas": [],
                                 "full_text": ""},
        "svgs": [
            {"label": "logo", "viewBox": "0 0 120 40", "outerHTML": logo_markup,
             "isLogo": True},
            {"label": "icon", "viewBox": "0 0 24 24", "outerHTML": plain_markup,
             "isLogo": False},
            {"label": "dup", "viewBox": "0 0 120 40", "outerHTML": logo_markup,
             "isLogo": True},  # duplicate content -> deduped by hash
            {"label": "tiny", "outerHTML": tiny_markup, "isLogo": False},
        ],
        "assets": {"logo": None, "hero": None, "og_image": None, "favicon": None,
                   "video": None, "lottie": None},
        "motion": {"libraries": [], "has_video_background": False, "has_canvas": False},
    }
    deps = _FakeDeps(_fake_page(raw), {})
    cfg = _settings()
    cfg.capture_max_svgs = 30
    cfg.capture_min_svg_bytes = 200

    from parsers.capture.orchestrator import run_capture
    outcome = await run_capture("https://x", "kb", "d1", cfg, deps)

    svg_refs = [a for a in outcome.profile.assets if a.format == "svg"]
    # logo + plain, dup deduped, tiny skipped
    assert len(svg_refs) == 2
    kinds = {a.kind for a in svg_refs}
    assert kinds == {"logo", "svg"}
    for a in svg_refs:
        assert a.storage_key.startswith("captures/kb/d1/assets/svgs/")
    logo_ref = next(a for a in svg_refs if a.kind == "logo")
    assert "/svgs/logo-" in logo_ref.storage_key
    plain_ref = next(a for a in svg_refs if a.kind == "svg")
    assert "/svgs/svg-" in plain_ref.storage_key
    # S3 puts happened for exactly the 2 stored SVGs (svg content-type)
    put_keys = [c.args[0] for c in deps.storage.put.call_args_list
                if "/assets/svgs/" in c.args[0]]
    assert len(put_keys) == 2


@pytest.mark.asyncio
async def test_run_capture_downloads_good_context_catalog_images():
    """Only good-context, large-enough, non-junk catalog images are stored, with
    a derived slug name; tracking pixels / tiny images are dropped; catalog
    images are vision_status=skipped (vision scoped to named assets)."""
    big = b"J" * 20000       # over capture_min_image_bytes
    tiny = b"t" * 100        # under -> dropped
    catalog = [
        {"url": "https://x/img/hero-shot.jpg", "type": "Image", "aboveFold": True,
         "contexts": ["img[src]"], "description": "Hero Product"},
        {"url": "https://x/pixel/track.gif", "type": "Image",
         "contexts": ["img[src]"]},  # junk (pixel) -> skipped
        {"url": "https://x/img/spacer.png", "type": "Image",
         "contexts": ["img[src]"]},  # too small -> skipped
        {"url": "https://x/font.woff2", "type": "Font",
         "contexts": ["css url()"]},  # wrong type -> skipped
        {"url": "https://x/nav/menu.png", "type": "Image",
         "contexts": ["onclick"]},   # no good context -> skipped
    ]
    raw = {
        "final_url": "https://x/final",
        "colors": {"samples": [{"color": "#0b0b0f", "area": 400000, "text": False}],
                   "css_vars": {}, "theme_color": None},
        "fonts": {"display": {"family": "Inter", "weight": 700, "selector": "h1"},
                  "body": {"family": "Inter", "weight": 400, "selector": "p"},
                  "face_srcs": {}},
        "spacing": {"margins": [8], "paddings": [16], "radii": [8],
                    "container_max_width": 1200, "section_gaps": []},
        "sections": [], "text": {"headline": "Hi", "tagline": "", "ctas": [],
                                 "full_text": ""},
        "assets": {"logo": None, "hero": None, "og_image": None, "favicon": None,
                   "video": None, "lottie": None},
        "motion": {"libraries": [], "has_video_background": False, "has_canvas": False},
    }
    page = _fake_page(raw)

    async def _catalog_eval(script, *a, **k):
        if "assetMap" in script:
            return list(catalog)
        if "nearestCaption" in script:
            return []
        return raw
    page.evaluate = _catalog_eval
    deps = _FakeDeps(page, {})
    cfg = _settings()

    async def _fake_fetch(url, *, max_bytes):
        if "hero-shot" in url:
            return big, "image/jpeg"
        if "spacer" in url:
            return tiny, "image/png"
        return None

    from unittest.mock import patch
    with patch("parsers.capture._assets.fetch_asset_bytes", new=_fake_fetch):
        from parsers.capture.orchestrator import run_capture
        outcome = await run_capture("https://x", "kb", "d1", cfg, deps)

    img_refs = [a for a in outcome.profile.assets if a.kind == "image"]
    assert len(img_refs) == 1
    ref = img_refs[0]
    assert ref.storage_key == "captures/kb/d1/assets/hero-product.jpg"
    assert ref.url == "https://x/img/hero-shot.jpg"
    assert ref.vision_status == "skipped"   # non-goal: no vision for bulk catalog images
    put_keys = [c.args[0] for c in deps.storage.put.call_args_list]
    assert "captures/kb/d1/assets/hero-product.jpg" in put_keys


@pytest.mark.asyncio
async def test_run_capture_catalog_image_cap_logs_truncation(caplog):
    import logging as _logging
    imgs = [{"url": f"https://x/img/pic{i}.png", "type": "Image",
             "contexts": ["img[src]"]} for i in range(5)]
    raw = {
        "final_url": "https://x/final",
        "colors": {"samples": [{"color": "#0b0b0f", "area": 400000, "text": False}],
                   "css_vars": {}, "theme_color": None},
        "fonts": {"display": {"family": "Inter", "weight": 700, "selector": "h1"},
                  "body": {"family": "Inter", "weight": 400, "selector": "p"},
                  "face_srcs": {}},
        "spacing": {"margins": [8], "paddings": [16], "radii": [8],
                    "container_max_width": 1200, "section_gaps": []},
        "sections": [], "text": {"headline": "Hi", "tagline": "", "ctas": [],
                                 "full_text": ""},
        "assets": {"logo": None, "hero": None, "og_image": None, "favicon": None,
                   "video": None, "lottie": None},
        "motion": {"libraries": [], "has_video_background": False, "has_canvas": False},
    }
    page = _fake_page(raw)

    async def _catalog_eval(script, *a, **k):
        if "assetMap" in script:
            return list(imgs)
        if "nearestCaption" in script:
            return []
        return raw
    page.evaluate = _catalog_eval
    deps = _FakeDeps(page, {})
    cfg = _settings()
    cfg.capture_max_catalog_images = 2

    async def _fake_fetch(url, *, max_bytes):
        return b"J" * 20000, "image/png"

    from unittest.mock import patch
    with caplog.at_level(_logging.INFO, logger="parsers.capture.orchestrator"):
        with patch("parsers.capture._assets.fetch_asset_bytes", new=_fake_fetch):
            from parsers.capture.orchestrator import run_capture
            outcome = await run_capture("https://x", "kb", "d1", cfg, deps)

    img_refs = [a for a in outcome.profile.assets if a.kind == "image"]
    assert len(img_refs) == 2   # capped
    assert any("catalog image" in r.message.lower() and "3" in r.message
               for r in caplog.records)


@pytest.mark.asyncio
async def test_run_capture_dedups_duplicate_catalog_url_within_catalog():
    """M2: the same URL appearing twice within asset_catalog is fetched/stored
    once — the in-download seen_urls check (which also ADDs) dedups it."""
    big = b"J" * 20000
    dup = {"url": "https://x/img/same.jpg", "type": "Image", "aboveFold": True,
           "contexts": ["img[src]"], "description": "Same Pic"}
    catalog = [dict(dup), dict(dup)]   # identical URL twice
    raw = {
        "final_url": "https://x/final",
        "colors": {"samples": [{"color": "#0b0b0f", "area": 400000, "text": False}],
                   "css_vars": {}, "theme_color": None},
        "fonts": {"display": {"family": "Inter", "weight": 700, "selector": "h1"},
                  "body": {"family": "Inter", "weight": 400, "selector": "p"},
                  "face_srcs": {}},
        "spacing": {"margins": [8], "paddings": [16], "radii": [8],
                    "container_max_width": 1200, "section_gaps": []},
        "sections": [], "text": {"headline": "Hi", "tagline": "", "ctas": [],
                                 "full_text": ""},
        "assets": {"logo": None, "hero": None, "og_image": None, "favicon": None,
                   "video": None, "lottie": None},
        "motion": {"libraries": [], "has_video_background": False, "has_canvas": False},
    }
    page = _fake_page(raw)

    async def _catalog_eval(script, *a, **k):
        if "assetMap" in script:
            return list(catalog)
        if "nearestCaption" in script:
            return []
        return raw
    page.evaluate = _catalog_eval
    deps = _FakeDeps(page, {})
    cfg = _settings()

    fetches: list[str] = []

    async def _fake_fetch(url, *, max_bytes):
        fetches.append(url)
        return big, "image/jpeg"

    from unittest.mock import patch
    with patch("parsers.capture._assets.fetch_asset_bytes", new=_fake_fetch):
        from parsers.capture.orchestrator import run_capture
        outcome = await run_capture("https://x", "kb", "d1", cfg, deps)

    img_refs = [a for a in outcome.profile.assets if a.kind == "image"]
    assert len(img_refs) == 1                       # stored once
    assert fetches.count("https://x/img/same.jpg") == 1   # fetched once


@pytest.mark.asyncio
async def test_run_capture_catalog_image_does_not_clobber_named_hero():
    """Export-path collision guard: a catalog image whose derived slug would be
    ``hero`` must NOT reuse the named-hero stem (else both route to the same
    ``capture/assets/hero.jpg`` ZIP member and the catalog image silently
    clobbers the named logo/hero/favicon). The reserved-stem seed of
    ``used_names`` forces a suffix/dedup so the two land on distinct keys."""
    big = b"J" * 20000
    # aboveFold + heading "Hero" -> derived slug base is "hero" (reserved stem).
    catalog = [
        {"url": "https://x/i/00.jpg", "type": "Image", "aboveFold": True,
         "contexts": ["img[src]"], "nearestHeading": "Hero"},
    ]
    raw = {
        "final_url": "https://x/final",
        "colors": {"samples": [{"color": "#0b0b0f", "area": 400000, "text": False}],
                   "css_vars": {}, "theme_color": None},
        "fonts": {"display": {"family": "Inter", "weight": 700, "selector": "h1"},
                  "body": {"family": "Inter", "weight": 400, "selector": "p"},
                  "face_srcs": {}},
        "spacing": {"margins": [8], "paddings": [16], "radii": [8],
                    "container_max_width": 1200, "section_gaps": []},
        "sections": [], "text": {"headline": "Hi", "tagline": "", "ctas": [],
                                 "full_text": ""},
        # A NAMED hero asset present too -> stored as captures/kb/d1/assets/hero.jpg.
        "assets": {"logo": None, "hero": "https://x/hero.jpg", "og_image": None,
                   "favicon": None, "video": None, "lottie": None},
        "motion": {"libraries": [], "has_video_background": False, "has_canvas": False},
    }
    page = _fake_page(raw)

    async def _catalog_eval(script, *a, **k):
        if "assetMap" in script:
            return list(catalog)
        if "nearestCaption" in script:
            return []
        return raw
    page.evaluate = _catalog_eval
    # named hero flows through upload_image_refs -> hydrate; give it a row so the
    # named AssetRef (kind="hero") lands in the profile with hero.jpg.
    hydrate = {"hero.jpg": ("img-hero", "captures/kb/d1/assets/hero.jpg")}
    deps = _FakeDeps(page, hydrate)
    cfg = _settings()

    async def _fake_fetch(url, *, max_bytes):
        return big, "image/jpeg"

    from unittest.mock import patch
    with patch("parsers.capture._assets.fetch_asset_bytes", new=_fake_fetch):
        from parsers.capture.orchestrator import run_capture
        outcome = await run_capture("https://x", "kb", "d1", cfg, deps)

    assets = outcome.profile.assets
    named_hero = next(a for a in assets if a.kind == "hero")
    catalog_img = next(a for a in assets if a.kind == "image")
    # The catalog image must NOT reuse the named hero's stem.
    assert named_hero.storage_key == "captures/kb/d1/assets/hero.jpg"
    assert catalog_img.storage_key != named_hero.storage_key
    assert not catalog_img.storage_key.endswith("/hero.jpg")

    # And the derived ZIP members are distinct (the actual collision surface).
    from parsers.capture.export import _asset_zip_path
    named_path = _asset_zip_path(
        {"kind": named_hero.kind, "format": named_hero.format}, named_hero.storage_key)
    cat_path = _asset_zip_path(
        {"kind": catalog_img.kind, "format": catalog_img.format},
        catalog_img.storage_key)
    assert named_path == "capture/assets/hero.jpg"
    assert cat_path != named_path


def _full_quality_raw():
    """A raw dict that assess_quality classifies as 'full' (long text + sections
    + color samples) so the Phase-6 animation/shader collection is gated ON."""
    return {
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
        "text": {"headline": "Real content headline", "tagline": "world",
                 "ctas": ["Start"], "full_text": "Real content. " * 40},
        "assets": {"logo": None, "hero": None, "og_image": None, "favicon": None,
                   "video": None, "lottie": None},
        "motion": {"libraries": ["gsap"], "has_video_background": False,
                   "has_canvas": True},
    }


@pytest.mark.asyncio
async def test_run_capture_collects_animation_catalog_and_shaders_on_full():
    raw = _full_quality_raw()
    page = _fake_page(raw)   # default canned anim + shaders
    deps = _FakeDeps(page, {})

    from parsers.capture.orchestrator import run_capture
    outcome = await run_capture("https://x", "kb", "d1", _settings(), deps)

    prof = outcome.profile.model_dump()
    assert prof["capture_quality"] == "full"
    cat = prof["animation_catalog"]
    assert cat is not None
    assert cat["summary"]["webAnimations"] == 1
    assert cat["summary"]["cssDeclarations"] == 1
    assert cat["summary"]["scrollTargets"] == 1
    assert cat["summary"]["canvases"] == 2
    # Init scripts were injected (pre-nav) before goto.
    assert page.add_init_script.await_count == 2
    # Shaders were collected + deduped.
    assert prof["shaders"] == _CANNED_SHADERS


@pytest.mark.asyncio
async def test_run_capture_cdp_failure_degrades_to_empty_cdp_animations():
    """No real CDP (new_cdp_session raises) -> catalog is still built with
    cdpAnimations: [] and the capture never aborts."""
    raw = _full_quality_raw()
    page = _fake_page(raw)
    # Make the fake page's CDP unavailable, as a real degraded page would be.
    page.context.new_cdp_session = AsyncMock(side_effect=RuntimeError("no cdp"))
    deps = _FakeDeps(page, {})

    from parsers.capture.orchestrator import run_capture
    outcome = await run_capture("https://x", "kb", "d1", _settings(), deps)

    prof = outcome.profile.model_dump()
    assert prof["animation_catalog"] is not None
    assert prof["animation_catalog"]["cdpAnimations"] == []
    assert prof["animation_catalog"]["summary"]["cdpAnimations"] == 0


@pytest.mark.asyncio
async def test_run_capture_skips_animation_collection_when_not_full():
    """Partial/blocked quality gates the animation + shader collection OFF."""
    raw = _full_quality_raw()
    raw["text"]["full_text"] = "tiny"   # -> partial/blocked
    seen_scripts: list = []

    page = _fake_page(raw)
    orig = page.evaluate

    async def _spy(script, *a, **k):
        seen_scripts.append(script)
        return await orig(script, *a, **k)
    page.evaluate = _spy
    deps = _FakeDeps(page, {})

    from parsers.capture.orchestrator import run_capture
    outcome = await run_capture("https://x", "kb", "d1", _settings(), deps)

    prof = outcome.profile.model_dump()
    assert prof["capture_quality"] != "full"
    assert prof["animation_catalog"] is None
    assert prof["shaders"] == []
    # The animation collector JS was never evaluated (gated off).
    assert not any("getAnimations" in s for s in seen_scripts)


@pytest.mark.asyncio
async def test_run_capture_upgrades_libraries_via_detect_libraries():
    """Phase 6: motion_hints.libraries is built by detect_libraries — merging the
    script-src sniff, DOM fingerprints, and (full quality) shader fingerprints —
    not the raw script-src list. The extractor's `fingerprints` block is consumed
    and NOT leaked onto MotionHints."""
    raw = _full_quality_raw()
    raw["motion"] = {
        "libraries": ["gsap"],           # cheap script-src hit
        "fingerprints": {"nextRoot": True, "svelte": True},
        "has_video_background": False, "has_canvas": True,
    }
    # Shader source with BOTH three uniforms -> "Three.js (confirmed via shaders)".
    shaders = [{"type": "vertex",
                "source": "uniform mat4 modelViewMatrix; uniform mat4 projectionMatrix;"}]
    page = _fake_page(raw, shaders=shaders)
    deps = _FakeDeps(page, {})

    from parsers.capture.orchestrator import run_capture
    outcome = await run_capture("https://x", "kb", "d1", _settings(), deps)

    prof = outcome.profile.model_dump()
    libs = prof["motion_hints"]["libraries"]
    assert "gsap" in libs                       # script-src hit preserved
    assert "Next.js" in libs                     # #__next fingerprint
    assert "Svelte" in libs                       # svelte class fingerprint
    assert "WebGL" in libs                         # shader present
    assert any("Three.js" in x for x in libs)      # shader-confirmed three
    # fingerprints must not have leaked onto the persisted MotionHints.
    assert "fingerprints" not in prof["motion_hints"]


@pytest.mark.asyncio
async def test_run_capture_partial_quality_passes_through_raw_libs():
    """Below full quality, shaders aren't collected; libraries still merge the
    script-src sniff + fingerprints (detect_libraries with shaders=[])."""
    raw = _full_quality_raw()
    raw["text"]["full_text"] = "tiny"   # -> partial/blocked
    raw["motion"] = {"libraries": ["lottie"],
                     "fingerprints": {"react": True},
                     "has_video_background": False, "has_canvas": False}
    page = _fake_page(raw)
    deps = _FakeDeps(page, {})

    from parsers.capture.orchestrator import run_capture
    outcome = await run_capture("https://x", "kb", "d1", _settings(), deps)

    libs = outcome.profile.model_dump()["motion_hints"]["libraries"]
    assert "lottie" in libs
    assert "React" in libs
    assert "WebGL" not in libs   # no shaders collected at partial quality


def test_is_latin_subset_hashed_and_named():
    from parsers.capture.orchestrator import _is_latin_subset
    # Hashed Next.js face (the -s suffix used to break the old .woff-anchored
    # regex) is treated as latin-neutral -> True.
    assert _is_latin_subset("https://x/_next/static/media/19cfc7226ec3afaa-s.woff2")
    # An explicit `latin` token wins.
    assert _is_latin_subset("https://x/fonts/inter-latin.woff2")
    # An explicit non-latin subset is de-prioritized.
    assert not _is_latin_subset("https://x/fonts/inter-cyrillic.woff2")
    # ...and a hashed/latin face sorts ahead of a cyrillic one.
    urls = ["https://x/fonts/inter-cyrillic.woff2",
            "https://x/_next/static/media/19cfc7226ec3afaa-s.woff2"]
    ordered = sorted(urls, key=lambda u: 0 if _is_latin_subset(u) else 1)
    assert ordered[0].endswith("19cfc7226ec3afaa-s.woff2")
    assert ordered[1].endswith("inter-cyrillic.woff2")


def _synth_woff2(family="Inter", subfamily="Regular", weight=400) -> bytes:
    """A tiny valid woff2 face so font_file_metadata returns real metadata."""
    import io as _io
    from fontTools.fontBuilder import FontBuilder
    from fontTools.ttLib.tables._g_l_y_f import Glyph
    cps = [0x41, 0x42]
    order = [".notdef", "g0", "g1"]
    fb = FontBuilder(1000, isTTF=True)
    fb.setupGlyphOrder(order)
    fb.setupCharacterMap({0x41: "g0", 0x42: "g1"})
    fb.setupGlyf({n: Glyph() for n in order})
    fb.setupHorizontalMetrics({n: (500, 0) for n in order})
    fb.setupHorizontalHeader(ascent=800, descent=-200)
    fb.setupNameTable({"familyName": family, "styleName": subfamily,
                       "psName": f"{family}-{subfamily}"})
    fb.setupOS2(usWeightClass=weight, fsSelection=0x40)
    fb.setupPost()
    fb.font.flavor = "woff2"
    buf = _io.BytesIO()
    fb.save(buf)
    return buf.getvalue()


def _font_raw(face_srcs):
    return {
        "final_url": "https://x/final",
        "colors": {"samples": [{"color": "#0b0b0f", "area": 400000, "text": False}],
                   "css_vars": {}, "theme_color": None},
        "fonts": {"display": {"family": "Inter", "weight": 700, "selector": "h1"},
                  "body": {"family": "Inter", "weight": 400, "selector": "p"},
                  "face_srcs": face_srcs},
        "spacing": {"margins": [8], "paddings": [16], "radii": [8],
                    "container_max_width": 1200, "section_gaps": []},
        "sections": [], "text": {"headline": "Hi", "tagline": "", "ctas": [],
                                 "full_text": ""},
        "assets": {"logo": None, "hero": None, "og_image": None, "favicon": None,
                   "video": None, "lottie": None},
        "motion": {"libraries": [], "has_video_background": False, "has_canvas": False},
    }


def _font_settings():
    cfg = _settings()
    cfg.capture_max_fonts_per_family = 6
    cfg.capture_max_total_fonts = 30
    return cfg


@pytest.mark.asyncio
async def test_run_capture_prefers_latin_subset_and_collects_metadata():
    """A family with a CJK and a Latin subset: Latin is fetched first; both are
    stored under assets/fonts/ and their fonttools metadata is collected."""
    face_srcs = {"inter": ["https://x/fonts/inter-cjk.woff2",
                           "https://x/fonts/inter-latin.woff2"]}
    raw = _font_raw(face_srcs)
    deps = _FakeDeps(_fake_page(raw), {})
    cfg = _font_settings()

    fetch_order: list[str] = []

    async def _fake_fetch(url, *, max_bytes):
        fetch_order.append(url)
        # distinct bytes per URL so they don't content-dedup
        return _synth_woff2(subfamily=url.rsplit("/", 1)[-1]), "font/woff2"

    from unittest.mock import patch
    with patch("parsers.capture._assets.fetch_asset_bytes", new=_fake_fetch):
        from parsers.capture.orchestrator import run_capture
        outcome = await run_capture("https://x", "kb", "d1", cfg, deps)

    # Latin subset fetched (role-font pass fetches srcs[0]=cjk; the bounded pass
    # sorts Latin ahead and skips the already-seen cjk URL).
    assert "https://x/fonts/inter-latin.woff2" in fetch_order
    ff = outcome.profile.font_files
    assert len(ff) == 2
    for m in ff:
        assert m["storage_key"].startswith("captures/kb/d1/assets/fonts/")
        assert m["identified"] is True
        assert m["family"] == "Inter"
    # The role-font passes fetch srcs[0] (cjk) and mark it seen; the BOUNDED pass
    # then iterates Latin-first, fetching ONLY latin (cjk is url-deduped, never
    # refetched). So the last bounded fetch is latin, and cjk is not refetched.
    assert fetch_order[-1] == "https://x/fonts/inter-latin.woff2"
    assert fetch_order.count("https://x/fonts/inter-latin.woff2") == 1
    # cjk fetched only by the role pass(es), never by the bounded loop.
    assert "https://x/fonts/inter-cjk.woff2" in fetch_order


@pytest.mark.asyncio
async def test_run_capture_enforces_per_family_font_cap():
    urls = [f"https://x/fonts/inter-{i}.woff2" for i in range(10)]
    face_srcs = {"inter": urls}
    raw = _font_raw(face_srcs)
    deps = _FakeDeps(_fake_page(raw), {})
    cfg = _font_settings()
    cfg.capture_max_fonts_per_family = 3

    async def _fake_fetch(url, *, max_bytes):
        return _synth_woff2(subfamily=url.rsplit("/", 1)[-1]), "font/woff2"

    from unittest.mock import patch
    with patch("parsers.capture._assets.fetch_asset_bytes", new=_fake_fetch):
        from parsers.capture.orchestrator import run_capture
        outcome = await run_capture("https://x", "kb", "d1", cfg, deps)

    # role-font pass stored inter-0 (srcs[0]); bounded pass adds up to family cap.
    # Total per-family faces capped at 3 (the bounded loop counts fam faces).
    assert len(outcome.profile.font_files) <= 4  # role dup counted separately
    put_font_keys = [c.args[0] for c in deps.storage.put.call_args_list
                     if "/assets/fonts/" in c.args[0]]
    assert len(put_font_keys) <= 4


@pytest.mark.asyncio
async def test_run_capture_total_font_cap_logs_truncation(caplog):
    import logging as _logging
    # 5 families x 2 faces each = 10 faces; cap at 4 -> truncation logged.
    face_srcs = {f"fam{i}": [f"https://x/fonts/f{i}-a.woff2",
                             f"https://x/fonts/f{i}-b.woff2"] for i in range(5)}
    raw = _font_raw(face_srcs)
    deps = _FakeDeps(_fake_page(raw), {})
    cfg = _font_settings()
    cfg.capture_max_total_fonts = 4

    async def _fake_fetch(url, *, max_bytes):
        return _synth_woff2(subfamily=url.rsplit("/", 1)[-1]), "font/woff2"

    from unittest.mock import patch
    with caplog.at_level(_logging.INFO, logger="parsers.capture.orchestrator"):
        with patch("parsers.capture._assets.fetch_asset_bytes", new=_fake_fetch):
            from parsers.capture.orchestrator import run_capture
            outcome = await run_capture("https://x", "kb", "d1", cfg, deps)

    assert len(outcome.profile.font_files) <= 4
    assert any("total-font cap" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_run_capture_dedups_faces_by_content_hash():
    """The same face bytes reached via role-font and the bounded pass store once."""
    # role font (display Inter) resolves srcs[0]; the same URL also in the family
    # list. Identical bytes -> single stored face.
    same = "https://x/fonts/inter.woff2"
    face_srcs = {"inter": [same]}
    raw = _font_raw(face_srcs)
    deps = _FakeDeps(_fake_page(raw), {})
    cfg = _font_settings()

    body = _synth_woff2()

    async def _fake_fetch(url, *, max_bytes):
        return body, "font/woff2"   # identical bytes every call

    from unittest.mock import patch
    with patch("parsers.capture._assets.fetch_asset_bytes", new=_fake_fetch):
        from parsers.capture.orchestrator import run_capture
        outcome = await run_capture("https://x", "kb", "d1", cfg, deps)

    assert len(outcome.profile.font_files) == 1   # content-hash deduped
    put_font_keys = [c.args[0] for c in deps.storage.put.call_args_list
                     if "/assets/fonts/" in c.args[0]]
    assert len(put_font_keys) == 1


# ---- Phase 7: video manifest (network discovery + previews + bounded download) ----

def _dom_video_eval(raw, dom_videos):
    """evaluate router variant that returns canned DOM video descriptors for
    VIDEO_DESCRIPTORS_JS; delegates the rest to the standard router."""
    base = _eval_router(raw)

    async def _run(script, *args, **kwargs):
        if "nearestCaption" in script:      # VIDEO_DESCRIPTORS_JS
            return list(dom_videos)
        return await base(script, *args, **kwargs)
    return _run


@pytest.mark.asyncio
async def test_run_capture_builds_video_manifest_from_network_and_dom():
    """A direct-ext network URL (driven through the registered response handler
    during goto) + a DOM video merge into video_manifest; the direct-ext body is
    downloaded to assets/videos/ and an HLS master is discovered but NOT downloaded."""
    raw = _full_quality_raw()
    dom_videos = [{"src": "https://x/dom-hero.mp4", "width": 1280, "height": 720,
                   "poster": "https://x/poster.jpg", "top": 0, "filename": "dom-hero.mp4"}]
    page = _fake_page(raw)
    page.evaluate = _dom_video_eval(raw, dom_videos)

    # Emit network responses during navigation (handler already registered pre-goto).
    async def _goto(*a, **k):
        _emit_response(page, "https://x/net-clip.webm", "video/webm")
        _emit_response(page, "https://x/live/master.m3u8", "application/vnd.apple.mpegurl")
    page.goto = AsyncMock(side_effect=_goto)

    deps = _FakeDeps(page, {})
    cfg = _settings()

    async def _fake_fetch(url, *, max_bytes):
        # Only direct-ext bodies are ever fetched.
        return b"V" * 2048, "video/mp4"

    from unittest.mock import patch
    with patch("parsers.capture._assets.fetch_asset_bytes", new=_fake_fetch):
        from parsers.capture.orchestrator import run_capture
        outcome = await run_capture("https://x", "kb", "d1", cfg, deps)

    vm = outcome.profile.video_manifest
    assert vm is not None
    urls = {v["url"] for v in vm["videos"]}
    assert "https://x/dom-hero.mp4" in urls
    assert "https://x/net-clip.webm" in urls
    assert "https://x/live/master.m3u8" in urls
    hls = next(v for v in vm["videos"] if v["url"].endswith(".m3u8"))
    assert hls["download"] is False

    # Downloaded video bodies land as kind="video" AssetRefs under assets/videos/.
    video_refs = [a for a in outcome.profile.assets if a.kind == "video"]
    assert video_refs, "expected at least one downloaded video body"
    for a in video_refs:
        assert a.storage_key.startswith("captures/kb/d1/assets/videos/")
    # The HLS master was never fetched/stored.
    put_keys = [c.args[0] for c in deps.storage.put.call_args_list]
    assert not any("master" in k for k in put_keys)


@pytest.mark.asyncio
async def test_run_capture_video_manifest_no_download_when_not_full():
    """Discovery + manifest happen at all quality levels, but bodies are only
    downloaded when capture_quality == 'full' (don't pull MBs off a challenge page)."""
    # Thin content -> not full quality (design-styles/page.html gated off).
    raw = {
        "final_url": "https://x/final",
        "colors": {"samples": [{"color": "#0b0b0f", "area": 400000, "text": False}],
                   "css_vars": {}, "theme_color": None},
        "fonts": {"display": {"family": "Inter", "weight": 700, "selector": "h1"},
                  "body": {"family": "Inter", "weight": 400, "selector": "p"},
                  "face_srcs": {}},
        "spacing": {"margins": [8], "paddings": [16], "radii": [8],
                    "container_max_width": 1200, "section_gaps": []},
        "sections": [], "text": {"headline": "Hi", "tagline": "", "ctas": [],
                                 "full_text": ""},
        "assets": {"logo": None, "hero": None, "og_image": None, "favicon": None,
                   "video": None, "lottie": None},
        "motion": {"libraries": [], "has_video_background": False, "has_canvas": False},
    }
    dom_videos = [{"src": "https://x/dom.mp4", "width": 640, "height": 360,
                   "filename": "dom.mp4"}]
    page = _fake_page(raw)
    page.evaluate = _dom_video_eval(raw, dom_videos)

    async def _goto(*a, **k):
        _emit_response(page, "https://x/net.mp4", "video/mp4")
    page.goto = AsyncMock(side_effect=_goto)
    deps = _FakeDeps(page, {})

    fetched = []

    async def _fake_fetch(url, *, max_bytes):
        fetched.append(url)
        return b"V" * 2048, "video/mp4"

    from unittest.mock import patch
    with patch("parsers.capture._assets.fetch_asset_bytes", new=_fake_fetch):
        from parsers.capture.orchestrator import run_capture
        outcome = await run_capture("https://x", "kb", "d1", _settings(), deps)

    prof = outcome.profile
    assert prof.capture_quality != "full"
    # Manifest still built from discovery.
    assert prof.video_manifest is not None
    assert {v["url"] for v in prof.video_manifest["videos"]} >= {
        "https://x/dom.mp4", "https://x/net.mp4"}
    # But nothing was downloaded.
    assert fetched == []
    assert [a for a in prof.assets if a.kind == "video"] == []


@pytest.mark.asyncio
async def test_run_capture_video_download_count_cap_logs_truncation(caplog):
    """capture_max_video_downloads bounds how many bodies are fetched; the
    truncation is logged (no silent cap)."""
    import logging as _logging
    raw = _full_quality_raw()
    page = _fake_page(raw)
    page.evaluate = _dom_video_eval(raw, [])

    async def _goto(*a, **k):
        for i in range(5):
            _emit_response(page, f"https://x/clip{i}.mp4", "video/mp4")
    page.goto = AsyncMock(side_effect=_goto)
    deps = _FakeDeps(page, {})

    cfg = _settings()
    cfg.capture_max_video_downloads = 2

    fetched = []

    async def _fake_fetch(url, *, max_bytes):
        fetched.append(url)
        return b"V" * 2048, "video/mp4"

    from unittest.mock import patch
    with patch("parsers.capture._assets.fetch_asset_bytes", new=_fake_fetch):
        with caplog.at_level(_logging.INFO):
            from parsers.capture.orchestrator import run_capture
            await run_capture("https://x", "kb", "d1", cfg, deps)

    assert len(fetched) == 2   # capped at capture_max_video_downloads
    assert any("video download" in r.message.lower() and "cap" in r.message.lower()
               for r in caplog.records)


@pytest.mark.asyncio
async def test_run_capture_video_download_time_budget_logs_truncation(caplog):
    """A cumulative wall-clock budget stops STARTING new downloads once exceeded;
    the budget truncation is logged. The clock is injected (no real sleeps)."""
    import logging as _logging
    raw = _full_quality_raw()
    page = _fake_page(raw)
    page.evaluate = _dom_video_eval(raw, [])

    async def _goto(*a, **k):
        for i in range(4):
            _emit_response(page, f"https://x/clip{i}.mp4", "video/mp4")
    page.goto = AsyncMock(side_effect=_goto)
    deps = _FakeDeps(page, {})

    cfg = _settings()
    cfg.capture_max_video_downloads = 10        # count cap won't bite
    cfg.capture_video_download_budget_s = 5.0

    fetched = []

    async def _fake_fetch(url, *, max_bytes):
        fetched.append(url)
        return b"V" * 2048, "video/mp4"

    # Deterministic monotonic clock: 0, then jumps past the 5s budget so the
    # SECOND download's pre-check trips the budget (first download still runs).
    ticks = iter([0.0, 0.0, 100.0, 100.0, 100.0, 100.0])

    from unittest.mock import patch
    with patch("parsers.capture._assets.fetch_asset_bytes", new=_fake_fetch), \
         patch("parsers.capture.orchestrator._monotonic",
               new=lambda: next(ticks, 100.0)):
        with caplog.at_level(_logging.INFO):
            from parsers.capture.orchestrator import run_capture
            await run_capture("https://x", "kb", "d1", cfg, deps)

    assert len(fetched) == 1   # only the first download started before the budget blew
    assert any("budget" in r.message.lower() for r in caplog.records)


@pytest.mark.asyncio
async def test_run_capture_degrades_when_page_lacks_on_support():
    """A page whose .on raises (CDP-style / unsupported) must not break capture —
    network discovery is simply skipped; DOM discovery + manifest still work."""
    raw = _full_quality_raw()
    dom_videos = [{"src": "https://x/dom.mp4", "width": 640, "height": 360,
                   "filename": "dom.mp4"}]
    page = _fake_page(raw)
    page.evaluate = _dom_video_eval(raw, dom_videos)
    page.on = MagicMock(side_effect=RuntimeError("on not supported"))
    deps = _FakeDeps(page, {})

    from unittest.mock import patch
    async def _fake_fetch(url, *, max_bytes):
        return b"V" * 2048, "video/mp4"
    with patch("parsers.capture._assets.fetch_asset_bytes", new=_fake_fetch):
        from parsers.capture.orchestrator import run_capture
        outcome = await run_capture("https://x", "kb", "d1", _settings(), deps)

    # Capture completed; DOM discovery still populated the manifest.
    vm = outcome.profile.video_manifest
    assert vm is not None
    assert {v["url"] for v in vm["videos"]} == {"https://x/dom.mp4"}


def json_dumps(obj):
    import json
    return json.dumps(obj)


# ── Phase 8: lottie discovery + download + manifest ─────────────────────────
def _lottie_json(nm="Anim", w=200, h=100, fr=30, ip=0, op=60, layers=2):
    import json
    return json.dumps({"v": "5.7", "ip": ip, "op": op, "w": w, "h": h, "fr": fr,
                       "nm": nm, "layers": [{"ty": 4}] * layers}).encode()


def _dotlottie_bytes(nm="Zipped"):
    import io as _io, json as _json, zipfile as _zip
    buf = _io.BytesIO()
    with _zip.ZipFile(buf, "w") as zf:
        zf.writestr("manifest.json", "{}")
        zf.writestr("animations/data.json",
                    _json.dumps({"v": "5.7", "ip": 0, "op": 30, "w": 100, "h": 100,
                                 "fr": 30, "nm": nm, "layers": [{"ty": 4}]}))
    return buf.getvalue()


@pytest.mark.asyncio
async def test_run_capture_downloads_lotties_and_builds_manifest():
    raw = _full_quality_raw()
    raw["assets"]["lotties"] = ["https://x/a.json", "https://x/b.lottie"]
    page = _fake_page(raw)
    deps = _FakeDeps(page, {})
    cfg = _settings()

    async def _fake_fetch(url, *, max_bytes):
        if url.endswith(".lottie"):
            return _dotlottie_bytes("FromZip"), "application/octet-stream"
        return _lottie_json("FromJson"), "application/json"

    from unittest.mock import patch
    with patch("parsers.capture._assets.fetch_asset_bytes", new=_fake_fetch):
        from parsers.capture.orchestrator import run_capture
        outcome = await run_capture("https://x", "kb", "d1", cfg, deps)

    lm = outcome.profile.lottie_manifest
    assert lm is not None
    names = {e["name"] for e in lm["lotties"]}
    assert names == {"FromJson", "FromZip"}
    assert lm["meta"]["discovered"] == 2
    # No transient parsed dict leaks into the persisted manifest.
    assert all("_parsed" not in e for e in lm["lotties"])
    # Animation JSON stored + routed as kind="lottie_json" under assets/lottie/.
    refs = [a for a in outcome.profile.assets if a.kind == "lottie_json"]
    assert len(refs) == 2
    for a in refs:
        assert a.storage_key.startswith("captures/kb/d1/assets/lottie/animation-")


@pytest.mark.asyncio
async def test_run_capture_lottie_dedups_by_content_hash():
    raw = _full_quality_raw()
    raw["assets"]["lotties"] = ["https://x/a.json", "https://x/dup.json"]
    page = _fake_page(raw)
    deps = _FakeDeps(page, {})

    async def _fake_fetch(url, *, max_bytes):
        return _lottie_json("Same"), "application/json"  # identical content

    from unittest.mock import patch
    with patch("parsers.capture._assets.fetch_asset_bytes", new=_fake_fetch):
        from parsers.capture.orchestrator import run_capture
        outcome = await run_capture("https://x", "kb", "d1", _settings(), deps)

    lm = outcome.profile.lottie_manifest
    assert len(lm["lotties"]) == 1     # deduped
    refs = [a for a in outcome.profile.assets if a.kind == "lottie_json"]
    assert len(refs) == 1


@pytest.mark.asyncio
async def test_run_capture_lottie_rejects_invalid_and_omits_manifest():
    raw = _full_quality_raw()
    raw["assets"]["lotties"] = ["https://x/notlottie.json"]
    page = _fake_page(raw)
    deps = _FakeDeps(page, {})

    async def _fake_fetch(url, *, max_bytes):
        import json
        return json.dumps({"hello": "world"}).encode(), "application/json"

    from unittest.mock import patch
    with patch("parsers.capture._assets.fetch_asset_bytes", new=_fake_fetch):
        from parsers.capture.orchestrator import run_capture
        outcome = await run_capture("https://x", "kb", "d1", _settings(), deps)

    assert outcome.profile.lottie_manifest is None
    assert not [a for a in outcome.profile.assets if a.kind == "lottie_json"]


@pytest.mark.asyncio
async def test_run_capture_lottie_cap_logs_truncation(caplog):
    raw = _full_quality_raw()
    raw["assets"]["lotties"] = [f"https://x/a{i}.json" for i in range(5)]
    page = _fake_page(raw)
    deps = _FakeDeps(page, {})
    cfg = _settings()
    cfg.capture_max_lotties = 2

    counter = {"n": 0}

    async def _fake_fetch(url, *, max_bytes):
        counter["n"] += 1
        return _lottie_json(f"L{counter['n']}"), "application/json"

    from unittest.mock import patch
    import logging
    with patch("parsers.capture._assets.fetch_asset_bytes", new=_fake_fetch):
        from parsers.capture.orchestrator import run_capture
        with caplog.at_level(logging.INFO):
            outcome = await run_capture("https://x", "kb", "d1", cfg, deps)

    assert len(outcome.profile.lottie_manifest["lotties"]) == 2
    assert counter["n"] == 2                       # only 2 sources fetched
    assert any("lottie cap" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_run_capture_lottie_includes_legacy_single_url():
    raw = _full_quality_raw()
    raw["assets"]["lottie"] = "https://x/legacy.json"   # only the legacy single field
    page = _fake_page(raw)
    deps = _FakeDeps(page, {})

    async def _fake_fetch(url, *, max_bytes):
        return _lottie_json("Legacy"), "application/json"

    from unittest.mock import patch
    with patch("parsers.capture._assets.fetch_asset_bytes", new=_fake_fetch):
        from parsers.capture.orchestrator import run_capture
        outcome = await run_capture("https://x", "kb", "d1", _settings(), deps)

    lm = outcome.profile.lottie_manifest
    assert lm is not None and lm["lotties"][0]["name"] == "Legacy"


# ── Phase 8: lottie mid-frame previews (best-effort, fake page) ─────────────
@pytest.mark.asyncio
async def test_run_capture_lottie_previews_rendered_when_page_renders():
    """A fake page that 'renders' (set_content ok, screenshot returns bytes) yields
    a lottie_preview AssetRef + preview path in the manifest entry."""
    raw = _full_quality_raw()
    raw["assets"]["lotties"] = ["https://x/a.json"]
    page = _fake_page(raw)
    # Default fake page: set_content/wait_for_function are AsyncMocks; screenshot
    # returns b"PNG". So a preview PNG is produced.
    page.set_content = AsyncMock()
    page.wait_for_function = AsyncMock()
    deps = _FakeDeps(page, {})

    async def _fake_fetch(url, *, max_bytes):
        return _lottie_json("Hero"), "application/json"

    from unittest.mock import patch
    with patch("parsers.capture._assets.fetch_asset_bytes", new=_fake_fetch):
        from parsers.capture.orchestrator import run_capture
        outcome = await run_capture("https://x", "kb", "d1", _settings(), deps)

    lm = outcome.profile.lottie_manifest
    assert lm["meta"]["previews"] == 1
    assert lm["lotties"][0]["preview"] == "assets/lottie/previews/animation-0-preview.png"
    prev_refs = [a for a in outcome.profile.assets if a.kind == "lottie_preview"]
    assert len(prev_refs) == 1
    assert prev_refs[0].storage_key.endswith(
        "assets/lottie/previews/animation-0-preview.png")


@pytest.mark.asyncio
async def test_run_capture_lottie_previews_degrade_when_render_fails(caplog):
    """A page whose screenshot render raises -> NO previews, manifest still emitted,
    and the degradation is LOGGED (no silent skip). Never aborts the capture."""
    raw = _full_quality_raw()
    raw["assets"]["lotties"] = ["https://x/a.json"]
    page = _fake_page(raw)
    page.set_content = AsyncMock()
    page.wait_for_function = AsyncMock()

    # Screenshot raises ONLY for the transparent lottie preview render (omit_background),
    # so ordinary screenshots earlier in the capture still succeed.
    async def _shot(*a, **k):
        if k.get("omit_background"):
            raise RuntimeError("render blocked")
        return b"PNG"
    page.screenshot = AsyncMock(side_effect=_shot)
    deps = _FakeDeps(page, {})

    async def _fake_fetch(url, *, max_bytes):
        return _lottie_json("Hero"), "application/json"

    from unittest.mock import patch
    import logging
    with patch("parsers.capture._assets.fetch_asset_bytes", new=_fake_fetch):
        from parsers.capture.orchestrator import run_capture
        with caplog.at_level(logging.INFO):
            outcome = await run_capture("https://x", "kb", "d1", _settings(), deps)

    lm = outcome.profile.lottie_manifest
    assert lm is not None                       # manifest still emitted
    assert lm["meta"]["previews"] == 0          # no previews
    assert not [a for a in outcome.profile.assets if a.kind == "lottie_preview"]
    assert "preview" not in lm["lotties"][0]
    assert any("lottie preview render skipped" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_run_capture_lottie_preview_skips_oversized(caplog):
    """A lottie whose JSON exceeds capture_max_lottie_bytes is stored but its preview
    is skipped (logged). Manifest entry has no preview."""
    raw = _full_quality_raw()
    raw["assets"]["lotties"] = ["https://x/big.json"]
    page = _fake_page(raw)
    page.set_content = AsyncMock()
    page.wait_for_function = AsyncMock()
    deps = _FakeDeps(page, {})
    cfg = _settings()
    cfg.capture_max_lottie_bytes = 50           # tiny cap -> preview skipped

    async def _fake_fetch(url, *, max_bytes):
        return _lottie_json("Big", layers=5), "application/json"

    from unittest.mock import patch
    import logging
    with patch("parsers.capture._assets.fetch_asset_bytes", new=_fake_fetch):
        from parsers.capture.orchestrator import run_capture
        with caplog.at_level(logging.INFO):
            outcome = await run_capture("https://x", "kb", "d1", cfg, deps)

    lm = outcome.profile.lottie_manifest
    assert lm["meta"]["previews"] == 0
    assert [a for a in outcome.profile.assets if a.kind == "lottie_json"]  # JSON stored
    assert any("lottie preview render skipped" in r.message for r in caplog.records)


# ── Phase 8: scroll + asset contact sheets (Pillow) ─────────────────────────
def _real_png(w=40, h=30, color=(120, 60, 30)) -> bytes:
    import io as _io
    from PIL import Image
    buf = _io.BytesIO()
    Image.new("RGB", (w, h), color).save(buf, format="PNG")
    return buf.getvalue()


@pytest.mark.asyncio
async def test_run_capture_builds_scroll_contact_sheet():
    raw = _full_quality_raw()
    page = _fake_page(raw)
    page.screenshot = AsyncMock(return_value=_real_png())   # decodable shots
    deps = _FakeDeps(page, {})

    from parsers.capture.orchestrator import run_capture
    outcome = await run_capture("https://x", "kb", "d1", _settings(), deps)

    sheets = [a for a in outcome.profile.assets if a.kind == "contact_sheet"]
    scroll = [a for a in sheets if "/screenshots/" in a.storage_key]
    assert scroll, "expected a scroll contact sheet"
    for a in scroll:
        assert a.storage_key.endswith(".jpg")
        assert "/screenshots/contact-sheet-" in a.storage_key


@pytest.mark.asyncio
async def test_run_capture_builds_asset_contact_sheet_from_catalog_images():
    raw = _full_quality_raw()
    page = _fake_page(raw)
    page.screenshot = AsyncMock(return_value=_real_png())
    # Two good-context catalog images so the asset sheet has content.
    page.evaluate = _eval_router_with_catalog(raw, [
        {"url": "https://x/a.png", "type": "Image", "contexts": ["img[src]"]},
        {"url": "https://x/b.png", "type": "Image", "contexts": ["img[src]"]},
    ])
    deps = _FakeDeps(page, {})
    cfg = _settings()
    cfg.capture_min_image_bytes = 1     # tiny PNGs pass the size gate

    async def _fake_fetch(url, *, max_bytes):
        return _real_png(color=(9, 9, 9)), "image/png"

    from unittest.mock import patch
    with patch("parsers.capture._assets.fetch_asset_bytes", new=_fake_fetch):
        from parsers.capture.orchestrator import run_capture
        outcome = await run_capture("https://x", "kb", "d1", cfg, deps)

    sheets = [a for a in outcome.profile.assets if a.kind == "contact_sheet"]
    asset_sheets = [a for a in sheets
                    if "/assets/contact-sheet-" in a.storage_key]
    assert asset_sheets, "expected an asset contact sheet"


def _eval_router_with_catalog(raw, catalog):
    """Like the default router but ASSET_CATALOG_JS returns `catalog`."""
    base = _eval_router(raw)

    async def _run(script, *args, **kwargs):
        if "assetMap" in script:
            return catalog
        return await base(script, *args, **kwargs)
    return _run


# ── Phase 8: SVG contact sheet (best-effort browser raster) ─────────────────
def _svg_png_b64(color=(2, 3, 4)) -> str:
    import base64, io as _io
    from PIL import Image
    buf = _io.BytesIO()
    Image.new("RGB", (20, 20), color).save(buf, format="PNG")
    return base64.b64encode(buf.getvalue()).decode()


def _raw_with_svgs():
    raw = _full_quality_raw()
    raw["svgs"] = [{"outerHTML": "<svg width='40'/>", "label": "logo", "isLogo": True,
                    "viewBox": "", "width": 40, "height": 40},
                   {"outerHTML": "<svg width='40'/>", "label": "menu", "isLogo": False,
                    "viewBox": "", "width": 40, "height": 40}]
    return raw


def _eval_router_svg(raw, raster):
    """Default router + SVG raster branch (_SVG_RASTER_JS -> `raster`, a value or
    a callable that may raise)."""
    base = _eval_router(raw)

    async def _run(script, *args, **kwargs):
        if "createObjectURL" in script:        # _SVG_RASTER_JS
            if callable(raster):
                return raster()
            return raster
        return await base(script, *args, **kwargs)
    return _run


@pytest.mark.asyncio
async def test_run_capture_builds_svg_contact_sheet_when_page_rasters():
    raw = _raw_with_svgs()
    page = _fake_page(raw)
    page.screenshot = AsyncMock(return_value=_real_png())
    page.evaluate = _eval_router_svg(raw, _svg_png_b64())
    deps = _FakeDeps(page, {})

    from parsers.capture.orchestrator import run_capture
    outcome = await run_capture("https://x", "kb", "d1", _settings(), deps)

    svg_sheets = [a for a in outcome.profile.assets
                  if a.kind == "contact_sheet" and "/assets/svgs/contact-sheet-" in a.storage_key]
    assert svg_sheets, "expected an SVG contact sheet"


@pytest.mark.asyncio
async def test_run_capture_svg_sheet_degrades_when_raster_unavailable(caplog):
    raw = _raw_with_svgs()
    page = _fake_page(raw)
    page.screenshot = AsyncMock(return_value=_real_png())

    def _boom():
        raise RuntimeError("no canvas in this env")
    page.evaluate = _eval_router_svg(raw, _boom)
    deps = _FakeDeps(page, {})

    import logging
    from parsers.capture.orchestrator import run_capture
    with caplog.at_level(logging.INFO):
        outcome = await run_capture("https://x", "kb", "d1", _settings(), deps)

    # No SVG sheet; scroll sheet (pure Pillow) still built; degradation logged.
    svg_sheets = [a for a in outcome.profile.assets
                  if a.kind == "contact_sheet" and "/assets/svgs/" in a.storage_key]
    assert svg_sheets == []
    assert any("svg contact sheet skipped" in r.message for r in caplog.records)
    scroll = [a for a in outcome.profile.assets
              if a.kind == "contact_sheet" and "/screenshots/" in a.storage_key]
    assert scroll, "scroll sheet must still build even when SVG raster degrades"
