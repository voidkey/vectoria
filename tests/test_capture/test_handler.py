import pytest
from unittest.mock import AsyncMock, MagicMock, patch


def _eval_router(raw):
    """page.evaluate stub: media-catalog scripts return an empty list (so the
    media path runs end-to-end without a real browser); the Phase-6 animation
    collector / shader read return empty structures; every other script
    (run_extract / design-styles / page.html) gets the canned ``raw`` dict."""
    async def _run(script, *args, **kwargs):
        if "assetMap" in script:            # ASSET_CATALOG_JS
            return []
        if "nearestCaption" in script:      # VIDEO_DESCRIPTORS_JS
            return []
        if "getAnimations" in script:       # COLLECT_ANIMATIONS_JS
            return {"webAnimations": [], "cssDeclarations": [],
                    "scrollTargets": [], "canvasCount": 0}
        if "__capturedShaders" in script:   # collect_shaders read
            return []
        return raw
    return _run


def _wire_cdp(page):
    """Give a fake page a Playwright-shaped CDP session (sync .on; async
    .send/.detach) so start_cdp_animation_capture exercises without leaking an
    un-awaited coroutine warning. Returns the page for chaining."""
    session = MagicMock()
    session.on = MagicMock()
    session.send = AsyncMock()
    session.detach = AsyncMock()
    page.add_init_script = AsyncMock()
    page.context.new_cdp_session = AsyncMock(return_value=session)
    return page


@pytest.mark.asyncio
async def test_handle_capture_happy_path():
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
                 "full_text": "Hello world. Full page body copy here."},
        "assets": {"logo": None, "hero": None, "og_image": None, "favicon": None,
                   "video": None, "lottie": None},
        "motion": {"libraries": ["gsap"], "has_video_background": False, "has_canvas": True},
    }
    updates = {}

    async def fake_update(doc_id, **kw):
        updates.update(kw)

    ctx = AsyncMock()
    page = AsyncMock()
    page.goto = AsyncMock()
    page.evaluate = _eval_router(raw)
    page.screenshot = AsyncMock(return_value=b"PNG")
    page.viewport_size = {"width": 1280, "height": 800}
    page.wait_for_timeout = AsyncMock()
    ctx.new_page = AsyncMock(return_value=_wire_cdp(page))

    psession = MagicMock()
    psession.return_value.__aenter__ = AsyncMock(return_value=ctx)
    psession.return_value.__aexit__ = AsyncMock(return_value=False)

    settings = MagicMock(vision_base_url="", capture_render_timeout=30.0,
                         capture_settle_ms=1500, capture_max_screenshots=10,
                         capture_viewport_width=1280, capture_viewport_height=800,
                         capture_scroll_step_frac=0.8, capture_scroll_step_ms=0,
                         capture_scroll_max_steps=0, capture_networkidle_timeout=0,
                         capture_img_wait_ms=0, capture_section_settle_ms=0,
                         capture_max_asset_bytes=1000, capture_max_screenshot_height=20000,
                         capture_color_delta_e=10.0,
                         capture_asset_catalog_cap=200, capture_video_cap=20)

    with (
        patch("worker.handlers.update_doc", new=fake_update),
        patch("worker.handlers.reresolve_and_check_ssrf", new=AsyncMock()),
        patch("worker.handlers.parse_session", new=psession),
        patch("worker.handlers.stream_upload_and_store_refs", new=AsyncMock(return_value=2)),
        patch("worker.handlers.get_settings", return_value=settings),
        patch("worker.handlers.get_storage", new=AsyncMock(return_value=AsyncMock())),
        patch("worker.handlers.enqueue", new=AsyncMock()),
        patch("worker.handlers._capture_hydrate_image_ids", new=AsyncMock(return_value={
            "screenshot-above_fold.png": ("s1", "images/kb/d1/a.png"),
            "screenshot-full_page.png": ("s2", "images/kb/d1/b.png"),
        })),
    ):
        from worker.handlers import handle_capture
        await handle_capture({"doc_id": "d1", "kb_id": "kb", "url": "https://x"})

    assert updates.get("status") == "completed"
    assert updates.get("index_status") == "skipped"
    assert isinstance(updates.get("profile"), dict)
    prof = updates["profile"]
    assert prof["text"]["headline"] == "Hello"
    assert prof["text"]["full_text"] == "Hello world. Full page body copy here."
    assert prof["motion_hints"]["libraries"] == ["gsap"]
    assert {c["role"] for c in prof["colors"]} >= {"background", "text"}
    # 2 screenshots captured (above_fold + full_page), no image assets
    assert len(prof["screenshots"]) == 2
    assert updates["image_status"] == "completed"


@pytest.mark.asyncio
async def test_handle_capture_no_screenshots_no_assets_image_status_none():
    """If nothing was captured, image_status is none (not completed)."""
    raw = {
        "final_url": "https://x", "colors": {"samples": [], "css_vars": {}, "theme_color": None},
        "fonts": {"display": {}, "body": {}, "face_srcs": {}},
        "spacing": {}, "sections": [], "text": {},
        "assets": {"logo": None, "hero": None, "og_image": None, "favicon": None,
                   "video": None, "lottie": None},
        "motion": {},
    }
    updates = {}

    async def fake_update(doc_id, **kw):
        updates.update(kw)

    ctx, page = AsyncMock(), AsyncMock()
    page.goto = AsyncMock(); page.evaluate = _eval_router(raw)
    page.screenshot = AsyncMock(return_value=b""); page.wait_for_timeout = AsyncMock()
    page.viewport_size = {"width": 1280, "height": 800}
    ctx.new_page = AsyncMock(return_value=_wire_cdp(page))
    psession = MagicMock()
    psession.return_value.__aenter__ = AsyncMock(return_value=ctx)
    psession.return_value.__aexit__ = AsyncMock(return_value=False)
    settings = MagicMock(vision_base_url="", capture_render_timeout=30.0,
                         capture_settle_ms=0, capture_max_screenshots=0,
                         capture_viewport_width=1280, capture_viewport_height=800,
                         capture_scroll_step_frac=0.8, capture_scroll_step_ms=0,
                         capture_scroll_max_steps=0, capture_networkidle_timeout=0,
                         capture_img_wait_ms=0, capture_section_settle_ms=0,
                         capture_max_asset_bytes=1000, capture_max_screenshot_height=20000,
                         capture_color_delta_e=10.0,
                         capture_asset_catalog_cap=200, capture_video_cap=20)
    with (
        patch("worker.handlers.update_doc", new=fake_update),
        patch("worker.handlers.reresolve_and_check_ssrf", new=AsyncMock()),
        patch("worker.handlers.parse_session", new=psession),
        patch("worker.handlers.stream_upload_and_store_refs", new=AsyncMock(return_value=0)),
        patch("worker.handlers.get_settings", return_value=settings),
        patch("worker.handlers.get_storage", new=AsyncMock(return_value=AsyncMock())),
        patch("worker.handlers.enqueue", new=AsyncMock()),
        patch("worker.handlers._capture_hydrate_image_ids", new=AsyncMock(return_value={})),
    ):
        from worker.handlers import handle_capture
        await handle_capture({"doc_id": "d1", "kb_id": "kb", "url": "https://x"})
    assert updates["image_status"] == "none"


@pytest.mark.asyncio
async def test_handle_capture_nonraster_assets_skip_vision():
    """Raster hero → vision batch; SVG logo + .ico favicon → no-vision batch
    (vision model can't decode svg/ico, so describing them always fails)."""
    raw = {
        "final_url": "https://x", "colors": {"samples": [], "css_vars": {}, "theme_color": None},
        "fonts": {"display": {}, "body": {}, "face_srcs": {}},
        "spacing": {}, "sections": [], "text": {},
        "assets": {"logo": "https://x/logo.svg", "hero": "https://x/hero.png",
                   "favicon": "https://x/f.ico", "og_image": None,
                   "video": None, "lottie": None},
        "motion": {},
    }
    calls = []

    async def fake_upload(refs, **kw):
        calls.append((kw["vision_configured"], [r.name for r in refs]))
        return len(refs)

    async def fake_fetch(url, **kw):
        if url.endswith(".svg"):
            return (b"<svg/>", "image/svg+xml")
        if url.endswith(".ico"):
            return (b"x", "image/x-icon")
        return (b"x", "image/png")

    ctx, page = AsyncMock(), AsyncMock()
    page.goto = AsyncMock(); page.evaluate = _eval_router(raw)
    page.screenshot = AsyncMock(return_value=b"PNG"); page.wait_for_timeout = AsyncMock()
    page.viewport_size = {"width": 1280, "height": 800}
    ctx.new_page = AsyncMock(return_value=_wire_cdp(page))
    psession = MagicMock()
    psession.return_value.__aenter__ = AsyncMock(return_value=ctx)
    psession.return_value.__aexit__ = AsyncMock(return_value=False)
    settings = MagicMock(vision_base_url="http://vision", capture_render_timeout=30.0,
                         capture_settle_ms=0, capture_max_screenshots=0,
                         capture_viewport_width=1280, capture_viewport_height=800,
                         capture_scroll_step_frac=0.8, capture_scroll_step_ms=0,
                         capture_scroll_max_steps=0, capture_networkidle_timeout=0,
                         capture_img_wait_ms=0, capture_section_settle_ms=0,
                         capture_max_asset_bytes=1000, capture_max_screenshot_height=20000,
                         capture_color_delta_e=10.0,
                         capture_asset_catalog_cap=200, capture_video_cap=20)
    with (
        patch("worker.handlers.update_doc", new=AsyncMock()),
        patch("worker.handlers.reresolve_and_check_ssrf", new=AsyncMock()),
        patch("worker.handlers.parse_session", new=psession),
        patch("worker.handlers.stream_upload_and_store_refs", new=fake_upload),
        patch("parsers.capture._assets.fetch_asset_bytes", new=fake_fetch),
        patch("worker.handlers.get_settings", return_value=settings),
        patch("worker.handlers.get_storage", new=AsyncMock(return_value=AsyncMock())),
        patch("worker.handlers.enqueue", new=AsyncMock()),
        patch("worker.handlers._capture_hydrate_image_ids", new=AsyncMock(return_value={})),
    ):
        from worker.handlers import handle_capture
        await handle_capture({"doc_id": "d1", "kb_id": "kb", "url": "https://x"})

    vision_names = [n for v, names in calls if v is True for n in names]
    novision_names = [n for v, names in calls if v is False for n in names]
    assert "hero.png" in vision_names                 # raster asset IS described
    assert "logo.svg" not in vision_names             # svg logo NOT sent to vision
    assert "logo.svg" in novision_names
    assert "favicon.ico" in novision_names


@pytest.mark.asyncio
async def test_handle_capture_ssrf_reject_marks_failed_no_retry():
    """SSRF/URL rejection is terminal: doc marked failed, exception swallowed
    (no queue-retry storm on a private-IP URL)."""
    from api.errors import AppError, ErrorCode
    updates = {}

    async def fake_update(doc_id, **kw):
        updates.update(kw)

    with (
        patch("worker.handlers.update_doc", new=fake_update),
        patch("worker.handlers.reresolve_and_check_ssrf",
              new=AsyncMock(side_effect=AppError(400, ErrorCode.INVALID_URL, "private"))),
        patch("worker.handlers.get_settings", return_value=MagicMock(vision_base_url="")),
    ):
        from worker.handlers import handle_capture
        await handle_capture({"doc_id": "d1", "kb_id": "kb", "url": "http://127.0.0.1"})

    assert updates["status"] == "failed"
    assert updates["error_type"] == "url_fetch_error"


@pytest.mark.asyncio
async def test_handle_capture_unexpected_error_marks_failed_and_reraises():
    """Unexpected errors mark failed AND re-raise so the queue retries."""
    updates = {}

    async def fake_update(doc_id, **kw):
        updates.update(kw)

    with (
        patch("worker.handlers.update_doc", new=fake_update),
        patch("worker.handlers.reresolve_and_check_ssrf",
              new=AsyncMock(side_effect=RuntimeError("boom"))),
        patch("worker.handlers.get_settings", return_value=MagicMock(vision_base_url="")),
    ):
        from worker.handlers import handle_capture
        with pytest.raises(RuntimeError):
            await handle_capture({"doc_id": "d1", "kb_id": "kb", "url": "https://x"})

    assert updates["status"] == "failed"
    assert updates["error_type"] == "parse_error"


@pytest.mark.asyncio
async def test_handle_capture_fractional_section_gap_does_not_crash():
    """Regression: section_gaps are subpixel floats from getBoundingClientRect;
    Spacing.section_gap is an int field, so a fractional min() used to raise a
    Pydantic ValidationError and fail the whole capture. It must be rounded."""
    raw = {
        "final_url": "https://x/final",
        "colors": {"samples": [{"color": "#0b0b0f", "area": 400000, "text": False}],
                   "css_vars": {}, "theme_color": None},
        "fonts": {"display": {"family": "Inter", "weight": 700, "selector": "h1"},
                  "body": {"family": "Inter", "weight": 400, "selector": "p"},
                  "face_srcs": {}},
        # section_gaps intentionally fractional; container_max_width stays int.
        "spacing": {"margins": [8, 16], "paddings": [16], "radii": [8],
                    "container_max_width": 1200, "section_gaps": [96.7, 150.2]},
        "sections": [{"index": 0, "heading": "Hero", "classNames": [], "bg": "#0b0b0f",
                      "rect": {"y": 0, "height": 600}}],
        "text": {"headline": "Hello", "tagline": "world", "ctas": ["Start"],
                 "full_text": "Hello world. Full page body copy here."},
        "assets": {"logo": None, "hero": None, "og_image": None, "favicon": None,
                   "video": None, "lottie": None},
        "motion": {"libraries": [], "has_video_background": False, "has_canvas": False},
    }
    updates = {}

    async def fake_update(doc_id, **kw):
        updates.update(kw)

    ctx, page = AsyncMock(), AsyncMock()
    page.goto = AsyncMock(); page.evaluate = _eval_router(raw)
    page.screenshot = AsyncMock(return_value=b"PNG"); page.wait_for_timeout = AsyncMock()
    page.viewport_size = {"width": 1280, "height": 800}
    ctx.new_page = AsyncMock(return_value=_wire_cdp(page))
    psession = MagicMock()
    psession.return_value.__aenter__ = AsyncMock(return_value=ctx)
    psession.return_value.__aexit__ = AsyncMock(return_value=False)
    settings = MagicMock(vision_base_url="", capture_render_timeout=30.0,
                         capture_settle_ms=0, capture_max_screenshots=0,
                         capture_viewport_width=1280, capture_viewport_height=800,
                         capture_scroll_step_frac=0.8, capture_scroll_step_ms=0,
                         capture_scroll_max_steps=0, capture_networkidle_timeout=0,
                         capture_img_wait_ms=0, capture_section_settle_ms=0,
                         capture_max_asset_bytes=1000, capture_max_screenshot_height=20000,
                         capture_color_delta_e=10.0,
                         capture_asset_catalog_cap=200, capture_video_cap=20)
    with (
        patch("worker.handlers.update_doc", new=fake_update),
        patch("worker.handlers.reresolve_and_check_ssrf", new=AsyncMock()),
        patch("worker.handlers.parse_session", new=psession),
        patch("worker.handlers.stream_upload_and_store_refs", new=AsyncMock(return_value=0)),
        patch("worker.handlers.get_settings", return_value=settings),
        patch("worker.handlers.get_storage", new=AsyncMock(return_value=AsyncMock())),
        patch("worker.handlers.enqueue", new=AsyncMock()),
        patch("worker.handlers._capture_hydrate_image_ids", new=AsyncMock(return_value={})),
    ):
        from worker.handlers import handle_capture
        await handle_capture({"doc_id": "d1", "kb_id": "kb", "url": "https://x"})

    assert updates.get("status") == "completed"       # did NOT crash on the float
    gap = updates["profile"]["spacing"]["section_gap"]
    assert gap == 97 and isinstance(gap, int)          # round(96.7)
