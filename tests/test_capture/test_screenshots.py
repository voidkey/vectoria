import pytest
from unittest.mock import AsyncMock

from parsers.capture._screenshots import capture_screenshots, prepare_page


@pytest.mark.asyncio
async def test_capture_screenshots_caps_and_labels():
    page = AsyncMock()
    page.screenshot = AsyncMock(return_value=b"PNG")
    page.viewport_size = {"width": 1280, "height": 800}
    sections = [{"index": i, "rect": {"y": i * 500, "height": 480}} for i in range(20)]
    shots = await capture_screenshots(page, sections, max_screenshots=5, max_height=20000)
    assert len(shots) == 5                       # above_fold + full_page + 3 sections
    assert shots[0]["kind"] == "above_fold"
    assert shots[1]["kind"] == "full_page"
    assert shots[2]["kind"] == "section"
    assert all(s["bytes"] == b"PNG" for s in shots)


@pytest.mark.asyncio
async def test_capture_sections_scroll_into_view_before_shot():
    """Each section is scrolled to its top before being screenshotted."""
    page = AsyncMock()
    page.screenshot = AsyncMock(return_value=b"PNG")
    page.viewport_size = {"width": 1280, "height": 800}
    sections = [{"index": 0, "rect": {"y": 1500, "height": 600}}]
    await capture_screenshots(page, sections, max_screenshots=10, max_height=20000,
                              section_settle_ms=0)
    # the section's y (1500) was passed to a scrollTo evaluate call
    scroll_ys = [c.args[1] for c in page.evaluate.await_args_list if len(c.args) > 1]
    assert 1500 in scroll_ys


@pytest.mark.asyncio
async def test_prepare_page_runs_walk_when_enabled():
    page = AsyncMock()
    page.viewport_size = {"width": 1280, "height": 800}
    await prepare_page(page, step_frac=0.8, step_ms=0, max_steps=60, img_wait_ms=0)
    page.evaluate.assert_awaited_once()
    js, arg = page.evaluate.await_args.args
    assert "scrollTo" in js and "document.fonts" in js   # walk + font wait in one pass
    assert arg["maxSteps"] == 60


@pytest.mark.asyncio
async def test_prepare_page_disabled_when_max_steps_zero():
    page = AsyncMock()
    await prepare_page(page, step_frac=0.8, step_ms=0, max_steps=0, img_wait_ms=0)
    page.evaluate.assert_not_awaited()


@pytest.mark.asyncio
async def test_prepare_page_swallows_errors():
    page = AsyncMock()
    page.evaluate = AsyncMock(side_effect=RuntimeError("navigated away"))
    await prepare_page(page, step_frac=0.8, step_ms=0, max_steps=5, img_wait_ms=0)  # no raise
