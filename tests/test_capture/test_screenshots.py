import pytest
from unittest.mock import AsyncMock

from parsers.capture._screenshots import autoscroll_page, capture_screenshots


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
async def test_autoscroll_walks_down_and_returns_to_top():
    page = AsyncMock()
    page.viewport_size = {"width": 1280, "height": 800}
    page.evaluate = AsyncMock(return_value=2000)   # scrollHeight; scrollTo returns ignored
    await autoscroll_page(page, step_frac=0.8, step_ms=0, max_steps=60)
    calls = [c.args[0] for c in page.evaluate.await_args_list]
    scroll_ys = [c.args[1] for c in page.evaluate.await_args_list if len(c.args) > 1]
    assert scroll_ys[0] == 0                        # starts at top
    assert scroll_ys == sorted(scroll_ys)           # monotonic walk down
    assert "scrollTo(0, 0)" in calls[-1]            # ends back at the top


@pytest.mark.asyncio
async def test_autoscroll_disabled_when_max_steps_zero():
    page = AsyncMock()
    await autoscroll_page(page, step_frac=0.8, step_ms=0, max_steps=0)
    page.evaluate.assert_not_awaited()


@pytest.mark.asyncio
async def test_autoscroll_swallows_page_errors():
    page = AsyncMock()
    page.viewport_size = {"width": 1280, "height": 800}
    page.evaluate = AsyncMock(side_effect=RuntimeError("navigated away"))
    await autoscroll_page(page, step_frac=0.8, step_ms=0, max_steps=5)  # no raise
