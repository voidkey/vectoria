import pytest
from unittest.mock import AsyncMock

from parsers.capture._screenshots import capture_screenshots


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
