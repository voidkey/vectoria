import pytest
from unittest.mock import AsyncMock

from parsers.capture._extract import EXTRACT_JS, run_extract


def test_extract_js_is_string():
    assert isinstance(EXTRACT_JS, str)
    assert "getComputedStyle" in EXTRACT_JS
    assert "samples" in EXTRACT_JS
    assert "innerText" in EXTRACT_JS      # visible full-text capture


@pytest.mark.asyncio
async def test_run_extract_passes_through():
    page = AsyncMock()
    page.evaluate = AsyncMock(return_value={"final_url": "https://x/final"})
    out = await run_extract(page)
    page.evaluate.assert_awaited_once()
    assert out["final_url"] == "https://x/final"
