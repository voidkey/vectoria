"""OcrParser: rapidocr-based image OCR.

Real OCR in tests is slow (~500ms for model load + inference) and
non-deterministic across CPU architectures, so we mock the engine
instead of running real inference. The unmocked path is exercised
end-to-end on deploy-host deploy smoke tests.
"""
import io
from unittest.mock import patch, MagicMock

import pytest


@pytest.fixture(autouse=True)
def _disable_isolation(monkeypatch):
    from config import get_settings
    monkeypatch.setattr(get_settings(), "parser_isolation", False)


# ---------------------------------------------------------------------------
# Engine metadata
# ---------------------------------------------------------------------------

def test_engine_name_and_supported_types():
    from parsers.ocr_parser import OcrParser
    assert OcrParser.engine_name == "ocr-native"
    assert ".png" in OcrParser.supported_types
    assert ".jpg" in OcrParser.supported_types
    assert ".webp" in OcrParser.supported_types


def test_is_available_with_dep_present():
    from parsers.ocr_parser import OcrParser
    assert OcrParser.is_available()


# ---------------------------------------------------------------------------
# Parse path (engine mocked — no real OCR inference in the test loop)
# ---------------------------------------------------------------------------

def _one_pixel_png() -> bytes:
    from PIL import Image
    buf = io.BytesIO()
    Image.new("RGB", (2, 2), "white").save(buf, format="PNG")
    return buf.getvalue()


@pytest.mark.asyncio
async def test_parse_returns_joined_lines():
    from parsers.ocr_parser import OcrParser

    fake_result = MagicMock()
    fake_result.txts = ("hello", "world")

    with patch("parsers.ocr_parser._get_engine") as mock_get:
        mock_get.return_value = MagicMock(return_value=fake_result)
        result = await OcrParser().parse(_one_pixel_png(), filename="scan.png")

    assert "hello" in result.content
    assert "world" in result.content
    assert result.title == "scan"


@pytest.mark.asyncio
async def test_parse_empty_txts_returns_empty_content():
    from parsers.ocr_parser import OcrParser

    fake_result = MagicMock()
    fake_result.txts = ()

    with patch("parsers.ocr_parser._get_engine") as mock_get:
        mock_get.return_value = MagicMock(return_value=fake_result)
        result = await OcrParser().parse(_one_pixel_png(), filename="blank.png")

    assert result.content == ""


@pytest.mark.asyncio
async def test_parse_returns_empty_image_refs():
    from parsers.ocr_parser import OcrParser

    fake_result = MagicMock()
    fake_result.txts = ("x",)

    with patch("parsers.ocr_parser._get_engine") as mock_get:
        mock_get.return_value = MagicMock(return_value=fake_result)
        result = await OcrParser().parse(_one_pixel_png(), filename="x.png")

    assert result.image_refs == []


@pytest.mark.asyncio
async def test_parse_handles_malformed_bytes():
    from parsers.ocr_parser import OcrParser
    result = await OcrParser().parse(b"not an image", filename="bad.png")
    assert result.content == ""


# ---------------------------------------------------------------------------
# Registry dispatch
# ---------------------------------------------------------------------------

def test_registry_picks_ocr_native_for_images():
    from parsers.registry import registry
    for ext in (".png", ".jpg", ".jpeg", ".tiff", ".bmp"):
        assert registry.auto_select(filename=f"x{ext}") == "ocr-native"
