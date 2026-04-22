"""PdfiumParser: pypdfium2-based PDF text fallback.

Guards the W6-2 replacement of docling's PDF fallback slot:
  * registry picks mineru > pdfium > markitdown for .pdf
  * supported_types is ``.pdf`` only
  * malformed bytes → empty content, no raise
  * basic text-extraction path round-trips
"""
import io

import pytest


@pytest.fixture(autouse=True)
def _disable_isolation(monkeypatch):
    from config import get_settings
    monkeypatch.setattr(get_settings(), "parser_isolation", False)


# ---------------------------------------------------------------------------
# Engine metadata
# ---------------------------------------------------------------------------

def test_engine_name_and_supported_types():
    from parsers.pdfium_parser import PdfiumParser
    assert PdfiumParser.engine_name == "pdfium"
    assert PdfiumParser.supported_types == [".pdf"]


def test_is_available_with_dep_present():
    from parsers.pdfium_parser import PdfiumParser
    assert PdfiumParser.is_available()


# ---------------------------------------------------------------------------
# Text extraction
# ---------------------------------------------------------------------------

def _minimal_pdf_with_text(text: str) -> bytes:
    """Build a tiny valid PDF with a single page carrying ``text``
    using pypdfium2 is awkward (it's read-only). Use reportlab if
    available; otherwise skip the test.
    """
    try:
        from reportlab.pdfgen import canvas
    except ImportError:
        pytest.skip("reportlab not installed — skip end-to-end PDF test")

    buf = io.BytesIO()
    c = canvas.Canvas(buf)
    c.drawString(100, 750, text)
    c.save()
    return buf.getvalue()


@pytest.mark.asyncio
async def test_parse_extracts_page_text():
    from parsers.pdfium_parser import PdfiumParser
    pdf = _minimal_pdf_with_text("hello pdfium world")
    result = await PdfiumParser().parse(pdf, filename="sample.pdf")
    assert "hello pdfium world" in result.content
    assert "## Page 1" in result.content
    assert result.title == "sample"


@pytest.mark.asyncio
async def test_parse_title_from_filename_stem():
    from parsers.pdfium_parser import PdfiumParser
    pdf = _minimal_pdf_with_text("x")
    result = await PdfiumParser().parse(pdf, filename="quarterly_report.pdf")
    assert result.title == "quarterly_report"


@pytest.mark.asyncio
async def test_parse_returns_empty_image_refs():
    from parsers.pdfium_parser import PdfiumParser
    pdf = _minimal_pdf_with_text("x")
    result = await PdfiumParser().parse(pdf, filename="x.pdf")
    assert result.image_refs == []


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_parse_handles_malformed_bytes():
    from parsers.pdfium_parser import PdfiumParser
    result = await PdfiumParser().parse(b"not a pdf", filename="bad.pdf")
    assert result.content == ""
    assert result.image_refs == []


# ---------------------------------------------------------------------------
# Registry dispatch
# ---------------------------------------------------------------------------

def test_registry_prefers_pdfium_when_mineru_unavailable(monkeypatch):
    """The chain: mineru (VLM layout) → pdfium (plain text) → markitdown.
    When mineru is unconfigured, pdfium wins (not markitdown).
    """
    from parsers.registry import registry
    from parsers.mineru_parser import MinerUParser
    # Force mineru unavailable regardless of test env config.
    monkeypatch.setattr(MinerUParser, "is_available", classmethod(lambda cls: False))
    engine = registry.auto_select(filename="doc.pdf")
    assert engine == "pdfium"


def test_registry_pdf_chain_includes_pdfium():
    """Even when mineru IS available (test env has a URL), the chain
    must list pdfium as the next fallback after mineru.
    """
    from parsers.registry import _EXT_PREFERENCE
    chain = _EXT_PREFERENCE[".pdf"]
    assert chain[0] == "mineru"
    assert "pdfium" in chain
    assert chain.index("pdfium") < chain.index("markitdown")
