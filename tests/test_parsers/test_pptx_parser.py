"""PptxParser (python-pptx) — native .pptx text extraction with
speaker-notes support.

Speaker notes are the docling-drop gap this parser fills. Guards:
  * each slide emits as ``## Slide N: Title`` section
  * notes-slide text is nested as ``### Notes``
  * tables render as pipe-markdown
  * empty frames / blank paragraphs are dropped
  * image_refs is always empty (PptxImageExtractor owns that path)
  * registry picks pptx-native over docling
"""
import io

import pytest


def _build_pptx_with_notes() -> bytes:
    """Build a 2-slide deck with title, body, and speaker notes."""
    from pptx import Presentation
    from pptx.util import Inches

    prs = Presentation()
    prs.core_properties.title = "Test Deck"

    # Slide 0: title + body + notes
    s0 = prs.slides.add_slide(prs.slide_layouts[1])  # title + content
    s0.shapes.title.text = "Opening"
    s0.placeholders[1].text = "Body paragraph one."
    s0.notes_slide.notes_text_frame.text = (
        "Presenter says: start with a question."
    )

    # Slide 1: title + body, no notes
    s1 = prs.slides.add_slide(prs.slide_layouts[1])
    s1.shapes.title.text = "Closing"
    s1.placeholders[1].text = "Bullet body"

    buf = io.BytesIO()
    prs.save(buf)
    return buf.getvalue()


@pytest.fixture(autouse=True)
def _disable_isolation(monkeypatch):
    from config import get_settings
    monkeypatch.setattr(get_settings(), "parser_isolation", False)


# ---------------------------------------------------------------------------
# Engine metadata
# ---------------------------------------------------------------------------

def test_engine_name_and_supported_types():
    from parsers.pptx_parser import PptxParser
    assert PptxParser.engine_name == "pptx-native"
    assert ".pptx" in PptxParser.supported_types
    assert ".ppt" in PptxParser.supported_types


def test_is_available_with_dep_present():
    from parsers.pptx_parser import PptxParser
    assert PptxParser.is_available()


# ---------------------------------------------------------------------------
# Text extraction
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_parse_emits_slide_sections_with_titles():
    from parsers.pptx_parser import PptxParser
    refs_bytes = _build_pptx_with_notes()
    result = await PptxParser().parse(refs_bytes, filename="deck.pptx")

    assert "# Test Deck" in result.content
    assert "## Slide 1: Opening" in result.content
    assert "## Slide 2: Closing" in result.content


@pytest.mark.asyncio
async def test_parse_includes_speaker_notes_under_notes_heading():
    """The signature capability over docling: notes text surfaces."""
    from parsers.pptx_parser import PptxParser
    result = await PptxParser().parse(
        _build_pptx_with_notes(), filename="deck.pptx",
    )

    assert "### Notes" in result.content
    assert "start with a question" in result.content
    # Only slide 0 had notes — heading should appear exactly once.
    assert result.content.count("### Notes") == 1


@pytest.mark.asyncio
async def test_parse_includes_body_paragraphs():
    from parsers.pptx_parser import PptxParser
    result = await PptxParser().parse(
        _build_pptx_with_notes(), filename="deck.pptx",
    )
    assert "Body paragraph one" in result.content
    assert "Bullet body" in result.content


@pytest.mark.asyncio
async def test_parse_title_from_core_properties():
    from parsers.pptx_parser import PptxParser
    result = await PptxParser().parse(
        _build_pptx_with_notes(), filename="x.pptx",
    )
    assert result.title == "Test Deck"


@pytest.mark.asyncio
async def test_parse_title_falls_back_to_filename_when_no_core_title():
    from parsers.pptx_parser import PptxParser
    from pptx import Presentation

    prs = Presentation()
    prs.slides.add_slide(prs.slide_layouts[5])
    buf = io.BytesIO()
    prs.save(buf)

    result = await PptxParser().parse(buf.getvalue(), filename="untitled.pptx")
    assert result.title == "untitled"


# ---------------------------------------------------------------------------
# Image path: always empty — the PptxImageExtractor plugin owns it
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_parse_returns_empty_image_refs():
    """PptxImageExtractor (W4-c plugin) fills image_refs via the W4-b
    override seam. Having the parser also emit them would be
    duplicated slide-walking work.
    """
    from parsers.pptx_parser import PptxParser
    result = await PptxParser().parse(
        _build_pptx_with_notes(), filename="deck.pptx",
    )
    assert result.image_refs == []


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_parse_handles_malformed_bytes():
    from parsers.pptx_parser import PptxParser
    result = await PptxParser().parse(b"not a pptx", filename="bad.pptx")
    assert result.content == ""
    assert result.image_refs == []


# ---------------------------------------------------------------------------
# Registry dispatch
# ---------------------------------------------------------------------------

def test_registry_picks_pptx_native_over_docling():
    from parsers.registry import registry
    engine = registry.auto_select(filename="deck.pptx")
    assert engine == "pptx-native", (
        f"expected pptx-native, got {engine!r}; "
        "_EXT_PREFERENCE ordering probably regressed"
    )
