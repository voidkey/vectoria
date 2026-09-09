"""PptxParser (python-pptx) — native .pptx text + image extraction.

Speaker notes (text and images) are the docling-drop gap the native
parser fills. Guards:
  * each slide emits as ``## Slide N: Title`` section
  * notes-slide text is nested as ``### Notes``
  * tables render as pipe-markdown
  * empty frames / blank paragraphs are dropped
  * body pictures surface in ``image_refs`` with
    ``slide_NNN_img_M`` naming
  * notes-slide pictures surface as ``slide_NNN_notes_img_M``
  * registry picks pptx-native over everything else
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
# Image path: body + notes pictures in a single slide walk (W6-6
# merged the old PptxImageExtractor back into the parser)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_parse_produces_body_picture_image_refs():
    """Body Picture shapes must surface in image_refs with the
    ``slide_NNN_img_M`` naming convention.
    """
    from pptx import Presentation
    from pptx.util import Inches
    from PIL import Image
    from parsers.pptx_parser import PptxParser

    png = io.BytesIO()
    Image.new("RGB", (8, 8), color="green").save(png, format="PNG")
    png.seek(0)

    prs = Presentation()
    slide = prs.slides.add_slide(prs.slide_layouts[6])  # blank
    slide.shapes.add_picture(png, Inches(1), Inches(1), Inches(1), Inches(1))
    out = io.BytesIO()
    prs.save(out)

    result = await PptxParser().parse(out.getvalue(), filename="deck.pptx")
    assert len(result.image_refs) == 1
    assert result.image_refs[0].name.startswith("slide_000_img_0")
    # Dimensions propagated from the slide geometry (1 inch × 96 DPI).
    assert result.image_refs[0].width == 96
    assert result.image_refs[0].height == 96


@pytest.mark.asyncio
async def test_parse_returns_empty_image_refs_on_deck_without_pictures():
    """The text-only fixture has no pictures, so image_refs is empty.
    Guards against accidental ghost refs.
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
async def test_parse_emits_page_count_equal_to_slide_count():
    """ParseResult.page_count carries the slide total so the worker can
    persist it without re-inspecting the deck. The two-slide fixture
    must report exactly 2 — pinning the contract that the count comes
    from ``len(prs.slides)`` and not from an off-by-one source.
    """
    from parsers.pptx_parser import PptxParser
    result = await PptxParser().parse(
        _build_pptx_with_notes(), filename="deck.pptx",
    )
    assert result.page_count == 2


@pytest.mark.asyncio
async def test_parse_handles_malformed_bytes():
    from parsers.pptx_parser import PptxParser
    result = await PptxParser().parse(b"not a pptx", filename="bad.pptx")
    assert result.content == ""
    assert result.image_refs == []


@pytest.mark.asyncio
async def test_parse_handles_picture_shape_without_embedded_image():
    """Regression: real-world decks contain Picture shapes whose
    blipFill has no r:embed attribute (linked picture, broken
    template asset, etc.). python-pptx's Picture.image is a
    @property that raises ``ValueError("no embedded image")`` when
    rId is None. Pre-fix, the picture walk used
    ``getattr(shape, 'image', None)`` which only swallows
    AttributeError, so this ValueError bubbled up and killed parse
    for the whole deck on the very first such slide.
    """
    from pptx import Presentation
    from pptx.util import Inches
    from pptx.oxml.ns import qn
    from PIL import Image
    from parsers.pptx_parser import PptxParser

    png = io.BytesIO()
    Image.new("RGB", (8, 8), color="red").save(png, format="PNG")
    png.seek(0)

    prs = Presentation()
    slide = prs.slides.add_slide(prs.slide_layouts[6])  # blank
    pic = slide.shapes.add_picture(
        png, Inches(1), Inches(1), Inches(1), Inches(1),
    )
    # Strip the r:embed attribute — same shape we hit in production
    # decks where the original asset is missing/linked.
    blip = pic._pic.find(qn("p:blipFill") + "/" + qn("a:blip"))
    del blip.attrib[qn("r:embed")]
    assert pic._pic.blip_rId is None
    buf = io.BytesIO()
    prs.save(buf)

    result = await PptxParser().parse(buf.getvalue(), filename="bad_pic.pptx")
    # No usable image, so no ImageRef. The whole deck still parses.
    assert result.image_refs == []
    assert "## Slide 1" in result.content


@pytest.mark.asyncio
async def test_parse_survives_unrecognized_shape_type():
    """Regression: WPS-flavored decks carry `<p:sp>` elements with no
    geometry element (no a:prstGeom / a:custGeom) and no txBox
    attribute. python-pptx classifies those lazily in
    ``Shape.shape_type`` and raises NotImplementedError("Shape
    instance of unrecognized shape type") — even though the shape's
    text frame is perfectly readable. Pre-fix, the slide walk compared
    ``shape.shape_type == 6`` for group detection, so one such shape
    killed text extraction for the whole deck (and markitdown, built
    on the same library, died identically → terminal empty_content).
    The isinstance-based walk must extract BOTH texts.
    """
    from lxml import etree
    from pptx import Presentation
    from pptx.util import Inches
    from parsers.pptx_parser import PptxParser

    prs = Presentation()
    slide = prs.slides.add_slide(prs.slide_layouts[6])  # blank
    tb = slide.shapes.add_textbox(Inches(1), Inches(1), Inches(4), Inches(1))
    tb.text_frame.text = "healthy textbox"

    # Clone the textbox sp, then strip the txBox attribute and all
    # geometry — reproducing the unclassifiable shape byte-for-byte.
    ns = {
        "a": "http://schemas.openxmlformats.org/drawingml/2006/main",
        "p": "http://schemas.openxmlformats.org/presentationml/2006/main",
    }
    sp = tb._element
    bad = etree.fromstring(etree.tostring(sp))
    cnv = bad.find(".//p:nvSpPr/p:cNvSpPr", ns)
    cnv.attrib.pop("txBox", None)
    for geom in bad.findall(".//a:prstGeom", ns) + bad.findall(".//a:custGeom", ns):
        geom.getparent().remove(geom)
    bad.find(".//a:t", ns).text = "text inside unclassifiable shape"
    sp.getparent().append(bad)

    buf = io.BytesIO()
    prs.save(buf)

    result = await PptxParser().parse(buf.getvalue(), filename="wps.pptx")
    assert "healthy textbox" in result.content
    assert "text inside unclassifiable shape" in result.content


@pytest.mark.asyncio
async def test_parse_recurses_into_group_shapes():
    """Group recursion moved from ``shape_type == 6`` to
    ``isinstance(shape, GroupShape)`` — pin that grouped text and
    grouped pictures still surface.
    """
    from pptx import Presentation
    from pptx.util import Inches
    from PIL import Image
    from parsers.pptx_parser import PptxParser

    png = io.BytesIO()
    Image.new("RGB", (8, 8), color="blue").save(png, format="PNG")
    png.seek(0)

    prs = Presentation()
    slide = prs.slides.add_slide(prs.slide_layouts[6])  # blank
    group = slide.shapes.add_group_shape()
    tb = group.shapes.add_textbox(Inches(1), Inches(1), Inches(2), Inches(1))
    tb.text_frame.text = "grouped text"
    group.shapes.add_picture(png, Inches(1), Inches(2), Inches(1), Inches(1))
    buf = io.BytesIO()
    prs.save(buf)

    result = await PptxParser().parse(buf.getvalue(), filename="grouped.pptx")
    assert "grouped text" in result.content
    assert len(result.image_refs) == 1


@pytest.mark.asyncio
async def test_parse_survives_pil_unidentifiable_image_blob():
    """Regression: ``Image.content_type`` sniffs the blob through PIL
    (content_type → ext → PIL.Image.open), which raises
    UnidentifiedImageError — an OSError — on payloads PIL can't read.
    The old ``except (AttributeError, ValueError)`` missed it, so one
    corrupt/exotic image killed the whole deck's extraction. Corrupt
    the media part in the saved zip to reproduce; the deck must still
    parse and the junk image must be skipped, not crash.
    """
    import zipfile
    from pptx import Presentation
    from pptx.util import Inches
    from PIL import Image
    from parsers.pptx_parser import PptxParser

    png = io.BytesIO()
    Image.new("RGB", (8, 8), color="red").save(png, format="PNG")
    png.seek(0)

    prs = Presentation()
    slide = prs.slides.add_slide(prs.slide_layouts[6])  # blank
    tb = slide.shapes.add_textbox(Inches(1), Inches(1), Inches(4), Inches(1))
    tb.text_frame.text = "text survives corrupt image"
    slide.shapes.add_picture(png, Inches(1), Inches(2), Inches(1), Inches(1))
    buf = io.BytesIO()
    prs.save(buf)

    # Rewrite the package with the media part replaced by junk bytes
    # no sniffer (PIL's or ours) can identify.
    out = io.BytesIO()
    with zipfile.ZipFile(buf) as zin, zipfile.ZipFile(out, "w") as zout:
        for item in zin.infolist():
            data = zin.read(item.filename)
            if item.filename.startswith("ppt/media/"):
                data = b"\x00JUNK-NOT-AN-IMAGE" * 4
            zout.writestr(item, data)

    result = await PptxParser().parse(out.getvalue(), filename="corrupt_img.pptx")
    assert "text survives corrupt image" in result.content
    assert result.image_refs == []


def test_sniff_image_mime_magic_bytes():
    """The fallback sniffer must recognize the formats we map (so a
    PIL-rejected but well-formed blob keeps its image) and return None
    on junk (so the caller skips it).
    """
    from parsers.pptx_parser import _sniff_image_mime

    emf = b"\x01\x00\x00\x00" + b"\x00" * 36 + b" EMF" + b"\x00" * 16
    assert _sniff_image_mime(emf) == "image/x-emf"
    assert _sniff_image_mime(b"\x00JUNK") is None
    assert _sniff_image_mime(b"\x89PNG\r\n\x1a\n") == "image/png"
    assert _sniff_image_mime(b"RIFF\x00\x00\x00\x00WEBPVP8 ") == "image/webp"


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
