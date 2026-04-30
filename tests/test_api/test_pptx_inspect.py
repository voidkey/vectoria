"""Unit tests for ``api.pptx_inspect.count_pptx_slides``.

Counts slides by listing the OPC zip's ``ppt/slides/slideN.xml``
entries — no XML parse, no python-pptx import. Must be fail-open
on bad zips, and must NOT count slide layouts / masters / rels
(which also live under ``ppt/`` and would inflate the count).
"""
import io
import zipfile

from api.pptx_inspect import count_pptx_slides


def _make_pptx(num_slides: int, *, with_decoys: bool = True) -> bytes:
    """Hand-roll a minimal OPC zip with N slide parts.

    Avoids a heavy python-pptx dep just for tests. We deliberately
    add slide-layout / slide-master / rels entries so the regex
    counter is forced to discriminate against them — a naive
    ``startswith("ppt/slides/")`` match would over-count.
    """
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("[Content_Types].xml", "<x/>")
        for i in range(1, num_slides + 1):
            zf.writestr(f"ppt/slides/slide{i}.xml", "<x/>")
            if with_decoys:
                # Sibling rels file — must NOT be counted as a slide.
                zf.writestr(f"ppt/slides/_rels/slide{i}.xml.rels", "<x/>")
        if with_decoys:
            # Layouts + masters live under ppt/ but aren't slides.
            zf.writestr("ppt/slideLayouts/slideLayout1.xml", "<x/>")
            zf.writestr("ppt/slideLayouts/slideLayout2.xml", "<x/>")
            zf.writestr("ppt/slideMasters/slideMaster1.xml", "<x/>")
    return buf.getvalue()


def test_count_pptx_slides_returns_count():
    raw = _make_pptx(7)

    assert count_pptx_slides(raw) == 7


def test_count_pptx_slides_ignores_layouts_masters_and_rels():
    """The directory ``ppt/slides/`` also contains ``_rels/`` files,
    and ``ppt/`` siblings include layouts + masters. None of those
    are slides — over-counting them would have the gate reject
    valid 100-slide decks at a 200-slide threshold once a few
    layouts push past the limit.
    """
    raw = _make_pptx(3, with_decoys=True)

    # Exactly 3, even though the zip has more ``ppt/...`` entries.
    assert count_pptx_slides(raw) == 3


def test_count_pptx_slides_handles_large_decks():
    raw = _make_pptx(500, with_decoys=False)

    assert count_pptx_slides(raw) == 500


def test_count_pptx_slides_returns_none_on_bad_zip():
    """Fail-open: malformed zip must NOT raise. Lets the parser chain
    surface a real diagnostic instead of a 500 from the upload path.
    """
    assert count_pptx_slides(b"not a zip file") is None


def test_count_pptx_slides_returns_none_on_empty_bytes():
    assert count_pptx_slides(b"") is None
