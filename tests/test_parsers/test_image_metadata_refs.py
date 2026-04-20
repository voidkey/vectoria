"""extract_metadata_into_refs — the bridge between parser output and
upload pipeline. Must:
  * fill alt/context/section_title by matching ref.name against markdown
  * fill width/height for refs that don't have them (materializes bytes)
  * drop refs smaller than MIN_DIMENSION *and release them* so their
    factory-captured state is eligible for GC
  * preserve parser-provided dims without re-materializing
"""
import pytest

from parsers.image_metadata import extract_metadata_into_refs
from parsers.image_ref import ImageRef


def _ref(name: str, width=None, height=None, payload: bytes = b"x") -> ImageRef:
    return ImageRef(
        name=name, mime="image/png",
        width=width, height=height,
        _factory=lambda d=payload: d,
    )


def test_matches_markdown_reference_and_fills_alt_context():
    markdown = (
        "# Title\n\n"
        "Some text before.\n\n"
        "![the caption](img_0001.png)\n\n"
        "Some text after."
    )
    refs = [_ref("img_0001.png", width=300, height=300)]

    out = extract_metadata_into_refs(markdown, refs)

    assert len(out) == 1
    r = out[0]
    assert r.alt == "the caption"
    assert "Some text before" in r.context
    assert "Some text after" in r.context
    assert r.section_title == "Title"


def test_filters_out_small_images_and_releases_them():
    refs = [
        _ref("big.png", width=500, height=500),
        _ref("tiny.png", width=10, height=10),
    ]
    out = extract_metadata_into_refs("![](big.png)\n![](tiny.png)", refs)

    names = [r.name for r in out]
    assert names == ["big.png"]

    # Critical streaming invariant: filtered refs must have released
    # their factory so captured state (PIL.Image / base64 str) is GC-able
    # immediately, not at end of request.
    tiny = [r for r in refs if r.name == "tiny.png"][0]
    assert tiny.consumed, "filtered-out ref must be released"


def test_preserves_parser_provided_dimensions_without_materialize():
    """If a parser already set width/height, the extractor must not call
    materialize() — avoiding unnecessary PIL decode work for parsers
    that already know dims (docling).
    """
    calls = []

    def factory():
        calls.append(1)
        return b"x"

    ref = ImageRef(
        name="img.png", mime="image/png",
        width=400, height=300, _factory=factory,
    )
    out = extract_metadata_into_refs("![](img.png)", [ref])

    assert len(out) == 1
    assert calls == [], "factory must not be called when dims are pre-set"


def test_unmatched_refs_appended_at_end_with_empty_alt():
    """Images not referenced in markdown still flow through — they're
    appended with empty alt and doc-end position so they're uploaded
    but rank after referenced ones.
    """
    refs = [_ref("orphan.png", width=300, height=300)]
    out = extract_metadata_into_refs("# Just text", refs)

    assert len(out) == 1
    assert out[0].alt == ""
    assert out[0].markdown_pos == len("# Just text")


def test_returns_same_ref_objects_in_order():
    """Refs are mutated in place, not copied — downstream can compare by
    identity and keep stable references.
    """
    refs = [
        _ref("a.png", width=300, height=300),
        _ref("b.png", width=300, height=300),
    ]
    out = extract_metadata_into_refs("![](b.png)\n![](a.png)", refs)

    assert [r.name for r in out] == ["b.png", "a.png"]
    # Identity, not equality:
    assert out[0] is refs[1]
    assert out[1] is refs[0]
