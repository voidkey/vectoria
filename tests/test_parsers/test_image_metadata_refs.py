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


def _ref(name: str, width=None, height=None, page=None, payload: bytes = b"x") -> ImageRef:
    return ImageRef(
        name=name, mime="image/png",
        width=width, height=height, page=page,
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


def test_paged_orphan_lands_before_later_page_anchor():
    """MinerU often extracts a chart/table from an early page but
    doesn't write a markdown reference for it. With ``page`` known,
    the extractor must place the orphan ahead of the first
    later-page matched ref — not at doc_len, where it would inherit
    the document's last heading and tail context. Symptom of the
    bug: an orphan with page=3 ends up tagged with page-21's
    section title, making the page field look wrong next to its
    surrounding metadata.
    """
    markdown = (
        "# Theme\n\n"
        "Front matter on page 3.\n\n"
        "# 波段原点\n\n"
        "![p5](images/p5.png)\n\n"
        "# 货架陈列重点\n\n"
        "Tail content.\n"
    )
    refs = [
        _ref("p5.png", width=300, height=300, page=5),
        _ref("orphan_p3.png", width=300, height=300, page=3),
    ]

    out = extract_metadata_into_refs(markdown, refs)

    by_name = {r.name: r for r in out}
    # Orphan must come BEFORE the page-5 ref in the output array,
    # matching its physical-page order.
    assert [r.name for r in out] == ["orphan_p3.png", "p5.png"]
    # Orphans have no real markdown anchor — context and section_title
    # must stay empty so we don't surface next-page text as if it
    # belonged to this image.
    assert by_name["orphan_p3.png"].context == ""
    assert by_name["orphan_p3.png"].section_title == ""
    # Matched refs are unaffected.
    assert by_name["p5.png"].section_title == "波段原点"


def test_paged_orphan_after_all_matches_lands_at_end():
    """Orphan whose page is greater than every matched ref's page
    has no later anchor to slot before — falls through to doc_len,
    which is correct (it really IS document tail).
    """
    markdown = "# Top\n\n![p2](images/p2.png)\n\n# Tail"
    refs = [
        _ref("p2.png", width=300, height=300, page=2),
        _ref("orphan_p9.png", width=300, height=300, page=9),
    ]

    out = extract_metadata_into_refs(markdown, refs)

    assert [r.name for r in out] == ["p2.png", "orphan_p9.png"]
    assert out[1].markdown_pos == len(markdown)


def test_orphan_without_page_still_tails():
    """Backwards compat: refs from non-paginated sources (docx, html)
    have page=None. They keep the legacy doc_len placement so existing
    behaviour is unchanged.
    """
    markdown = "# Top\n\n![a](a.png)\n\n# Bottom"
    refs = [
        _ref("a.png", width=300, height=300),
        _ref("orphan.png", width=300, height=300),  # page=None
    ]

    out = extract_metadata_into_refs(markdown, refs)

    assert [r.name for r in out] == ["a.png", "orphan.png"]
    assert out[1].markdown_pos == len(markdown)


def test_multiple_paged_orphans_sort_by_page():
    """Several orphans with different pages must each land at their
    own page-anchored slot — they shouldn't all collapse to the same
    position just because they're all unmatched.
    """
    markdown = (
        "![p5](images/p5.png)\n\n"
        "![p10](images/p10.png)\n\n"
        "![p20](images/p20.png)\n"
    )
    refs = [
        _ref("p5.png", width=300, height=300, page=5),
        _ref("p10.png", width=300, height=300, page=10),
        _ref("p20.png", width=300, height=300, page=20),
        _ref("orphan_p3.png", width=300, height=300, page=3),
        _ref("orphan_p15.png", width=300, height=300, page=15),
    ]

    out = extract_metadata_into_refs(markdown, refs)

    # Output order follows page order: p3 orphan first, then p5,
    # p10, p15 orphan, p20.
    assert [r.name for r in out] == [
        "orphan_p3.png",
        "p5.png",
        "p10.png",
        "orphan_p15.png",
        "p20.png",
    ]


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
