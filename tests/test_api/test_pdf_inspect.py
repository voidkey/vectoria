"""Unit tests for ``api.pdf_inspect.count_pdf_pages``.

Counts pages from PDF bytes via pypdfium2's xref-only load. Must be
fail-open: a malformed PDF returns ``None`` rather than raising,
because the upload gate is meant to reject *valid* PDFs over policy,
not double-up as a corruption rejector (the parser chain handles that).
"""
from api.pdf_inspect import count_pdf_pages


def _make_pdf(num_pages: int) -> bytes:
    """Hand-roll a minimal valid PDF with ``num_pages`` empty pages.

    Avoids a heavy reportlab/fpdf dep just for tests; pypdfium2 is
    read-only. The structure is the bare minimum pypdfium2 accepts:
    catalog → pages tree → N empty page objects, then an xref table.
    """
    objs = ["1 0 obj\n<</Type/Catalog/Pages 2 0 R>>\nendobj\n"]
    page_obj_nums = list(range(3, 3 + num_pages))
    kids = " ".join(f"{n} 0 R" for n in page_obj_nums)
    objs.append(
        f"2 0 obj\n<</Type/Pages/Kids[{kids}]/Count {num_pages}>>\nendobj\n",
    )
    for n in page_obj_nums:
        objs.append(
            f"{n} 0 obj\n<</Type/Page/Parent 2 0 R/MediaBox[0 0 612 792]>>\nendobj\n",
        )

    body = "%PDF-1.4\n"
    offsets: list[int] = []
    for o in objs:
        offsets.append(len(body))
        body += o
    xref_pos = len(body)
    body += f"xref\n0 {len(objs) + 1}\n"
    body += "0000000000 65535 f \n"
    for off in offsets:
        body += f"{off:010d} 00000 n \n"
    body += (
        f"trailer\n<</Size {len(objs) + 1}/Root 1 0 R>>\n"
        f"startxref\n{xref_pos}\n%%EOF\n"
    )
    return body.encode("latin-1")


def test_count_pdf_pages_returns_count_for_valid_pdf():
    raw = _make_pdf(7)

    assert count_pdf_pages(raw) == 7


def test_count_pdf_pages_handles_large_page_count():
    """The gate's whole purpose is to catch high page counts, so the
    counter must not silently truncate or error past some threshold —
    test well above the default 200-page policy.
    """
    raw = _make_pdf(500)

    assert count_pdf_pages(raw) == 500


def test_count_pdf_pages_returns_none_on_garbage_bytes():
    """Fail-open: malformed input must NOT raise, so the upload path
    can fall through to existing parser-level error handling rather
    than 500 the request.
    """
    assert count_pdf_pages(b"not a pdf at all") is None


def test_count_pdf_pages_returns_none_on_empty_bytes():
    assert count_pdf_pages(b"") is None
