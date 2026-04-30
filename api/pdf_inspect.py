"""Metadata-only PDF inspection for upload-time gates.

Reads just enough of a PDF (header + xref + page tree root) to count
pages — no rendering, no OCR. Uses pypdfium2, already a project
dependency for the lightweight fallback parser; ``len(PdfDocument)``
returns once the cross-reference table is parsed, which is fast even
for multi-hundred-page docs (~ms, not seconds).

Kept as a separate module from ``mime_sniff`` because the inspections
are conceptually distinct: MIME sniff gates "is this even a PDF",
this gates "is the PDF reasonable to ingest" — and we may grow the
latter (encrypted PDF detection, form/JS detection, ...) over time.
"""
from __future__ import annotations

import io
import logging

logger = logging.getLogger(__name__)


def count_pdf_pages(raw: bytes) -> int | None:
    """Return the page count of a PDF, or ``None`` if it can't be loaded.

    Fail-open by design: a malformed PDF that pypdfium2 chokes on isn't
    this gate's problem — the downstream parser chain will surface a
    parse error if appropriate. The gate only exists to reject *valid*
    PDFs whose page count exceeds policy, so a load failure must not
    short-circuit the pipeline.
    """
    try:
        import pypdfium2 as pdfium
    except ImportError:
        # Defensive — pypdfium2 is required by parsers/pdfium_parser,
        # but a slim deployment that drops it shouldn't break uploads.
        logger.warning("pypdfium2 unavailable; skipping pdf page-count gate")
        return None
    try:
        pdf = pdfium.PdfDocument(io.BytesIO(raw))
    except Exception:
        logger.debug("pdf page-count load failed (corrupt PDF?)", exc_info=True)
        return None
    try:
        return len(pdf)
    finally:
        pdf.close()
