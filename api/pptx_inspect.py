"""Metadata-only PPTX inspection for upload-time gates.

Counts slides by listing the OPC zip's ``ppt/slides/slideN.xml``
entries. No XML parse, no python-pptx import — just stdlib zipfile.
Microsecond-level for any file size, even attacker-grown decks.

Sibling to ``api.pdf_inspect``; same fail-open contract for the
same reason (the gate exists to reject *valid* files over policy,
not to double-up as a corruption rejector).
"""
from __future__ import annotations

import io
import logging
import re
import zipfile

logger = logging.getLogger(__name__)

# OPC packaging spec: slide parts live under ``ppt/slides/`` and are
# named ``slide1.xml``, ``slide2.xml``, ... — no zero-padding, sequential.
# Avoids over-matching ``ppt/slides/_rels/slide1.xml.rels`` and
# ``ppt/slideLayouts/...`` (which would inflate the count).
_SLIDE_PART = re.compile(r"^ppt/slides/slide\d+\.xml$")


def count_pptx_slides(raw: bytes) -> int | None:
    """Return the slide count of a PPTX, or ``None`` if it can't be read.

    Fail-open by design: a zip that won't open isn't this gate's
    problem. The downstream parser chain will surface a parse error
    if appropriate; the gate only exists to reject valid, oversized
    decks early.
    """
    try:
        zf = zipfile.ZipFile(io.BytesIO(raw))
    except (zipfile.BadZipFile, OSError):
        logger.debug("pptx slide-count load failed (bad zip?)", exc_info=True)
        return None
    try:
        return sum(1 for n in zf.namelist() if _SLIDE_PART.match(n))
    finally:
        zf.close()
