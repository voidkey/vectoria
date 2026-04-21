"""Per-filetype image extractors complementary to text parsers.

Why
---
Today each text parser (docling, mineru, url handlers) produces the
``image_refs`` list as part of its ``ParseResult`` — fine when the
parser's native image story is good enough. But some cases want a
different extractor than the parser uses:

  * .pptx speaker notes: docling drops them; python-pptx reads
    ``slide.notes_slide.notes_text_frame`` and related ``shape.image``
    directly.
  * PDF native images: mineru roundtrips via base64 in the HTTP body;
    pypdfium2 reads them directly from the PDF dict tables, avoiding
    encode/decode overhead.
  * OCR-gated extraction: run OCR first, keep only figures that
    contain text.

Rather than push every variant into the parser, a file-type-scoped
extractor can REPLACE the parser's image_refs after parse completes.

Design
------
  * ``BaseImageExtractor`` — Protocol, ``match(mime, ext) -> bool`` and
    ``async extract(source, filename) -> list[ImageRef]``.
  * Registry lookup is reverse-chronological so later registrations
    override earlier ones without removal ceremony.
  * ``extract_override(source, filename)`` is the ingest-pipeline seam:
    returns ``None`` when no extractor matches (→ caller keeps the
    parser's refs), otherwise returns the replacement list.

Zero extractors are registered from this module — W4-b is the
framework, the concrete implementations land in later commits
(W4-c PptxImageExtractor, W4-d PdfImageExtractor, ...).
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Protocol, runtime_checkable

from parsers.image_ref import ImageRef

logger = logging.getLogger(__name__)


@runtime_checkable
class BaseImageExtractor(Protocol):
    """Contract for per-filetype image extractors.

    ``match`` is a pure function of the file's mime and/or extension —
    no network, no disk. The pipeline calls it cheaply while scanning
    the registry, so keep it fast.

    ``extract`` receives the raw source bytes and an optional filename
    (for extension inspection) and returns a list of ``ImageRef``. An
    empty list is meaningful — "this extractor ran and found no
    images" — and is distinct from "no extractor matched" (the
    pipeline sees ``None`` from ``extract_override`` in that case).
    """

    def match(self, *, mime: str = "", ext: str = "") -> bool: ...

    async def extract(
        self,
        source: bytes,
        *,
        filename: str = "",
    ) -> list[ImageRef]: ...


_extractors: list[BaseImageExtractor] = []


def register_image_extractor(extractor: BaseImageExtractor) -> None:
    """Append an extractor to the registry.

    Registration order matters only for tie-breaks — lookup iterates
    in reverse, so later registrations win. Specific-first ordering
    makes the common "register the defaults, override selectively at
    test setup" pattern work intuitively.
    """
    _extractors.append(extractor)


def find_image_extractor(
    *, mime: str = "", ext: str = "",
) -> BaseImageExtractor | None:
    """Return the first registered extractor matching ``mime`` / ``ext``.

    Reverse iteration so the most-recently-registered extractor for a
    given type wins — this lets tests or runtime extensions override
    defaults without modifying the base registration code.
    """
    for e in reversed(_extractors):
        try:
            if e.match(mime=mime, ext=ext):
                return e
        except Exception:
            # A buggy match() shouldn't take down the whole pipeline —
            # skip and try the next.
            logger.exception("image extractor match() raised; skipping")
    return None


async def extract_override(
    source: bytes,
    *,
    filename: str = "",
    mime: str = "",
) -> list[ImageRef] | None:
    """Look up a registered extractor for this file type and run it.

    Returns:
        * ``list[ImageRef]`` — the replacement list; caller should
          discard whatever the parser produced and use this instead.
          An empty list is valid ("no images according to the
          extractor").
        * ``None`` — no extractor matched; caller keeps parser-produced
          ``image_refs`` unchanged.

    Never raises — extractor failures are logged and treated as
    ``None`` (fall back to parser refs). The pipeline is never worse
    off than without the plugin.
    """
    ext = ""
    if filename and "." in filename:
        ext = Path(filename).suffix.lower()

    extractor = find_image_extractor(mime=mime, ext=ext)
    if extractor is None:
        return None
    try:
        return await extractor.extract(source, filename=filename)
    except Exception:
        logger.exception(
            "image extractor failed for filename=%s; falling back to parser refs",
            filename,
        )
        return None


def _clear_for_tests() -> None:
    """Reset the registry. Tests only."""
    _extractors.clear()
