"""Native .docx parser: mammoth for text, python-docx for images.

Why a native parser when docling already handles .docx
------------------------------------------------------
docling internally uses python-docx and walks the OOXML — the same
tool we'd use directly. What we gain by going direct:

* No ``convert_lock`` serialisation. docling declares a process-wide
  lock because its full pipeline (PDF path) isn't thread-safe;
  .docx parsing gets caught up in it even though it doesn't need to.
* No docling model load when a worker only handles .docx traffic.
  With W4-a's lazy import the cost is paid on first use, but a
  .docx-only worker now never pays it at all.
* A much smaller dependency surface to audit / secure.

Quality parity
--------------
mammoth's markdown output preserves heading hierarchy (via docx
``w:pStyle`` → ``# ## ###``), bullet / numbered lists, tables (with
pipe-style output), and bold / italic / links. That's the feature
set our RAG pipeline actually consumes (splitter + outline extraction
both key off markdown headings).

Images
------
mammoth by default inlines images as base64 data-URIs in the
markdown, which would balloon content_len and break the
``max_content_chars`` gate. We override the image handler to emit
plain ``![](name)`` placeholders + capture the blobs into
``ImageRef`` lazy factories. Downstream image pipeline (phash,
streaming upload, vision) is identical to the other parsers.
"""
from __future__ import annotations

import asyncio
import io
import logging
from pathlib import Path

from config import get_settings
from parsers.base import BaseParser, ParseResult
from parsers.convert import LEGACY_FORMAT_MAP, convert_legacy_format
from parsers.image_ref import ImageRef
from parsers.isolation import run_isolated

logger = logging.getLogger(__name__)


class DocxParser(BaseParser):
    engine_name = "docx-native"
    supported_types = [".docx", ".doc"]

    @classmethod
    def is_available(cls) -> bool:
        try:
            import mammoth  # noqa: F401
            import docx  # noqa: F401
        except ImportError:
            return False
        return True

    async def parse(
        self, source: bytes | str, filename: str = "", **kwargs,
    ) -> ParseResult:
        cfg = get_settings()
        if not cfg.parser_isolation:
            return await asyncio.get_running_loop().run_in_executor(
                None, self._parse_sync, source, filename,
            )
        return await run_isolated(
            _docx_parse_worker, source, filename, timeout=cfg.parser_timeout,
        )

    def _parse_sync(self, source: bytes | str, filename: str) -> ParseResult:
        import mammoth
        from docx import Document

        raw = source if isinstance(source, bytes) else source.encode()

        # .doc (legacy binary) needs LibreOffice to become .docx first.
        # ``convert_legacy_format`` writes to a tmp file and returns the
        # converted path; callers drop that tmp at the end. We avoid
        # piping bytes through twice by re-reading the converted file.
        suffix = Path(filename).suffix.lower()
        if suffix in LEGACY_FORMAT_MAP:
            import tempfile
            with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
                tmp.write(raw)
                tmp_path = tmp.name
            try:
                converted_path = convert_legacy_format(tmp_path, suffix)
                try:
                    with open(converted_path, "rb") as f:
                        raw = f.read()
                finally:
                    Path(converted_path).unlink(missing_ok=True)
            finally:
                Path(tmp_path).unlink(missing_ok=True)

        # Collect images as ImageRefs while mammoth walks the doc.
        # ``mammoth.images.img_element`` receives a mammoth Image and
        # returns the attrs for the <img> tag it will emit. We capture
        # the blob and return a stable placeholder name that we can
        # match against later in markdown.
        image_refs: list[ImageRef] = []

        def _image_handler(image):
            idx = len(image_refs)
            content_type = image.content_type or "image/png"
            ext = _mime_to_ext(content_type)
            name = f"image_{idx:04d}{ext}"

            # Capture the blob into a local bytes object via mammoth's
            # ``open()`` context manager — data is read eagerly because
            # mammoth closes the inner stream once img_element returns.
            with image.open() as stream:
                blob = stream.read()

            def _factory(data=blob) -> bytes:
                return data

            image_refs.append(ImageRef(
                name=name,
                mime=content_type,
                _factory=_factory,
            ))
            return {"src": name, "alt": ""}

        try:
            result = mammoth.convert_to_markdown(
                io.BytesIO(raw),
                convert_image=mammoth.images.img_element(_image_handler),
            )
            markdown = result.value or ""
        except Exception:
            logger.exception("mammoth conversion failed for %s", filename)
            return ParseResult(content="", title=Path(filename).stem)

        # Title: heuristic — first heading, else filename stem.
        title = _extract_first_heading(markdown) or Path(filename).stem

        return ParseResult(
            content=markdown,
            title=title,
            image_refs=image_refs,
        )


_MIME_EXT = {
    "image/png": ".png",
    "image/jpeg": ".jpg",
    "image/gif": ".gif",
    "image/bmp": ".bmp",
    "image/tiff": ".tiff",
    "image/webp": ".webp",
    "image/x-emf": ".emf",
    "image/x-wmf": ".wmf",
}


def _mime_to_ext(mime: str) -> str:
    return _MIME_EXT.get(mime.lower(), ".png")


def _extract_first_heading(markdown: str) -> str:
    """Return the text of the first ``# heading`` line, or empty string."""
    for line in markdown.splitlines():
        stripped = line.strip()
        if stripped.startswith("# ") and not stripped.startswith("##"):
            return stripped[2:].strip()
    return ""


def _docx_parse_worker(source: bytes | str, filename: str) -> ParseResult:
    """Subprocess entry for DocxParser. Must be module-level so
    ProcessPoolExecutor workers can pickle it.
    """
    return DocxParser()._parse_sync(source, filename)
