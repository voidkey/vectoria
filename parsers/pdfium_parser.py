"""Native .pdf text extractor via pypdfium2.

Role
----
Fallback PDF parser when MinerU is unavailable (breaker open, URL not
configured). Before W6-2 this slot was docling, which dragged torch +
transformers + vision models (~400 MB lazy-loaded, ~1.5 GB on disk)
just to support a path that fires rarely. pypdfium2 is a thin wrapper
around the same PDFium engine Chromium uses — ~5 MB install, no ML
models, pure text extraction.

Scope
-----
Text-only. No OCR (scanned PDFs return empty content); no layout
reconstruction beyond per-page text order. MinerU remains the primary
PDF parser for layout-sensitive / vision-requiring documents; pdfium
is the "grab plain text so ingest completes" fallback.

Images are not produced here — a future PdfImageExtractor plugin
(W4-b framework) could render embedded images if that use case shows
up. Current traffic doesn't request it.
"""
from __future__ import annotations

import asyncio
import io
import logging
from pathlib import Path

from config import get_settings
from parsers.base import BaseParser, ParseResult
from parsers.isolation import run_isolated

logger = logging.getLogger(__name__)

# Per-page character cap so a pathological PDF with one giant embedded
# text stream can't blow past ``max_content_chars``. Real PDFs stay
# well under this; the cap is a safety net, not an expected limit.
_MAX_CHARS_PER_PAGE = 100_000


class PdfiumParser(BaseParser):
    engine_name = "pdfium"
    supported_types = [".pdf"]

    @classmethod
    def is_available(cls) -> bool:
        try:
            import pypdfium2  # noqa: F401
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
        # Heavy tier: pypdfium2 is pure Python + C extension; a
        # malformed PDF can segfault the C side. Isolating in
        # subprocess keeps that contained.
        return await run_isolated(
            _pdfium_parse_worker, source, filename,
            timeout=cfg.parser_timeout, tier="heavy",
        )

    def _parse_sync(self, source: bytes | str, filename: str) -> ParseResult:
        import pypdfium2 as pdfium

        raw = source if isinstance(source, bytes) else source.encode()
        title = Path(filename).stem

        try:
            pdf = pdfium.PdfDocument(io.BytesIO(raw))
        except Exception:
            logger.exception("pypdfium2 failed to load %s", filename)
            return ParseResult(content="", title=title)

        try:
            pages_md: list[str] = []
            for page_idx, page in enumerate(pdf):
                try:
                    text_page = page.get_textpage()
                    try:
                        text = text_page.get_text_bounded()
                    finally:
                        text_page.close()
                    page.close()
                except Exception:
                    logger.exception(
                        "pypdfium2 page %d extraction failed for %s",
                        page_idx, filename,
                    )
                    continue

                text = (text or "").strip()
                if len(text) > _MAX_CHARS_PER_PAGE:
                    text = text[:_MAX_CHARS_PER_PAGE] + "\n_(page truncated)_"
                if text:
                    pages_md.append(f"## Page {page_idx + 1}\n\n{text}")
        finally:
            try:
                pdf.close()
            except Exception:
                pass

        if not pages_md:
            return ParseResult(content="", title=title)

        content = f"# {title}\n\n" + "\n\n".join(pages_md) + "\n"
        return ParseResult(content=content, title=title, image_refs=[])


def _pdfium_parse_worker(source: bytes | str, filename: str) -> ParseResult:
    """Subprocess entry for PdfiumParser. Must be module-level so
    ProcessPoolExecutor workers can pickle it.
    """
    return PdfiumParser()._parse_sync(source, filename)
