"""Image OCR via rapidocr (ONNX runtime).

Replaces docling's image path with a ~150 MB ONNX model stack
(rapidocr-onnxruntime internals). Docling used torch + transformers +
layout-analysis models for this job (~400 MB lazy load, ~1.5 GB on
disk); rapidocr is purpose-built for OCR and runs faster on CPU.

Scope
-----
PNG/JPG/JPEG/TIFF/BMP → plain text. No layout / no tables / no
formula recognition — downstream RAG chunking doesn't benefit from
them for image inputs, and keeping this narrow means the parser has
no surprise dependencies.

Model load is deferred until first use via a module-level lazy
singleton (mirrors the docling lazy-import pattern from W4-a). A
worker that never OCRs an image never pays the ONNX load cost.

Images produced as ``image_refs`` are empty — the input IS the image,
so there's nothing to extract besides the source bytes (which the
worker already holds via ``storage_key``).
"""
from __future__ import annotations

import asyncio
import importlib.util
import io
import logging
import threading
from pathlib import Path
from typing import TYPE_CHECKING

from config import get_settings
from parsers.base import BaseParser, ParseResult
from parsers.isolation import run_isolated

if TYPE_CHECKING:
    from rapidocr import RapidOCR  # noqa: F401

logger = logging.getLogger(__name__)

# Lazy availability probe. ``importlib.util.find_spec`` checks the
# module exists without importing it. Importing rapidocr eagerly pulls
# onnxruntime which is ~200 MB wired memory; defer until first parse.
_RAPIDOCR_AVAILABLE = importlib.util.find_spec("rapidocr") is not None


_ocr_engine: "RapidOCR | None" = None
_ocr_lock = threading.Lock()


def _get_engine() -> "RapidOCR":
    global _ocr_engine  # noqa: PLW0603
    if _ocr_engine is not None:
        return _ocr_engine
    with _ocr_lock:
        if _ocr_engine is None:
            from rapidocr import RapidOCR
            _ocr_engine = RapidOCR()
            logger.info("rapidocr engine initialised (ONNX models loaded)")
    return _ocr_engine


class OcrParser(BaseParser):
    engine_name = "ocr-native"
    supported_types = [".png", ".jpg", ".jpeg", ".tiff", ".bmp", ".webp"]

    @classmethod
    def is_available(cls) -> bool:
        return _RAPIDOCR_AVAILABLE

    async def parse(
        self, source: bytes | str, filename: str = "", **kwargs,
    ) -> ParseResult:
        cfg = get_settings()
        if not cfg.parser_isolation:
            return await asyncio.get_running_loop().run_in_executor(
                None, self._parse_sync, source, filename,
            )
        # Heavy tier: ONNX runtime can segfault on malformed input;
        # the 200+ MB model load also deserves a contained process.
        return await run_isolated(
            _ocr_parse_worker, source, filename,
            timeout=cfg.parser_timeout, tier="heavy",
        )

    def _parse_sync(self, source: bytes | str, filename: str) -> ParseResult:
        from PIL import Image

        raw = source if isinstance(source, bytes) else source.encode()
        title = Path(filename).stem

        try:
            img = Image.open(io.BytesIO(raw)).convert("RGB")
        except Exception:
            logger.exception("failed to decode image %s", filename)
            return ParseResult(content="", title=title)

        try:
            import numpy as np
            arr = np.array(img)
        except Exception:
            logger.exception("failed to convert image to ndarray %s", filename)
            return ParseResult(content="", title=title)

        try:
            engine = _get_engine()
            result = engine(arr)
        except Exception:
            logger.exception("rapidocr inference failed for %s", filename)
            return ParseResult(content="", title=title)

        # rapidocr returns a RapidOCROutput; txts is a tuple of strings
        # aligned with boxes+scores. Empty when no text detected.
        txts = getattr(result, "txts", None) or []
        if not txts:
            return ParseResult(content="", title=title)

        # Join lines; rapidocr preserves reading order when it can.
        text = "\n".join(t for t in txts if t and t.strip())
        if not text.strip():
            return ParseResult(content="", title=title)

        content = f"# {title}\n\n{text}\n"
        return ParseResult(content=content, title=title, image_refs=[])


def _ocr_parse_worker(source: bytes | str, filename: str) -> ParseResult:
    """Subprocess entry for OcrParser. Module-level so ProcessPoolExecutor
    workers can pickle it.
    """
    return OcrParser()._parse_sync(source, filename)
