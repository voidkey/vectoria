import asyncio
import importlib.util
import io
import logging
import tempfile
import threading
from pathlib import Path
from typing import TYPE_CHECKING

from config import get_settings
from parsers.base import BaseParser, ParseResult
from parsers.convert import LEGACY_FORMAT_MAP, convert_legacy_format
from parsers.image_ref import BytesFactory, ImageRef
from parsers.isolation import run_isolated

if TYPE_CHECKING:
    from docling.document_converter import DocumentConverter  # noqa: F401

logger = logging.getLogger(__name__)

# Lazy availability probe: ``importlib.util.find_spec`` checks the module
# exists without importing it. Importing ``docling`` eagerly pulls in
# torch + transformers + vision model stacks (~400 MB RSS) whether or
# not we ever run a parse — and in practice we don't, because MinerU is
# the PDF primary and docling is only the fallback. Defer both the
# availability flag and the real imports until ``_get_converter()``
# actually needs them.
_DOCLING_AVAILABLE = importlib.util.find_spec("docling") is not None


_converter: "DocumentConverter | None" = None
_converter_lock = threading.Lock()
# Serializes convert() calls: docling's thread-safety isn't documented, and a
# single parse can peak >1GB RAM — parallel parses would risk OOM on small hosts.
_convert_lock = threading.Lock()


def _get_converter() -> "DocumentConverter":
    """Lazily build a single process-wide DocumentConverter.

    First call into this function is where docling actually gets
    imported — the heavy torch / transformers / vision model stack
    only hits RSS at the moment we commit to parsing something docling
    owns, not at worker startup. Subsequent calls return the cached
    instance.
    """
    global _converter  # noqa: PLW0603
    if _converter is not None:
        return _converter
    with _converter_lock:
        if _converter is None:
            # Heavy imports live inside the lock so the ~400 MB model
            # load only happens for the first real parse request.
            from docling.document_converter import DocumentConverter
            from docling.datamodel.pipeline_options import PdfPipelineOptions
            from docling.datamodel.base_models import InputFormat
            from docling.document_converter import ImageFormatOption, PdfFormatOption

            pipeline_opts = PdfPipelineOptions(generate_picture_images=True)
            _converter = DocumentConverter(
                format_options={
                    InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_opts),
                    InputFormat.IMAGE: ImageFormatOption(pipeline_options=pipeline_opts),
                },
            )
            logger.info("docling DocumentConverter initialised (models loaded)")
    return _converter


class DoclingParser(BaseParser):
    engine_name = "docling"
    # Shrunk after W4-d/e/f: Office (.docx/.pptx/.xlsx) is now served by
    # the native mammoth / python-pptx / openpyxl parsers, whose deps
    # are hard-pinned in pyproject so they're always available — meaning
    # docling in an Office fallback chain was unreachable dead code.
    # docling still owns PDF fallback (behind mineru) and image OCR,
    # which have no native replacement in the current stack.
    supported_types = [".pdf", ".png", ".jpg", ".jpeg", ".tiff", ".bmp"]

    @classmethod
    def is_available(cls) -> bool:
        return _DOCLING_AVAILABLE

    async def parse(self, source: bytes | str, filename: str = "", **kwargs) -> ParseResult:
        cfg = get_settings()
        if not cfg.parser_isolation:
            return await asyncio.get_running_loop().run_in_executor(
                None, self._parse_sync, source, filename,
            )
        return await run_isolated(
            _docling_parse_worker, source, filename, timeout=cfg.parser_timeout,
        )

    def _parse_sync(self, source: bytes | str, filename: str) -> ParseResult:
        converter = _get_converter()

        suffix = Path(filename).suffix.lower() or ".pdf"
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            if isinstance(source, bytes):
                tmp.write(source)
            else:
                tmp.write(source.encode())
            tmp_path = tmp.name

        converted_path = None
        try:
            if suffix in LEGACY_FORMAT_MAP:
                converted_path = convert_legacy_format(tmp_path, suffix)
                logger.info("Converted %s → %s", suffix, converted_path)
            with _convert_lock:
                result = converter.convert(converted_path or tmp_path)
        finally:
            Path(tmp_path).unlink(missing_ok=True)
            if converted_path:
                Path(converted_path).unlink(missing_ok=True)

        if result.status.name != "SUCCESS":
            return ParseResult(content="", title=Path(filename).stem)

        markdown = result.document.export_to_markdown()
        image_refs = self._extract_image_refs(result)
        title = Path(filename).stem

        return ParseResult(content=markdown, title=title, image_refs=image_refs)

    def _extract_image_refs(self, result) -> list[ImageRef]:
        """Materialise PNG bytes from Docling pictures into picklable refs.

        Why eager-encode: when ``parser_isolation`` is on (production
        default), the returned ``ParseResult`` is pickled back from a
        subprocess. Docling's ``picture`` / ``document`` objects are
        not picklable, so a lazy closure capturing them would fail the
        round-trip. Widths/heights come from PIL.Image.size (cheap — the
        image is already decoded in docling's internals).
        """
        refs: list[ImageRef] = []
        try:
            for idx, picture in enumerate(result.document.pictures):
                pil_img = picture.get_image(doc=result.document)
                if pil_img is None:
                    continue
                w, h = pil_img.size
                buf = io.BytesIO()
                pil_img.save(buf, format="PNG")
                refs.append(ImageRef(
                    name=f"image_{idx:04d}.png", mime="image/png",
                    width=w, height=h, _factory=BytesFactory(buf.getvalue()),
                ))
        except Exception:
            logger.exception("docling image extraction failed")
        return refs


def _docling_parse_worker(source: bytes | str, filename: str) -> ParseResult:
    """Subprocess entry for DoclingParser. Must be importable (module-level)
    so ProcessPoolExecutor workers can pickle it.
    """
    return DoclingParser()._parse_sync(source, filename)
