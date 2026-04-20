import asyncio
import io
import logging
import tempfile
import threading
from pathlib import Path

from config import get_settings
from parsers.base import BaseParser, ParseResult
from parsers.convert import LEGACY_FORMAT_MAP, convert_legacy_format
from parsers.image_ref import ImageRef
from parsers.isolation import run_isolated

logger = logging.getLogger(__name__)

try:
    from docling.document_converter import DocumentConverter
    from docling.datamodel.pipeline_options import PdfPipelineOptions
    _DOCLING_AVAILABLE = True
except ImportError:
    _DOCLING_AVAILABLE = False


_converter: "DocumentConverter | None" = None
_converter_lock = threading.Lock()
# Serializes convert() calls: docling's thread-safety isn't documented, and a
# single parse can peak >1GB RAM — parallel parses would risk OOM on small hosts.
_convert_lock = threading.Lock()


def _get_converter() -> "DocumentConverter":
    """Lazily build a single process-wide DocumentConverter.

    Docling loads large layout/OCR models on first convert(); reusing one
    instance avoids re-allocating them per request.
    """
    global _converter  # noqa: PLW0603
    if _converter is not None:
        return _converter
    with _converter_lock:
        if _converter is None:
            from docling.datamodel.base_models import InputFormat
            from docling.document_converter import ImageFormatOption, PdfFormatOption

            pipeline_opts = PdfPipelineOptions(generate_picture_images=True)
            _converter = DocumentConverter(
                format_options={
                    InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_opts),
                    InputFormat.IMAGE: ImageFormatOption(pipeline_options=pipeline_opts),
                },
            )
    return _converter


class DoclingParser(BaseParser):
    engine_name = "docling"
    supported_types = [".pdf", ".docx", ".doc", ".pptx", ".ppt", ".xlsx", ".xls", ".png", ".jpg", ".jpeg", ".tiff", ".bmp"]

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
        """Build lazy ImageRef list from Docling pictures.

        The factory captures the docling ``picture`` + ``document`` by
        default-arg; ``materialize()`` re-encodes to PNG on demand. This
        keeps PNG bytes out of the heap until the upload pipeline asks
        for them, and lets ``release()`` drop the capture afterwards.
        Widths/heights are read eagerly (PIL.Image.size is cheap — the
        image is already decoded in docling's internals) so metadata
        extraction doesn't need to materialize bytes just for dims.
        """
        refs: list[ImageRef] = []
        try:
            for idx, picture in enumerate(result.document.pictures):
                pil_img = picture.get_image(doc=result.document)
                if pil_img is None:
                    continue
                w, h = pil_img.size
                name = f"image_{idx:04d}.png"

                def _factory(pic=picture, doc=result.document) -> bytes:
                    img = pic.get_image(doc=doc)
                    buf = io.BytesIO()
                    img.save(buf, format="PNG")
                    return buf.getvalue()

                refs.append(ImageRef(
                    name=name, mime="image/png",
                    width=w, height=h, _factory=_factory,
                ))
        except Exception:
            logger.exception("docling image extraction failed")
        return refs


def _docling_parse_worker(source: bytes | str, filename: str) -> ParseResult:
    """Subprocess entry for DoclingParser. Must be importable (module-level)
    so ProcessPoolExecutor workers can pickle it.
    """
    return DoclingParser()._parse_sync(source, filename)
