import asyncio
import io
import subprocess
import tempfile
from pathlib import Path

from parsers.base import BaseParser, ParseResult

try:
    from docling.document_converter import DocumentConverter
    from docling.datamodel.pipeline_options import PdfPipelineOptions
    _DOCLING_AVAILABLE = True
except ImportError:
    _DOCLING_AVAILABLE = False


_LEGACY_FORMAT_MAP = {".doc": ".docx", ".ppt": ".pptx"}


class DoclingParser(BaseParser):
    engine_name = "docling"
    supported_types = [".pdf", ".docx", ".doc", ".pptx", ".ppt", ".xlsx", ".xls", ".png", ".jpg", ".jpeg", ".tiff", ".bmp"]

    @classmethod
    def is_available(cls) -> bool:
        return _DOCLING_AVAILABLE

    async def parse(self, source: bytes | str, filename: str = "", **kwargs) -> ParseResult:
        return await asyncio.get_running_loop().run_in_executor(
            None, self._parse_sync, source, filename
        )

    def _parse_sync(self, source: bytes | str, filename: str) -> ParseResult:
        from docling.datamodel.base_models import InputFormat
        from docling.document_converter import ImageFormatOption, PdfFormatOption

        pipeline_opts = PdfPipelineOptions(generate_picture_images=True)
        converter = DocumentConverter(
            format_options={
                InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_opts),
                InputFormat.IMAGE: ImageFormatOption(pipeline_options=pipeline_opts),
            },
        )

        suffix = Path(filename).suffix.lower() or ".pdf"
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            if isinstance(source, bytes):
                tmp.write(source)
            else:
                tmp.write(source.encode())
            tmp_path = tmp.name

        converted_path = None
        try:
            if suffix in _LEGACY_FORMAT_MAP:
                converted_path = str(Path(tmp_path).with_suffix(_LEGACY_FORMAT_MAP[suffix]))
                self._convert_legacy(tmp_path, suffix)
            parse_path = converted_path or tmp_path
            result = converter.convert(parse_path)
        finally:
            Path(tmp_path).unlink(missing_ok=True)
            if converted_path:
                Path(converted_path).unlink(missing_ok=True)

        if result.status.name != "SUCCESS":
            return ParseResult(content="", images={}, title=Path(filename).stem)

        markdown = result.document.export_to_markdown()
        images = self._extract_images(result)
        title = Path(filename).stem

        return ParseResult(content=markdown, images=images, title=title)

    @staticmethod
    def _convert_legacy(src_path: str, suffix: str) -> None:
        """Convert .doc/.ppt to .docx/.pptx via LibreOffice."""
        target_fmt = _LEGACY_FORMAT_MAP[suffix]
        out_dir = Path(src_path).parent
        result = subprocess.run(
            ["libreoffice", "--headless", "--convert-to",
             target_fmt.lstrip("."), "--outdir", str(out_dir), src_path],
            check=True, timeout=120, capture_output=True,
        )
        expected = Path(src_path).with_suffix(target_fmt)
        if not expected.exists():
            raise RuntimeError(
                f"LibreOffice conversion produced no output: {expected}\n"
                f"stderr: {result.stderr.decode(errors='replace')}"
            )

    def _extract_images(self, result) -> dict[str, bytes]:
        """Extract embedded images from Docling result."""
        images: dict[str, bytes] = {}
        try:
            for idx, picture in enumerate(result.document.pictures):
                pil_img = picture.get_image(doc=result.document)
                if pil_img is None:
                    continue
                buf = io.BytesIO()
                pil_img.save(buf, format="PNG")
                fname = f"image_{idx:04d}.png"
                images[fname] = buf.getvalue()
        except Exception:
            pass
        return images
