import asyncio
import tempfile
from pathlib import Path

from parsers.base import BaseParser, ParseResult

try:
    from docling.document_converter import DocumentConverter
    _DOCLING_AVAILABLE = True
except ImportError:
    _DOCLING_AVAILABLE = False


class DoclingParser(BaseParser):
    engine_name = "docling"
    supported_types = [".pdf", ".docx", ".pptx", ".xlsx", ".xls"]

    @classmethod
    def is_available(cls) -> bool:
        return _DOCLING_AVAILABLE

    async def parse(self, source: bytes | str, filename: str = "", **kwargs) -> ParseResult:
        return await asyncio.get_running_loop().run_in_executor(
            None, self._parse_sync, source, filename
        )

    def _parse_sync(self, source: bytes | str, filename: str) -> ParseResult:
        converter = DocumentConverter()

        suffix = Path(filename).suffix or ".pdf"
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            if isinstance(source, bytes):
                tmp.write(source)
            else:
                tmp.write(source.encode())
            tmp_path = tmp.name

        try:
            result = converter.convert(tmp_path)
        finally:
            Path(tmp_path).unlink(missing_ok=True)

        if result.status.name != "SUCCESS":
            return ParseResult(content="", images={}, title=Path(filename).stem)

        markdown = result.document.export_to_markdown()
        images = self._extract_images(result)
        title = Path(filename).stem

        return ParseResult(content=markdown, images=images, title=title)

    def _extract_images(self, result) -> dict[str, bytes]:
        """Extract embedded images from Docling result."""
        images: dict[str, bytes] = {}
        try:
            for page in result.document.pages:
                for img_item in getattr(page, "images", []):
                    fname = getattr(img_item, "uri", None) or f"img_{len(images)}.png"
                    data = getattr(img_item, "data", None)
                    if data:
                        images[str(fname)] = data
        except Exception:
            pass
        return images
