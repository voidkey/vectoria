import asyncio
import tempfile
from pathlib import Path

from parsers.base import BaseParser, ParseResult

try:
    from markitdown import MarkItDown
    _AVAILABLE = True
except ImportError:
    _AVAILABLE = False


class MarkitdownParser(BaseParser):
    engine_name = "markitdown"
    supported_types = [".pdf", ".docx", ".xlsx", ".xls", ".csv", ".md", ".txt", ".pptx"]

    @classmethod
    def is_available(cls) -> bool:
        return _AVAILABLE

    async def parse(self, source: bytes | str, filename: str = "", **kwargs) -> ParseResult:
        return await asyncio.get_event_loop().run_in_executor(
            None, self._parse_sync, source, filename
        )

    def _parse_sync(self, source: bytes | str, filename: str) -> ParseResult:
        md = MarkItDown()
        suffix = Path(filename).suffix or ".txt"

        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            tmp.write(source if isinstance(source, bytes) else source.encode())
            tmp_path = tmp.name

        try:
            result = md.convert(tmp_path)
            content = result.text_content or ""
        except Exception:
            content = ""
        finally:
            Path(tmp_path).unlink(missing_ok=True)

        return ParseResult(content=content, images={}, title=Path(filename).stem)
