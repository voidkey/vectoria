import asyncio
import logging
import tempfile
from pathlib import Path

from config import get_settings
from parsers.base import BaseParser, ParseResult
from parsers.convert import LEGACY_FORMAT_MAP, convert_legacy_format
from parsers.isolation import run_isolated

logger = logging.getLogger(__name__)

try:
    from markitdown import MarkItDown
    _AVAILABLE = True
except ImportError:
    _AVAILABLE = False


class MarkitdownParser(BaseParser):
    engine_name = "markitdown"
    supported_types = [".pdf", ".docx", ".doc", ".xlsx", ".xls", ".csv", ".md", ".txt", ".pptx", ".ppt"]

    @classmethod
    def is_available(cls) -> bool:
        return _AVAILABLE

    async def parse(self, source: bytes | str, filename: str = "", **kwargs) -> ParseResult:
        cfg = get_settings()
        if not cfg.parser_isolation:
            return await asyncio.get_running_loop().run_in_executor(
                None, self._parse_sync, source, filename,
            )
        return await run_isolated(
            _markitdown_parse_worker, source, filename, timeout=cfg.parser_timeout,
        )

    def _parse_sync(self, source: bytes | str, filename: str) -> ParseResult:
        if not _AVAILABLE:
            raise RuntimeError("markitdown is not installed. Run: uv add markitdown")
        md = MarkItDown()
        suffix = Path(filename).suffix or ".txt"

        tmp_path = ""
        converted_path = None
        try:
            with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
                tmp.write(source if isinstance(source, bytes) else source.encode())
                tmp_path = tmp.name
            if suffix in LEGACY_FORMAT_MAP:
                converted_path = convert_legacy_format(tmp_path, suffix)
                logger.info("Converted %s → %s", suffix, converted_path)
            result = md.convert(converted_path or tmp_path)
            content = result.text_content or ""
        except Exception:
            logger.exception("markitdown parse failed for %s", filename)
            content = ""
        finally:
            if tmp_path:
                Path(tmp_path).unlink(missing_ok=True)
            if converted_path:
                Path(converted_path).unlink(missing_ok=True)

        return ParseResult(content=content, title=Path(filename).stem)


def _markitdown_parse_worker(source: bytes | str, filename: str) -> ParseResult:
    """Subprocess entry for MarkitdownParser. Module-level so workers can pickle it."""
    return MarkitdownParser()._parse_sync(source, filename)
