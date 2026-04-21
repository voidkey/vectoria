from parsers.base import BaseParser

# Extension -> preferred engine order (first available wins).
# Native lightweight parsers are listed first; docling stays as a
# fallback for cases where the native path fails to import (unlikely
# given the deps are pinned) and is retained for .png/.jpg/.tiff (its
# OCR path has no native equivalent yet).
_EXT_PREFERENCE: dict[str, list[str]] = {
    ".pdf":  ["mineru", "docling", "markitdown"],
    ".docx": ["docx-native", "docling", "markitdown"],
    ".doc":  ["docx-native", "docling", "markitdown"],
    ".pptx": ["pptx-native", "docling", "markitdown"],
    ".ppt":  ["pptx-native", "docling", "markitdown"],
    ".xlsx": ["docling", "markitdown"],
    ".xls":  ["docling", "markitdown"],
    ".png":  ["docling"],
    ".jpg":  ["docling"],
    ".jpeg": ["docling"],
    ".tiff": ["docling"],
    ".bmp":  ["docling"],
    ".csv":  ["markitdown"],
    ".md":   ["markitdown"],
    ".txt":  ["markitdown"],
    "url":   ["url"],
}


class ParserRegistry:
    def __init__(self):
        self._engines: dict[str, type[BaseParser]] = {}

    def register(self, parser_cls: type[BaseParser]) -> None:
        self._engines[parser_cls.engine_name] = parser_cls

    def get_by_engine(self, engine: str) -> BaseParser:
        cls = self._engines.get(engine)
        if not cls:
            raise ValueError(f"Unknown engine: {engine!r}")
        return cls()

    def auto_select(self, filename: str = "", url: str = "") -> str:
        if url:
            search_type = "url"
            preferred = _EXT_PREFERENCE.get("url", ["url"])
        else:
            ext = ("." + filename.rsplit(".", 1)[-1]).lower() if "." in filename else ""
            search_type = ext
            preferred = _EXT_PREFERENCE.get(ext, ["markitdown"])

        # 1. Try preferred engines in order (by engine name)
        result = self._first_available(preferred)
        if result in self._engines:
            return result

        # 2. Fallback: find any registered+available engine by supported_types
        for name, cls in self._engines.items():
            if search_type in cls.supported_types and cls.is_available():
                return name

        # 3. Nothing registered — return preferred[0], get_by_engine will raise
        return preferred[0]

    def _first_available(self, engines: list[str]) -> str:
        for name in engines:
            cls = self._engines.get(name)
            if cls and cls.is_available():
                return name
        # fallback: return first regardless of availability
        return engines[0]

    def supported_types(self) -> list[str]:
        """Return deduplicated list of supported file extensions and 'url'."""
        types: set[str] = set()
        for cls in self._engines.values():
            if cls.is_available():
                types.update(cls.supported_types)
        return sorted(types)

    def list_engines(self) -> list[dict]:
        return [
            {
                "name": cls.engine_name,
                "supported_types": cls.supported_types,
                "available": cls.is_available(),
            }
            for cls in self._engines.values()
        ]


registry = ParserRegistry()

# Auto-register built-in parsers
from parsers.docx_parser import DocxParser  # noqa: E402
registry.register(DocxParser)
from parsers.pptx_parser import PptxParser  # noqa: E402
registry.register(PptxParser)
from parsers.docling_parser import DoclingParser  # noqa: E402
registry.register(DoclingParser)
from parsers.mineru_parser import MinerUParser  # noqa: E402
registry.register(MinerUParser)
from parsers.url import UrlParser  # noqa: E402
registry.register(UrlParser)
from parsers.markitdown_parser import MarkitdownParser  # noqa: E402
registry.register(MarkitdownParser)
