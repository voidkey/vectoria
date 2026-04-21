from parsers.base import BaseParser

# Extension -> preferred engine order (first available wins).
#
# Office (.docx/.doc/.pptx/.ppt/.xlsx/.xls): native-only. The native
# parsers' deps (mammoth+python-docx, python-pptx, openpyxl) are hard
# pins in pyproject so is_available() always returns True — any
# trailing docling/markitdown entry here was dead code. Native parsers
# catch exceptions internally and return empty content, matching what
# a fallback would produce on a file mammoth/python-pptx/openpyxl
# can't load anyway.
#
# PDF: mineru is primary (VLM layout parsing). docling stays as a
# fallback for cases where mineru is unavailable (model not
# downloaded, etc.); markitdown is a text-only last resort.
#
# Images (.png/.jpg/.jpeg/.tiff/.bmp): docling is the only engine —
# its OCR path has no native equivalent in the stack today. A future
# rapidocr/paddleocr-based parser could replace it.
_EXT_PREFERENCE: dict[str, list[str]] = {
    ".pdf":  ["mineru", "docling", "markitdown"],
    ".docx": ["docx-native"],
    ".doc":  ["docx-native"],
    ".pptx": ["pptx-native"],
    ".ppt":  ["pptx-native"],
    ".xlsx": ["xlsx-native"],
    ".xls":  ["xlsx-native"],
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
from parsers.xlsx_parser import XlsxParser  # noqa: E402
registry.register(XlsxParser)
from parsers.docling_parser import DoclingParser  # noqa: E402
registry.register(DoclingParser)
from parsers.mineru_parser import MinerUParser  # noqa: E402
registry.register(MinerUParser)
from parsers.url import UrlParser  # noqa: E402
registry.register(UrlParser)
from parsers.markitdown_parser import MarkitdownParser  # noqa: E402
registry.register(MarkitdownParser)
