from parsers.base import BaseParser

# Extension -> preferred engine order (first available wins)
_EXT_PREFERENCE: dict[str, list[str]] = {
    ".pdf":  ["docling", "mineru", "markitdown"],
    ".docx": ["docling", "markitdown"],
    ".pptx": ["docling", "markitdown"],
    ".xlsx": ["docling", "markitdown"],
    ".xls":  ["docling", "markitdown"],
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
            preferred = ["url"]
        else:
            ext = ("." + filename.rsplit(".", 1)[-1]).lower() if "." in filename else ""
            preferred = _EXT_PREFERENCE.get(ext, ["markitdown"])

        # First try preferred engines that are registered and available
        result = self._first_available(preferred)
        if result in self._engines:
            return result

        # Fallback: search all registered engines by supported_types
        if url:
            search_type = "url"
        else:
            ext = ("." + filename.rsplit(".", 1)[-1]).lower() if "." in filename else ""
            search_type = ext

        for engine_name, parser_cls in self._engines.items():
            if search_type in parser_cls.supported_types and parser_cls.is_available():
                return engine_name

        # Final fallback: return first from preference list regardless
        return preferred[0] if preferred else ""

    def _first_available(self, engines: list[str]) -> str:
        for name in engines:
            cls = self._engines.get(name)
            if cls and cls.is_available():
                return name
        # fallback: return first regardless of availability
        return engines[0]

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
