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
            result = self._first_available(preferred)

            # If the result is not registered, find any registered engine that supports URLs
            if result not in self._engines:
                for engine_name, parser_cls in self._engines.items():
                    if "url" in parser_cls.supported_types and parser_cls.is_available():
                        return engine_name

            return result

        ext = ("." + filename.rsplit(".", 1)[-1]).lower() if "." in filename else ""
        preferred = _EXT_PREFERENCE.get(ext, ["markitdown"])

        # Try preferred engines first
        result = self._first_available(preferred)

        # If the result is not registered, find any registered engine that supports this extension
        if result not in self._engines:
            for engine_name, parser_cls in self._engines.items():
                if ext in parser_cls.supported_types and parser_cls.is_available():
                    return engine_name

        return result

    def _first_available(self, engines: list[str]) -> str:
        for name in engines:
            cls = self._engines.get(name)
            if cls and cls.is_available():
                return name
        # fallback: return first registered engine that matches the list, or first in list if none match
        for name in engines:
            if name in self._engines:
                return name
        return engines[0] if engines else ""

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
