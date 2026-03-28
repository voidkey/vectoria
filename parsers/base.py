from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import ClassVar


@dataclass
class ParseResult:
    content: str          # Markdown text
    images: dict[str, bytes]  # {filename: raw bytes}
    title: str = ""
    image_urls: list[str] | None = None  # URLs for deferred download


class BaseParser(ABC):
    engine_name: ClassVar[str] = ""
    supported_types: ClassVar[list[str]] = []  # file extensions like ".pdf", or ["url"]

    @classmethod
    def is_available(cls) -> bool:
        return True

    @abstractmethod
    async def parse(self, source: bytes | str, filename: str = "", **kwargs) -> ParseResult:
        ...
