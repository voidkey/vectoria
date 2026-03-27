from dataclasses import dataclass
from abc import ABC, abstractmethod


@dataclass
class ParseResult:
    content: str          # Markdown text
    images: dict[str, bytes]  # {filename: raw bytes}
    title: str = ""


class BaseParser(ABC):
    engine_name: str = ""
    supported_types: list[str] = []  # file extensions like ".pdf", or ["url"]

    @classmethod
    def is_available(cls) -> bool:
        return True

    @abstractmethod
    async def parse(self, source: bytes | str, filename: str = "", **kwargs) -> ParseResult:
        ...
