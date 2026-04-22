from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from typing import ClassVar

from parsers.image_ref import ImageRef


@dataclass
class ParseResult:
    content: str          # Markdown text
    title: str = ""
    image_urls: list[str] | None = None  # URLs for deferred download
    # Parsers that produce embedded images (mineru, docx-native, etc.)
    # populate this list with lazy factories. Downstream uses
    # ``api.image_stream.stream_upload_and_store_refs`` (ingest path)
    # or ``stream_upload_refs`` (/analyze) to upload with a
    # bounded-concurrency release-as-you-go loop so peak memory stays
    # O(concurrency × avg_image_size) instead of O(total_image_bytes).
    image_refs: list[ImageRef] = field(default_factory=list)


class BaseParser(ABC):
    engine_name: ClassVar[str] = ""
    supported_types: ClassVar[list[str]] = []  # file extensions like ".pdf", or ["url"]

    @classmethod
    def is_available(cls) -> bool:
        return True

    @abstractmethod
    async def parse(self, source: bytes | str, filename: str = "", **kwargs) -> ParseResult:
        ...
