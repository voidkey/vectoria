from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from typing import ClassVar

from parsers.image_ref import ImageRef


@dataclass
class ParseResult:
    content: str          # Markdown text
    # Legacy eager-bytes field. Kept for parsers that have no images
    # (markitdown, url text). For image-producing parsers use
    # ``image_refs`` which holds lazy factories instead, so the upload
    # pipeline can stream bytes to S3 without keeping the whole set
    # resident.
    images: dict[str, bytes] = field(default_factory=dict)
    title: str = ""
    image_urls: list[str] | None = None  # URLs for deferred download
    # Preferred surface for new code. Parsers that produce embedded
    # images (docling, mineru) populate this list; downstream uses
    # ``api.image_stream.stream_upload_and_store_refs`` (ingest path)
    # or ``stream_upload_refs`` (/analyze) to upload with a
    # bounded-concurrency release-as-you-go loop.
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
