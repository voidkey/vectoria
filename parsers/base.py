from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from typing import ClassVar

from parsers.image_ref import ImageRef


class PermanentParseError(Exception):
    """Marker: this file/URL cannot be parsed by *any* engine, and
    retrying with the same engine won't help either.

    The worker handler short-circuits on this — marks the doc failed
    immediately and returns success to the queue, so:
      * no fallback attempts in this attempt (chain is futile)
      * no queue retries (next attempt would hit the same wall)
      * no dead-task alert noise (operator has nothing to action)

    Use sparingly: only for truly hopeless cases where you can prove
    the failure is content-intrinsic, not infrastructure-transient.
    Examples:
      * URL on a known anti-bot blacklist (parsers/url/_blacklist.py)
      * file claims one extension but bytes match a blocked family
        (we currently reject those at the API gate, not here)

    For ambiguous failures (network, library bug, edge case) keep
    raising the original exception — handler's per-attempt fallback
    + queue retry exists exactly for those.
    """


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
    # When True AND content length (post-.strip()) is below
    # cfg.min_content_chars AND image_urls is non-empty, the worker
    # treats the doc as ``image_only`` (completed, but index_document skipped)
    # instead of ``failed``. Only handlers whose data source is
    # structured (API-backed or reliable structured DOM) should opt in;
    # HTML-scraped handlers must keep this False so silent anti-bot
    # failures don't get laundered into valid docs.
    allow_image_only: bool = False


class BaseParser(ABC):
    engine_name: ClassVar[str] = ""
    supported_types: ClassVar[list[str]] = []  # file extensions like ".pdf", or ["url"]

    @classmethod
    def is_available(cls) -> bool:
        return True

    @abstractmethod
    async def parse(self, source: bytes | str, filename: str = "", **kwargs) -> ParseResult:
        ...
