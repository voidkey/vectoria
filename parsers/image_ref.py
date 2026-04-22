"""Lazy, streaming-friendly reference to a parsed image.

Background
----------
Before this module, parsers returned a ``dict[str, bytes]`` for every
image in a document. That dict stayed alive through the full ingest
pipeline (parse → metadata extraction → concurrent S3 uploads → DB
inserts), and ``asyncio.gather`` on all upload tasks multiplied the
resident footprint. For a 100-image docx, peak RSS spikes could exceed
500 MB on a 4 GB worker.

Design
------
Each ``ImageRef`` carries metadata + a ``_factory`` that lazily produces
the bytes. The upload pipeline iterates refs with a concurrency-bounded
semaphore, materializes one ref at a time, uploads, then calls
``release()`` so the factory's captured closure state (PIL.Image,
base64 string, temp file, ...) can be garbage-collected.

The win compared to the dict model:
  - parse phase: MinerU no longer decodes all base64 upfront; docling
    holds references into its own result (smaller than a dict of PNGs).
  - upload phase: at most N decoded images live at once (N = semaphore
    size), not every image in the doc.

Contract
--------
  * ``materialize()`` returns bytes; may be called multiple times
    (factory is idempotent).
  * ``release()`` drops the factory; subsequent ``materialize()`` raises.
  * Fields like ``alt`` / ``context`` / ``section_title`` are filled by
    ``parsers.image_metadata.extract_metadata_into_refs`` after parsing —
    parsers leave them at their defaults.
"""
import base64
from dataclasses import dataclass, field
from typing import Callable


class BytesFactory:
    """Picklable factory wrapping pre-materialized bytes.

    Parsers running under ``parsers.isolation.run_isolated`` return
    ``ParseResult`` across a ProcessPoolExecutor boundary, which pickles
    the result. Nested/local ``def _factory`` closures fail to unpickle
    in the parent with ``Can't get local object ...<locals>._factory``,
    because pickle records functions by qualified name and local-scope
    names aren't addressable from outside. This module-level callable
    class pickles cleanly via its class path.
    """
    __slots__ = ("_data",)

    def __init__(self, data: bytes) -> None:
        self._data = data

    def __call__(self) -> bytes:
        return self._data


class Base64Factory:
    """Picklable factory that decodes a base64 payload on demand.

    Holding the b64 string instead of decoded bytes is ~1.33× smaller;
    the decoded bytes live only from ``materialize()`` through the
    single upload call before ``release()`` drops this factory too.
    """
    __slots__ = ("_payload",)

    def __init__(self, payload: str) -> None:
        self._payload = payload

    def __call__(self) -> bytes:
        return base64.b64decode(self._payload)


@dataclass
class ImageRef:
    """Pointer to a single image in a parsed document."""

    # ---- Filled by parser --------------------------------------------------
    name: str
    """Markdown-referenced path, e.g. ``"image_0001.png"``. Used to match
    ``![alt](path)`` references in the exported markdown during metadata
    extraction."""

    mime: str
    """Content type, e.g. ``"image/png"``."""

    width: int | None = None
    """Set by parser when cheaply known (e.g. PIL.Image.size for docling).
    Filled by metadata extraction otherwise."""

    height: int | None = None

    # ---- Filled by parsers.image_metadata.extract_metadata_into_refs -------
    alt: str = ""
    context: str = ""
    section_title: str = ""
    markdown_pos: int | None = None

    # ---- Lazy bytes --------------------------------------------------------
    # MUST be picklable — ``ParseResult`` crosses a ProcessPoolExecutor
    # boundary when ``parser_isolation`` is on, and pickle records
    # functions by qualified name. Use ``BytesFactory`` / ``Base64Factory``
    # above (module-level callables) or any top-level function; never a
    # nested ``def`` / lambda that closes over parser-local state.
    _factory: Callable[[], bytes] | None = field(default=None, repr=False)

    def materialize(self) -> bytes:
        """Produce the image bytes. Raises if already released.

        Safe to call multiple times; the factory is responsible for being
        idempotent (all current factories are).
        """
        if self._factory is None:
            raise RuntimeError(
                f"ImageRef({self.name}): materialize() called after release()",
            )
        return self._factory()

    def release(self) -> None:
        """Drop the factory reference so any closure-captured state (PIL
        images, base64 strings, etc.) becomes eligible for GC. Idempotent.
        """
        self._factory = None

    @property
    def consumed(self) -> bool:
        return self._factory is None
