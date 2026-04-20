"""Bounded-concurrency streaming image uploader.

Iterates ``ImageRef`` objects, materialises bytes for at most N at a
time, uploads each to object storage, and releases the ref so the
factory's captured state can be GC'd. The goal is that heap occupancy
during the upload phase scales with *concurrency* (a small constant)
rather than *document size* (potentially hundreds of images).

Two public entry points:

  ``stream_upload_and_store_refs``  — main ingest path: writes DB rows
  for each uploaded image in the same transaction and hands
  ``vision_status`` (``pending`` / ``skipped``) based on dimension gate.

  ``stream_upload_refs``             — /analyze and other read-only
  consumers: uploads refs and returns presigned URLs, no DB writes.
"""
from __future__ import annotations

import asyncio
import logging
import math
import uuid
from pathlib import Path

from parsers.image_metadata import detect_mime_type
from parsers.image_ref import ImageRef
from storage import get_storage

logger = logging.getLogger(__name__)

_DEFAULT_CONCURRENCY = 3
# Vision LLM's minimum useful image dimension. Anything smaller is
# marked ``skipped`` so the vision worker doesn't spend calls on favicons
# and separator decorations.
_MIN_VISION_DIM = 200


_MIME_EXT = {
    "image/png": ".png",
    "image/jpeg": ".jpg",
    "image/gif": ".gif",
    "image/webp": ".webp",
    "image/bmp": ".bmp",
    "image/tiff": ".tiff",
}


def _safe_filename_factory():
    """Return a closure that dedupes filenames within a batch."""
    used: set[str] = set()

    def _pick(ref: ImageRef, probe_bytes: bytes | None) -> str:
        # Derive base name from the markdown path (strips query strings
        # and directory components that sometimes appear in URL-derived
        # names).
        base = Path(ref.name.split("?")[0]).name or f"image-{uuid.uuid4().hex[:8]}"
        if "." not in base:
            # Infer extension from bytes or declared mime — avoids S3
            # objects without extensions which confuse downstream tools.
            mime = (
                detect_mime_type(probe_bytes, fallback=ref.mime)
                if probe_bytes else ref.mime
            )
            base = f"{base}{_MIME_EXT.get(mime, '.png')}"
        if base not in used:
            used.add(base)
            return base
        stem = Path(base).stem
        suffix = Path(base).suffix
        i = 1
        while True:
            candidate = f"{stem}_{i}{suffix}"
            if candidate not in used:
                used.add(candidate)
                return candidate
            i += 1

    return _pick


def _vision_status_for(
    ref: ImageRef, vision_configured: bool,
) -> str:
    """Decide whether this image should be enqueued for vision analysis."""
    if not vision_configured:
        return "skipped"
    if ref.width is None or ref.height is None:
        # Unknown dims — leave the door open so low-resolution images
        # still get a chance if vision budget allows.
        return "pending"
    if ref.width >= _MIN_VISION_DIM and ref.height >= _MIN_VISION_DIM:
        return "pending"
    return "skipped"


async def stream_upload_and_store_refs(
    refs: list[ImageRef],
    *,
    kb_id: str,
    doc_id: str,
    vision_configured: bool,
    concurrency: int = _DEFAULT_CONCURRENCY,
) -> int:
    """Upload each ref to S3, create ``DocumentImage`` rows, return count.

    Invariants:
      * At most ``concurrency`` refs are materialised at once — peak
        memory is O(concurrency × avg_image_size), independent of the
        number of images in the document.
      * Each ref is ``release()``-ed after its upload returns, so the
        factory's captured closure state (PIL.Image, base64 string,
        docling document, ...) is eligible for GC immediately.
      * DB writes batch at the end in a single transaction — consistent
        with the prior ``upload_and_store_images`` contract.
    """
    if not refs:
        return 0

    # Locally to avoid an import cycle at module load.
    from db.base import get_session
    from db.models import DocumentImage

    obj_storage = await get_storage()
    key_prefix = f"images/{kb_id}/{doc_id}"
    sem = asyncio.Semaphore(concurrency)
    name_picker = _safe_filename_factory()

    # Each _upload_one returns a DocumentImage row on success, None on
    # per-image failure. Failures are isolated so a single S3 hiccup on
    # one image doesn't cancel the other uploads in flight — partial
    # progress is strictly better than none when the worker would
    # otherwise retry the whole parse from scratch.
    async def _upload_one(ref: ImageRef, image_index: int) -> DocumentImage | None:
        async with sem:
            try:
                # materialize off the event loop — factories may do PIL
                # work (docling) or base64 decode (mineru) which are
                # CPU-bound and would otherwise block other async tasks.
                data = await asyncio.to_thread(ref.materialize)
            except Exception:
                logger.exception("materialize failed for %s", ref.name)
                ref.release()
                return None

            try:
                safe_name = name_picker(ref, data)
                s3_key = f"{key_prefix}/{safe_name}"
                content_type = detect_mime_type(data, fallback=ref.mime)
                await obj_storage.put(s3_key, data, content_type=content_type)
            except Exception:
                logger.exception(
                    "image upload failed for %s (s3 put raised)", ref.name,
                )
                return None
            finally:
                # Drop the factory capture ASAP, whether upload succeeded
                # or not — in the failure case we definitely want the
                # memory back.
                ref.release()

            return DocumentImage(
                id=str(uuid.uuid4()),
                doc_id=doc_id, kb_id=kb_id,
                storage_key=s3_key,
                filename=safe_name,
                width=ref.width, height=ref.height,
                alt=ref.alt, context=ref.context,
                section_title=ref.section_title,
                description="",
                vision_status=_vision_status_for(ref, vision_configured),
                image_index=image_index,
            )

    # return_exceptions=True so a single failure doesn't cancel the
    # rest of the batch. Any exception that escaped _upload_one (should
    # not happen given the try/except above, but belt-and-braces) is
    # logged and discarded.
    raw_records = await asyncio.gather(
        *(_upload_one(r, i) for i, r in enumerate(refs)),
        return_exceptions=True,
    )
    records: list[DocumentImage] = []
    for item in raw_records:
        if isinstance(item, DocumentImage):
            records.append(item)
        elif isinstance(item, BaseException):
            logger.exception(
                "unexpected image upload error (escaped _upload_one)",
                exc_info=item,
            )
    if not records:
        return 0

    async with get_session() as session:
        for rec in records:
            session.add(rec)
        await session.commit()

    return len(records)


async def stream_upload_refs(
    refs: list[ImageRef],
    *,
    key_prefix: str | None = None,
    concurrency: int = _DEFAULT_CONCURRENCY,
) -> list[dict]:
    """Upload refs without touching the DB. Used by the /analyze response.

    Returns a list of ``{"id": safe_name, "url": presigned, "context":
    "", "type": "unknown"}`` dicts, compatible with ``ImageInfo``.
    """
    if not refs:
        return []

    obj_storage = await get_storage()
    prefix = key_prefix or f"images/_analyze/{uuid.uuid4()}"
    sem = asyncio.Semaphore(concurrency)
    name_picker = _safe_filename_factory()

    async def _upload_one(ref: ImageRef) -> dict | None:
        async with sem:
            try:
                data = await asyncio.to_thread(ref.materialize)
            except Exception:
                logger.exception("materialize failed for %s", ref.name)
                ref.release()
                return None
            try:
                safe_name = name_picker(ref, data)
                s3_key = f"{prefix}/{safe_name}"
                content_type = detect_mime_type(data, fallback=ref.mime)
                await obj_storage.put(s3_key, data, content_type=content_type)
                url = await obj_storage.presign_url(s3_key)
            except Exception:
                logger.exception("image upload failed for %s", ref.name)
                return None
            finally:
                ref.release()
            return {"id": safe_name, "url": url, "context": "", "type": "unknown"}

    results = await asyncio.gather(
        *(_upload_one(r) for r in refs),
        return_exceptions=True,
    )
    return [r for r in results if isinstance(r, dict)]


# Backwards-compatible helper so call sites that still produce
# ``dict[str, bytes]`` (e.g. the URL parser's download path) can slot
# into the streaming pipeline without a full parser rewrite.
def refs_from_dict(images: dict[str, bytes]) -> list[ImageRef]:
    """Wrap a pre-decoded dict as lazy ImageRefs for the streaming path.

    **The parse-phase win is forfeit here** — the caller already paid
    the cost of having all image bytes resident when it built the dict.
    What this helper buys is *upload-phase* release-as-you-go: each ref
    is released after its upload so the closure's captured bytes can be
    GC'd incrementally, instead of the whole dict staying pinned for
    the duration of the gather().

    Callers that want both wins (parse + upload) need to produce refs
    natively from their data source, e.g. docling/mineru parsers.
    """
    out: list[ImageRef] = []
    for name, data in images.items():
        mime = detect_mime_type(data, fallback="image/png")

        def _factory(d=data) -> bytes:
            return d

        out.append(ImageRef(
            name=name, mime=mime, _factory=_factory,
        ))
    return out
