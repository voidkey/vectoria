"""Task handlers — the actual work that runs in worker processes.

Each handler receives a `payload` dict (deserialized from the task row) and
runs the heavy I/O: splitting, embedding, vector upsert, vision analysis,
image downloads, etc.

These are the same operations that previously ran inside
`asyncio.create_task()` in the API process. Extracting them here means:
  - A crash or OOM only affects the worker, not the API.
  - A failed run is retried automatically (the queue handles this).
  - Progress and timing are tracked via the tasks table.
"""

import asyncio
import logging
import traceback

from sqlalchemy import select

from config import get_settings
from db.base import get_session
from db.helpers import load_doc, update_doc
from db.models import Document, DocumentImage
from infra.metrics import (
    DOCUMENT_OUTCOMES, PARSE_EMPTY_TOTAL, observe_parse,
)
from parsers.image_metadata import extract_metadata_into_refs
from parsers.registry import registry
from rag.embedder import get_embedder
from splitter.splitter import Splitter
from storage import get_storage
from vectorstore.base import ChunkData
from vectorstore.pgvector import PgVectorStore

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Handler registry
# ---------------------------------------------------------------------------

HANDLERS: dict[str, object] = {}  # task_type -> async handler callable


def _register(name: str):
    def decorator(fn):
        HANDLERS[name] = fn
        return fn
    return decorator


async def dispatch(task_type: str, payload: dict) -> None:
    handler = HANDLERS.get(task_type)
    if handler is None:
        raise ValueError(f"Unknown task type: {task_type}")
    await handler(payload)


# ---------------------------------------------------------------------------
# parse_document
# ---------------------------------------------------------------------------

@_register("parse_document")
async def handle_parse_document(payload: dict) -> None:
    """Fetch source, parse to markdown, upload inline images, then fan out.

    This runs everything the API ``_ingest`` used to do synchronously.
    Moving it here lets the API return a ``queued`` response in ~ms
    instead of holding the request open while MinerU/docling chew on
    a large PDF.

    Failures are classified:
      * empty content / content too large → terminal ``failed`` (no retry —
        the input won't fix itself on retry)
      * other exceptions → re-raised so the queue retries with backoff
    """
    doc_id = payload["doc_id"]
    kb_id = payload["kb_id"]
    storage_key = payload.get("storage_key")
    source = payload["source"]
    filename = payload.get("filename", "")
    selected_engine = payload["selected_engine"]

    # Guard: the doc may have been deleted, or a prior attempt may have
    # already completed this work. Don't re-parse or double-enqueue.
    async with get_session() as session:
        result = await session.execute(
            select(Document).where(Document.id == doc_id),
        )
        doc = result.scalar_one_or_none()
    if doc is None:
        logger.info("parse_document: doc %s missing, skipping", doc_id)
        return
    if doc.status in ("completed", "indexing"):
        logger.info(
            "parse_document: doc %s already past parse (%s), skipping",
            doc_id, doc.status,
        )
        return

    await update_doc(doc_id, status="parsing")

    # Fetch the source bytes (file) or URL string.
    if storage_key:
        obj_storage = await get_storage()
        raw: bytes | str = await obj_storage.get(storage_key)
    else:
        raw = source

    parser = registry.get_by_engine(selected_engine)
    try:
        async with observe_parse(selected_engine):
            parse_result = await parser.parse(raw, filename=filename)
    except Exception as e:
        logger.exception("parse_document: parse failed doc=%s", doc_id)
        DOCUMENT_OUTCOMES.labels(outcome="parse_error").inc()
        await update_doc(
            doc_id, status="failed",
            error_msg=f"Parsing failed: {e}"[:500],
            error_type="parse_error",
            error_trace=traceback.format_exc(),
        )
        raise  # re-raise → queue handles retry/backoff/dead-letter

    # Drop the source bytes ASAP; the parser may have materialised them
    # into structures held on parse_result but `raw` itself can go.
    raw = None  # noqa: F841

    content = parse_result.content
    cfg = get_settings()

    # Permanent failures: empty or oversized content won't become valid
    # on retry. Three outcomes depending on what the parser produced and
    # whether the handler is a structured source that legitimately
    # yields image-first posts:
    if len(content.strip()) < cfg.min_content_chars:
        has_image_urls = bool(parse_result.image_urls)
        if parse_result.allow_image_only and has_image_urls:
            # Structured-source handler (xhs / x syndication API)
            # returned a post whose body is below threshold but has
            # images. Treat as image_only: completed + index skipped,
            # but still run the image download + vision pipeline so
            # figures are stored. Retrieval on these docs will match
            # by title / metadata only; image semantics are not fed
            # into the embedding index in Phase 1 (see Phase 3).
            logger.info(
                "parse_document: image_only doc=%s (body %d < %d, images=%d)",
                doc_id, len(content.strip()), cfg.min_content_chars,
                len(parse_result.image_urls or []),
            )
            DOCUMENT_OUTCOMES.labels(outcome="image_only").inc()
            await update_doc(
                doc_id,
                title=parse_result.title or source,
                content=content,
                status="completed",
                error_type="image_only",
                error_msg="",
                error_trace=None,
                image_status="pending",
            )
            from worker.queue import enqueue
            await enqueue("download_and_store_images", {
                "kb_id": kb_id, "doc_id": doc_id,
                "source_url": source,
                "image_urls": parse_result.image_urls,
            })
            return

        logger.warning(
            "parse_document: empty content for doc %s (len=%d < %d)",
            doc_id, len(content.strip()), cfg.min_content_chars,
        )
        PARSE_EMPTY_TOTAL.labels(engine=selected_engine).inc()
        DOCUMENT_OUTCOMES.labels(outcome="empty_content").inc()
        await update_doc(
            doc_id, status="failed",
            error_msg="Parsing returned empty or below-threshold content",
            error_type="empty_content",
        )
        return

    if len(content) > cfg.max_content_chars:
        logger.warning(
            "parse_document: content too large (%d > %d) doc=%s",
            len(content), cfg.max_content_chars, doc_id,
        )
        DOCUMENT_OUTCOMES.labels(outcome="too_large").inc()
        await update_doc(
            doc_id, status="failed",
            error_msg=(
                f"Parsed content exceeds {cfg.max_content_chars} characters"
            ),
            error_type="too_large",
        )
        return

    has_image_urls = bool(parse_result.image_urls)
    has_inline_images = bool(parse_result.image_refs)
    image_status = "pending" if (has_image_urls or has_inline_images) else "none"
    vision_configured = bool(cfg.vision_base_url)

    await update_doc(
        doc_id,
        title=parse_result.title or filename or source,
        content=content, status="indexing",
        image_status=image_status,
        error_msg="", error_type=None, error_trace=None,
    )

    if not has_image_urls and has_inline_images:
        from api.image_stream import stream_upload_and_store_refs
        enriched = extract_metadata_into_refs(content, parse_result.image_refs)
        await stream_upload_and_store_refs(
            enriched, kb_id=kb_id, doc_id=doc_id,
            vision_configured=vision_configured,
        )
        await update_doc(doc_id, image_status="completed")

    # Follow-up tasks. Do this last so a crash mid-handler doesn't leave
    # the queue with duplicate index_document tasks competing.
    from worker.queue import enqueue
    await enqueue("index_document", {"doc_id": doc_id, "kb_id": kb_id})

    if has_image_urls:
        await enqueue("download_and_store_images", {
            "kb_id": kb_id, "doc_id": doc_id,
            "source_url": source, "image_urls": parse_result.image_urls,
        })
    elif has_inline_images and vision_configured:
        await enqueue("analyze_images", {"kb_id": kb_id, "doc_id": doc_id})


# ---------------------------------------------------------------------------
# index_document
# ---------------------------------------------------------------------------

@_register("index_document")
async def handle_index_document(payload: dict) -> None:
    doc_id = payload["doc_id"]
    kb_id = payload["kb_id"]
    doc = await load_doc(doc_id)
    content = doc.content

    cfg = get_settings()
    splitter = Splitter(
        chunk_size=cfg.splitter_chunk_size,
        chunk_overlap=cfg.splitter_chunk_overlap,
    )
    chunks = splitter.split(content)

    embedder = get_embedder()
    texts = [c.content for c in chunks]
    try:
        embeddings = await embedder.embed_batch(texts) if texts else []
        chunk_data = [
            ChunkData(
                id=c.id, doc_id=doc_id, kb_id=kb_id,
                content=c.content, embedding=embeddings[i],
                chunk_index=c.index, parent_id=None,
            )
            for i, c in enumerate(chunks)
        ]
        async with await PgVectorStore.create() as store:
            await store.upsert(chunk_data)
    except Exception as e:
        logger.exception("index_document: indexing failed doc=%s", doc_id)
        DOCUMENT_OUTCOMES.labels(outcome="indexing_error").inc()
        await update_doc(
            doc_id, status="failed",
            error_msg=f"Indexing failed: {e}"[:500],
            error_type="indexing_error",
            error_trace=traceback.format_exc(),
        )
        raise  # re-raise for queue retry/backoff

    DOCUMENT_OUTCOMES.labels(outcome="completed").inc()
    await update_doc(
        doc_id, chunk_count=len(chunk_data), status="completed",
        error_msg="", error_type=None, error_trace=None,
    )


# ---------------------------------------------------------------------------
# analyze_images
# ---------------------------------------------------------------------------

@_register("analyze_images")
async def handle_analyze_images(payload: dict) -> None:
    kb_id = payload["kb_id"]
    doc_id = payload["doc_id"]

    from vision.client import VisionClient
    from storage import get_storage

    cfg = get_settings()
    client = VisionClient(
        base_url=cfg.vision_base_url,
        api_key=cfg.vision_api_key.get_secret_value(),
        model=cfg.vision_model,
    )
    if not client.is_configured:
        return

    obj_storage = await get_storage()

    async with get_session() as session:
        result = await session.execute(
            select(DocumentImage).where(
                DocumentImage.doc_id == doc_id,
                DocumentImage.vision_status == "pending",
            )
        )
        pending = result.scalars().all()

    if not pending:
        return

    sem = asyncio.Semaphore(5)

    async def _describe_one(img: DocumentImage):
        async with sem:
            try:
                img_bytes = await obj_storage.get(img.storage_key)
                description = await client.describe(
                    img_bytes,
                    context=img.context,
                    section_title=img.section_title,
                    alt=img.alt,
                )
                status = "completed" if description else "failed"
            except Exception:
                logger.exception("Vision analysis failed for image %s", img.id)
                description = ""
                status = "failed"

            async with get_session() as sess:
                result = await sess.execute(
                    select(DocumentImage).where(DocumentImage.id == img.id)
                )
                record = result.scalar_one_or_none()
                if record:
                    record.description = description
                    record.vision_status = status
                    await sess.commit()

    await asyncio.gather(*(_describe_one(img) for img in pending))


# ---------------------------------------------------------------------------
# download_and_store_images
# ---------------------------------------------------------------------------

@_register("download_and_store_images")
async def handle_download_and_store_images(payload: dict) -> None:
    kb_id = payload["kb_id"]
    doc_id = payload["doc_id"]
    source_url = payload["source_url"]
    image_urls = payload["image_urls"]
    doc = await load_doc(doc_id)
    content = doc.content

    from api.image_stream import refs_from_dict, stream_upload_and_store_refs
    from parsers.image_metadata import extract_metadata_into_refs
    from parsers.url import download_images_for_url

    # ``download_images_for_url`` threads the source URL's handler:
    # platform-specific Referer/UA headers + image URL canonicalisation
    # (WeChat forces wx_fmt=jpeg, future handlers swap size variants).
    images = await download_images_for_url(source_url, image_urls)
    if not images:
        await update_doc(doc_id, image_status="completed")
        return

    # Adapt the dict into the shared streaming pipeline. Assigning the
    # returned refs then clearing the dict lets the closures hold the
    # only live references to image bytes — each upload can release its
    # bytes as soon as the put() completes.
    refs = refs_from_dict(images)
    images = None  # drop the dict so closures become sole owners

    enriched = extract_metadata_into_refs(content, refs)

    cfg = get_settings()
    vision_configured = bool(cfg.vision_base_url)

    await stream_upload_and_store_refs(
        enriched,
        kb_id=kb_id, doc_id=doc_id,
        vision_configured=vision_configured,
    )

    await update_doc(doc_id, image_status="completed")

    if vision_configured:
        from worker.queue import enqueue
        await enqueue("analyze_images", {"kb_id": kb_id, "doc_id": doc_id})
