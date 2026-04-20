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

from sqlalchemy import select

from config import get_settings
from db.base import get_session
from db.helpers import load_doc, update_doc
from db.models import Document, DocumentImage
from infra.metrics import observe_parse
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
        await update_doc(
            doc_id, status="failed",
            error_msg=f"Parsing failed: {e}"[:500],
        )
        raise  # re-raise → queue handles retry/backoff/dead-letter

    # Drop the source bytes ASAP; the parser may have materialised them
    # into structures held on parse_result but `raw` itself can go.
    raw = None  # noqa: F841

    content = parse_result.content

    # Permanent failures: empty or oversized content won't become valid
    # on retry, so mark terminal and return (don't re-raise).
    if not content or content.isspace():
        logger.warning("parse_document: empty content for doc %s", doc_id)
        await update_doc(
            doc_id, status="failed",
            error_msg="Parsing returned empty content",
        )
        return

    cfg = get_settings()
    if len(content) > cfg.max_content_chars:
        logger.warning(
            "parse_document: content too large (%d > %d) doc=%s",
            len(content), cfg.max_content_chars, doc_id,
        )
        await update_doc(
            doc_id, status="failed",
            error_msg=(
                f"Parsed content exceeds {cfg.max_content_chars} characters"
            ),
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
        image_status=image_status, error_msg="",
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

    splitter = Splitter(chunk_size=512, chunk_overlap=64, parent_chunk_size=1024)
    chunks = splitter.split(content)

    indexable = [c for c in chunks if c.parent_id is None]
    embedder = get_embedder()
    texts = [c.content for c in indexable]
    embeddings = await embedder.embed_batch(texts) if texts else []

    chunk_data = [
        ChunkData(
            id=c.id, doc_id=doc_id, kb_id=kb_id,
            content=c.content, embedding=embeddings[i],
            chunk_index=c.index, parent_id=c.parent_id,
        )
        for i, c in enumerate(indexable)
    ]
    async with await PgVectorStore.create() as store:
        await store.upsert(chunk_data)

    await update_doc(
        doc_id, chunk_count=len(chunk_data), status="completed", error_msg="",
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
    from parsers.url import download_images, get_wechat_headers

    headers = get_wechat_headers(source_url)
    images = await asyncio.get_running_loop().run_in_executor(
        None, download_images, image_urls, headers,
    )
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
