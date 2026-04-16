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
from parsers.image_metadata import extract_image_metadata
from rag.embedder import get_embedder
from splitter.splitter import Splitter
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

    from parsers.url_parser import download_images, get_wechat_headers

    headers = get_wechat_headers(source_url)
    images = await asyncio.get_running_loop().run_in_executor(
        None, download_images, image_urls, headers,
    )
    if not images:
        return

    image_metas = extract_image_metadata(content, images)

    cfg = get_settings()
    vision_configured = bool(cfg.vision_base_url)

    from api.image_utils import upload_and_store_images
    await upload_and_store_images(
        images=images,
        image_metas=image_metas,
        kb_id=kb_id,
        doc_id=doc_id,
        vision_configured=vision_configured,
    )

    await handle_analyze_images({"kb_id": kb_id, "doc_id": doc_id})
