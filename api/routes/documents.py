import asyncio
import hashlib
import uuid
import logging
from fastapi import APIRouter, UploadFile, File, Query
from sqlalchemy import select, func
from sqlalchemy.orm import selectinload

from api.schemas import DocumentResponse, DocumentIngestResponse, DocumentURLRequest, DocumentSourceURLResponse, DocumentListResponse, OutlineItem
from api.errors import AppError, ErrorCode
from api.url_validation import validate_url
from db.base import get_session
from db.helpers import update_doc
from db.models import Document, KnowledgeBase
from parsers.registry import registry
from parsers.outline import extract_outline
from parsers.image_metadata import extract_image_metadata
from storage import get_storage
from vectorstore.pgvector import PgVectorStore
from config import get_settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/knowledgebases")

# Concurrency gate: bounds how many file/URL ingestions run at once in the API
# process. Each ingestion holds uploaded bytes + parsed content in memory; this
# prevents N concurrent uploads from piling up N × 50 MB and OOM-ing the API.
# Initialized lazily (asyncio objects must be created inside the event loop).
_ingest_sem: asyncio.Semaphore | None = None


def _get_ingest_sem() -> asyncio.Semaphore:
    global _ingest_sem  # noqa: PLW0603
    if _ingest_sem is None:
        _ingest_sem = asyncio.Semaphore(get_settings().max_concurrent_ingestions)
    return _ingest_sem


def _doc_to_response(doc: Document) -> DocumentResponse:
    return DocumentResponse(
        id=doc.id, kb_id=doc.kb_id, title=doc.title, source=doc.source,
        chunk_count=doc.chunk_count,
        status=doc.status, error_msg=doc.error_msg,
        created_at=doc.created_at.isoformat(),
    )


def _dedup_response(doc: Document) -> DocumentIngestResponse:
    """Build an ingest response from an existing document (dedup hit)."""
    return DocumentIngestResponse(
        id=doc.id, kb_id=doc.kb_id, title=doc.title,
        source=doc.source, chunk_count=doc.chunk_count,
        status=doc.status, error_msg=doc.error_msg or "",
        created_at=doc.created_at.isoformat(),
        content=doc.content or "",
        outline=[], image_count=0,
    )


async def _ingest(
    kb_id: str, raw: bytes | str, *, filename: str, source: str,
    selected_engine: str, storage_key: str | None, doc_id: str | None = None,
    file_hash: str | None = None,
) -> DocumentIngestResponse:
    """Shared ingest logic: parse, store images, create DB record, kick off background tasks."""
    sem = _get_ingest_sem()
    if sem.locked():
        raise AppError(
            429, ErrorCode.INGEST_BUSY,
            "Too many concurrent ingestions; try again shortly",
        )
    await sem.acquire()
    try:
        doc_id = doc_id or str(uuid.uuid4())

        try:
            parser = registry.get_by_engine(selected_engine)
            parse_result = await parser.parse(raw, filename=filename)
        except Exception as e:
            logger.exception("Document parsing failed")
            raise AppError(422, ErrorCode.PARSE_ERROR, f"Parsing failed: {e}")

        content = parse_result.content
        title = parse_result.title or (filename or source)

        if not content or content.isspace():
            logger.warning("Parsed content is empty for source: %s", source)
            raise AppError(422, ErrorCode.EMPTY_CONTENT, "Parsing returned empty content")

        max_chars = get_settings().max_content_chars
        if len(content) > max_chars:
            logger.warning(
                "Parsed content too large (%d chars > %d) for source: %s",
                len(content), max_chars, source,
            )
            raise AppError(
                413, ErrorCode.CONTENT_TOO_LARGE,
                f"Parsed content exceeds {max_chars} characters",
            )

        outline = extract_outline(content)

        has_image_urls = bool(parse_result.image_urls)
        has_inline_images = bool(parse_result.images)
        image_status = "pending" if (has_image_urls or has_inline_images) else "none"

        async with get_session() as session:
            doc = Document(
                id=doc_id, kb_id=kb_id, title=title,
                source=source, parse_engine=selected_engine,
                status="indexing", storage_key=storage_key,
                file_hash=file_hash,
                content=content,
                image_status=image_status,
            )
            session.add(doc)
            await session.commit()
            await session.refresh(doc)


        image_count = 0

        if not has_image_urls and has_inline_images:
            image_metas = extract_image_metadata(content, parse_result.images)
            vision_configured = bool(get_settings().vision_base_url)
            from api.image_utils import upload_and_store_images
            image_count = await upload_and_store_images(
                images=parse_result.images,
                image_metas=image_metas,
                kb_id=kb_id,
                doc_id=doc_id,
                vision_configured=vision_configured,
            )
            await update_doc(doc_id, image_status="completed")

        from worker.queue import enqueue
        await enqueue("index_document", {"doc_id": doc_id, "kb_id": kb_id})

        if has_image_urls:
            await enqueue("download_and_store_images", {
                "kb_id": kb_id, "doc_id": doc_id,
                "source_url": source, "image_urls": parse_result.image_urls,
            })
            image_count = len(parse_result.image_urls)
        elif parse_result.images:
            await enqueue("analyze_images", {"kb_id": kb_id, "doc_id": doc_id})

        return DocumentIngestResponse(
            id=doc.id, kb_id=doc.kb_id, title=doc.title, source=doc.source,
            chunk_count=0, status=doc.status, error_msg="",
            created_at=doc.created_at.isoformat(),
            content=content,
            outline=outline,
            image_count=image_count,
            image_status=image_status,
        )
    finally:
        sem.release()


async def _validate_kb(kb_id: str):
    async with get_session() as session:
        kb_result = await session.execute(select(KnowledgeBase).where(KnowledgeBase.id == kb_id))
        if not kb_result.scalar_one_or_none():
            raise AppError(404, ErrorCode.NOT_FOUND, "KnowledgeBase not found")


@router.post("/{kb_id}/documents/file", response_model=DocumentIngestResponse, status_code=201)
async def ingest_file(kb_id: str, file: UploadFile = File(...)):
    await _validate_kb(kb_id)

    cfg = get_settings()

    # Size gate #1: use Starlette's reported size if present. Cheapest rejection
    # path — fail before we even call .read() on multipart body.
    if file.size is not None and file.size > cfg.max_upload_bytes:
        raise AppError(
            413, ErrorCode.UPLOAD_TOO_LARGE,
            f"File exceeds {cfg.max_upload_bytes} bytes",
        )

    filename = file.filename or "upload"
    selected_engine = registry.auto_select(filename=filename)
    raw = await file.read()

    # Size gate #2: some clients / transports don't set Content-Length reliably,
    # so re-check after the read in case file.size was None.
    if len(raw) > cfg.max_upload_bytes:
        raise AppError(
            413, ErrorCode.UPLOAD_TOO_LARGE,
            f"File exceeds {cfg.max_upload_bytes} bytes",
        )

    # Per-KB file-hash dedup. Idempotency for accidental retries and
    # double-uploads that previously re-ran parse + embed and OOM'd the host.
    file_hash = hashlib.md5(raw).hexdigest()
    existing = await _find_existing_by_hash(kb_id, file_hash)
    if existing is not None:
        logger.info(
            "Dedup hit: kb=%s hash=%s existing_doc=%s status=%s",
            kb_id, file_hash, existing.id, existing.status,
        )
        return _dedup_response(existing)

    doc_id = str(uuid.uuid4())
    obj_storage = await get_storage()
    storage_key = f"upload_files/{kb_id}/{doc_id}/{filename}"
    await obj_storage.put(storage_key, raw, content_type=file.content_type or "")

    return await _ingest(
        kb_id, raw, filename=filename, source=filename,
        selected_engine=selected_engine, storage_key=storage_key, doc_id=doc_id,
        file_hash=file_hash,
    )


async def _find_existing_by_hash(kb_id: str, file_hash: str) -> Document | None:
    """Return an existing live document for this (kb_id, file_hash), if any.

    Only `completed` or `indexing` rows count as live — a prior `failed`
    attempt should not block a fresh retry.
    """
    async with get_session() as session:
        result = await session.execute(
            select(Document)
            .where(
                Document.kb_id == kb_id,
                Document.file_hash == file_hash,
                Document.status.in_(("completed", "indexing")),
            )
            .order_by(Document.created_at.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()


@router.post("/{kb_id}/documents/url", response_model=DocumentIngestResponse, status_code=201)
async def ingest_url(kb_id: str, body: DocumentURLRequest):
    await validate_url(body.url)
    await _validate_kb(kb_id)

    # URL dedup: hash the URL string itself.
    url_hash = hashlib.md5(body.url.encode()).hexdigest()
    existing = await _find_existing_by_hash(kb_id, url_hash)
    if existing is not None:
        logger.info("URL dedup hit: kb=%s url=%s existing_doc=%s", kb_id, body.url, existing.id)
        return _dedup_response(existing)

    selected_engine = registry.auto_select(url=body.url)
    return await _ingest(
        kb_id, body.url, filename="", source=body.url,
        selected_engine=selected_engine, storage_key=None,
        file_hash=url_hash,
    )


@router.get("/{kb_id}/documents", response_model=DocumentListResponse)
async def list_documents(kb_id: str, offset: int = Query(0, ge=0), limit: int = Query(50, ge=1, le=200)):
    await _validate_kb(kb_id)
    async with get_session() as session:
        total = await session.scalar(
            select(func.count()).select_from(Document).where(Document.kb_id == kb_id)
        )
        result = await session.execute(
            select(Document).where(Document.kb_id == kb_id)
            .order_by(Document.created_at.desc())
            .offset(offset).limit(limit)
        )
        docs = result.scalars().all()
        return DocumentListResponse(
            total=total or 0, offset=offset, limit=limit,
            items=[_doc_to_response(doc) for doc in docs],
        )


@router.get("/{kb_id}/documents/{doc_id}", response_model=DocumentIngestResponse)
async def get_document(kb_id: str, doc_id: str):
    async with get_session() as session:
        result = await session.execute(
            select(Document)
            .options(selectinload(Document.images))
            .where(Document.id == doc_id, Document.kb_id == kb_id)
        )
        doc = result.scalar_one_or_none()
        if not doc:
            raise AppError(404, ErrorCode.NOT_FOUND, "Document not found")
        outline = extract_outline(doc.content) if doc.content else []
        image_count = len(doc.images)
        return DocumentIngestResponse(
            id=doc.id, kb_id=doc.kb_id, title=doc.title, source=doc.source,
            chunk_count=doc.chunk_count,
            status=doc.status, error_msg=doc.error_msg,
            created_at=doc.created_at.isoformat(),
            content=doc.content,
            outline=[OutlineItem(**item) for item in outline],
            image_count=image_count,
            image_status=doc.image_status,
        )


@router.get("/{kb_id}/documents/{doc_id}/source_url", response_model=DocumentSourceURLResponse)
async def get_document_source_url(kb_id: str, doc_id: str):
    async with get_session() as session:
        result = await session.execute(
            select(Document).where(Document.id == doc_id, Document.kb_id == kb_id)
        )
        doc = result.scalar_one_or_none()
        if not doc:
            raise AppError(404, ErrorCode.NOT_FOUND, "Document not found")

    if doc.storage_key:
        obj_storage = await get_storage()
        url = await obj_storage.presign_url(doc.storage_key)
        return DocumentSourceURLResponse(doc_id=doc.id, source_type="file", url=url)
    else:
        return DocumentSourceURLResponse(doc_id=doc.id, source_type="url", url=doc.source)


@router.delete("/{kb_id}/documents/{doc_id}", status_code=204)
async def delete_document(kb_id: str, doc_id: str):
    async with get_session() as session:
        result = await session.execute(
            select(Document).where(Document.id == doc_id, Document.kb_id == kb_id)
        )
        doc = result.scalar_one_or_none()
        if not doc:
            raise AppError(404, ErrorCode.NOT_FOUND, "Document not found")

        async with await PgVectorStore.create() as store:
            await store.delete_by_doc(doc_id)

        # Clean up S3 files (uploaded doc + images)
        obj_storage = await get_storage()
        if doc.storage_key:
            await obj_storage.delete(doc.storage_key)
        await obj_storage.delete_prefix(f"images/{kb_id}/{doc_id}/")

        await session.delete(doc)
        await session.commit()
