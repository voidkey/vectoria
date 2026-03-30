import asyncio
import uuid
import logging
from fastapi import APIRouter, UploadFile, File, HTTPException
from sqlalchemy import select, func as sa_func
from sqlalchemy.orm import selectinload

from api.schemas import DocumentResponse, DocumentIngestResponse, DocumentURLRequest, OutlineItem
from db.base import get_session
from db.models import Document, KnowledgeBase
from parsers.registry import registry
from parsers.outline import extract_outline
from parsers.image_metadata import extract_image_metadata
from storage import get_storage
from splitter.splitter import Splitter
from rag.embedder import Embedder
from vectorstore.pgvector import PgVectorStore
from vectorstore.base import ChunkData
from config import get_settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/knowledgebases")


def _doc_to_response(doc: Document) -> DocumentResponse:
    return DocumentResponse(
        id=doc.id, kb_id=doc.kb_id, title=doc.title, source=doc.source,
        chunk_count=doc.chunk_count,
        status=doc.status, error_msg=doc.error_msg,
        created_at=doc.created_at.isoformat(),
    )


async def _update_doc(doc_id: str, **fields):
    async with get_session() as session:
        result = await session.execute(select(Document).where(Document.id == doc_id))
        doc = result.scalar_one_or_none()
        if doc:
            for k, v in fields.items():
                setattr(doc, k, v)
            await session.commit()


async def _index_document(doc_id: str, kb_id: str, content: str):
    """Background task: chunk, embed, store vectors. Update status on completion."""
    try:
        splitter = Splitter(chunk_size=512, chunk_overlap=64, parent_chunk_size=1024)
        chunks = splitter.split(content)

        indexable = [c for c in chunks if c.parent_id is None]
        embedder = Embedder()
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

        await _update_doc(
            doc_id, chunk_count=len(chunk_data), status="completed", error_msg="",
        )
    except Exception as e:
        logger.exception("Indexing failed: %s", doc_id)
        await _update_doc(doc_id, status="failed", error_msg=str(e))


async def _analyze_images_with_vision(kb_id: str, doc_id: str):
    """Background task: run vision LLM on pending images, update descriptions."""
    from db.models import DocumentImage
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

    # Load pending images
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

    sem = asyncio.Semaphore(5)  # bounded concurrency

    async def _describe_one(img: DocumentImage):
        async with sem:
            try:
                img_bytes = await obj_storage.get(img.storage_key)
                description = await client.describe(img_bytes)
                status = "completed" if description else "failed"
            except Exception:
                logger.exception("Vision analysis failed for image %s", img.id)
                description = ""
                status = "failed"

            async with get_session() as session:
                result = await session.execute(
                    select(DocumentImage).where(DocumentImage.id == img.id)
                )
                record = result.scalar_one_or_none()
                if record:
                    record.description = description
                    record.vision_status = status
                    await session.commit()

    await asyncio.gather(*(_describe_one(img) for img in pending))


async def _download_and_store_images(
    image_urls: list[str], kb_id: str, doc_id: str, source_url: str,
    content: str,
):
    """Background task: download images from URLs, upload to S3, store records, trigger vision."""
    try:
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

        await _analyze_images_with_vision(kb_id, doc_id)
    except Exception:
        logger.exception("Background image processing failed for doc %s", doc_id)


async def _ingest(
    kb_id: str, raw: bytes | str, *, filename: str, source: str,
    selected_engine: str, storage_key: str | None, doc_id: str | None = None,
) -> DocumentIngestResponse:
    """Shared ingest logic: parse, store images, create DB record, kick off background tasks."""
    doc_id = doc_id or str(uuid.uuid4())

    # Parse (synchronous — this is what the caller waits for)
    try:
        parser = registry.get_by_engine(selected_engine)
        parse_result = await parser.parse(raw, filename=filename)
    except Exception as e:
        logger.exception("Document parsing failed")
        raise HTTPException(status_code=422, detail=f"Parsing failed: {e}")

    content = parse_result.content
    title = parse_result.title or (filename or source)
    outline = extract_outline(content)

    # Image handling: background for URL sources, sync for file sources
    has_image_urls = bool(parse_result.image_urls)
    image_count = 0

    if not has_image_urls and parse_result.images:
        # File-based parsers: images already in memory, upload synchronously
        image_metas = extract_image_metadata(content, parse_result.images)
        cfg = get_settings()
        vision_configured = bool(cfg.vision_base_url)
        from api.image_utils import upload_and_store_images
        image_count = await upload_and_store_images(
            images=parse_result.images,
            image_metas=image_metas,
            kb_id=kb_id,
            doc_id=doc_id,
            vision_configured=vision_configured,
        )

    # Create document record
    async with get_session() as session:
        doc = Document(
            id=doc_id, kb_id=kb_id, title=title,
            source=source, parse_engine=selected_engine,
            status="indexing", storage_key=storage_key,
            content=content,
        )
        session.add(doc)
        await session.commit()
        await session.refresh(doc)

    # --- Async phase: index in background ---
    asyncio.create_task(_index_document(doc_id, kb_id, content))

    if has_image_urls:
        # URL sources: download + upload + vision all in background
        asyncio.create_task(_download_and_store_images(
            parse_result.image_urls, kb_id, doc_id, source, content,
        ))
        # Optimistic count; actual may be lower after background download
        image_count = len(parse_result.image_urls)
    elif parse_result.images:
        # File sources: images already uploaded, just run vision
        asyncio.create_task(_analyze_images_with_vision(kb_id, doc_id))

    return DocumentIngestResponse(
        id=doc.id, kb_id=doc.kb_id, title=doc.title, source=doc.source,
        chunk_count=0, status=doc.status, error_msg="",
        created_at=doc.created_at.isoformat(),
        content=content,
        outline=outline,
        image_count=image_count,
    )


async def _validate_kb(kb_id: str):
    async with get_session() as session:
        kb_result = await session.execute(select(KnowledgeBase).where(KnowledgeBase.id == kb_id))
        if not kb_result.scalar_one_or_none():
            raise HTTPException(status_code=404, detail="KnowledgeBase not found")


@router.post("/{kb_id}/documents/file", response_model=DocumentIngestResponse, status_code=201)
async def ingest_file(kb_id: str, file: UploadFile = File(...)):
    await _validate_kb(kb_id)

    filename = file.filename or "upload"
    selected_engine = registry.auto_select(filename=filename)
    raw = await file.read()

    doc_id = str(uuid.uuid4())
    obj_storage = await get_storage()
    storage_key = f"upload_files/{kb_id}/{doc_id}/{filename}"
    await obj_storage.put(storage_key, raw, content_type=file.content_type or "")

    return await _ingest(
        kb_id, raw, filename=filename, source=filename,
        selected_engine=selected_engine, storage_key=storage_key, doc_id=doc_id,
    )


@router.post("/{kb_id}/documents/url", response_model=DocumentIngestResponse, status_code=201)
async def ingest_url(kb_id: str, body: DocumentURLRequest):
    await _validate_kb(kb_id)

    selected_engine = registry.auto_select(url=body.url)
    return await _ingest(
        kb_id, body.url, filename="", source=body.url,
        selected_engine=selected_engine, storage_key=None,
    )


@router.get("/{kb_id}/documents", response_model=list[DocumentResponse])
async def list_documents(kb_id: str):
    await _validate_kb(kb_id)
    async with get_session() as session:
        result = await session.execute(
            select(Document).where(Document.kb_id == kb_id).order_by(Document.created_at.desc())
        )
        docs = result.scalars().all()
        return [_doc_to_response(doc) for doc in docs]


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
            raise HTTPException(status_code=404, detail="Document not found")
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
        )


@router.delete("/{kb_id}/documents/{doc_id}", status_code=204)
async def delete_document(kb_id: str, doc_id: str):
    async with get_session() as session:
        result = await session.execute(
            select(Document).where(Document.id == doc_id, Document.kb_id == kb_id)
        )
        doc = result.scalar_one_or_none()
        if not doc:
            raise HTTPException(status_code=404, detail="Document not found")

        async with await PgVectorStore.create() as store:
            await store.delete_by_doc(doc_id)

        await session.delete(doc)
        await session.commit()
