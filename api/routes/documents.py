import asyncio
import uuid
import logging
from typing import Optional

from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from sqlalchemy import select

from api.schemas import DocumentResponse, DocumentIngestResponse
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


@router.post("/{kb_id}/documents", response_model=DocumentIngestResponse, status_code=201)
async def ingest_document(
    kb_id: str,
    url: Optional[str] = Form(None),
    file: Optional[UploadFile] = File(None),
):
    # Validate KB exists
    async with get_session() as session:
        kb_result = await session.execute(select(KnowledgeBase).where(KnowledgeBase.id == kb_id))
        if not kb_result.scalar_one_or_none():
            raise HTTPException(status_code=404, detail="KnowledgeBase not found")

    if not url and not file:
        raise HTTPException(status_code=422, detail="Provide either 'url' or 'file'")

    doc_id = str(uuid.uuid4())
    storage_key = None

    # --- Synchronous phase: parse immediately ---
    if url:
        selected_engine = registry.auto_select(url=url)
        raw: bytes | str = url
        source = url
        filename = ""
    else:
        filename = file.filename or "upload"
        selected_engine = registry.auto_select(filename=filename)
        raw = await file.read()
        source = filename

        obj_storage = await get_storage()
        storage_key = f"upload_files/{kb_id}/{doc_id}/{filename}"
        await obj_storage.put(storage_key, raw, content_type=file.content_type or "")

    # Parse (synchronous — this is what the agent waits for)
    try:
        parser = registry.get_by_engine(selected_engine)
        parse_result = await parser.parse(raw, filename=filename)
    except Exception as e:
        logger.exception("Document parsing failed")
        raise HTTPException(status_code=422, detail=f"Parsing failed: {e}")

    content = parse_result.content
    title = parse_result.title or (filename or source)
    outline = extract_outline(content)

    # Extract image metadata and upload to S3
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
        )
        session.add(doc)
        await session.commit()
        await session.refresh(doc)

    # --- Async phase: index + vision in background ---
    asyncio.create_task(_index_document(doc_id, kb_id, content))
    asyncio.create_task(_analyze_images_with_vision(kb_id, doc_id))

    return DocumentIngestResponse(
        id=doc.id, kb_id=doc.kb_id, title=doc.title, source=doc.source,
        chunk_count=0, status=doc.status, error_msg="",
        created_at=doc.created_at.isoformat(),
        content=content,
        outline=outline,
        image_count=image_count,
    )


@router.get("/{kb_id}/documents", response_model=list[DocumentResponse])
async def list_documents(kb_id: str):
    async with get_session() as session:
        kb_result = await session.execute(select(KnowledgeBase).where(KnowledgeBase.id == kb_id))
        if not kb_result.scalar_one_or_none():
            raise HTTPException(status_code=404, detail="KnowledgeBase not found")

        result = await session.execute(
            select(Document).where(Document.kb_id == kb_id).order_by(Document.created_at.desc())
        )
        docs = result.scalars().all()
        return [_doc_to_response(doc) for doc in docs]


@router.get("/{kb_id}/documents/{doc_id}", response_model=DocumentResponse)
async def get_document(kb_id: str, doc_id: str):
    async with get_session() as session:
        result = await session.execute(
            select(Document).where(Document.id == doc_id, Document.kb_id == kb_id)
        )
        doc = result.scalar_one_or_none()
        if not doc:
            raise HTTPException(status_code=404, detail="Document not found")
        return _doc_to_response(doc)


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
