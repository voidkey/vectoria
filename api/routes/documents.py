import asyncio
import uuid
import logging
from typing import Optional

from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from sqlalchemy import select

from api.schemas import DocumentResponse
from db.base import get_session
from db.models import Document, KnowledgeBase
from parsers.registry import registry
from splitter.splitter import Splitter
from rag.embedder import Embedder
from store.pgvector import PgVectorStore
from store.base import ChunkData

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/knowledgebases")


def _doc_to_response(doc: Document) -> DocumentResponse:
    return DocumentResponse(
        id=doc.id, kb_id=doc.kb_id, title=doc.title, source=doc.source,
        engine=doc.parse_engine, chunk_count=doc.chunk_count,
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


async def _process_document(doc_id: str, kb_id: str, raw: bytes | str, filename: str, selected_engine: str):
    """Background task: parse, split, embed, store, then update document status."""
    try:
        parser = registry.get_by_engine(selected_engine)
        parse_result = await parser.parse(raw, filename=filename)

        splitter = Splitter(chunk_size=512, chunk_overlap=64, parent_chunk_size=1024)
        chunks = splitter.split(parse_result.content)

        # Only embed indexable chunks (non-parent) to avoid wasting API calls
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
            doc_id, title=parse_result.title or filename,
            chunk_count=len(chunk_data), status="completed", error_msg="",
        )
    except Exception as e:
        logger.exception("Document processing failed: %s", doc_id)
        await _update_doc(doc_id, status="failed", error_msg=str(e))


@router.post("/{kb_id}/documents", response_model=DocumentResponse, status_code=201)
async def ingest_document(
    kb_id: str,
    url: Optional[str] = Form(None),
    engine: str = Form("auto"),
    file: Optional[UploadFile] = File(None),
):
    async with get_session() as session:
        kb_result = await session.execute(select(KnowledgeBase).where(KnowledgeBase.id == kb_id))
        if not kb_result.scalar_one_or_none():
            raise HTTPException(status_code=404, detail="KnowledgeBase not found")

    if not url and not file:
        raise HTTPException(status_code=422, detail="Provide either 'url' or 'file'")

    if url:
        selected_engine = engine if engine != "auto" else registry.auto_select(url=url)
        raw: bytes | str = url
        source = url
        filename = ""
    else:
        filename = file.filename or "upload"
        selected_engine = engine if engine != "auto" else registry.auto_select(filename=filename)
        raw = await file.read()
        source = filename

    doc_id = str(uuid.uuid4())
    async with get_session() as session:
        doc = Document(
            id=doc_id, kb_id=kb_id, title=source,
            source=source, parse_engine=selected_engine,
            status="processing",
        )
        session.add(doc)
        await session.commit()
        await session.refresh(doc)
        resp = _doc_to_response(doc)

    asyncio.create_task(_process_document(doc_id, kb_id, raw, filename, selected_engine))
    return resp


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
