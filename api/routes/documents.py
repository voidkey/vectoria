import uuid
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from sqlalchemy import select

from api.schemas import DocumentResponse
from db.base import get_session as _get_session
from db.models import Document, KnowledgeBase
from parsers.registry import registry
from splitter.splitter import Splitter
from rag.embedder import Embedder
from store.pgvector import PgVectorStore
from store.base import ChunkData
from config import get_settings

router = APIRouter(prefix="/knowledgebases")


def get_session():
    return asynccontextmanager(_get_session)()


@router.post("/{kb_id}/documents", response_model=DocumentResponse, status_code=201)
async def ingest_document(
    kb_id: str,
    url: Optional[str] = Form(None),
    engine: str = Form("auto"),
    file: Optional[UploadFile] = File(None),
):
    async with get_session() as session:
        # Verify KB exists
        kb_result = await session.execute(select(KnowledgeBase).where(KnowledgeBase.id == kb_id))
        if not kb_result.scalar_one_or_none():
            raise HTTPException(status_code=404, detail="KnowledgeBase not found")

    if not url and not file:
        raise HTTPException(status_code=422, detail="Provide either 'url' or 'file'")

    cfg = get_settings()

    # Parse
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

    parser = registry.get_by_engine(selected_engine)
    parse_result = await parser.parse(raw, filename=filename)

    # Split
    splitter = Splitter(chunk_size=512, chunk_overlap=64, parent_chunk_size=1024)
    chunks = splitter.split(parse_result.content)

    # Embed
    embedder = Embedder()
    texts = [c.content for c in chunks]
    embeddings = await embedder.embed_batch(texts) if texts else []

    # Upsert to vector store
    doc_id = str(uuid.uuid4())
    store = await PgVectorStore.create()
    chunk_data = [
        ChunkData(
            id=c.id, doc_id=doc_id, kb_id=kb_id,
            content=c.content, embedding=embeddings[i],
            chunk_index=c.index, parent_id=c.parent_id,
        )
        for i, c in enumerate(chunks)
        if c.parent_id is None  # only index child (or flat) chunks
    ]
    await store.upsert(chunk_data)
    await store.close()

    # Save metadata
    async with get_session() as session:
        doc = Document(
            id=doc_id, kb_id=kb_id, title=parse_result.title or source,
            source=source, parse_engine=selected_engine, chunk_count=len(chunk_data),
        )
        session.add(doc)
        await session.commit()
        await session.refresh(doc)

        return DocumentResponse(
            id=doc.id, kb_id=doc.kb_id, title=doc.title, source=doc.source,
            engine=doc.parse_engine, chunk_count=doc.chunk_count,
            created_at=doc.created_at.isoformat(),
        )
