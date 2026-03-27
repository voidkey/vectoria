from contextlib import asynccontextmanager

from fastapi import APIRouter, HTTPException
from sqlalchemy import select

from api.schemas import KnowledgeBaseCreate, KnowledgeBaseResponse
from db.base import get_session as _get_session
from db.models import KnowledgeBase
from store.pgvector import PgVectorStore

router = APIRouter(prefix="/knowledgebases")


# Wrap the async generator so it can be used as an async context manager,
# and also so tests can patch it as a regular callable returning a context manager.
def get_session():
    return asynccontextmanager(_get_session)()


@router.post("", response_model=KnowledgeBaseResponse, status_code=201)
async def create_kb(body: KnowledgeBaseCreate):
    async with get_session() as session:
        kb = KnowledgeBase(name=body.name, description=body.description)
        session.add(kb)
        await session.commit()
        await session.refresh(kb)
        return KnowledgeBaseResponse(
            id=kb.id, name=kb.name, description=kb.description,
            created_at=kb.created_at.isoformat(),
        )


@router.get("", response_model=list[KnowledgeBaseResponse])
async def list_kbs():
    async with get_session() as session:
        result = await session.execute(select(KnowledgeBase))
        kbs = result.scalars().all()
        return [
            KnowledgeBaseResponse(id=kb.id, name=kb.name, description=kb.description,
                                  created_at=kb.created_at.isoformat())
            for kb in kbs
        ]


@router.delete("/{kb_id}", status_code=204)
async def delete_kb(kb_id: str):
    async with get_session() as session:
        result = await session.execute(select(KnowledgeBase).where(KnowledgeBase.id == kb_id))
        kb = result.scalar_one_or_none()
        if not kb:
            raise HTTPException(status_code=404, detail="KnowledgeBase not found")

        # Delete vectors
        store = await PgVectorStore.create()
        await store.delete_by_kb(kb_id)
        await store.close()

        await session.delete(kb)
        await session.commit()
