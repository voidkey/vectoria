from fastapi import APIRouter
from fastapi import Query
from sqlalchemy import select, func

from api.errors import AppError, ErrorCode
from api.schemas import KnowledgeBaseCreate, KnowledgeBaseResponse, KnowledgeBaseListResponse
from db.base import get_session
from db.models import KnowledgeBase
from storage import get_storage
from vectorstore.pgvector import PgVectorStore

router = APIRouter(prefix="/knowledgebases")


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


@router.get("", response_model=KnowledgeBaseListResponse)
async def list_kbs(offset: int = Query(0, ge=0), limit: int = Query(50, ge=1, le=200)):
    async with get_session() as session:
        total = await session.scalar(select(func.count()).select_from(KnowledgeBase))
        result = await session.execute(
            select(KnowledgeBase).order_by(KnowledgeBase.created_at.desc())
            .offset(offset).limit(limit)
        )
        kbs = result.scalars().all()
        return KnowledgeBaseListResponse(
            total=total or 0, offset=offset, limit=limit,
            items=[
                KnowledgeBaseResponse(id=kb.id, name=kb.name, description=kb.description,
                                      created_at=kb.created_at.isoformat())
                for kb in kbs
            ],
        )


@router.delete("/{kb_id}", status_code=204)
async def delete_kb(kb_id: str):
    async with get_session() as session:
        result = await session.execute(select(KnowledgeBase).where(KnowledgeBase.id == kb_id))
        kb = result.scalar_one_or_none()
        if not kb:
            raise AppError(404, ErrorCode.NOT_FOUND, "KnowledgeBase not found")

        # Delete vectors
        async with await PgVectorStore.create() as store:
            await store.delete_by_kb(kb_id)

        # Clean up S3 files (uploaded docs + images)
        obj_storage = await get_storage()
        await obj_storage.delete_prefix(f"upload_files/{kb_id}/")
        await obj_storage.delete_prefix(f"images/{kb_id}/")

        await session.delete(kb)
        await session.commit()
