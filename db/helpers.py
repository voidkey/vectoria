"""Shared DB helpers used by both API routes and worker handlers."""

from sqlalchemy import select

from db.base import get_session
from db.models import Document


async def load_doc(doc_id: str) -> Document:
    """Load a Document by id, raising ValueError if not found."""
    async with get_session() as session:
        result = await session.execute(select(Document).where(Document.id == doc_id))
        doc = result.scalar_one_or_none()
    if doc is None:
        raise ValueError(f"Document {doc_id} not found")
    return doc


async def update_doc(doc_id: str, **fields) -> None:
    """Update fields on a Document by id."""
    async with get_session() as session:
        result = await session.execute(select(Document).where(Document.id == doc_id))
        doc = result.scalar_one_or_none()
        if doc:
            for k, v in fields.items():
                setattr(doc, k, v)
            await session.commit()
