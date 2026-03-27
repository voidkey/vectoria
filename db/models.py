import uuid
from datetime import datetime
from typing import Optional
from sqlalchemy import String, Text, DateTime, Integer, ForeignKey, func
from sqlalchemy.orm import Mapped, mapped_column, relationship
from db.base import Base


def _uuid() -> str:
    return str(uuid.uuid4())


class KnowledgeBase(Base):
    __tablename__ = "knowledge_bases"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str] = mapped_column(Text, default="", server_default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    documents: Mapped[list["Document"]] = relationship(
        back_populates="knowledge_base", cascade="all, delete-orphan"
    )


class Document(Base):
    __tablename__ = "documents"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    kb_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("knowledge_bases.id", ondelete="CASCADE"), nullable=False
    )
    title: Mapped[str] = mapped_column(String(500), default="", server_default="")
    source: Mapped[str] = mapped_column(Text, default="", server_default="")
    parse_engine: Mapped[str] = mapped_column(String(50), default="", server_default="")
    chunk_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    status: Mapped[str] = mapped_column(
        String(20), default="processing", server_default="processing", nullable=False
    )
    error_msg: Mapped[str] = mapped_column(Text, default="", server_default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    knowledge_base: Mapped["KnowledgeBase"] = relationship(back_populates="documents")
