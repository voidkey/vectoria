import uuid
from datetime import datetime
from typing import Optional
from sqlalchemy import JSON, String, Text, DateTime, Integer, ForeignKey, func
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
    storage_key: Mapped[str | None] = mapped_column(Text, nullable=True, default=None)
    # Legacy MD5 hash kept as read-only fallback during the W5-4
    # migration window. New writes populate ``file_hash_sha256`` and
    # leave MD5 NULL; dedup reads check sha256 first and only fall
    # back to MD5 when sha256 is NULL (pre-migration rows). Drop this
    # column in a later migration once all live rows have sha256.
    file_hash: Mapped[str | None] = mapped_column(
        String(32), nullable=True, default=None, index=True,
    )
    file_hash_sha256: Mapped[str | None] = mapped_column(
        String(64), nullable=True, default=None, index=True,
    )
    parse_engine: Mapped[str] = mapped_column(String(50), default="", server_default="")
    chunk_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    status: Mapped[str] = mapped_column(
        String(20), default="indexing", server_default="indexing", nullable=False
    )
    content: Mapped[str] = mapped_column(Text, default="", server_default="")
    error_msg: Mapped[str] = mapped_column(Text, default="", server_default="")
    image_status: Mapped[str] = mapped_column(
        String(20), default="none", server_default="none", nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    knowledge_base: Mapped["KnowledgeBase"] = relationship(back_populates="documents")
    images: Mapped[list["DocumentImage"]] = relationship(
        back_populates="document", cascade="all, delete-orphan"
    )


class DocumentImage(Base):
    __tablename__ = "document_images"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    doc_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("documents.id", ondelete="CASCADE"), nullable=False
    )
    kb_id: Mapped[str] = mapped_column(String(36), nullable=False)
    storage_key: Mapped[str] = mapped_column(Text, nullable=False)
    filename: Mapped[str] = mapped_column(String(500), nullable=False)
    width: Mapped[int | None] = mapped_column(Integer, nullable=True)
    height: Mapped[int | None] = mapped_column(Integer, nullable=True)
    alt: Mapped[str] = mapped_column(Text, default="", server_default="")
    context: Mapped[str] = mapped_column(Text, default="", server_default="")
    section_title: Mapped[str] = mapped_column(Text, default="", server_default="")
    description: Mapped[str] = mapped_column(Text, default="", server_default="")
    vision_status: Mapped[str] = mapped_column(
        String(20), default="pending", server_default="pending", nullable=False
    )
    image_index: Mapped[int] = mapped_column(Integer, nullable=False)
    # Perceptual hash (phash) of the image bytes. 16-hex-char string =
    # 64-bit hash. Populated at upload time when possible; nullable so
    # rows that predate W3-f (or where PIL couldn't decode the bytes)
    # don't block inserts. Dedup lookup uses indexed equality now,
    # Hamming-distance LSH later once we have real-traffic phash data.
    phash: Mapped[str | None] = mapped_column(
        String(16), nullable=True, index=True, default=None,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    document: Mapped["Document"] = relationship(back_populates="images")


class Task(Base):
    """Persistent task queue backed by PG + FOR UPDATE SKIP LOCKED.

    Replaces asyncio.create_task for background work (index, vision, etc.) so
    tasks survive process restarts and crashes, and worker OOM doesn't lose
    the job — it retries automatically.
    """
    __tablename__ = "tasks"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    task_type: Mapped[str] = mapped_column(String(50), nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    status: Mapped[str] = mapped_column(
        String(20), default="pending", server_default="pending", nullable=False,
    )
    priority: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    attempts: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    max_attempts: Mapped[int] = mapped_column(Integer, default=3, server_default="3")
    error: Mapped[str] = mapped_column(Text, default="", server_default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    locked_until: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
