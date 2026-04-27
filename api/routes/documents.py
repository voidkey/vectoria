import asyncio
import hashlib
import os
import time
import uuid
import logging
from fastapi import APIRouter, UploadFile, File, Query
from sqlalchemy import select, func
from sqlalchemy.orm import selectinload

from api.schemas import (
    DocumentResponse, DocumentIngestResponse, DocumentURLRequest,
    DocumentSourceURLResponse, DocumentListResponse, OutlineItem,
)
from api.errors import AppError, ErrorCode
from api.url_validation import validate_url
from db.base import get_session
from db.models import Document, KnowledgeBase
from parsers.registry import registry
from parsers.outline import extract_outline
from storage import get_storage
from vectorstore.pgvector import PgVectorStore
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


def _dedup_response(doc: Document) -> DocumentIngestResponse:
    """Build an ingest response from an existing document (dedup hit).

    ``content`` is deliberately empty: returning the existing doc's
    parsed text to a caller that happened to upload a file with the
    same hash would leak content across the (hash, kb) boundary —
    relevant now that we're on sha256 (W5-4) but was a real concern
    under MD5 collisions. Clients that need the full content can GET
    /documents/{id} with their normal auth.
    """
    return DocumentIngestResponse(
        id=doc.id, kb_id=doc.kb_id, title=doc.title,
        source=doc.source, chunk_count=doc.chunk_count,
        status=doc.status, error_msg=doc.error_msg or "",
        created_at=doc.created_at.isoformat(),
        content="",
        outline=[], image_count=0,
    )


# Statuses at which the parse stage is finished (or permanently failed).
# ``indexing`` means parse has handed off to embedding — content is in
# the DB, so ``?wait=true`` callers have the result they asked for.
_PARSE_TERMINAL_STATUSES = {"indexing", "completed", "failed"}


async def _wait_for_parse(doc_id: str) -> None:
    """Poll the Document row until parse has finished or timeout hits.

    Used by ``?wait=true`` to give sync-style callers the same shape
    they used to get before API slimming. If the timeout elapses we
    return silently — the caller sees whatever state the doc is in
    (typically ``queued``/``parsing``) and can fetch later via GET.
    """
    cfg = get_settings()
    deadline = time.monotonic() + cfg.ingest_wait_timeout_seconds
    poll = cfg.ingest_wait_poll_interval_seconds
    while time.monotonic() < deadline:
        async with get_session() as session:
            doc = await session.get(Document, doc_id)
            if doc is None or doc.status in _PARSE_TERMINAL_STATUSES:
                return
        await asyncio.sleep(poll)


def _queued_response(doc: Document) -> DocumentIngestResponse:
    """Response for the fast path where we just created the doc.

    Avoids a second round-trip to re-read what we already know — keeps
    the API response under ~10 ms in the ``wait=false`` default mode.
    """
    return DocumentIngestResponse(
        id=doc.id, kb_id=doc.kb_id, title=doc.title, source=doc.source,
        chunk_count=doc.chunk_count, status=doc.status,
        error_msg=doc.error_msg or "",
        created_at=doc.created_at.isoformat(),
        content="", outline=[], image_count=0,
        image_status=doc.image_status,
    )


async def _fresh_ingest_response(doc_id: str) -> DocumentIngestResponse:
    """Re-read the Document (including images) after ``?wait=true`` so
    the response reflects whatever state parse reached. Used only on the
    slow path — the fast path uses ``_queued_response`` on the in-memory
    row we just inserted.
    """
    async with get_session() as session:
        result = await session.execute(
            select(Document)
            .options(selectinload(Document.images))
            .where(Document.id == doc_id)
        )
        doc = result.scalar_one_or_none()
    if doc is None:
        # Racy delete between enqueue and wait completion — rare but
        # valid; surface as 404 rather than pretending success.
        raise AppError(404, ErrorCode.NOT_FOUND, "Document not found")

    outline = extract_outline(doc.content) if doc.content else []
    return DocumentIngestResponse(
        id=doc.id, kb_id=doc.kb_id, title=doc.title, source=doc.source,
        chunk_count=doc.chunk_count, status=doc.status,
        error_msg=doc.error_msg or "",
        created_at=doc.created_at.isoformat(),
        content=doc.content or "",
        outline=[OutlineItem(**item) for item in outline],
        image_count=len(doc.images),
        image_status=doc.image_status,
    )


async def _enqueue_ingest(
    kb_id: str, *,
    source: str, storage_key: str | None,
    filename: str, selected_engine: str,
    file_hash: str | None,
    file_hash_sha256: str | None = None,
    doc_id: str | None = None,
    wait: bool = False,
) -> DocumentIngestResponse:
    """Create a Document row in ``queued`` state and enqueue parse work.

    Returns immediately unless ``wait=True``, in which case polls the DB
    for up to ``ingest_wait_timeout_seconds`` so callers still get the
    parsed content inline. The wait window intentionally covers *parse*
    only (``status in indexing|completed|failed``) — embedding and image
    analysis keep running in the background regardless.
    """
    doc_id = doc_id or str(uuid.uuid4())

    from worker.queue import enqueue_in_session
    async with get_session() as session:
        # Fields explicitly seeded instead of relying on SA defaults so
        # the in-memory instance is immediately consistent — the fast
        # path returns ``doc`` without a re-read, and server_default
        # values only materialise on refresh from a real DB.
        doc = Document(
            id=doc_id, kb_id=kb_id,
            title=filename or source,
            source=source, parse_engine=selected_engine,
            status="queued",
            storage_key=storage_key,
            file_hash=file_hash,
            file_hash_sha256=file_hash_sha256,
            content="", image_status="pending",
            chunk_count=0, error_msg="",
        )
        session.add(doc)
        # Atomic with the Document: a DB blip between two separate
        # commits would otherwise orphan a queued doc with no worker
        # task, and ``_find_existing_by_hash`` would then dedup against
        # that wedged row on every retry.
        enqueue_in_session(session, "parse_document", {
            "doc_id": doc_id, "kb_id": kb_id,
            "storage_key": storage_key, "source": source,
            "filename": filename, "selected_engine": selected_engine,
        })
        await session.commit()
        await session.refresh(doc)

    if not wait:
        return _queued_response(doc)

    await _wait_for_parse(doc_id)
    return await _fresh_ingest_response(doc_id)


async def _validate_kb(kb_id: str):
    async with get_session() as session:
        kb_result = await session.execute(select(KnowledgeBase).where(KnowledgeBase.id == kb_id))
        if not kb_result.scalar_one_or_none():
            raise AppError(404, ErrorCode.NOT_FOUND, "KnowledgeBase not found")


def _claimed_ext_label(filename: str) -> str:
    """Bucket a filename's extension into the bounded EXT_FAMILIES key
    set so it's safe to use as a Prometheus label without inviting
    cardinality blowups from arbitrary attacker-supplied extensions.
    """
    from api.mime_sniff import EXT_FAMILIES
    _, ext = os.path.splitext(filename.lower())
    return ext if ext in EXT_FAMILIES else "other"


def _record_upload_reject(
    *, kb_id: str, filename: str, size: int, reason: str, **extra: object,
) -> None:
    """Single source of truth for upload-time 4xx rejections.

    Emits a WARN log with the unbounded specifics (kb / filename /
    size / detected family / limit) so operators can grep
    ``upload_rejected`` to recover the exact file behind a 4xx — the
    HTTP access log only has status code, not filename. Bumps the
    bounded ``UPLOAD_REJECTED_TOTAL`` counter so alert rules can fire
    on sustained rejection rate.
    """
    from infra.metrics import UPLOAD_REJECTED_TOTAL
    extra_str = " ".join(f"{k}={v}" for k, v in extra.items())
    logger.warning(
        "upload_rejected kb=%s filename=%s size=%d reason=%s %s",
        kb_id, filename, size, reason, extra_str,
    )
    UPLOAD_REJECTED_TOTAL.labels(
        reason=reason,
        claimed_ext=_claimed_ext_label(filename),
    ).inc()


@router.post("/{kb_id}/documents/file", response_model=DocumentIngestResponse, status_code=201)
async def ingest_file(
    kb_id: str,
    file: UploadFile = File(...),
    wait: bool = Query(
        False,
        description=(
            "When true, block until the parse stage is done (or timeout) "
            "and return the content in the response. Default false → "
            "immediate queued response; poll GET /documents/{id} for progress."
        ),
    ),
):
    await _validate_kb(kb_id)

    cfg = get_settings()

    # Size gate #1: use Starlette's reported size if present. Cheapest rejection
    # path — fail before we even call .read() on multipart body.
    if file.size is not None and file.size > cfg.max_upload_bytes:
        _record_upload_reject(
            kb_id=kb_id, filename=file.filename or "upload",
            size=file.size, reason="too_large", limit=cfg.max_upload_bytes,
        )
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
        _record_upload_reject(
            kb_id=kb_id, filename=filename, size=len(raw),
            reason="too_large", limit=cfg.max_upload_bytes,
        )
        raise AppError(
            413, ErrorCode.UPLOAD_TOO_LARGE,
            f"File exceeds {cfg.max_upload_bytes} bytes",
        )

    # MIME sniff gate: reject cross-family magic/extension mismatch when
    # STRICT_MIME_CHECK=True. In non-strict mode we still bump the counter
    # so operators can observe mismatches before flipping the flag on.
    from api.mime_sniff import check_mime
    from infra.metrics import UPLOAD_MIME_MISMATCH_TOTAL
    ok, detected = check_mime(filename, raw[:2048])
    if not ok:
        UPLOAD_MIME_MISMATCH_TOTAL.labels(
            claimed_ext=_claimed_ext_label(filename),
            detected=detected or "(none)",
        ).inc()
        if cfg.strict_mime_check:
            _record_upload_reject(
                kb_id=kb_id, filename=filename, size=len(raw),
                reason="mime_mismatch", detected=detected or "(none)",
            )
            _, claimed_ext = os.path.splitext(filename.lower())
            raise AppError(
                400, ErrorCode.MIME_MISMATCH,
                f"File content doesn't match extension {claimed_ext!r} "
                f"(detected family: {detected})",
            )
        logger.warning(
            "mime_mismatch (non-strict, allowed): filename=%s detected=%s",
            filename, detected,
        )

    # Per-KB file-hash dedup. Idempotency for accidental retries and
    # double-uploads that previously re-ran parse + embed and OOM'd the host.
    #
    # sha256 is the primary key for W5-4+; MD5 is still computed so
    # we can match pre-migration rows whose only hash is legacy MD5.
    # New writes leave MD5 NULL — no new MD5 rows get created.
    file_hash_sha256 = hashlib.sha256(raw).hexdigest()
    legacy_md5 = hashlib.md5(raw).hexdigest()
    existing = await _find_existing_by_hash(
        kb_id, sha256=file_hash_sha256, md5=legacy_md5,
    )
    if existing is not None:
        logger.info(
            "Dedup hit: kb=%s sha256=%s existing_doc=%s status=%s",
            kb_id, file_hash_sha256, existing.id, existing.status,
        )
        return _dedup_response(existing)

    doc_id = str(uuid.uuid4())
    obj_storage = await get_storage()
    storage_key = f"upload_files/{kb_id}/{doc_id}/{filename}"
    await obj_storage.put(storage_key, raw, content_type=file.content_type or "")

    # Drop the upload buffer before enqueue so concurrent requests don't
    # stack 50 MB each while waiting on the DB round-trip.
    raw = None  # noqa: F841

    return await _enqueue_ingest(
        kb_id,
        source=filename, storage_key=storage_key,
        filename=filename, selected_engine=selected_engine,
        file_hash=None, file_hash_sha256=file_hash_sha256,
        doc_id=doc_id, wait=wait,
    )


async def _find_existing_by_hash(
    kb_id: str, *,
    sha256: str | None = None,
    md5: str | None = None,
) -> Document | None:
    """Return an existing live document for ``(kb_id, hash)``, if any.

    Only non-``failed`` rows count as live — a prior ``failed`` attempt
    shouldn't block a fresh retry.

    Pass ``sha256`` (new primary dedup key, W5-4) and/or ``md5`` (URL
    hashes still use MD5 since URL strings are low-entropy and MD5's
    collision weakness doesn't change risk there). For file content,
    callers pass ``sha256`` and the query also matches pre-migration
    rows whose only hash is legacy MD5, via a second lookup.
    """
    _LIVE_STATUSES = ("completed", "indexing", "queued", "parsing")
    async with get_session() as session:
        if sha256 is not None:
            result = await session.execute(
                select(Document)
                .where(
                    Document.kb_id == kb_id,
                    Document.file_hash_sha256 == sha256,
                    Document.status.in_(_LIVE_STATUSES),
                )
                .order_by(Document.created_at.desc())
                .limit(1)
            )
            hit = result.scalar_one_or_none()
            if hit is not None:
                return hit
        if md5 is not None:
            result = await session.execute(
                select(Document)
                .where(
                    Document.kb_id == kb_id,
                    Document.file_hash == md5,
                    Document.status.in_(_LIVE_STATUSES),
                )
                .order_by(Document.created_at.desc())
                .limit(1)
            )
            return result.scalar_one_or_none()
        return None


@router.post("/{kb_id}/documents/url", response_model=DocumentIngestResponse, status_code=201)
async def ingest_url(
    kb_id: str,
    body: DocumentURLRequest,
    wait: bool = Query(False),
):
    await validate_url(body.url)
    await _validate_kb(kb_id)

    # URL dedup: sha256 of the URL string. Also check the legacy MD5
    # so pre-W5-4 URL ingests still dedup instead of being re-enqueued.
    url_sha256 = hashlib.sha256(body.url.encode()).hexdigest()
    legacy_url_md5 = hashlib.md5(body.url.encode()).hexdigest()
    existing = await _find_existing_by_hash(
        kb_id, sha256=url_sha256, md5=legacy_url_md5,
    )
    if existing is not None:
        logger.info("URL dedup hit: kb=%s url=%s existing_doc=%s", kb_id, body.url, existing.id)
        return _dedup_response(existing)

    selected_engine = registry.auto_select(url=body.url)
    return await _enqueue_ingest(
        kb_id,
        source=body.url, storage_key=None,
        filename="", selected_engine=selected_engine,
        file_hash=None, file_hash_sha256=url_sha256, wait=wait,
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
