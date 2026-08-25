"""Caller-supplied edited renditions of an already-parsed document.

Downstream consumers post-process our parse output — LLM cleanup,
restructuring, merging image descriptions back into prose — and need
somewhere to put the improved version so they can fetch it later.

The design deliberately keeps two *orthogonal* bodies per document:

  ``documents.content``  our parse output. Unchanged by these routes, and
                         still the only input to chunking/embedding.
  ``edits/...`` object   the caller's rendition, pointed at by
                         ``documents.edited_storage_key``.

Orthogonality is the whole point. Because a re-parse writes ``content``
and an edit writes the object key, neither can clobber the other — no
status-machine changes, no revision guard on ``parse_document``, and the
``retry_dead_docs`` reaper stays free to re-parse a failed document
without destroying an edit sitting on top of it.

The trade-off is explicit and documented (docs/API.md §4.5): **an edit
does not enter retrieval.** ``POST /query`` keeps matching against the
original parsed content. Wiring edits into the index means re-chunking
and re-embedding, which is a separate piece of work.
"""
import logging
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Depends, File, Query, UploadFile
from sqlalchemy import select, update

from api.errors import AppError, ErrorCode
from api.rate_limit import RATE_LIMITED_RESPONSE, rate_limit
from api.schemas import EditedContentRequest, EditedContentResponse
from config import get_settings
from db.base import get_session
from db.models import Document
from storage import get_storage

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/knowledgebases")


# Both write verbs share one bucket: from the caller's perspective
# "store my edited version" is a single operation, and splitting the
# bucket would just let a client double its throughput by alternating
# between the JSON and multipart forms.
_edit_limiter = Depends(rate_limit(
    "doc_edit",
    rate=lambda: get_settings().ratelimit_doc_edit_per_min,
    per_seconds=60,
))

# Filename used when the caller PUTs raw text without naming it.
_DEFAULT_TEXT_FILENAME = "content.md"


def _edit_prefix(kb_id: str, doc_id: str) -> str:
    """Prefix for every revision of one document's edited renditions.

    Also the deletion unit — ``delete_document`` drops this whole prefix,
    so any new key layout must stay underneath it.
    """
    return f"edits/{kb_id}/{doc_id}/"


def _edit_key(kb_id: str, doc_id: str, revision: int, filename: str) -> str:
    """Revision goes in the *path*, not the filename, so the caller's own
    filename survives round-tripping (the GET reports it back by taking
    the last path segment)."""
    return f"{_edit_prefix(kb_id, doc_id)}{revision}/{filename}"


def _safe_filename(name: str | None, *, default: str) -> str:
    """Reduce caller-supplied input to a single, harmless path segment.

    The key is built by concatenation, so an unsanitised ``../../`` or a
    leading ``/`` would let a caller write outside their own document's
    prefix — and the tenant check that guards the presigned-upload path
    (``complete_upload``) has no equivalent here because we build the key
    ourselves. ``Path(...).name`` collapses directory components; the
    query-string strip mirrors ``api.image_stream._safe_filename_factory``
    for names that came from a URL.
    """
    if not name:
        return default
    base = Path(name.split("?")[0].replace("\\", "/")).name.strip()
    # ``.`` / ``..`` survive Path(...).name and are not usable segments.
    if not base or base in {".", ".."}:
        return default
    return base[:200]


async def _load_editable_doc(kb_id: str, doc_id: str) -> Document:
    """Fetch the document and reject kinds that have no editable body.

    ``site_capture`` documents carry a SiteProfile JSON in ``profile``
    rather than prose in ``content``; there is nothing for a caller to
    have meaningfully "cleaned up", and accepting an edit would imply a
    rendition the capture export knows nothing about.
    """
    async with get_session() as session:
        result = await session.execute(
            select(Document).where(Document.id == doc_id, Document.kb_id == kb_id)
        )
        doc = result.scalar_one_or_none()
    if doc is None:
        raise AppError(404, ErrorCode.NOT_FOUND, "Document not found")
    if doc.kind != "document":
        raise AppError(
            400, ErrorCode.EDIT_NOT_SUPPORTED,
            f"Documents of kind {doc.kind!r} do not support edited content",
        )
    return doc


async def _claim_revision(
    kb_id: str, doc_id: str, *, base_revision: int | None,
) -> int:
    """Atomically bump ``edited_revision`` and return the claimed value.

    Lock-free on purpose. Two concurrent writers each get a distinct
    revision from the ``RETURNING`` clause, so they cannot collide on an
    object key — which is what would actually corrupt data (one caller's
    bytes landing under the other's revision). Whichever finishes its
    upload last wins the pointer, and the loser's bytes stay retrievable
    at their own key.

    Holding a ``FOR UPDATE`` row lock across the storage PUT instead
    would serialise writers behind a network round-trip; the atomic bump
    gets the same key-uniqueness guarantee for free.

    A crashed upload after a successful bump leaves a gap in the
    sequence. That is harmless: the counter is an identity, not a count.
    """
    stmt = (
        update(Document)
        .where(Document.id == doc_id, Document.kb_id == kb_id)
        .values(edited_revision=Document.edited_revision + 1)
        .returning(Document.edited_revision)
    )
    if base_revision is not None:
        stmt = stmt.where(Document.edited_revision == base_revision)

    async with get_session() as session:
        revision = await session.scalar(stmt)
        await session.commit()

    if revision is None:
        # The document existed a moment ago (_load_editable_doc), so an
        # empty RETURNING means the base_revision predicate failed —
        # someone else wrote in between.
        raise AppError(
            409, ErrorCode.EDIT_REVISION_CONFLICT,
            f"base_revision {base_revision} is stale; re-read "
            "GET .../edited and re-apply the edit on top of the current one",
        )
    return revision


async def _publish_revision(
    kb_id: str, doc_id: str, *, revision: int, key: str,
) -> datetime:
    """Point the document at ``key`` — but only if nothing newer landed.

    The ``edited_revision == revision`` predicate is what makes concurrent
    writes safe: a slower writer that claimed revision 3 and finished
    after revision 4 was published finds the predicate false and no-ops,
    leaving the newer rendition current instead of silently reverting the
    document to older bytes.
    """
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    async with get_session() as session:
        await session.execute(
            update(Document)
            .where(
                Document.id == doc_id,
                Document.kb_id == kb_id,
                Document.edited_revision == revision,
            )
            .values(edited_storage_key=key, edited_at=now)
        )
        await session.commit()
    return now


async def _store_edit(
    kb_id: str, doc_id: str, *,
    raw: bytes, filename: str, content_type: str,
    base_revision: int | None,
) -> EditedContentResponse:
    """Shared tail of both write verbs: claim → upload → publish."""
    revision = await _claim_revision(
        kb_id, doc_id, base_revision=base_revision,
    )
    key = _edit_key(kb_id, doc_id, revision, filename)

    obj_storage = await get_storage()
    await obj_storage.put(key, raw, content_type=content_type)
    edited_at = await _publish_revision(
        kb_id, doc_id, revision=revision, key=key,
    )

    logger.info(
        "edited content stored: kb=%s doc=%s rev=%d key=%s bytes=%d",
        kb_id, doc_id, revision, key, len(raw),
    )
    url = await obj_storage.presign_url(key)
    return EditedContentResponse(
        doc_id=doc_id, kb_id=kb_id, revision=revision,
        filename=filename, object_key=key, url=url,
        edited_at=edited_at.isoformat(),
    )


@router.put(
    "/{kb_id}/documents/{doc_id}/edited",
    response_model=EditedContentResponse,
    responses=RATE_LIMITED_RESPONSE,
    dependencies=[_edit_limiter],
)
async def put_edited_content(
    kb_id: str, doc_id: str, body: EditedContentRequest,
) -> EditedContentResponse:
    """Store a text/markdown edited rendition of an already-parsed doc."""
    await _load_editable_doc(kb_id, doc_id)

    cfg = get_settings()
    if not body.content.strip():
        raise AppError(
            400, ErrorCode.EMPTY_CONTENT,
            "Edited content is empty or whitespace-only",
        )
    if len(body.content) > cfg.max_content_chars:
        raise AppError(
            413, ErrorCode.CONTENT_TOO_LARGE,
            f"Edited content exceeds {cfg.max_content_chars} characters",
            error_data={
                "current": len(body.content), "limit": cfg.max_content_chars,
            },
        )

    return await _store_edit(
        kb_id, doc_id,
        raw=body.content.encode("utf-8"),
        filename=_safe_filename(body.filename, default=_DEFAULT_TEXT_FILENAME),
        content_type="text/markdown; charset=utf-8",
        base_revision=body.base_revision,
    )


@router.post(
    "/{kb_id}/documents/{doc_id}/edited/file",
    response_model=EditedContentResponse,
    responses=RATE_LIMITED_RESPONSE,
    dependencies=[_edit_limiter],
)
async def upload_edited_file(
    kb_id: str, doc_id: str,
    file: UploadFile = File(...),
    base_revision: int | None = Query(
        None,
        description=(
            "Optimistic lock: when supplied, must equal the document's "
            "current edited_revision or the write is rejected with 409."
        ),
    ),
) -> EditedContentResponse:
    """Store an edited rendition that the caller already has as a file.

    The bytes are stored verbatim — no MIME sniff, no parse, no page
    gate. Those gates exist to protect the *parse* pipeline from input it
    cannot handle, and nothing here feeds that pipeline: this is an
    opaque artifact the caller hands back to themselves later. Only size
    is enforced, because that is a cost to us regardless of content.
    """
    await _load_editable_doc(kb_id, doc_id)

    cfg = get_settings()
    if file.size is not None and file.size > cfg.max_upload_bytes:
        raise AppError(
            413, ErrorCode.UPLOAD_TOO_LARGE,
            f"File exceeds {cfg.max_upload_bytes} bytes",
            error_data={"current": file.size, "limit": cfg.max_upload_bytes},
        )

    raw = await file.read()
    if not raw:
        raise AppError(
            400, ErrorCode.EMPTY_UPLOAD,
            "Uploaded file is empty (0 bytes)",
        )
    # Re-check after the read: not every client/transport sets a reliable
    # Content-Length, same reason ``_run_upload_gates`` double-checks.
    if len(raw) > cfg.max_upload_bytes:
        raise AppError(
            413, ErrorCode.UPLOAD_TOO_LARGE,
            f"File exceeds {cfg.max_upload_bytes} bytes",
            error_data={"current": len(raw), "limit": cfg.max_upload_bytes},
        )

    return await _store_edit(
        kb_id, doc_id,
        raw=raw,
        filename=_safe_filename(file.filename, default="edited.bin"),
        content_type=file.content_type or "application/octet-stream",
        base_revision=base_revision,
    )


@router.get(
    "/{kb_id}/documents/{doc_id}/edited",
    response_model=EditedContentResponse,
)
async def get_edited_content(
    kb_id: str, doc_id: str,
    include_content: bool = Query(
        False,
        description=(
            "Inline the artifact in the response body as UTF-8 text. Left "
            "null for artifacts that aren't valid UTF-8 — fetch `url` "
            "instead."
        ),
    ),
) -> EditedContentResponse:
    doc = await _load_editable_doc(kb_id, doc_id)
    if not doc.edited_storage_key:
        raise AppError(
            404, ErrorCode.EDIT_NOT_FOUND,
            "Document has no edited content",
        )

    key = doc.edited_storage_key
    obj_storage = await get_storage()
    url = await obj_storage.presign_url(key)

    content: str | None = None
    if include_content:
        try:
            content = (await obj_storage.get(key)).decode("utf-8")
        except UnicodeDecodeError:
            # Binary rendition (e.g. a cleaned-up .docx). Not an error —
            # the caller still gets metadata plus a download URL.
            logger.info(
                "edited content not UTF-8, returning url only: doc=%s key=%s",
                doc_id, key,
            )

    return EditedContentResponse(
        doc_id=doc_id, kb_id=kb_id,
        revision=doc.edited_revision,
        filename=key.rsplit("/", 1)[-1],
        object_key=key, url=url,
        edited_at=doc.edited_at.isoformat() if doc.edited_at else None,
        content=content,
    )


@router.delete("/{kb_id}/documents/{doc_id}/edited", status_code=204)
async def delete_edited_content(kb_id: str, doc_id: str) -> None:
    """Withdraw the edited rendition; the document falls back to ``content``.

    Unlinks rather than deletes: the stored objects stay put so a caller
    who withdrew by mistake can be recovered from, and so does the
    ``edited_revision`` counter, so the next write claims a fresh key
    instead of overwriting the withdrawn one. Everything is reclaimed
    together when the document itself is deleted.
    """
    doc = await _load_editable_doc(kb_id, doc_id)
    if not doc.edited_storage_key:
        raise AppError(
            404, ErrorCode.EDIT_NOT_FOUND,
            "Document has no edited content",
        )

    async with get_session() as session:
        await session.execute(
            update(Document)
            .where(Document.id == doc_id, Document.kb_id == kb_id)
            .values(edited_storage_key=None, edited_at=None)
        )
        await session.commit()
    logger.info(
        "edited content withdrawn: kb=%s doc=%s rev=%d",
        kb_id, doc_id, doc.edited_revision,
    )
