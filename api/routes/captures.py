"""Website capture endpoints: create (enqueue), get (poll), export."""
import io
import logging
import uuid

from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import select

from api.errors import AppError, ErrorCode, error_fields
from api.schemas import CaptureResponse, CreateCaptureRequest
from api.url_validation import validate_url
from api.doc_title import title_from_url
from db.base import get_session
from db.models import Document, DocumentImage, KnowledgeBase
from storage import get_storage
from worker.queue import enqueue_in_session

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/knowledgebases")


async def _validate_kb(kb_id: str) -> None:
    async with get_session() as session:
        if not (await session.execute(
                select(KnowledgeBase).where(KnowledgeBase.id == kb_id))).scalar_one_or_none():
            raise AppError(404, ErrorCode.NOT_FOUND, "KnowledgeBase not found")


async def _load_capture(kb_id: str, cid: str) -> Document | None:
    async with get_session() as session:
        return (await session.execute(select(Document).where(
            Document.id == cid, Document.kb_id == kb_id,
            Document.kind == "site_capture"))).scalar_one_or_none()


def _capture_not_found() -> AppError:
    return AppError(404, ErrorCode.CAPTURE_NOT_FOUND, "Capture not found")


@router.post("/{kb_id}/captures", status_code=202, response_model=CaptureResponse)
async def create_capture(kb_id: str, body: CreateCaptureRequest) -> CaptureResponse:
    await _validate_kb(kb_id)
    await validate_url(body.url)          # format + SSRF; raises AppError(400)
    doc_id = str(uuid.uuid4())
    async with get_session() as session:
        # Placeholder until run_capture reports the page's own title;
        # the raw URL is both unreadable and (for share links) wider
        # than the title column.
        doc = Document(id=doc_id, kb_id=kb_id, kind="site_capture",
                       title=title_from_url(body.url), source=body.url,
                       status="queued",
                       index_status="skipped", image_status="none")
        session.add(doc)
        enqueue_in_session(session, "capture_site",
                           {"doc_id": doc_id, "kb_id": kb_id, "url": body.url})
        await session.commit()
        await session.refresh(doc)
    return CaptureResponse(id=doc.id, kb_id=kb_id, status=doc.status,
                           image_status=doc.image_status,
                           created_at=doc.created_at.isoformat())


async def _hydrate_profile(doc: Document) -> dict | None:
    """Refresh presigned URLs + vision fields in the stored profile."""
    if not doc.profile:
        return None
    profile = dict(doc.profile)
    storage = await get_storage()
    async with get_session() as session:
        imgs = (await session.execute(select(DocumentImage).where(
            DocumentImage.doc_id == doc.id))).scalars().all()
    by_id = {i.id: i for i in imgs}
    for a in profile.get("assets", []):
        if a.get("storage_key"):
            a["url"] = await storage.presign_url(a["storage_key"])
        img = by_id.get(a.get("image_id"))
        if img is not None:
            a["description"], a["vision_status"] = img.description, img.vision_status
    for s in profile.get("screenshots", []):
        img = by_id.get(s.get("image_id"))
        if img is not None:
            s["url"] = await storage.presign_url(img.storage_key)
    for role in ("display", "body"):
        for f in profile.get("fonts", {}).get(role, {}).get("files", []):
            if f.get("url"):
                f["url"] = await storage.presign_url(f["url"])
    return profile


@router.get("/{kb_id}/captures/{cid}", response_model=CaptureResponse)
async def get_capture(kb_id: str, cid: str) -> CaptureResponse:
    doc = await _load_capture(kb_id, cid)
    if doc is None:
        raise _capture_not_found()
    return CaptureResponse(id=doc.id, kb_id=kb_id, status=doc.status,
                           image_status=doc.image_status, error_msg=doc.error_msg,
                           profile=await _hydrate_profile(doc),
                           created_at=doc.created_at.isoformat(),
                           **error_fields(doc.error_code))


@router.get("/{kb_id}/captures/{cid}/export")
async def export_capture(kb_id: str, cid: str, format: str = Query("hyperframes")):
    if format != "hyperframes":
        raise AppError(422, ErrorCode.VALIDATION_ERROR, f"Unknown export format: {format}")
    doc = await _load_capture(kb_id, cid)
    if doc is None:
        raise _capture_not_found()
    if doc.status != "completed" or not doc.profile:
        raise AppError(409, ErrorCode.VALIDATION_ERROR, "Capture not completed")
    from parsers.capture.export import build_hyperframes_zip
    data = await build_hyperframes_zip(doc)
    return StreamingResponse(
        io.BytesIO(data), media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="capture-{cid}.zip"'})
