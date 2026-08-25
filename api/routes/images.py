import asyncio

from fastapi import APIRouter
from fastapi.responses import RedirectResponse
from sqlalchemy import select

from api.errors import AppError, ErrorCode
from api.schemas import DocumentImageResponse, DocumentImagesListResponse
from api.image_utils import compute_aspect_ratio
from db.base import get_session
from db.models import Document, DocumentImage
from storage import get_storage

router = APIRouter(prefix="/knowledgebases")


@router.get(
    "/{kb_id}/documents/{doc_id}/images",
    response_model=DocumentImagesListResponse,
)
async def get_document_images(kb_id: str, doc_id: str):
    async with get_session() as session:
        # Verify document exists
        doc_result = await session.execute(
            select(Document).where(Document.id == doc_id, Document.kb_id == kb_id)
        )
        if not doc_result.scalar_one_or_none():
            raise AppError(404, ErrorCode.NOT_FOUND, "Document not found")

        # Fetch all images for this document
        img_result = await session.execute(
            select(DocumentImage)
            .where(DocumentImage.doc_id == doc_id)
            .order_by(DocumentImage.image_index)
        )
        db_images = img_result.scalars().all()

    if not db_images:
        return DocumentImagesListResponse(doc_id=doc_id, images=[])

    # Generate presigned URLs in parallel
    obj_storage = await get_storage()

    async def _presign(img: DocumentImage) -> DocumentImageResponse:
        url = await obj_storage.presign_url(img.storage_key)
        return DocumentImageResponse(
            id=img.id,
            url=url,
            filename=img.filename,
            index=img.image_index,
            width=img.width,
            height=img.height,
            aspect_ratio=compute_aspect_ratio(img.width or 0, img.height or 0),
            description=img.description,
            vision_status=img.vision_status,
            alt=img.alt,
            context=img.context,
            section_title=img.section_title,
            page=img.page,
        )

    images = await asyncio.gather(*(_presign(img) for img in db_images))
    return DocumentImagesListResponse(doc_id=doc_id, images=list(images))


@router.get(
    "/{kb_id}/documents/{doc_id}/images/{image_id}",
    status_code=307,
    response_class=RedirectResponse,
)
async def get_document_image(kb_id: str, doc_id: str, image_id: str):
    """Stable, non-expiring handle for one image; 307s to a fresh presign.

    The list endpoint above hands out presigned URLs, which expire. That
    is fine for a UI rendering a document right now, but it breaks the
    edited-content flow: a caller who cleans up our markdown will embed
    whatever image URLs they were given, store that markdown back via
    ``PUT .../edited``, and end up with a document full of dead links
    once the signatures lapse. This route is the URL that is safe to
    persist — it re-signs on every request, so it never goes stale.

    307 rather than 302 so the method is preserved verbatim, and
    ``no-store`` so an intermediary can't cache the redirect past the
    lifetime of the signature it points at.
    """
    async with get_session() as session:
        result = await session.execute(
            select(DocumentImage)
            .join(Document, Document.id == DocumentImage.doc_id)
            # kb_id is filtered on the Document, not the denormalised copy
            # on DocumentImage, so tenant isolation holds even if the two
            # ever drift.
            .where(
                DocumentImage.id == image_id,
                DocumentImage.doc_id == doc_id,
                Document.kb_id == kb_id,
            )
        )
        img = result.scalar_one_or_none()
    if img is None:
        raise AppError(404, ErrorCode.NOT_FOUND, "Image not found")

    obj_storage = await get_storage()
    url = await obj_storage.presign_url(img.storage_key)
    return RedirectResponse(url, status_code=307, headers={"Cache-Control": "no-store"})
