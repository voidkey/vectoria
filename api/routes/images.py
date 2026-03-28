import asyncio

from fastapi import APIRouter, HTTPException
from sqlalchemy import select

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
            raise HTTPException(status_code=404, detail="Document not found")

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
        )

    images = await asyncio.gather(*(_presign(img) for img in db_images))
    return DocumentImagesListResponse(doc_id=doc_id, images=list(images))
