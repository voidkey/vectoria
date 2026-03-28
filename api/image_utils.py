import asyncio
import math
import mimetypes
import uuid
from pathlib import Path

from api.schemas import ImageInfo
from storage import get_storage


def compute_aspect_ratio(w: int, h: int) -> str:
    """Compute a human-friendly aspect ratio string like '16:9' or '3:2'."""
    if not w or not h:
        return ""
    g = math.gcd(w, h)
    rw, rh = w // g, h // g
    # Snap to common ratios if close
    common = [(16, 9), (4, 3), (3, 2), (1, 1), (9, 16), (3, 4), (2, 3)]
    for cw, ch in common:
        if abs(rw / rh - cw / ch) < 0.05:
            return f"{cw}:{ch}"
    # If reduced ratio is too large, approximate
    if rw > 20 or rh > 20:
        ratio = w / h
        for cw, ch in common:
            if abs(ratio - cw / ch) < 0.1:
                return f"{cw}:{ch}"
        return f"{rw}:{rh}"
    return f"{rw}:{rh}"


async def upload_images(result, extract_images: bool, prefix: str = "") -> list[ImageInfo]:
    """Upload extracted images to object storage and return presigned URLs.

    Used by /analyze endpoints (lightweight, no DB records).
    """
    if not extract_images or not result.images:
        return []

    obj_storage = await get_storage()
    key_prefix = prefix or f"images/_analyze/{uuid.uuid4()}"

    async def _upload_one(img_name: str, img_bytes: bytes) -> ImageInfo:
        safe_name = Path(img_name).name
        key = f"{key_prefix}/{safe_name}"
        content_type = mimetypes.guess_type(safe_name)[0] or "image/png"
        await obj_storage.put(key, img_bytes, content_type=content_type)
        url = await obj_storage.presign_url(key)
        return ImageInfo(id=safe_name, url=url, context="", type="unknown")

    images = await asyncio.gather(
        *(_upload_one(name, data) for name, data in result.images.items())
    )
    return list(images)


async def upload_and_store_images(
    images: dict[str, bytes],
    image_metas: list,
    kb_id: str,
    doc_id: str,
    vision_configured: bool,
) -> int:
    """Upload images to S3 and create DocumentImage DB records.

    Returns the number of images stored.
    """
    if not image_metas:
        return 0

    from db.base import get_session
    from db.models import DocumentImage
    from storage import get_storage

    obj_storage = await get_storage()
    key_prefix = f"images/{kb_id}/{doc_id}"
    storage_keys: dict[str, str] = {}  # filename -> S3 key

    async def _upload_one(meta) -> None:
        safe_name = Path(meta.filename).name
        key = f"{key_prefix}/{safe_name}"
        content_type = mimetypes.guess_type(safe_name)[0] or "image/png"
        await obj_storage.put(key, images[meta.filename], content_type=content_type)
        storage_keys[meta.filename] = key

    # Upload all images in parallel
    await asyncio.gather(*(_upload_one(m) for m in image_metas))

    # Create DB records
    async with get_session() as session:
        for meta in image_metas:
            min_vision_dim = 200
            if vision_configured and (
                (meta.width is None or meta.height is None)
                or (meta.width >= min_vision_dim and meta.height >= min_vision_dim)
            ):
                vs = "pending"
            else:
                vs = "skipped"

            img_record = DocumentImage(
                id=str(uuid.uuid4()),
                doc_id=doc_id,
                kb_id=kb_id,
                storage_key=storage_keys[meta.filename],
                filename=meta.filename,
                width=meta.width,
                height=meta.height,
                alt=meta.alt,
                context=meta.context,
                section_title=meta.section_title,
                description="",
                vision_status=vs,
                image_index=meta.index,
            )
            session.add(img_record)
        await session.commit()

    return len(image_metas)
