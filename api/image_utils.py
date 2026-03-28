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

    used_names: set[str] = set()

    async def _upload_one(img_name: str, img_bytes: bytes) -> ImageInfo:
        base = Path(img_name.split("?")[0]).name or "image.jpg"
        safe_name = base
        i = 1
        while safe_name in used_names:
            stem, _, ext = base.rpartition(".")
            safe_name = f"{stem}_{i}.{ext}" if ext else f"{base}_{i}"
            i += 1
        used_names.add(safe_name)

        key = f"{key_prefix}/{safe_name}"
        content_type = mimetypes.guess_type(safe_name)[0] or "image/png"
        await obj_storage.put(key, img_bytes, content_type=content_type)
        presigned = await obj_storage.presign_url(key)
        return ImageInfo(id=safe_name, url=presigned, context="", type="unknown")

    infos = await asyncio.gather(
        *(_upload_one(name, data) for name, data in result.images.items())
    )
    return list(infos)


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
    used_names: set[str] = set()

    def _derive_filename(raw_key: str) -> str:
        """Derive a safe, unique filename from a key (may be a URL or filename)."""
        base = Path(raw_key.split("?")[0]).name or "image.jpg"
        if base not in used_names:
            used_names.add(base)
            return base
        stem, _, ext = base.rpartition(".")
        i = 1
        while True:
            candidate = f"{stem}_{i}.{ext}" if ext else f"{base}_{i}"
            if candidate not in used_names:
                used_names.add(candidate)
                return candidate
            i += 1

    # Derive filenames and prepare upload data
    upload_plan: list[tuple[str, str, str]] = []  # (original_key, safe_name, s3_key)
    for meta in image_metas:
        safe_name = _derive_filename(meta.filename)
        s3_key = f"{key_prefix}/{safe_name}"
        upload_plan.append((meta.filename, safe_name, s3_key))
        meta.filename = safe_name  # replace URL/raw key with safe filename

    async def _upload_one(original_key: str, s3_key: str) -> None:
        safe_name = Path(s3_key).name
        content_type = mimetypes.guess_type(safe_name)[0] or "image/png"
        await obj_storage.put(s3_key, images[original_key], content_type=content_type)

    await asyncio.gather(*(_upload_one(ok, sk) for ok, _, sk in upload_plan))

    # Create DB records
    async with get_session() as session:
        for (_, _, s3_key), meta in zip(upload_plan, image_metas):
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
                storage_key=s3_key,
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
