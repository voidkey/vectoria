import asyncio
import mimetypes
import uuid
from pathlib import Path

from api.schemas import ImageInfo
from storage import get_storage


async def upload_images(result, extract_images: bool, prefix: str = "") -> list[ImageInfo]:
    """Upload extracted images to object storage and return presigned URLs."""
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
