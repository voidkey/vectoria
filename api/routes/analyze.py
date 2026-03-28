import uuid
from pathlib import Path

from fastapi import APIRouter, UploadFile, File, Form, HTTPException

from api.schemas import AnalyzeResponse, AnalyzeURLRequest, ImageInfo
from storage import get_storage
from parsers.registry import registry

router = APIRouter()


async def _build_images(result, extract_images: bool, prefix: str = "") -> list[ImageInfo]:
    """Upload extracted images to object storage and return presigned URLs."""
    if not extract_images or not result.images:
        return []

    obj_storage = await get_storage()
    request_id = str(uuid.uuid4())
    key_prefix = prefix or f"images/_analyze/{request_id}"

    images: list[ImageInfo] = []
    for img_name, img_bytes in result.images.items():
        safe_name = Path(img_name).name
        key = f"{key_prefix}/{safe_name}"
        await obj_storage.put(key, img_bytes, content_type="image/png")
        url = await obj_storage.presign_url(key)
        images.append(ImageInfo(
            id=safe_name,
            url=url,
            context="",
            type="unknown",
        ))
    return images


@router.post("/analyze/file", response_model=AnalyzeResponse)
async def analyze_file(
    file: UploadFile = File(...),
    engine: str = Form("auto"),
    extract_images: bool = Form(True),
):
    """Parse an uploaded file into Markdown."""
    filename = file.filename or "upload"
    raw = await file.read()

    selected_engine = engine if engine != "auto" else registry.auto_select(filename=filename)
    parser = registry.get_by_engine(selected_engine)
    result = await parser.parse(raw, filename=filename)

    return AnalyzeResponse(
        title=result.title or Path(filename).stem,
        source=filename,
        markdown=result.content,
        images=await _build_images(result, extract_images),
    )


@router.post("/analyze/url", response_model=AnalyzeResponse)
async def analyze_url(body: AnalyzeURLRequest):
    """Parse a URL into Markdown."""
    selected_engine = (
        body.engine if body.engine != "auto"
        else registry.auto_select(url=body.url)
    )
    parser = registry.get_by_engine(selected_engine)
    result = await parser.parse(body.url, filename="")

    return AnalyzeResponse(
        title=result.title or body.url,
        source=body.url,
        markdown=result.content,
        images=await _build_images(result, body.extract_images),
    )
