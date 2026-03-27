import uuid
from pathlib import Path

from fastapi import APIRouter, UploadFile, File, Form, HTTPException

from api.schemas import AnalyzeResponse, AnalyzeURLRequest, ImageInfo
from config import get_settings
from parsers.registry import registry

router = APIRouter()


def _build_images(result, extract_images: bool) -> list[ImageInfo]:
    """Store extracted images to disk and return their metadata."""
    if not extract_images or not result.images:
        return []

    cfg = get_settings()
    request_id = str(uuid.uuid4())
    img_dir = Path(cfg.storage_path) / request_id
    img_dir.mkdir(parents=True, exist_ok=True)

    images: list[ImageInfo] = []
    for img_name, img_bytes in result.images.items():
        safe_name = Path(img_name).name
        (img_dir / safe_name).write_bytes(img_bytes)
        images.append(ImageInfo(
            id=safe_name,
            url=f"/files/{request_id}/{safe_name}",
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
        images=_build_images(result, extract_images),
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
        images=_build_images(result, body.extract_images),
    )
