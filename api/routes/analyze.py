import uuid
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, UploadFile, File, HTTPException, Request

from api.schemas import AnalyzeResponse, ImageInfo
from config import get_settings
from parsers.registry import registry

router = APIRouter()


@router.post("/analyze", response_model=AnalyzeResponse)
async def analyze(
    request: Request,
    file: Optional[UploadFile] = File(None),
):
    content_type = request.headers.get("content-type", "")
    cfg = get_settings()

    if "multipart/form-data" in content_type or file:
        # File upload path
        if not file:
            raise HTTPException(status_code=422, detail="Provide either 'url' or 'file'")
        url = None
        engine = "auto"
        extract_images = True
        filename = file.filename or "upload"
        raw: bytes | str = await file.read()
    elif "application/json" in content_type:
        # JSON path
        body = await request.json()
        url = body.get("url")
        engine = body.get("engine", "auto")
        extract_images = body.get("extract_images", True)
        filename = ""
        raw = url or ""
        if not url:
            raise HTTPException(status_code=422, detail="Provide either 'url' or 'file'")
    else:
        raise HTTPException(status_code=422, detail="Provide either 'url' or 'file'")

    # Select and run parser
    if url:
        selected_engine = engine if engine != "auto" else registry.auto_select(url=url)
        source_label = url
    else:
        selected_engine = engine if engine != "auto" else registry.auto_select(filename=filename)
        source_label = filename

    parser = registry.get_by_engine(selected_engine)
    result = await parser.parse(raw, filename=filename)

    # Store images and build URLs
    request_id = str(uuid.uuid4())
    images: list[ImageInfo] = []

    if extract_images and result.images:
        img_dir = Path(cfg.storage_path) / request_id
        img_dir.mkdir(parents=True, exist_ok=True)

        for img_name, img_bytes in result.images.items():
            safe_name = Path(img_name).name
            (img_dir / safe_name).write_bytes(img_bytes)
            images.append(ImageInfo(
                id=safe_name,
                url=f"/files/{request_id}/{safe_name}",
                context="",
                type="unknown",
            ))

    return AnalyzeResponse(
        title=result.title or Path(source_label).stem,
        source=source_label,
        markdown=result.content,
        images=images,
    )
