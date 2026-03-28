from pathlib import Path

from fastapi import APIRouter, UploadFile, File, Form

from api.schemas import AnalyzeResponse, AnalyzeURLRequest
from api.image_utils import upload_images
from parsers.registry import registry
from parsers.outline import extract_outline

router = APIRouter()


@router.post("/analyze/file", response_model=AnalyzeResponse)
async def analyze_file(
    file: UploadFile = File(...),
    extract_images: bool = Form(True),
):
    """Parse an uploaded file into Markdown."""
    filename = file.filename or "upload"
    raw = await file.read()

    selected_engine = registry.auto_select(filename=filename)
    parser = registry.get_by_engine(selected_engine)
    result = await parser.parse(raw, filename=filename)

    images = await upload_images(result, extract_images)
    outline = extract_outline(result.content)

    return AnalyzeResponse(
        title=result.title or Path(filename).stem,
        source=filename,
        content=result.content,
        outline=outline,
        image_count=len(images),
        images=images,
    )


@router.post("/analyze/url", response_model=AnalyzeResponse)
async def analyze_url(body: AnalyzeURLRequest):
    """Parse a URL into Markdown."""
    selected_engine = registry.auto_select(url=body.url)
    parser = registry.get_by_engine(selected_engine)
    result = await parser.parse(body.url, filename="")

    images = await upload_images(result, body.extract_images)
    outline = extract_outline(result.content)

    return AnalyzeResponse(
        title=result.title or body.url,
        source=body.url,
        content=result.content,
        outline=outline,
        image_count=len(images),
        images=images,
    )
