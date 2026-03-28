from pathlib import Path

from fastapi import APIRouter, UploadFile, File, Form, HTTPException

from api.schemas import AnalyzeResponse, AnalyzeURLRequest
from api.image_utils import upload_images
from parsers.registry import registry

router = APIRouter()


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
        images=await upload_images(result, extract_images),
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
        images=await upload_images(result, body.extract_images),
    )
