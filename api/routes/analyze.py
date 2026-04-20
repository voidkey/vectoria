import asyncio
from pathlib import Path

from fastapi import APIRouter, UploadFile, File, Form

from api.image_stream import refs_from_dict, stream_upload_refs
from api.schemas import AnalyzeResponse, AnalyzeURLRequest, ImageInfo
from infra.metrics import observe_parse
from parsers.registry import registry
from parsers.outline import extract_outline
from parsers.image_metadata import extract_metadata_into_refs

router = APIRouter()


def _to_image_info(items: list[dict]) -> list[ImageInfo]:
    return [ImageInfo(**i) for i in items]


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
    async with observe_parse(selected_engine):
        result = await parser.parse(raw, filename=filename)

    # Enrichment fills alt/context/section_title and filters too-small images.
    # Operates in-place on result.image_refs.
    enriched = (
        extract_metadata_into_refs(result.content, result.image_refs)
        if result.image_refs else []
    )
    filtered_count = len(enriched)

    images = (
        _to_image_info(await stream_upload_refs(enriched))
        if extract_images and enriched else []
    )
    outline = extract_outline(result.content)

    return AnalyzeResponse(
        title=result.title or Path(filename).stem,
        source=filename,
        content=result.content,
        outline=outline,
        image_count=filtered_count,
        images=images,
    )


@router.post("/analyze/url", response_model=AnalyzeResponse)
async def analyze_url(body: AnalyzeURLRequest):
    """Parse a URL into Markdown."""
    selected_engine = registry.auto_select(url=body.url)
    parser = registry.get_by_engine(selected_engine)
    async with observe_parse(selected_engine):
        result = await parser.parse(body.url, filename="")

    # URL parsers return image_urls (deferred downloads). For /analyze we
    # materialise them synchronously so the response can include presigned
    # URLs. The download is adapted into the same ImageRef pipeline to
    # share streaming/release semantics.
    if body.extract_images and result.image_urls and not result.image_refs:
        from parsers.url import download_images, get_wechat_headers

        headers = get_wechat_headers(body.url)
        downloaded = await download_images(result.image_urls, headers=headers)
        result.image_refs = refs_from_dict(downloaded)
        # drop strong refs so only ImageRef closures hold bytes.
        downloaded = None  # noqa: F841

    enriched = (
        extract_metadata_into_refs(result.content, result.image_refs)
        if result.image_refs else []
    )
    filtered_count = len(enriched)

    images = (
        _to_image_info(await stream_upload_refs(enriched))
        if body.extract_images and enriched else []
    )
    outline = extract_outline(result.content)

    return AnalyzeResponse(
        title=result.title or body.url,
        source=body.url,
        content=result.content,
        outline=outline,
        image_count=filtered_count,
        images=images,
    )
