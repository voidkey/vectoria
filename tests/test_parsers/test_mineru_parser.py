import pytest
import base64
import json
import httpx
from unittest.mock import patch, AsyncMock, MagicMock
from parsers.mineru_parser import MinerUParser
from parsers.base import ParseResult


def _make_response(md_content: str, images: dict, content_list: list | None = None) -> dict:
    doc: dict = {"md_content": md_content, "images": images}
    if content_list is not None:
        doc["content_list"] = content_list
    return {"results": {"document": doc}}


@pytest.mark.asyncio
async def test_parse_returns_markdown():
    fake_resp = _make_response("# Doc\n\nContent.", {})

    with patch("parsers.mineru_parser.httpx.AsyncClient") as MockClient, \
         patch("parsers.mineru_parser.get_settings") as mock_settings:
        mock_settings.return_value.mineru_api_url = "http://gpu:8000"
        mock_settings.return_value.mineru_backend = "pipeline"
        mock_settings.return_value.mineru_language = "ch"
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = fake_resp
        MockClient.return_value.__aenter__.return_value.post = AsyncMock(return_value=mock_resp)

        parser = MinerUParser(api_url="http://gpu:8000")
        result = await parser.parse(b"%PDF fake", filename="test.pdf")

    assert "Doc" in result.content
    assert result.image_refs == []


@pytest.mark.asyncio
async def test_parse_yields_lazy_image_refs():
    """Images are surfaced as ``ImageRef`` with a factory that decodes
    base64 on demand — not as a pre-decoded dict. materialize() must
    return the original decoded bytes.
    """
    png_bytes = b"\x89PNG\r\n\x1a\n" + b"\x00" * 20
    b64 = "data:image/png;base64," + base64.b64encode(png_bytes).decode()
    fake_resp = _make_response("![img](images/fig1.png)", {"fig1.png": b64})

    with patch("parsers.mineru_parser.httpx.AsyncClient") as MockClient, \
         patch("parsers.mineru_parser.get_settings") as mock_settings:
        mock_settings.return_value.mineru_api_url = "http://gpu:8000"
        mock_settings.return_value.mineru_backend = "pipeline"
        mock_settings.return_value.mineru_language = "ch"
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = fake_resp
        MockClient.return_value.__aenter__.return_value.post = AsyncMock(return_value=mock_resp)

        parser = MinerUParser(api_url="http://gpu:8000")
        result = await parser.parse(b"%PDF fake", filename="test.pdf")

    # Dict field is empty (legacy shape unused); payload lives in refs.
    assert len(result.image_refs) == 1
    ref = result.image_refs[0]
    assert ref.name == "fig1.png"
    assert ref.mime == "image/png"
    # Factory decodes on demand — repeated calls return the same bytes.
    assert ref.materialize() == png_bytes
    assert ref.materialize() == png_bytes
    # release() drops the factory; subsequent materialize() must raise so
    # accidental use-after-release is loud rather than silent.
    ref.release()
    with pytest.raises(RuntimeError):
        ref.materialize()


@pytest.mark.asyncio
async def test_empty_url_returns_empty():
    with patch("parsers.mineru_parser.get_settings") as mock_settings:
        mock_settings.return_value.mineru_api_url = ""
        parser = MinerUParser(api_url="")
        result = await parser.parse(b"data", filename="test.pdf")
        assert result.content == ""
        assert result.title == "test"


def test_engine_name():
    assert MinerUParser.engine_name == "mineru"


@pytest.mark.asyncio
async def test_parse_raises_on_http_error():
    with patch("parsers.mineru_parser.httpx.AsyncClient") as MockClient, \
         patch("parsers.mineru_parser.get_settings") as mock_settings:
        mock_settings.return_value.mineru_api_url = "http://gpu:8000"
        mock_settings.return_value.mineru_backend = "pipeline"
        mock_settings.return_value.mineru_language = "ch"
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            "500 Server Error", request=MagicMock(), response=MagicMock()
        )
        MockClient.return_value.__aenter__.return_value.post = AsyncMock(return_value=mock_resp)

        parser = MinerUParser(api_url="http://gpu:8000")
        with pytest.raises(httpx.HTTPStatusError):
            await parser.parse(b"%PDF fake", filename="test.pdf")


def test_is_available_false_when_no_url():
    with patch("parsers.mineru_parser.get_settings") as mock_settings:
        mock_settings.return_value.mineru_api_url = ""
        assert MinerUParser.is_available() is False


def test_is_available_true_when_url_set():
    with patch("parsers.mineru_parser.get_settings") as mock_settings:
        mock_settings.return_value.mineru_api_url = "http://gpu:8000"
        assert MinerUParser.is_available() is True


@pytest.mark.asyncio
async def test_parse_fills_page_from_content_list():
    """``ImageRef.page`` should reflect MinerU's per-image ``page_idx``
    from ``content_list``, converted to 1-based. Image dict keys may be
    bare basenames while content_list uses ``images/<name>``; basename
    match must paper over that.
    """
    png = b"\x89PNG\r\n\x1a\n" + b"\x00" * 4
    b64 = "data:image/png;base64," + base64.b64encode(png).decode()
    fake_resp = _make_response(
        "![a](images/a.png)\n![b](images/b.png)\n![c](images/c.png)",
        {"a.png": b64, "b.png": b64, "c.png": b64},
        content_list=[
            {"type": "text", "text": "ignored", "page_idx": 0},
            {"type": "image", "img_path": "images/a.png", "page_idx": 0},
            {"type": "image", "img_path": "images/b.png", "page_idx": 4},
            # c.png missing from content_list — page should stay None.
        ],
    )

    with patch("parsers.mineru_parser.httpx.AsyncClient") as MockClient, \
         patch("parsers.mineru_parser.get_settings") as mock_settings:
        mock_settings.return_value.mineru_api_url = "http://gpu:8000"
        mock_settings.return_value.mineru_backend = "pipeline"
        mock_settings.return_value.mineru_language = "ch"
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = fake_resp
        MockClient.return_value.__aenter__.return_value.post = AsyncMock(return_value=mock_resp)

        parser = MinerUParser(api_url="http://gpu:8000")
        result = await parser.parse(b"%PDF fake", filename="test.pdf")

    by_name = {r.name: r for r in result.image_refs}
    assert by_name["a.png"].page == 1
    assert by_name["b.png"].page == 5
    assert by_name["c.png"].page is None


@pytest.mark.asyncio
async def test_parse_fills_page_when_content_list_is_json_string():
    """Real MinerU returns ``content_list`` as a JSON-encoded string,
    not an already-parsed list. Iterating the raw string walks
    characters and silently yields page=None for every image — exactly
    the bug observed during smoke testing. Parser must json.loads first.
    """
    png = b"\x89PNG\r\n\x1a\n" + b"\x00" * 4
    b64 = "data:image/png;base64," + base64.b64encode(png).decode()
    cl_json = json.dumps([
        {"type": "text", "text": "header", "page_idx": 0},
        {"type": "image", "img_path": "images/a.png", "page_idx": 2},
        {"type": "table", "img_path": "images/t.png", "page_idx": 6},
    ])
    fake_resp = _make_response(
        "![a](images/a.png)\n![t](images/t.png)",
        {"a.png": b64, "t.png": b64},
        content_list=cl_json,  # JSON string, mirroring real MinerU shape
    )

    with patch("parsers.mineru_parser.httpx.AsyncClient") as MockClient, \
         patch("parsers.mineru_parser.get_settings") as mock_settings:
        mock_settings.return_value.mineru_api_url = "http://gpu:8000"
        mock_settings.return_value.mineru_backend = "pipeline"
        mock_settings.return_value.mineru_language = "ch"
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = fake_resp
        MockClient.return_value.__aenter__.return_value.post = AsyncMock(return_value=mock_resp)

        parser = MinerUParser(api_url="http://gpu:8000")
        result = await parser.parse(b"%PDF fake", filename="test.pdf")

    by_name = {r.name: r for r in result.image_refs}
    assert by_name["a.png"].page == 3
    assert by_name["t.png"].page == 7  # 'table' type is still mapped


@pytest.mark.asyncio
async def test_parse_no_content_list_leaves_page_none():
    """Older MinerU response or non-paginated source: page must stay
    ``None`` rather than crash on the missing field.
    """
    png = b"\x89PNG\r\n\x1a\n" + b"\x00" * 4
    b64 = "data:image/png;base64," + base64.b64encode(png).decode()
    fake_resp = _make_response("![x](x.png)", {"x.png": b64})  # no content_list

    with patch("parsers.mineru_parser.httpx.AsyncClient") as MockClient, \
         patch("parsers.mineru_parser.get_settings") as mock_settings:
        mock_settings.return_value.mineru_api_url = "http://gpu:8000"
        mock_settings.return_value.mineru_backend = "pipeline"
        mock_settings.return_value.mineru_language = "ch"
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = fake_resp
        MockClient.return_value.__aenter__.return_value.post = AsyncMock(return_value=mock_resp)

        parser = MinerUParser(api_url="http://gpu:8000")
        result = await parser.parse(b"%PDF fake", filename="test.pdf")

    assert result.image_refs[0].page is None


@pytest.mark.asyncio
async def test_parse_files_schema_variant():
    """MinerU sometimes returns results.files instead of results.document"""
    fake_resp = {"results": {"files": {"md_content": "# Alt", "images": {}}}}

    with patch("parsers.mineru_parser.httpx.AsyncClient") as MockClient, \
         patch("parsers.mineru_parser.get_settings") as mock_settings:
        mock_settings.return_value.mineru_api_url = "http://gpu:8000"
        mock_settings.return_value.mineru_backend = "pipeline"
        mock_settings.return_value.mineru_language = "ch"
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = fake_resp
        MockClient.return_value.__aenter__.return_value.post = AsyncMock(return_value=mock_resp)

        parser = MinerUParser(api_url="http://gpu:8000")
        result = await parser.parse(b"%PDF fake", filename="test.pdf")

    assert "Alt" in result.content
