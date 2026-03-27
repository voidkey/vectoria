import pytest
import base64
import httpx
from unittest.mock import patch, AsyncMock, MagicMock
from parsers.mineru_parser import MinerUParser
from parsers.base import ParseResult


def _make_response(md_content: str, images: dict) -> dict:
    return {"results": {"document": {"md_content": md_content, "images": images}}}


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
    assert result.images == {}


@pytest.mark.asyncio
async def test_parse_decodes_images():
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

    assert "fig1.png" in result.images
    assert result.images["fig1.png"] == png_bytes


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
