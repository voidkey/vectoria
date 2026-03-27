import pytest
import base64
from unittest.mock import patch, AsyncMock, MagicMock
from parsers.mineru_parser import MinerUParser
from parsers.base import ParseResult


def _make_response(md_content: str, images: dict) -> dict:
    return {"results": {"document": {"md_content": md_content, "images": images}}}


@pytest.mark.asyncio
async def test_parse_returns_markdown():
    fake_resp = _make_response("# Doc\n\nContent.", {})

    with patch("parsers.mineru_parser.httpx.AsyncClient") as MockClient:
        mock_resp = MagicMock()
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

    with patch("parsers.mineru_parser.httpx.AsyncClient") as MockClient:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = fake_resp
        MockClient.return_value.__aenter__.return_value.post = AsyncMock(return_value=mock_resp)

        parser = MinerUParser(api_url="http://gpu:8000")
        result = await parser.parse(b"%PDF fake", filename="test.pdf")

    assert "fig1.png" in result.images
    assert result.images["fig1.png"] == png_bytes


@pytest.mark.asyncio
async def test_empty_url_returns_empty():
    parser = MinerUParser(api_url="")
    result = await parser.parse(b"data", filename="test.pdf")
    assert result.content == ""


def test_engine_name():
    assert MinerUParser.engine_name == "mineru"
