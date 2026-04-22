import pytest
from unittest.mock import patch, AsyncMock
from parsers.base import ParseResult
from parsers.image_ref import ImageRef


def _png_bytes() -> bytes:
    # Stub payload; the dim-filter gate is bypassed by pre-setting
    # width/height on the ref (as real parsers do). Content just needs
    # to round-trip unchanged through the upload.
    return b"\x89PNG\r\n\x1a\n" + b"\x00" * 16


@pytest.mark.asyncio
async def test_analyze_url(client):
    img = _png_bytes()
    fake_result = ParseResult(
        content="# Article\n\nSome content.",
        title="My Article",
        image_refs=[
            ImageRef(
                name="img1.png", mime="image/png",
                width=300, height=300,
                _factory=lambda d=img: d,
            ),
        ],
    )

    mock_storage = AsyncMock()
    mock_storage.presign_url = AsyncMock(return_value="https://signed-url/img1.png")

    with (
        patch("api.routes.analyze.registry") as mock_reg,
        patch("api.image_stream.get_storage", return_value=mock_storage),
    ):
        mock_parser = AsyncMock()
        mock_parser.parse = AsyncMock(return_value=fake_result)
        mock_reg.auto_select.return_value = "url"
        mock_reg.get_by_engine.return_value = mock_parser

        resp = await client.post("/v1/analyze/url", json={"url": "https://example.com"})

    assert resp.status_code == 200
    body = resp.json()
    assert body["title"] == "My Article"
    assert "Article" in body["content"]
    assert body["image_count"] == 1
    assert len(body["images"]) == 1
    assert body["images"][0]["id"] == "img1.png"
    assert body["images"][0]["url"] == "https://signed-url/img1.png"
    assert body["outline"] == [{"level": 1, "title": "Article"}]
    mock_storage.put.assert_called_once()


@pytest.mark.asyncio
async def test_analyze_file_upload(client):
    fake_result = ParseResult(content="# PDF Content", title="test")

    with patch("api.routes.analyze.registry") as mock_reg:
        mock_parser = AsyncMock()
        mock_parser.parse = AsyncMock(return_value=fake_result)
        mock_reg.auto_select.return_value = "docling"
        mock_reg.get_by_engine.return_value = mock_parser

        resp = await client.post(
            "/v1/analyze/file",
            files={"file": ("test.pdf", b"%PDF fake content", "application/pdf")},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert "PDF Content" in body["content"]
    assert body["outline"] == [{"level": 1, "title": "PDF Content"}]
    assert body["image_count"] == 0


@pytest.mark.asyncio
async def test_analyze_url_no_engine_param(client):
    """Verify the engine parameter is no longer accepted."""
    fake_result = ParseResult(content="# Test", title="Test")

    with patch("api.routes.analyze.registry") as mock_reg:
        mock_parser = AsyncMock()
        mock_parser.parse = AsyncMock(return_value=fake_result)
        mock_reg.auto_select.return_value = "url"
        mock_reg.get_by_engine.return_value = mock_parser

        resp = await client.post(
            "/v1/analyze/url",
            json={"url": "https://example.com", "engine": "docling"},
        )

    assert resp.status_code == 200
    mock_reg.auto_select.assert_called_once_with(url="https://example.com")
