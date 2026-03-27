import pytest
from unittest.mock import patch, AsyncMock
from parsers.base import ParseResult


@pytest.mark.asyncio
async def test_analyze_url(client):
    fake_result = ParseResult(
        content="# Article\n\nSome content.",
        images={"img1.png": b"\x89PNG\r\n" + b"\x00" * 10},
        title="My Article",
    )

    with patch("api.routes.analyze.registry") as mock_reg:
        mock_parser = AsyncMock()
        mock_parser.parse = AsyncMock(return_value=fake_result)
        mock_reg.auto_select.return_value = "url"
        mock_reg.get_by_engine.return_value = mock_parser

        resp = await client.post("/analyze", json={"url": "https://example.com"})

    assert resp.status_code == 200
    body = resp.json()
    assert body["title"] == "My Article"
    assert "Article" in body["markdown"]
    assert len(body["images"]) == 1
    assert body["images"][0]["id"] == "img1.png"


@pytest.mark.asyncio
async def test_analyze_missing_input(client):
    resp = await client.post("/analyze", json={})
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_analyze_file_upload(client):
    fake_result = ParseResult(content="# PDF Content", images={}, title="test")

    with patch("api.routes.analyze.registry") as mock_reg:
        mock_parser = AsyncMock()
        mock_parser.parse = AsyncMock(return_value=fake_result)
        mock_reg.auto_select.return_value = "docling"
        mock_reg.get_by_engine.return_value = mock_parser

        resp = await client.post(
            "/analyze",
            files={"file": ("test.pdf", b"%PDF fake content", "application/pdf")},
        )

    assert resp.status_code == 200
    assert "PDF Content" in resp.json()["markdown"]
