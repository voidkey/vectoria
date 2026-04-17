import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from parsers.url._x import XHandler
from parsers.base import ParseResult


def test_handler_match():
    h = XHandler()
    assert h.match("https://x.com/user/status/12345")
    assert h.match("https://twitter.com/user/status/12345")
    assert not h.match("https://x.com/user")
    assert not h.match("https://example.com/status/12345")


def test_handler_download_headers():
    h = XHandler()
    assert h.download_headers("https://x.com/user/status/12345") is None


@pytest.mark.asyncio
async def test_handler_parse_tweet():
    api_response = {
        "user": {"name": "Test User", "screen_name": "testuser"},
        "text": "Hello world tweet",
        "mediaDetails": [
            {"media_url_https": "https://pbs.twimg.com/media/img1.jpg"},
        ],
    }

    mock_resp = MagicMock()
    mock_resp.json.return_value = api_response
    mock_resp.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=mock_resp)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("parsers.url._x.httpx.AsyncClient", return_value=mock_client):
        h = XHandler()
        result = await h.parse("https://x.com/testuser/status/12345")

    assert "Hello world tweet" in result.content
    assert "Test User" in result.content
    assert "https://pbs.twimg.com/media/img1.jpg" in result.image_urls


@pytest.mark.asyncio
async def test_handler_parse_returns_empty_on_api_failure():
    mock_client = AsyncMock()
    mock_client.get = AsyncMock(side_effect=Exception("API down"))
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("parsers.url._x.httpx.AsyncClient", return_value=mock_client):
        h = XHandler()
        result = await h.parse("https://x.com/user/status/99999")

    assert result.content == ""
