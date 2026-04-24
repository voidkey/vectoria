import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from parsers.url._x import XHandler, canonicalize_x_image_url, get_x_headers
from parsers.base import ParseResult


def test_handler_match():
    h = XHandler()
    assert h.match("https://x.com/user/status/12345")
    assert h.match("https://twitter.com/user/status/12345")
    assert not h.match("https://x.com/user")
    assert not h.match("https://example.com/status/12345")


def test_handler_download_headers_set_for_x_urls():
    """W3-d: X handler now sets UA + Referer for image fetches.

    Before W3-d the handler returned None; after, we send a desktop
    Chrome UA and twitter.com Referer so pbs.twimg.com's hotlink
    protection (if enabled) can't reject us for looking like a bot.
    """
    h = XHandler()
    hdrs = h.download_headers("https://x.com/user/status/12345")
    assert hdrs is not None
    assert "Chrome" in hdrs["User-Agent"]
    assert hdrs["Referer"] == "https://twitter.com/"


def test_handler_download_headers_none_for_non_x_urls():
    assert get_x_headers("https://example.com/") is None


# --- Image URL canonicalization (W3-d) ---

def test_canonicalize_adds_name_orig():
    """Downsized variant → full-resolution original."""
    url = "https://pbs.twimg.com/media/abc.jpg?format=jpg&name=small"
    out = canonicalize_x_image_url(url)
    assert "name=orig" in out
    assert "name=small" not in out


def test_canonicalize_replaces_any_size_name():
    for size in ("thumb", "small", "medium", "large", "360x240"):
        url = f"https://pbs.twimg.com/media/abc?name={size}"
        out = canonicalize_x_image_url(url)
        assert "name=orig" in out, f"failed for size={size}"


def test_canonicalize_adds_jpg_format_when_missing():
    """Without Accept-header hints the CDN may serve WebP; we prefer JPEG."""
    url = "https://pbs.twimg.com/media/abc?name=large"
    out = canonicalize_x_image_url(url)
    assert "format=jpg" in out


def test_canonicalize_preserves_existing_format():
    """If the URL already pins a format (e.g. png), we must not swap it."""
    url = "https://pbs.twimg.com/media/abc?format=png&name=large"
    out = canonicalize_x_image_url(url)
    assert "format=png" in out
    assert "format=jpg" not in out


def test_canonicalize_handles_bare_url_without_query():
    """``https://pbs.twimg.com/media/abc.jpg`` with no query should get
    ``?format=jpg&name=orig`` appended.
    """
    url = "https://pbs.twimg.com/media/abc.jpg"
    out = canonicalize_x_image_url(url)
    assert "name=orig" in out
    assert "format=jpg" in out


def test_canonicalize_noop_for_non_twimg_hosts():
    assert canonicalize_x_image_url("https://mmbiz.qpic.cn/img") \
        == "https://mmbiz.qpic.cn/img"
    assert canonicalize_x_image_url("https://xhscdn.com/img.jpg") \
        == "https://xhscdn.com/img.jpg"


def test_canonicalize_is_idempotent():
    """Running canonicalize twice must produce the same output."""
    url = "https://pbs.twimg.com/media/abc?name=small"
    once = canonicalize_x_image_url(url)
    twice = canonicalize_x_image_url(once)
    assert once == twice


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


@pytest.mark.asyncio
async def test_x_success_sets_allow_image_only_true():
    """Syndication API is structured — X handler opts into image_only."""
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
        handler = XHandler()
        result = await handler.parse("https://x.com/testuser/status/123")

    assert result.allow_image_only is True
