import httpx
import pytest

from parsers.url import _http
from config import get_settings


def _client_with(handler):
    transport = httpx.MockTransport(handler)
    return _http.make_async_client(transport=transport)


@pytest.mark.asyncio
async def test_make_async_client_sets_max_redirects(monkeypatch):
    monkeypatch.setattr(get_settings(), "url_max_redirects", 3)
    client = _http.make_async_client()
    assert client.max_redirects == 3
    await client.aclose()


@pytest.mark.asyncio
async def test_fetch_capped_returns_body_and_url():
    def handler(request):
        return httpx.Response(200, text="hello", headers={"content-type": "text/html"})
    client = _client_with(handler)
    resp, body = await _http.fetch_capped(client, "https://x.test/p")
    assert body == b"hello"
    assert str(resp.url) == "https://x.test/p"
    await client.aclose()


@pytest.mark.asyncio
async def test_fetch_capped_aborts_oversized_by_content_length(monkeypatch):
    monkeypatch.setattr(get_settings(), "max_url_response_bytes", 10)
    def handler(request):
        return httpx.Response(200, content=b"x" * 100,
                              headers={"content-length": "100"})
    client = _client_with(handler)
    with pytest.raises(_http.ResponseTooLargeError):
        await _http.fetch_capped(client, "https://x.test/big")
    await client.aclose()


@pytest.mark.asyncio
async def test_fetch_capped_aborts_oversized_streamed(monkeypatch):
    monkeypatch.setattr(get_settings(), "max_url_response_bytes", 10)
    def handler(request):
        return httpx.Response(200, content=b"x" * 100)
    client = _client_with(handler)
    with pytest.raises(_http.ResponseTooLargeError):
        await _http.fetch_capped(client, "https://x.test/big")
    await client.aclose()


@pytest.mark.asyncio
async def test_fetch_capped_explicit_max_bytes_overrides_config(monkeypatch):
    # Generous config cap, but an explicit max_bytes=5 must win (pins the
    # `is None` check, not `or`, so max_bytes=0 would also be honored).
    monkeypatch.setattr(get_settings(), "max_url_response_bytes", 10_000_000)
    def handler(request):
        return httpx.Response(200, content=b"x" * 50)
    client = _client_with(handler)
    with pytest.raises(_http.ResponseTooLargeError):
        await _http.fetch_capped(client, "https://x.test/p", max_bytes=5)
    await client.aclose()
