import pytest
from unittest.mock import patch, MagicMock

from parsers.url import _fetch


def _resp(text, status=200):
    m = MagicMock()
    m.text = text
    m.status_code = status
    return m


async def _async_none(*a, **k):
    return None


@pytest.mark.asyncio
async def test_confirmed_block_does_not_retry(monkeypatch):
    monkeypatch.setattr(_fetch, "acquire_page_token", _async_none)
    calls = {"n": 0}

    def fake_get(url, **kw):
        calls["n"] += 1
        return _resp("<title>安全验证</title>block page")

    with patch.object(_fetch, "_cc_get", fake_get), \
         patch.object(_fetch, "detect_block_reason", return_value="anti-bot"):
        out = await _fetch.fetch_impersonated("https://x.test/a", retries=3)
    assert out is None
    assert calls["n"] == 1  # confirmed block => single attempt, no retry


@pytest.mark.asyncio
async def test_transient_error_retries(monkeypatch):
    monkeypatch.setattr(_fetch, "acquire_page_token", _async_none)
    monkeypatch.setattr(_fetch, "_sleep", _async_none)
    calls = {"n": 0}

    def boom(url, **kw):
        calls["n"] += 1
        raise RuntimeError("network")

    with patch.object(_fetch, "_cc_get", boom):
        out = await _fetch.fetch_impersonated("https://x.test/a", retries=3)
    assert out is None
    assert calls["n"] == 3  # transient errors still retry


@pytest.mark.asyncio
async def test_retry_on_block_retries_through_challenge(monkeypatch):
    """With retry_on_block=True a confirmed block is retryable: baike serves
    a verification page per-request (~38%), so a later attempt often succeeds."""
    monkeypatch.setattr(_fetch, "acquire_page_token", _async_none)
    monkeypatch.setattr(_fetch, "_sleep", _async_none)
    calls = {"n": 0}

    def fake_get(url, **kw):
        calls["n"] += 1
        return _resp("block" if calls["n"] < 3 else "<html>good</html>")

    def fake_detect(html, title):
        return "anti-bot" if html == "block" else None

    with patch.object(_fetch, "_cc_get", fake_get), \
         patch.object(_fetch, "detect_block_reason", side_effect=fake_detect):
        out = await _fetch.fetch_impersonated(
            "https://x.test/a", retries=4, retry_on_block=True)
    assert out == "<html>good</html>"
    assert calls["n"] == 3  # two blocks retried, third succeeds


@pytest.mark.asyncio
async def test_retry_on_block_exhausts_returns_none(monkeypatch):
    monkeypatch.setattr(_fetch, "acquire_page_token", _async_none)
    monkeypatch.setattr(_fetch, "_sleep", _async_none)
    calls = {"n": 0}

    def fake_get(url, **kw):
        calls["n"] += 1
        return _resp("block page")

    with patch.object(_fetch, "_cc_get", fake_get), \
         patch.object(_fetch, "detect_block_reason", return_value="anti-bot"):
        out = await _fetch.fetch_impersonated(
            "https://x.test/a", retries=4, retry_on_block=True)
    assert out is None
    assert calls["n"] == 4  # every attempt blocked, retried up to the budget


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [404, 410])
async def test_gone_status_raises_page_not_found(monkeypatch, status):
    """A 404/410 is a dead resource: raise PageNotFoundError immediately,
    without retrying and without scraping the error page as content."""
    from parsers.base import PageNotFoundError
    monkeypatch.setattr(_fetch, "acquire_page_token", _async_none)
    calls = {"n": 0}

    def fake_get(url, **kw):
        calls["n"] += 1
        return _resp("<title>404</title>not found page", status=status)

    with patch.object(_fetch, "_cc_get", fake_get):
        with pytest.raises(PageNotFoundError):
            await _fetch.fetch_impersonated("https://x.test/gone", retries=4,
                                            retry_on_block=True)
    assert calls["n"] == 1  # gone => no retry


@pytest.mark.asyncio
async def test_5xx_is_transient_and_retries(monkeypatch):
    """A 5xx is server-side/transient: drop the body and retry, don't treat
    it as content or as permanent."""
    monkeypatch.setattr(_fetch, "acquire_page_token", _async_none)
    monkeypatch.setattr(_fetch, "_sleep", _async_none)
    calls = {"n": 0}

    def fake_get(url, **kw):
        calls["n"] += 1
        return _resp("<html>upstream error body</html>", status=503)

    with patch.object(_fetch, "_cc_get", fake_get), \
         patch.object(_fetch, "detect_block_reason", return_value=None):
        out = await _fetch.fetch_impersonated("https://x.test/a", retries=3)
    assert out is None        # never accepted the 5xx body as content
    assert calls["n"] == 3    # retried through the transient error


@pytest.mark.asyncio
async def test_success_returns_html(monkeypatch):
    monkeypatch.setattr(_fetch, "acquire_page_token", _async_none)
    with patch.object(_fetch, "_cc_get", lambda u, **k: _resp("<html>good</html>")), \
         patch.object(_fetch, "detect_block_reason", return_value=None):
        out = await _fetch.fetch_impersonated("https://x.test/a", retries=3)
    assert out == "<html>good</html>"
