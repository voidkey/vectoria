import pytest
from unittest.mock import patch, MagicMock

from parsers.url import _fetch


def _resp(text):
    m = MagicMock()
    m.text = text
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
async def test_success_returns_html(monkeypatch):
    monkeypatch.setattr(_fetch, "acquire_page_token", _async_none)
    with patch.object(_fetch, "_cc_get", lambda u, **k: _resp("<html>good</html>")), \
         patch.object(_fetch, "detect_block_reason", return_value=None):
        out = await _fetch.fetch_impersonated("https://x.test/a", retries=3)
    assert out == "<html>good</html>"
