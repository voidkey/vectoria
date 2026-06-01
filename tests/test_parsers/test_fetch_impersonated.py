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
async def test_success_returns_html(monkeypatch):
    monkeypatch.setattr(_fetch, "acquire_page_token", _async_none)
    with patch.object(_fetch, "_cc_get", lambda u, **k: _resp("<html>good</html>")), \
         patch.object(_fetch, "detect_block_reason", return_value=None):
        out = await _fetch.fetch_impersonated("https://x.test/a", retries=3)
    assert out == "<html>good</html>"
