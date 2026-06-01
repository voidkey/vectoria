import pytest

from parsers.url import _fetch


@pytest.mark.asyncio
async def test_acquire_page_token_uses_config_rate(monkeypatch):
    calls = {}

    async def fake_acquire(key, *, rate, per_seconds):
        calls["key"], calls["rate"], calls["per"] = key, rate, per_seconds
        return True

    monkeypatch.setattr(_fetch, "_rl_acquire", fake_acquire)
    await _fetch.acquire_page_token("example.com")
    assert calls == {"key": "example.com", "rate": 1, "per": 2}
