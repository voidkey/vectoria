import pytest
from parsers.url import _fetch


@pytest.mark.asyncio
async def test_fetch_returns_html_on_clean_page(monkeypatch):
    calls = {"n": 0}
    def fake_get(url, **kw):
        calls["n"] += 1
        class R:
            text = "<html><body>" + ("正文。" * 100) + "</body></html>"
            status_code = 200
        return R()
    monkeypatch.setattr(_fetch, "_cc_get", fake_get)
    monkeypatch.setattr(_fetch, "_ratelimit", _noop_ratelimit)
    html = await _fetch.fetch_impersonated("https://baike.baidu.com/item/x")
    assert html is not None and "正文" in html
    assert calls["n"] == 1


@pytest.mark.asyncio
async def test_confirmed_block_does_not_retry(monkeypatch):
    # A confirmed anti-bot block must stop immediately — no inner retries.
    calls = {"n": 0}
    def fake_get(url, **kw):
        calls["n"] += 1
        class R:
            text = "<html><head><title>百度安全验证</title></head><body>请完成下方验证</body></html>"
            status_code = 200
        return R()
    monkeypatch.setattr(_fetch, "_cc_get", fake_get)
    monkeypatch.setattr(_fetch, "_ratelimit", _noop_ratelimit)
    monkeypatch.setattr(_fetch, "_sleep", _noop_sleep)
    html = await _fetch.fetch_impersonated("https://baike.baidu.com/item/x", retries=3)
    assert html is None
    assert calls["n"] == 1  # confirmed block => single attempt, no retry


@pytest.mark.asyncio
async def test_ratelimit_wait_does_not_consume_retries(monkeypatch):
    """Rate-limit misses must NOT eat fetch attempts: even if the token is
    unavailable for the first few checks, all `retries` real fetches happen.
    Uses transient errors (exception) to exercise the retry path."""
    fetch_calls = {"n": 0}
    rl_state = {"calls": 0}
    def fake_get(url, **kw):
        fetch_calls["n"] += 1
        raise RuntimeError("transient network error")
    async def flaky_ratelimit(host):
        # unavailable for first 2 checks of each acquire, then available
        rl_state["calls"] += 1
        return rl_state["calls"] % 3 == 0
    monkeypatch.setattr(_fetch, "_cc_get", fake_get)
    monkeypatch.setattr(_fetch, "_ratelimit", flaky_ratelimit)
    monkeypatch.setattr(_fetch, "_sleep", _noop_sleep)
    html = await _fetch.fetch_impersonated("https://baike.baidu.com/item/x", retries=3)
    assert html is None
    assert fetch_calls["n"] == 3   # all 3 retry attempts really fetched, despite RL waits


async def _noop_ratelimit(*a, **kw): return True
async def _noop_sleep(*a, **kw): return None
