import pytest
from parsers.url import _fetch


@pytest.mark.asyncio
async def test_fetch_returns_html_on_clean_page(monkeypatch):
    calls = {"n": 0}
    def fake_get(url, **kw):
        calls["n"] += 1
        class R: text = "<html><body>" + ("正文。" * 100) + "</body></html>"
        return R()
    monkeypatch.setattr(_fetch, "_cc_get", fake_get)
    monkeypatch.setattr(_fetch, "_ratelimit", _noop_ratelimit)
    html = await _fetch.fetch_impersonated("https://baike.baidu.com/item/x")
    assert html is not None and "正文" in html
    assert calls["n"] == 1


@pytest.mark.asyncio
async def test_fetch_retries_then_none_on_block(monkeypatch):
    calls = {"n": 0}
    def fake_get(url, **kw):
        calls["n"] += 1
        class R: text = "<html><head><title>百度安全验证</title></head><body>请完成下方验证</body></html>"
        return R()
    monkeypatch.setattr(_fetch, "_cc_get", fake_get)
    monkeypatch.setattr(_fetch, "_ratelimit", _noop_ratelimit)
    monkeypatch.setattr(_fetch, "_sleep", _noop_sleep)
    html = await _fetch.fetch_impersonated("https://baike.baidu.com/item/x", retries=3)
    assert html is None
    assert calls["n"] == 3


async def _noop_ratelimit(*a, **kw): return True
async def _noop_sleep(*a, **kw): return None
