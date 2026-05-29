import pytest
from parsers.url._baike import BaikeHandler
from parsers.base import AntiBotBlockedError


def test_match_baike():
    h = BaikeHandler()
    assert h.match("https://baike.baidu.com/item/%E8%9C%98%E8%9B%9B")
    assert not h.match("https://www.baidu.com/")
    assert not h.match("https://example.com/")


def test_registration_order_baike_before_generic():
    from parsers.url import find_handler
    from parsers.url._baike import BaikeHandler as _BH
    h = find_handler("https://baike.baidu.com/item/x")
    assert isinstance(h, _BH)


@pytest.mark.asyncio
async def test_fetch_all_fail_raises_antibot(monkeypatch):
    import parsers.url._baike as b
    async def fake_fetch(url, **kw): return None
    async def fake_openapi(url): return None
    monkeypatch.setattr(b, "fetch_impersonated", fake_fetch)
    monkeypatch.setattr(BaikeHandler, "_openapi_fallback", staticmethod(fake_openapi))
    with pytest.raises(AntiBotBlockedError):
        await BaikeHandler().parse("https://baike.baidu.com/item/x/123")
