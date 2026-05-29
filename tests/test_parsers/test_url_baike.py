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


def test_extract_baike_body_from_fixture():
    import pathlib
    from parsers.url._baike import BaikeHandler
    html = pathlib.Path("tests/test_parsers/fixtures/baike_spider.html").read_text(encoding="utf-8")
    r = BaikeHandler()._extract(html, "https://baike.baidu.com/item/%E8%9C%98%E8%9B%9B")
    assert "蜘蛛" in r.title
    assert "节肢动物" in r.content       # real body present (first sentence of lemma summary)
    assert "目录" not in r.content[:50]  # catalog noise stripped from the start
    assert len(r.content) > 2000         # full long-lemma text


def test_extract_fallback_when_primary_selector_misses(monkeypatch):
    """If the primary container selector matches nothing (e.g. baike rebuilt
    with new class hashes), the strip-tag fallback must still produce body."""
    import pathlib
    from parsers.url._baike import BaikeHandler
    import parsers.url._baike as b
    html = pathlib.Path("tests/test_parsers/fixtures/baike_spider.html").read_text(encoding="utf-8")
    # Simulate selector miss: rename the body class so the XPath finds 0 nodes
    broken = html.replace("para_", "XXXX_")
    r = BaikeHandler()._extract(broken, "https://baike.baidu.com/item/%E8%9C%98%E8%9B%9B")
    assert len(r.content) > 2000          # fallback still recovers full body
    assert "节肢动物" in r.content         # real text present


def test_extract_baike_images_from_fixture():
    import pathlib
    from parsers.url._baike import BaikeHandler
    html = pathlib.Path("tests/test_parsers/fixtures/baike_spider.html").read_text(encoding="utf-8")
    r = BaikeHandler()._extract(html, "https://baike.baidu.com/item/%E8%9C%98%E8%9B%9B")
    # baike content images are bkimg.cdn.bcebos.com/pic/... (no extension; via <img src/data-src>)
    assert any("bkimg.cdn.bcebos.com/pic/" in u for u in (r.image_urls or []))


def test_baike_download_headers_none():
    from parsers.url._baike import BaikeHandler
    assert BaikeHandler().download_headers("https://baike.baidu.com/item/x") is None


@pytest.mark.asyncio
async def test_openapi_fallback_lemma_id_match(monkeypatch):
    import parsers.url._baike as b
    async def fake_card(key): return {"newLemmaId": 65074591, "title": "神舟二十三号",
                                      "abstract": "由中国航天科技集团...", "desc": "载人飞船"}
    monkeypatch.setattr(b, "_baike_lemma_card", fake_card)
    r = await b.BaikeHandler._openapi_fallback("https://baike.baidu.com/item/x/65074591")
    assert r is not None and "中国航天" in r.content


@pytest.mark.asyncio
async def test_openapi_fallback_lemma_id_mismatch_rejected(monkeypatch):
    import parsers.url._baike as b
    async def fake_card(key): return {"newLemmaId": 5503879, "abstract": "另一个义项..."}
    monkeypatch.setattr(b, "_baike_lemma_card", fake_card)
    r = await b.BaikeHandler._openapi_fallback("https://baike.baidu.com/item/%E7%8E%8B%E4%B8%96%E8%BF%9B/61863047")
    assert r is None


@pytest.mark.asyncio
async def test_openapi_fallback_errno_returns_none(monkeypatch):
    import parsers.url._baike as b
    async def fake_card(key): return {"errno": 2}
    monkeypatch.setattr(b, "_baike_lemma_card", fake_card)
    r = await b.BaikeHandler._openapi_fallback("https://baike.baidu.com/item/x/123")
    assert r is None
