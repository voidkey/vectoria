import pytest
from parsers.url._handlers import (
    SiteHandler,
    register_handler,
    find_handler,
    extract_image_urls,
    extract_with_trafilatura,
    download_images,
)
from parsers.base import ParseResult


class FakeHandler:
    hosts = {"example.com"}

    def match(self, url: str) -> bool:
        from urllib.parse import urlparse
        return urlparse(url).hostname in self.hosts

    async def parse(self, url: str) -> ParseResult:
        return ParseResult(content="fake", title="Fake")

    def download_headers(self, url: str) -> dict[str, str] | None:
        return None


def test_register_and_find_handler():
    from parsers.url._handlers import _handlers
    handler = FakeHandler()
    # Insert before GenericHandler (catch-all) so our handler matches first
    _handlers.insert(0, handler)
    try:
        found = find_handler("https://example.com/page")
        assert found is handler
    finally:
        _handlers.remove(handler)


def test_find_handler_generic_is_catchall():
    """GenericHandler matches everything, so find_handler never returns None."""
    from parsers.url._generic import GenericHandler
    found = find_handler("https://unknown-domain.test/page")
    assert isinstance(found, GenericHandler)


def test_extract_image_urls_resolves_relative():
    html = '<img src="/img/a.jpg" /><img src="https://cdn.example.com/b.png" />'
    urls = extract_image_urls(html, "https://example.com/page")
    assert urls == ["https://example.com/img/a.jpg", "https://cdn.example.com/b.png"]


def test_extract_image_urls_matches_data_src():
    html = '<img data-src="https://mmbiz.qpic.cn/img1.jpg" src="" />'
    urls = extract_image_urls(html, "https://mp.weixin.qq.com/s/abc")
    assert urls == ["https://mmbiz.qpic.cn/img1.jpg"]


def test_extract_image_urls_filters_data_uris():
    html = '<img src="data:image/png;base64,abc" /><img src="https://example.com/real.jpg" />'
    urls = extract_image_urls(html, "https://example.com")
    assert urls == ["https://example.com/real.jpg"]


def test_extract_image_urls_caps_at_20(monkeypatch):
    """Cap honors settings.url_image_cap (here pinned to 20 for stability)."""
    class _Stub:
        url_image_cap = 20
    monkeypatch.setattr(
        "parsers.url._handlers.get_settings", lambda: _Stub(), raising=False
    )

    html = "".join(f'<img src="https://example.com/img{i}.jpg" />' for i in range(30))
    urls = extract_image_urls(html, "https://example.com")
    assert len(urls) == 20


def test_extract_with_trafilatura_returns_markdown():
    html = "<html><body><p>Hello world paragraph with enough text to be meaningful</p></body></html>"
    text = extract_with_trafilatura(html)
    assert "Hello" in text or text == ""


def test_extract_with_trafilatura_recovers_via_article_subtree():
    """WordPress / heavy-CMS regression: trafilatura's whole-page scoring
    rejects this kind of page (massive theme chrome — sidebar + ads +
    related posts + share bar dwarfing the actual body), so we retry on
    the largest plausible article container before giving up.

    Without the subtree-retry path, ``extract_with_trafilatura`` would
    return empty here and downstream callers would mark the doc as
    ``empty_content``. The synthetic fixture below mirrors the live
    failure shape we hit on wallstreetlogic.com.
    """
    body_para = (
        "One of the most disorienting things happening in financial "
        "markets right now is the behavior of gold. By every conventional "
        "piece of investment logic, a full-scale military conflict in the "
        "Middle East should send gold prices soaring. Yet gold has fallen, "
        "leaving investors and analysts confused about the disconnect "
        "between geopolitical risk and the gold price. "
    )
    article_body = "<p>" + body_para * 8 + "</p>"
    chrome_block = "".join(
        f'<div class="ad-slot-{i}"><a href="/promo/{i}">Sponsored {i}</a></div>'
        for i in range(40)
    )
    sidebar = "".join(
        f'<aside class="related-post"><h3>Related: Headline {i}</h3>'
        f'<a href="/p/{i}">Read more</a></aside>'
        for i in range(60)
    )
    html = (
        "<html lang='en-US'><head><title>Gold</title></head><body>"
        f'<header class="site-header"><nav>Home About Contact</nav></header>'
        f'<div class="page-wrapper">{chrome_block}'
        f'<div class="entry-content">{article_body}</div>'
        f'{sidebar}</div>'
        '<footer>Subscribe Privacy Terms</footer>'
        "</body></html>"
    )
    # First trafilatura pass should fail on the whole-page noise; the
    # subtree retry must find ``.entry-content`` and extract from there.
    text = extract_with_trafilatura(html)
    assert "disorienting things happening in financial markets" in text


async def test_download_images_with_headers():
    """Happy path: respects provided Referer header and returns bytes.

    After migrating to the capped factory, ``download_images`` calls
    ``make_async_client(timeout=10, headers=headers)`` in ``_http.py``.
    We capture the kwargs forwarded to ``make_async_client`` and verify
    the Referer header is included.
    """
    from unittest.mock import AsyncMock, MagicMock, patch
    from limits.aio.storage import MemoryStorage
    from infra import ratelimit
    import parsers.url._http as _http_mod

    ratelimit._reset_for_tests()
    ratelimit._set_storage_for_tests(MemoryStorage())
    try:
        fake_resp = MagicMock(status_code=200, content=b"\x89PNG fake")
        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        captured_kw: dict = {}

        def _spy_make(**kw):
            captured_kw.update(kw)
            return mock_client

        async def _fake_fetch(client, url, **kw):
            return fake_resp, fake_resp.content

        with patch.object(_http_mod, "make_async_client", side_effect=_spy_make), \
             patch.object(_http_mod, "fetch_capped", side_effect=_fake_fetch):
            result = await download_images(
                ["https://mmbiz.qpic.cn/img1.jpg"],
                headers={"Referer": "https://mp.weixin.qq.com/"},
            )

        assert "https://mmbiz.qpic.cn/img1.jpg" in result
        assert result["https://mmbiz.qpic.cn/img1.jpg"] == b"\x89PNG fake"
        # Headers are forwarded to make_async_client (per-session).
        sent_headers = captured_kw.get("headers") or {}
        assert sent_headers.get("Referer") == "https://mp.weixin.qq.com/"
    finally:
        ratelimit._reset_for_tests()


def _counter_sum(counter, **labels):
    """Sum the current value of a labelled Counter for given label set."""
    return counter.labels(**labels)._value.get()


def test_extract_image_urls_respects_url_image_cap(monkeypatch):
    from parsers.url._handlers import extract_image_urls
    from infra import metrics

    # Override settings without touching .env — construct a minimal
    # stub for the two attributes the code path needs.
    class _Stub:
        url_image_cap = 3
    monkeypatch.setattr("parsers.url._handlers.get_settings", lambda: _Stub(), raising=False)

    html = "".join(f'<img src="https://x/{i}.jpg">' for i in range(10))

    before = _counter_sum(metrics.URL_IMAGES_TRUNCATED_TOTAL, handler="generic")
    urls = extract_image_urls(html, "https://x/")
    after = _counter_sum(metrics.URL_IMAGES_TRUNCATED_TOTAL, handler="generic")

    assert len(urls) == 3
    assert after - before == 1  # inc once per parse, not per dropped image


def test_extract_image_urls_no_truncation_below_cap(monkeypatch):
    from parsers.url._handlers import extract_image_urls
    from infra import metrics

    class _Stub:
        url_image_cap = 50
    monkeypatch.setattr("parsers.url._handlers.get_settings", lambda: _Stub(), raising=False)

    html = '<img src="https://x/1.jpg"><img src="https://x/2.jpg">'

    before = _counter_sum(metrics.URL_IMAGES_TRUNCATED_TOTAL, handler="generic")
    urls = extract_image_urls(html, "https://x/")
    after = _counter_sum(metrics.URL_IMAGES_TRUNCATED_TOTAL, handler="generic")

    assert len(urls) == 2
    assert after == before


def test_raise_if_gone_raises_on_404_and_410():
    """404/410 → PageNotFoundError with the URL in the message; everything
    else (incl. anti-bot 403, transient 5xx, success 200) is a no-op."""
    from parsers.url._handlers import raise_if_gone
    from parsers.base import PageNotFoundError

    for status in (404, 410):
        with __import__("pytest").raises(PageNotFoundError, match="example.com"):
            raise_if_gone(status, "https://example.com/x")

    for status in (200, 301, 403, 401, 429, 500, 503):
        raise_if_gone(status, "https://example.com/x")  # must not raise


# --- rate gate wait budget -------------------------------------------------
#
# Regression cover for the 2026-09-18 vprod2 incident: image downloads went
# from serial to 5-wide concurrent, which made a *full* moving window the
# normal case rather than an exception. The gate's give-up budget was still
# the serial-era ~0.75 s, so it dropped images the bucket would have admitted
# a moment later — silently, since a dropped image still leaves the document
# `completed`.


async def test_gate_waits_out_a_saturated_window(monkeypatch):
    """A bucket that frees up after several blocked checks must not cost an
    image. The old 2-retry budget gave up on the 3rd block."""
    import parsers.url._handlers as h

    calls = {"n": 0}

    async def _fake_acquire(host, *, rate, per_seconds, denied_result="blocked"):
        calls["n"] += 1
        calls.setdefault("labels", []).append(denied_result)
        return calls["n"] > 6  # blocked well past the old 3-attempt ceiling

    slept: list[float] = []

    async def _fake_sleep(s):
        slept.append(s)

    monkeypatch.setattr(h, "rl_acquire", _fake_acquire)
    monkeypatch.setattr(h.asyncio, "sleep", _fake_sleep)

    assert await h._gate("https://mmbiz.qpic.cn/img1.jpg") is True
    assert calls["n"] == 7
    # Back-off is capped so a waiter re-checks promptly when capacity frees.
    assert slept and max(slept) <= h._GATE_MAX_BACKOFF_S


async def test_gate_gives_up_after_budget_and_records_the_loss(monkeypatch):
    """When the budget really is exhausted the image is lost — that has to be
    counted, not just logged, so the loss is alertable."""
    import parsers.url._handlers as h
    from infra import metrics

    async def _always_blocked(host, *, rate, per_seconds, denied_result="blocked"):
        return False

    monkeypatch.setattr(h, "rl_acquire", _always_blocked)

    before = _counter_sum(
        metrics.URL_IMAGES_DROPPED_TOTAL, key="mmbiz.qpic.cn", reason="rate_limit",
    )
    assert await h._gate("https://mmbiz.qpic.cn/img1.jpg", budget=0.05) is False
    after = _counter_sum(
        metrics.URL_IMAGES_DROPPED_TOTAL, key="mmbiz.qpic.cn", reason="rate_limit",
    )

    assert after - before == 1  # one per lost image


async def test_gate_budget_covers_a_real_full_window():
    """End-to-end against the real limiter: rate=1/s means the second image
    has to wait out a full window. It must arrive, not be dropped."""
    from limits.aio.storage import MemoryStorage
    from infra import ratelimit
    import parsers.url._handlers as h

    ratelimit._reset_for_tests()
    ratelimit._set_storage_for_tests(MemoryStorage())
    monkey_host = "slowcdn.example"
    original_rates = h._DOMAIN_RATES
    h._DOMAIN_RATES = ((monkey_host, 1, 1),)
    try:
        url = f"https://{monkey_host}/a.jpg"
        assert await h._gate(url) is True
        # Window is now full; the gate must wait for it to roll, not give up.
        assert await h._gate(url) is True
    finally:
        h._DOMAIN_RATES = original_rates
        ratelimit._reset_for_tests()


async def test_gate_counts_one_blocked_per_image_not_one_per_poll(monkeypatch):
    """`blocked` must stay "a request was denied", not "a poll was denied".

    The gate now polls a saturated window ~15 times where the old fixed
    retry count polled 3. If every poll counted as blocked, the series
    would track the back-off schedule rather than contention, and the
    0.1/s threshold on it would silently stop meaning anything.
    """
    import parsers.url._handlers as h

    seen: list[str] = []

    async def _fake_acquire(host, *, rate, per_seconds, denied_result="blocked"):
        seen.append(denied_result)
        return len(seen) > 5

    async def _fake_sleep(_s):
        pass

    monkeypatch.setattr(h, "rl_acquire", _fake_acquire)
    monkeypatch.setattr(h.asyncio, "sleep", _fake_sleep)

    assert await h._gate("https://mmbiz.qpic.cn/img1.jpg") is True
    assert seen[0] == "blocked"
    assert seen.count("blocked") == 1
    assert seen[1:].count("wait_poll") == len(seen) - 1
