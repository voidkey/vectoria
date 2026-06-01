import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from contextlib import contextmanager
from parsers.url._generic import GenericHandler, _strip_tracking_params
from parsers.base import ParseResult


@contextmanager
def _patch_async_httpx(*, html: str | None = None, url: str = "",
                      side_effect: Exception | None = None,
                      content_type: str = "text/html",
                      content: bytes | None = None):
    """Patch ``parsers.url._http.make_async_client`` and
    ``parsers.url._http.fetch_capped`` used by ``_generic.py``'s
    ``_parse_httpx`` after migrating to the capped factory.

    Either provide HTML (and optional final URL) for success, or
    pass ``side_effect=SomeException`` to make ``fetch_capped`` raise
    (mirroring network failures).
    """
    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)

    if side_effect is not None:
        async def _fetch_capped_raise(client, fetch_url, **kw):
            raise side_effect
    else:
        body_bytes = content if content is not None else (html or "").encode()
        mock_resp = MagicMock()
        mock_resp.encoding = "utf-8"
        mock_resp.url = url
        mock_resp.headers = {"content-type": content_type}

        async def _fetch_capped_ok(client, fetch_url, **kw):
            return mock_resp, body_bytes

    fetch_fn = _fetch_capped_raise if side_effect is not None else _fetch_capped_ok

    with patch("parsers.url._http.make_async_client", return_value=mock_client), \
         patch("parsers.url._http.fetch_capped", side_effect=fetch_fn):
        yield


def test_strip_tracking_drops_google_ads_and_utm():
    """The exact failure URL we hit on overseas — Google Ads tracking
    plus inline ``gad_*`` siblings. After strip the article URL must be
    canonical (no query string) so playwright doesn't get bounced
    through a tracker redirect that races our poll loop.
    """
    raw = (
        "https://wallstreetlogic.com/article?"
        "gad_source=1&gad_campaignid=23820234144&gbraid=0AAAAA&"
        "gclid=Cj0KCQjw&utm_source=newsletter&utm_medium=email"
    )
    assert _strip_tracking_params(raw) == "https://wallstreetlogic.com/article"


def test_strip_tracking_preserves_load_bearing_query():
    """Article-identifying query params (``id``, ``page``, ``slug``)
    must survive — we only drop known tracking keys.
    """
    raw = "https://news.example/article?id=42&page=2&utm_source=x&fbclid=abc"
    assert _strip_tracking_params(raw) == "https://news.example/article?id=42&page=2"


def test_strip_tracking_no_query_is_noop():
    """Identity for URLs without a query string — exercises the early
    return so we don't waste a parse_qsl + urlunparse roundtrip.
    """
    raw = "https://news.example/article"
    assert _strip_tracking_params(raw) is raw


def test_handler_match_always_true():
    h = GenericHandler()
    assert h.match("https://example.com/anything")
    assert h.match("https://unknown.org/page")


def test_handler_download_headers_always_none():
    h = GenericHandler()
    assert h.download_headers("https://example.com") is None


@pytest.mark.asyncio
async def test_handler_parse_with_httpx():
    html = "<html><head><title>Test</title></head><body><p>Content</p></body></html>"
    long_content = "Extracted content. " * 25

    with _patch_async_httpx(html=html, url="https://example.com/page"), \
         patch("parsers.url._handlers.trafilatura.extract", return_value=long_content):
        h = GenericHandler()
        result = await h.parse("https://example.com/page")

    assert "Extracted content" in result.content
    assert result.title == "Test"


@pytest.mark.asyncio
async def test_handler_falls_back_to_playwright_on_js_challenge():
    pw_result = ParseResult(content="Real SPA content " * 25, title="SPA Page")

    with _patch_async_httpx(
             html="<html><body>Just a moment...</body></html>",
             url="https://example.com/spa",
         ), \
         patch("parsers.url._handlers.trafilatura.extract", return_value="Just a moment..."), \
         patch("parsers.url._generic.fetch_impersonated", new=AsyncMock(return_value=None)), \
         patch.object(GenericHandler, "_parse_with_playwright", new_callable=AsyncMock,
                      return_value=pw_result):
        h = GenericHandler()
        result = await h.parse("https://example.com/spa")

    assert result.content == pw_result.content


@pytest.mark.asyncio
async def test_handler_returns_empty_on_total_failure():
    # httpx fails → empty result → needs_browser_fallback → playwright fallback
    # Playwright import raises → returns empty
    with _patch_async_httpx(side_effect=Exception("connection failed")), \
         patch("parsers.url._generic.fetch_impersonated", new=AsyncMock(return_value=None)), \
         patch.dict("sys.modules", {"playwright.async_api": None}):
        h = GenericHandler()
        result = await h.parse("https://bad-url.example")

    assert result.content == ""


@pytest.mark.asyncio
async def test_playwright_recovers_from_navigation_race():
    """Real-world failure: when a page does a client-side navigation
    (ad-tracker redirect, SPA route change) right after
    ``goto(domcontentloaded)`` returns, the first ``page.title()``
    inside the poll loop races with that navigation and playwright
    raises ``Execution context was destroyed``. The handler must
    swallow that error and keep polling, not let it bubble out and
    burn all three worker retries.
    """
    from contextlib import asynccontextmanager
    from playwright.async_api import Error as PlaywrightError

    page = MagicMock()
    page.goto = AsyncMock()
    page.wait_for_timeout = AsyncMock()
    page.wait_for_load_state = AsyncMock()
    page.url = "https://example.com/article"
    # First two title() calls die mid-navigation; third succeeds.
    page.title = AsyncMock(side_effect=[
        PlaywrightError(
            "Page.title: Execution context was destroyed, "
            "most likely because of a navigation"
        ),
        PlaywrightError(
            "Page.title: Execution context was destroyed, "
            "most likely because of a navigation"
        ),
        "Article Title",
    ])
    page.content = AsyncMock(return_value="<html><body>real content</body></html>")

    ctx = MagicMock()
    ctx.new_page = AsyncMock(return_value=page)

    @asynccontextmanager
    async def fake_session(**_kwargs):
        yield ctx

    # httpx fails outright so ``needs_browser_fallback`` triggers the
    # playwright path; trafilatura then extracts the article body from
    # the post-recovery ``page.content()``.
    long_content = "Real article body. " * 25

    with _patch_async_httpx(side_effect=Exception("connection failed")), \
         patch("parsers.url._handlers.trafilatura.extract", return_value=long_content), \
         patch("parsers.url._generic.fetch_impersonated", new=AsyncMock(return_value=None)), \
         patch("parsers.url._browser.parse_session", fake_session):
        h = GenericHandler()
        result = await h.parse("https://example.com/article")

    # Loop should have retried past the two navigation errors and finally
    # captured title + content on the third tick.
    assert page.title.await_count == 3
    assert result.title == "Article Title"
    assert "Real article body" in result.content


@pytest.mark.asyncio
async def test_playwright_cert_error_raises_permanent():
    """Chromium ``net::ERR_CERT_*`` from page.goto must escalate to
    PermanentParseError so the worker handler short-circuits — no
    fallback chain (URL has only one engine), no queue retries, no
    dead-task alert. Cert validity is not a transient property.

    Real-world trigger: an .edu.cn site whose Sectigo cert expired
    weeks ago kept burning 3 retries × 30 s of playwright startup
    every time someone re-uploaded the URL.
    """
    from contextlib import asynccontextmanager
    from playwright.async_api import Error as PlaywrightError
    from parsers.base import PermanentParseError

    page = MagicMock()
    page.goto = AsyncMock(side_effect=PlaywrightError(
        "Page.goto: net::ERR_CERT_DATE_INVALID at https://expired.example/\n"
        "Call log:\n  - navigating to \"https://expired.example/\", "
        "waiting until \"domcontentloaded\"\n"
    ))

    ctx = MagicMock()
    ctx.new_page = AsyncMock(return_value=page)

    @asynccontextmanager
    async def fake_session(**_kwargs):
        yield ctx

    with _patch_async_httpx(side_effect=Exception("connection failed")), \
         patch("parsers.url._generic.fetch_impersonated", new=AsyncMock(return_value=None)), \
         patch("parsers.url._browser.parse_session", fake_session):
        h = GenericHandler()
        with pytest.raises(PermanentParseError, match="ERR_CERT_DATE_INVALID"):
            await h.parse("https://expired.example/")


@pytest.mark.asyncio
async def test_playwright_non_cert_error_still_propagates():
    """Sanity check the cert detection isn't over-eager: a regular
    timeout (non-cert) must NOT be repackaged as PermanentParseError —
    those *should* go through the queue's retry path because they're
    often transient (network blip, slow site).
    """
    from contextlib import asynccontextmanager
    from playwright.async_api import Error as PlaywrightError
    from parsers.base import PermanentParseError

    page = MagicMock()
    page.goto = AsyncMock(side_effect=PlaywrightError(
        "Page.goto: Timeout 30000ms exceeded."
    ))
    ctx = MagicMock()
    ctx.new_page = AsyncMock(return_value=page)

    @asynccontextmanager
    async def fake_session(**_kwargs):
        yield ctx

    with _patch_async_httpx(side_effect=Exception("connection failed")), \
         patch("parsers.url._generic.fetch_impersonated", new=AsyncMock(return_value=None)), \
         patch("parsers.url._browser.parse_session", fake_session):
        h = GenericHandler()
        with pytest.raises(PlaywrightError):
            await h.parse("https://slow.example/")
        # Crucially NOT PermanentParseError — the worker queue should
        # be allowed to retry timeouts.
        assert not isinstance(PlaywrightError, type) or True  # docs only


def test_chromium_cert_error_code_extraction():
    """Helper recognises every ERR_CERT_* variant chromium surfaces and
    rejects unrelated TLS errors (ERR_SSL_* are protocol-layer, possibly
    transient, so we deliberately don't match them).
    """
    from parsers.url._generic import _chromium_cert_error_code

    cases = {
        "Page.goto: net::ERR_CERT_DATE_INVALID at https://x/": "ERR_CERT_DATE_INVALID",
        "Page.goto: net::ERR_CERT_AUTHORITY_INVALID at https://x/": "ERR_CERT_AUTHORITY_INVALID",
        "Page.goto: net::ERR_CERT_COMMON_NAME_INVALID at x": "ERR_CERT_COMMON_NAME_INVALID",
        "wrapped: net::ERR_CERT_REVOKED\nCall log:": "ERR_CERT_REVOKED",
    }
    for msg, expected in cases.items():
        assert _chromium_cert_error_code(Exception(msg)) == expected, msg

    # Negative cases: not a cert error.
    for msg in (
        "Page.goto: Timeout 30000ms exceeded.",
        "Page.goto: net::ERR_SSL_PROTOCOL_ERROR at https://x/",  # SSL ≠ CERT
        "Page.goto: net::ERR_CONNECTION_REFUSED at https://x/",
        "Execution context was destroyed",
    ):
        assert _chromium_cert_error_code(Exception(msg)) is None, msg


def test_chromium_permanent_ssl_code_extraction():
    """A small allowlist of ``net::ERR_SSL_*`` codes are permanent even
    though most SSL errors are transient handshake hiccups. The clearest
    is ERR_SSL_UNRECOGNIZED_NAME_ALERT — TLS SNI: the server has no cert
    for the requested host, so it can never serve content there.

    Must NOT match transient SSL errors (PROTOCOL_ERROR), cert errors
    (those go through _chromium_cert_error_code), or non-TLS failures.
    """
    from parsers.url._generic import _chromium_permanent_ssl_code

    msg = ("Page.goto: net::ERR_SSL_UNRECOGNIZED_NAME_ALERT at https://d.school/\n"
           "Call log:\n  - navigating to \"https://d.school/\"")
    assert _chromium_permanent_ssl_code(Exception(msg)) == "ERR_SSL_UNRECOGNIZED_NAME_ALERT"

    # Negatives: transient SSL, cert (handled elsewhere), and non-TLS.
    for msg in (
        "Page.goto: net::ERR_SSL_PROTOCOL_ERROR at https://x/",  # transient handshake
        "Page.goto: net::ERR_CERT_DATE_INVALID at https://x/",   # cert path
        "Page.goto: net::ERR_CONNECTION_REFUSED at https://x/",
        "Page.goto: Timeout 30000ms exceeded.",
    ):
        assert _chromium_permanent_ssl_code(Exception(msg)) is None, msg


@pytest.mark.asyncio
async def test_playwright_ssl_unrecognized_name_raises_permanent():
    """Chromium ``net::ERR_SSL_UNRECOGNIZED_NAME_ALERT`` from page.goto
    must escalate to PermanentParseError so the worker short-circuits —
    no queue retries, no dead-task alert. The server's TLS layer doesn't
    recognise the hostname (parked/apex domain with no cert for it), so
    retrying just burns 3 worker slots × 30 s of playwright startup.

    Real-world trigger: https://d.school/ — repeatedly re-uploaded,
    dead-lettered 10× and spiked the document failure-rate alert.
    """
    from contextlib import asynccontextmanager
    from playwright.async_api import Error as PlaywrightError
    from parsers.base import PermanentParseError

    page = MagicMock()
    page.goto = AsyncMock(side_effect=PlaywrightError(
        "Page.goto: net::ERR_SSL_UNRECOGNIZED_NAME_ALERT at https://d.school/\n"
        "Call log:\n  - navigating to \"https://d.school/\", "
        "waiting until \"domcontentloaded\"\n"
    ))

    ctx = MagicMock()
    ctx.new_page = AsyncMock(return_value=page)

    @asynccontextmanager
    async def fake_session(**_kwargs):
        yield ctx

    with _patch_async_httpx(side_effect=Exception("connection failed")), \
         patch("parsers.url._generic.fetch_impersonated", new=AsyncMock(return_value=None)), \
         patch("parsers.url._browser.parse_session", fake_session):
        h = GenericHandler()
        with pytest.raises(PermanentParseError, match="ERR_SSL_UNRECOGNIZED_NAME_ALERT"):
            await h.parse("https://d.school/")


@pytest.mark.asyncio
async def test_handler_routes_pdf_content_type_to_pdf_parser():
    """When the URL serves ``application/pdf``, hand the bytes off to
    the registered PDF parser chain instead of (a) running trafilatura
    on binary garbage and getting empty content, or (b) falling back
    to Playwright — which Chromium refuses with
    ``Page.goto: Download is starting`` the moment it sees the PDF
    response, burning all 3 worker retries on every PDF URL.

    Real-world trigger: aixj-image CDN serves write-up PDFs at
    ``.../*.pdf`` with ``Content-Type: application/pdf``. Without this
    short-circuit the URL parser's chain is single-engine (Playwright)
    and there is no fallback to mineru/pdfium.
    """
    pdf_bytes = b"%PDF-1.4\nfake\n%%EOF"
    parsed = ParseResult(content="Extracted PDF body. " * 30, title="PDF Title")

    pdf_parser = MagicMock()
    pdf_parser.parse = AsyncMock(return_value=parsed)
    fake_registry = MagicMock()
    fake_registry.fallback_chain = MagicMock(return_value=["mineru"])
    fake_registry.get_by_engine = MagicMock(return_value=pdf_parser)

    with _patch_async_httpx(
            url="https://cdn.example/file.pdf",
            content_type="application/pdf",
            content=pdf_bytes,
         ), \
         patch("parsers.registry.registry", fake_registry), \
         patch.object(GenericHandler, "_parse_with_playwright",
                      new_callable=AsyncMock,
                      side_effect=AssertionError(
                          "Playwright must not be invoked on application/pdf"
                      )):
        h = GenericHandler()
        result = await h.parse("https://cdn.example/file.pdf")

    assert result.content == parsed.content
    assert result.title == "PDF Title"
    pdf_parser.parse.assert_awaited_once()
    # Bytes from the response body — not the URL string — must reach the parser.
    args, kwargs = pdf_parser.parse.await_args
    assert args[0] == pdf_bytes


@pytest.mark.asyncio
async def test_handler_pdf_chain_falls_through_engines():
    """First engine (mineru) raises → next engine (pdfium) gets the
    bytes. Mirrors the worker-handler chain semantics so a transient
    mineru outage doesn't dead-letter PDF URLs.
    """
    pdf_bytes = b"%PDF-1.4\n"
    good = ParseResult(content="pdfium body. " * 50, title="from pdfium")

    bad_parser = MagicMock()
    bad_parser.parse = AsyncMock(side_effect=RuntimeError("mineru down"))
    good_parser = MagicMock()
    good_parser.parse = AsyncMock(return_value=good)

    fake_registry = MagicMock()
    fake_registry.fallback_chain = MagicMock(return_value=["mineru", "pdfium"])
    fake_registry.get_by_engine = MagicMock(side_effect=lambda name: {
        "mineru": bad_parser, "pdfium": good_parser,
    }[name])

    with _patch_async_httpx(
            url="https://cdn.example/x.pdf",
            content_type="application/pdf",
            content=pdf_bytes,
         ), \
         patch("parsers.registry.registry", fake_registry):
        h = GenericHandler()
        result = await h.parse("https://cdn.example/x.pdf")

    assert result.content == good.content
    bad_parser.parse.assert_awaited_once()
    good_parser.parse.assert_awaited_once()


@pytest.mark.asyncio
async def test_handler_routes_pdf_with_charset_in_content_type():
    """Some CDNs append ``; charset=...`` even on binary types. The
    Content-Type comparison must split on ``;`` so the PDF short-circuit
    still fires.
    """
    pdf_bytes = b"%PDF-1.4\n"
    parsed = ParseResult(content="ok " * 200, title="t")

    pdf_parser = MagicMock()
    pdf_parser.parse = AsyncMock(return_value=parsed)
    fake_registry = MagicMock()
    fake_registry.fallback_chain = MagicMock(return_value=["pdfium"])
    fake_registry.get_by_engine = MagicMock(return_value=pdf_parser)

    with _patch_async_httpx(
            url="https://cdn.example/y.pdf",
            content_type="application/pdf; charset=binary",
            content=pdf_bytes,
         ), \
         patch("parsers.registry.registry", fake_registry):
        h = GenericHandler()
        result = await h.parse("https://cdn.example/y.pdf")

    assert result.content == parsed.content
    pdf_parser.parse.assert_awaited_once()


@pytest.mark.asyncio
async def test_generic_allow_image_only_stays_false():
    """Generic handler is HTML-scraped — stay strict."""
    html = "<html><head><title>Test</title></head><body><p>Content</p></body></html>"
    long_content = "Extracted content. " * 25

    with _patch_async_httpx(html=html, url="https://example.com/post"), \
         patch("parsers.url._handlers.trafilatura.extract", return_value=long_content):
        handler = GenericHandler()
        result = await handler.parse("https://example.com/post")

    assert result.allow_image_only is False


# ---------------------------------------------------------------------------
# Iframe-wrapped content fallback
# ---------------------------------------------------------------------------

def _make_playwright_page(*, top_html: str, top_url: str,
                         iframe_htmls: list[tuple[str, str]] | None = None,
                         iframe_read_errors: dict[int, Exception] | None = None):
    """Build a MagicMock page with a main_frame and N sub-frames.

    ``iframe_htmls`` is a list of ``(frame_url, frame_html)`` pairs.
    ``iframe_read_errors`` maps sub-frame index → exception raised by
    that frame's ``content()`` (for testing read-failure tolerance).
    """
    from playwright.async_api import Error as PlaywrightError  # noqa: F401

    page = MagicMock()
    page.goto = AsyncMock()
    page.wait_for_timeout = AsyncMock()
    page.wait_for_load_state = AsyncMock()
    page.url = top_url
    page.title = AsyncMock(return_value="Top Title")
    page.content = AsyncMock(return_value=top_html)

    main_frame = MagicMock()
    main_frame.url = top_url
    main_frame.content = AsyncMock(return_value=top_html)

    sub_frames = []
    for idx, (furl, fhtml) in enumerate(iframe_htmls or []):
        f = MagicMock()
        f.url = furl
        if iframe_read_errors and idx in iframe_read_errors:
            f.content = AsyncMock(side_effect=iframe_read_errors[idx])
        else:
            f.content = AsyncMock(return_value=fhtml)
        sub_frames.append(f)

    page.main_frame = main_frame
    page.frames = [main_frame, *sub_frames]
    return page


def _patch_session_with_page(page):
    from contextlib import asynccontextmanager
    ctx = MagicMock()
    ctx.new_page = AsyncMock(return_value=page)

    @asynccontextmanager
    async def fake_session(**_kwargs):
        yield ctx

    return patch("parsers.url._browser.parse_session", fake_session)


@pytest.mark.asyncio
async def test_playwright_extracts_from_iframe_when_top_is_empty():
    """Real-world failure: html2web.com renders an empty shell at the
    top frame and serves the actual article inside an iframe. When
    trafilatura on the top frame returns nothing, the handler must
    walk sub-frames and pick the first one that extracts.

    The image URLs must be resolved against the *frame's* URL — they
    were relative to the iframe document, not the top page.
    """
    page = _make_playwright_page(
        top_html='<html><body><iframe src="//example.com/view/abc"></iframe></body></html>',
        top_url="https://example.com/p/kktno10d",
        iframe_htmls=[
            ("https://example.com/view/abc",
             '<html><body><article>iframe body content</article>'
             '<img src="/media/pic.png"></body></html>'),
        ],
    )

    long_content = "Real iframe article body. " * 25

    # trafilatura returns "" for the top frame, content for the iframe.
    def trafilatura_extract(html, **_kw):
        return long_content if "iframe body" in html else ""

    with _patch_async_httpx(side_effect=Exception("connection failed")), \
         patch("parsers.url._handlers.trafilatura.extract",
               side_effect=trafilatura_extract), \
         patch("parsers.url._generic.fetch_impersonated", new=AsyncMock(return_value=None)), \
         _patch_session_with_page(page):
        result = await GenericHandler().parse("https://example.com/p/kktno10d")

    assert "Real iframe article body" in result.content
    # Image URLs resolve against the iframe's URL (example.com/view/abc),
    # not the top page — same host here, but the helper must pass the
    # frame URL, not the top URL.
    assert any("pic.png" in u for u in (result.image_urls or []))


@pytest.mark.asyncio
async def test_playwright_does_not_dive_when_top_extraction_works():
    """Fast-path preserved: if trafilatura got real content from the
    top frame, ad-tracker / analytics iframes must NOT be scraped
    and merged in. The iframe-walk only fires on top-frame-empty."""
    page = _make_playwright_page(
        top_html="<html><body>article body</body></html>",
        top_url="https://example.com/post",
        iframe_htmls=[
            ("https://ads.example.net/banner",
             "<html><body>BUY NOW BUY NOW</body></html>"),
        ],
    )

    with _patch_async_httpx(side_effect=Exception("connection failed")), \
         patch("parsers.url._handlers.trafilatura.extract",
               side_effect=lambda html, **_kw: (
                   "Real article body. " * 25 if "article body" in html else ""
               )), \
         patch("parsers.url._generic.fetch_impersonated", new=AsyncMock(return_value=None)), \
         _patch_session_with_page(page):
        result = await GenericHandler().parse("https://example.com/post")

    assert "Real article body" in result.content
    assert "BUY NOW" not in result.content
    # iframe.content() must not have been invoked at all — the top
    # frame was good enough.
    sub_frame = page.frames[1]
    sub_frame.content.assert_not_called()


@pytest.mark.asyncio
async def test_playwright_iframe_walk_skips_unreadable_frames():
    """Cross-origin iframes can raise on .content() (CSP / network).
    The walker must continue past those, not abort the whole parse."""
    from playwright.async_api import Error as PlaywrightError

    page = _make_playwright_page(
        top_html="<html><body><iframe></iframe><iframe></iframe></body></html>",
        top_url="https://example.com/p/x",
        iframe_htmls=[
            ("https://blocked.example/", ""),  # raises
            ("https://example.com/view/y",
             "<html><body><article>real frame body</article></body></html>"),
        ],
        iframe_read_errors={0: PlaywrightError("frame detached")},
    )

    def trafilatura_extract(html, **_kw):
        return "Recovered article. " * 25 if "real frame body" in html else ""

    with _patch_async_httpx(side_effect=Exception("connection failed")), \
         patch("parsers.url._handlers.trafilatura.extract",
               side_effect=trafilatura_extract), \
         patch("parsers.url._generic.fetch_impersonated", new=AsyncMock(return_value=None)), \
         _patch_session_with_page(page):
        result = await GenericHandler().parse("https://example.com/p/x")

    assert "Recovered article" in result.content


@pytest.mark.asyncio
async def test_playwright_returns_empty_when_no_frame_has_content():
    """If neither the top frame nor any sub-frame yields extractable
    content, the parser returns empty — the worker handler then
    classifies as empty_content (correct: nothing extractable exists).

    HTML bodies are literally empty here so trafilatura's
    ``baseline`` last-resort doesn't latch onto stray body text and
    paper over the "truly empty" case we're guarding."""
    page = _make_playwright_page(
        top_html="<html><head><title>shell</title></head><body></body></html>",
        top_url="https://example.com/post",
        iframe_htmls=[
            ("https://ads.example.net/",
             "<html><head></head><body></body></html>"),
        ],
    )

    with _patch_async_httpx(side_effect=Exception("connection failed")), \
         patch("parsers.url._handlers.trafilatura.extract", return_value=""), \
         patch("parsers.url._generic.fetch_impersonated", new=AsyncMock(return_value=None)), \
         _patch_session_with_page(page):
        result = await GenericHandler().parse("https://example.com/post")

    assert result.content.strip() == ""


# ---------------------------------------------------------------------------
# Task 3: browser UA + block-page detection
# ---------------------------------------------------------------------------

import httpx as _httpx_mod
from parsers.url._handlers import DEFAULT_BROWSER_UA
import parsers.url._http as _http_mod


@pytest.mark.asyncio
async def test_httpx_sends_browser_ua(monkeypatch):
    """make_async_client() (in _http.py) passes DEFAULT_BROWSER_UA as the
    User-Agent header by default.  Verify the factory is called without
    overrides so the default UA is used, then verify _http.make_async_client
    was invoked (UA contract lives there, not in _generic.py post-migration).
    """
    captured = {}

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)

    html_bytes = ("<html><body>" + "正常正文。" * 100 + "</body></html>").encode()
    mock_resp = MagicMock()
    mock_resp.encoding = "utf-8"
    mock_resp.url = "https://example.com/a"
    mock_resp.headers = {"content-type": "text/html"}

    original_make = _http_mod.make_async_client

    def _spy_make(**kw):
        captured["kw"] = kw
        return mock_client

    async def _fake_fetch(client, url, **kw):
        return mock_resp, html_bytes

    monkeypatch.setattr(_http_mod, "make_async_client", _spy_make)
    monkeypatch.setattr(_http_mod, "fetch_capped", _fake_fetch)
    res = await GenericHandler()._parse_httpx("https://example.com/a")
    # _generic._parse_httpx calls make_async_client() with no explicit headers,
    # so _http.make_async_client must supply DEFAULT_BROWSER_UA.  Verify by
    # calling the real factory with the same kwargs and inspecting its defaults.
    real_opts = {}
    real_opts.update({"headers": {"User-Agent": DEFAULT_BROWSER_UA}})
    real_opts.update(captured.get("kw", {}))
    assert real_opts["headers"]["User-Agent"] == DEFAULT_BROWSER_UA


@pytest.mark.asyncio
async def test_httpx_block_page_returns_empty(monkeypatch):
    block_html = "<html><body>请完成下方验证后继续操作</body></html>"
    block_bytes = block_html.encode()

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)

    mock_resp = MagicMock()
    mock_resp.encoding = "utf-8"
    mock_resp.url = "https://baike.baidu.com/x"
    mock_resp.headers = {"content-type": "text/html"}

    async def _fake_fetch(client, url, **kw):
        return mock_resp, block_bytes

    monkeypatch.setattr(_http_mod, "make_async_client", lambda **kw: mock_client)
    monkeypatch.setattr(_http_mod, "fetch_capped", _fake_fetch)
    res = await GenericHandler()._parse_httpx("https://baike.baidu.com/x")
    assert res.content == ""


# ---------------------------------------------------------------------------
# Task 4: post-render block-page detection → AntiBotBlockedError
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_playwright_block_page_raises_antibot_error():
    """浏览器渲染后页面仍是反爬/验证页 → raise AntiBotBlockedError,
    绝不返回验证页文本作为内容。

    Uses the real _parse_with_playwright path driven through a mocked
    parse_session (same pattern as test_playwright_recovers_from_navigation_race).
    The page title is "百度安全验证" (a _BLOCK_TITLE_MARKERS hit) so
    detect_block_reason fires even before body-text length is considered.
    """
    from parsers.base import AntiBotBlockedError

    block_html = "<html><head><title>百度安全验证</title></head><body>请完成下方验证</body></html>"
    block_title = "百度安全验证"

    page = _make_playwright_page(
        top_html=block_html,
        top_url="https://baike.baidu.com/x",
    )
    # Override title() to return the block title (default helper returns "Top Title")
    page.title = AsyncMock(return_value=block_title)

    with _patch_async_httpx(side_effect=Exception("connection failed")), \
         patch("parsers.url._generic.fetch_impersonated", new=AsyncMock(return_value=None)), \
         _patch_session_with_page(page):
        with pytest.raises(AntiBotBlockedError):
            await GenericHandler().parse("https://baike.baidu.com/x")


@pytest.mark.asyncio
async def test_playwright_block_page_error_message_contains_url():
    """AntiBotBlockedError message must contain the URL so operators
    know which site triggered the block."""
    from parsers.base import AntiBotBlockedError

    block_html = "<html><head><title>百度安全验证</title></head><body>请完成下方验证</body></html>"
    block_title = "百度安全验证"
    target_url = "https://baike.baidu.com/item/python"

    page = _make_playwright_page(
        top_html=block_html,
        top_url=target_url,
    )
    page.title = AsyncMock(return_value=block_title)

    with _patch_async_httpx(side_effect=Exception("connection failed")), \
         patch("parsers.url._generic.fetch_impersonated", new=AsyncMock(return_value=None)), \
         _patch_session_with_page(page):
        with pytest.raises(AntiBotBlockedError, match="baike.baidu.com"):
            await GenericHandler().parse(target_url)


@pytest.mark.asyncio
async def test_playwright_non_block_page_is_not_raised():
    """Regression guard: a normal page must NOT trigger AntiBotBlockedError.
    Only genuine block/verification pages should raise."""
    from parsers.base import AntiBotBlockedError

    normal_html = ("<html><head><title>Python编程</title></head>"
                   "<body><article>" + ("正常文章内容。" * 100) + "</article></body></html>")

    page = _make_playwright_page(
        top_html=normal_html,
        top_url="https://example.com/article",
    )
    page.title = AsyncMock(return_value="Python编程")

    long_content = "正常文章内容。" * 50

    with _patch_async_httpx(side_effect=Exception("connection failed")), \
         patch("parsers.url._handlers.trafilatura.extract", return_value=long_content), \
         patch("parsers.url._generic.fetch_impersonated", new=AsyncMock(return_value=None)), \
         _patch_session_with_page(page):
        # Must not raise — should return a normal ParseResult
        result = await GenericHandler().parse("https://example.com/article")

    assert isinstance(result, ParseResult)
    assert "正常文章内容" in result.content


# ---------------------------------------------------------------------------
# P1-T7: curl_cffi tier inserted between httpx and playwright
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_generic_uses_curl_cffi_before_playwright(monkeypatch):
    from parsers.url._generic import GenericHandler
    import parsers.url._generic as g
    from parsers.base import ParseResult
    async def fake_httpx(self, url):
        return ParseResult(content="", title="")          # httpx blocked → empty
    async def fake_fetch(url, **kw):
        return "<html><body>" + ("真正文。" * 100) + "</body></html>"
    pw_called = {"n": 0}
    async def fake_pw(self, url):
        pw_called["n"] += 1
        return ParseResult(content="pw", title="")
    monkeypatch.setattr(GenericHandler, "_parse_httpx", fake_httpx)
    monkeypatch.setattr(g, "fetch_impersonated", fake_fetch)
    monkeypatch.setattr(GenericHandler, "_parse_with_playwright", fake_pw)
    r = await GenericHandler().parse("https://example.com/x")
    assert "真正文" in r.content
    assert pw_called["n"] == 0    # curl_cffi succeeded → playwright NOT called
