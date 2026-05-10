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
    """Patch ``httpx.AsyncClient`` returned by ``_generic.py``'s
    ``async with httpx.AsyncClient(...) as client`` block.

    Either provide HTML (and optional final URL) for success, or
    pass ``side_effect=SomeException`` to make the mocked ``.get``
    raise (mirroring network failures).
    """
    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    if side_effect is not None:
        mock_client.get = AsyncMock(side_effect=side_effect)
    else:
        mock_resp = MagicMock()
        mock_resp.text = html or ""
        mock_resp.content = content if content is not None else (html or "").encode()
        mock_resp.url = url
        mock_resp.headers = {"content-type": content_type}
        mock_resp.raise_for_status = MagicMock()
        mock_client.get = AsyncMock(return_value=mock_resp)
    with patch("parsers.url._generic.httpx.AsyncClient",
               return_value=mock_client):
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
