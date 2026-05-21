"""Generic URL handler — httpx fetch with playwright fallback."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import httpx

from parsers.base import ParseResult, PermanentParseError
from parsers.url._handlers import (
    extract_html_title,
    extract_image_urls,
    extract_with_trafilatura,
    needs_browser_fallback,
)

logger = logging.getLogger(__name__)


@dataclass
class _PdfHandled:
    """Sentinel: ``_parse_httpx`` already dispatched an ``application/pdf``
    response to the PDF parser chain — caller must accept ``result`` as
    final and skip the Playwright fallback path.
    """
    result: ParseResult


async def _parse_pdf_bytes(data: bytes, *, url: str) -> ParseResult:
    """Run the registered PDF parser chain on already-downloaded bytes.

    Mirrors the worker handler's per-engine fallback semantics for the
    ``.pdf`` extension, but operating on bytes already in hand instead
    of going back to the queue. Returns the first useful result; on
    empty / total failure, returns an empty ``ParseResult`` (the worker
    handler will then mark the doc ``empty_content`` rather than dead-
    lettering — much quieter than the previous "Page.goto: Download is
    starting" loop).
    """
    # Lazy import: registry imports url package at module init, so a
    # top-level import here would form a cycle.
    from parsers.registry import registry

    filename = url.rsplit("/", 1)[-1] or "downloaded.pdf"
    if not filename.lower().endswith(".pdf"):
        filename += ".pdf"
    last_exc: BaseException | None = None
    for engine_name in registry.fallback_chain(filename=filename):
        try:
            parser = registry.get_by_engine(engine_name)
        except ValueError:
            continue
        try:
            candidate = await parser.parse(data, filename=filename)
        except Exception as exc:
            last_exc = exc
            logger.warning(
                "url->pdf: %s failed on %s (%s: %s); trying next engine",
                engine_name, url, type(exc).__name__, exc,
            )
            continue
        if candidate.content.strip():
            return candidate
    if last_exc is not None:
        logger.warning(
            "url->pdf: every engine failed for %s; last error: %r",
            url, last_exc,
        )
    return ParseResult(content="", title="")


def _chromium_cert_error_code(exc: BaseException) -> str | None:
    """Return the ``ERR_CERT_*`` token from a playwright Error message,
    or ``None`` if the exception isn't a Chromium TLS-cert failure.

    Chromium surfaces all cert problems as ``net::ERR_CERT_<reason>``
    in the Page.goto error string (DATE_INVALID, AUTHORITY_INVALID,
    COMMON_NAME_INVALID, REVOKED, INVALID, …). Cert validity is not a
    transient state — site owners must renew/reconfigure — so we treat
    these as permanent and skip the queue's retry-and-alert path
    instead of burning three worker slots per dead URL.

    Deliberately does NOT match ``net::ERR_SSL_*`` or other TLS errors
    (protocol negotiation, handshake) — those can be transient
    (server reload, intermediate proxy hiccup) and are worth retrying.
    """
    msg = str(exc)
    marker = "net::ERR_CERT_"
    idx = msg.find(marker)
    if idx == -1:
        return None
    tail = msg[idx + len("net::") :]
    end = 0
    while end < len(tail) and (tail[end].isalnum() or tail[end] == "_"):
        end += 1
    return tail[:end] or None

_BROWSER_ONLY_DOMAINS = {"threads.net", "instagram.com"}

# Query-string keys we strip before fetching. Two reasons:
#   1. Many of these (Google Ads ``gclid``, ``gad_*``) trigger client-side
#      tracker scripts that ``history.replaceState`` after the document
#      loads, which races with our playwright poll loop and used to
#      surface as "Execution context was destroyed" before we taught the
#      loop to recover from it. Stripping cuts the race off at the source
#      so we don't depend on recovery for the common case.
#   2. They're never load-bearing for the article content itself.
#
# Exact key matches (not prefix) for tight scoping. Prefix groups
# (``gad_*``, ``utm_*``, ``mc_*``) are listed separately below — these
# camps have many siblings (``gad_source``, ``gad_campaignid``,
# ``utm_source``, ``utm_medium``, ``utm_id``, ``mc_cid``, ``mc_eid``)
# and listing each one risks missing a future addition.
_TRACKING_PARAM_KEYS = frozenset({
    "gclid", "gclsrc", "gbraid", "wbraid",     # Google Ads
    "fbclid",                                   # Facebook / Meta
    "yclid",                                    # Yandex
    "msclkid",                                  # Microsoft Ads
    "dclid",                                    # DoubleClick / GA4
    "ref", "ref_src", "ref_url",                # generic referral hints
})
_TRACKING_PARAM_PREFIXES = ("gad_", "utm_", "mc_")


def _strip_tracking_params(url: str) -> str:
    """Drop known ad / analytics tracking params from ``url``'s query.

    Site-specific handlers (mp.weixin, feishu, etc.) bypass GenericHandler,
    so their URL-encoded payload params are not at risk here.
    """
    parsed = urlparse(url)
    if not parsed.query:
        return url
    kept = [
        (k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True)
        if k not in _TRACKING_PARAM_KEYS
        and not any(k.startswith(p) for p in _TRACKING_PARAM_PREFIXES)
    ]
    new_query = urlencode(kept)
    if new_query == parsed.query:
        return url
    return urlunparse(parsed._replace(query=new_query))


def _browser_only(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return any(host == d or host.endswith("." + d) for d in _BROWSER_ONLY_DOMAINS)


class GenericHandler:
    def match(self, url: str) -> bool:
        return True

    def download_headers(self, url: str) -> dict[str, str] | None:
        return None

    async def parse(self, url: str) -> ParseResult:
        url = _strip_tracking_params(url)
        if _browser_only(url):
            return await self._parse_with_playwright(url)

        # Sentinel returned by _parse_httpx for application/pdf responses
        # — the bytes were already dispatched to the PDF parser chain and
        # the result is attached to the sentinel. Skip Playwright entirely
        # in that case: Chromium refuses PDF navigation with
        # ``Page.goto: Download is starting`` and would burn 3 retries.
        result = await self._parse_httpx(url)
        if isinstance(result, _PdfHandled):
            return result.result
        if needs_browser_fallback(result):
            return await self._parse_with_playwright(url)
        return result

    async def _parse_httpx(self, url: str) -> "ParseResult | _PdfHandled":
        """Async HTTP fetch. Previously dispatched sync ``httpx.get``
        via ``run_in_executor(None, ...)`` which shared the default
        thread pool with ``asyncio.to_thread`` hot paths elsewhere
        (image_stream, vision calls). Native async removes that
        coupling and stops generic-URL fetches from fighting for
        thread slots under concurrent load.

        For ``application/pdf`` responses, hands the bytes off to the
        registered PDF parser chain and returns a :class:`_PdfHandled`
        sentinel so the caller knows the result is final (no Playwright
        fallback). Without the sentinel, an empty PDF result would
        re-enter Playwright and re-trigger the original "Page.goto:
        Download is starting" failure.
        """
        try:
            async with httpx.AsyncClient(
                timeout=15, follow_redirects=True,
            ) as client:
                resp = await client.get(url)
            resp.raise_for_status()
        except Exception:
            return ParseResult(content="", title="")

        ct = resp.headers.get("content-type", "").split(";", 1)[0].strip().lower()
        if ct == "application/pdf":
            return _PdfHandled(await _parse_pdf_bytes(resp.content, url=url))

        downloaded = resp.text
        final_url = str(resp.url)
        text = extract_with_trafilatura(downloaded)
        title = extract_html_title(downloaded, final_url)
        img_urls = extract_image_urls(downloaded, final_url)
        return ParseResult(content=text, title=title, image_urls=img_urls)

    async def _parse_with_playwright(self, url: str) -> ParseResult:
        """Browser-pool fallback for JS-challenge / SPA / anti-bot sites.

        Uses the process-wide Chromium from ``_browser.parse_session`` so
        we pay the ~2-4 s launch cost once per worker instead of once per
        URL. ``block_heavy=False`` keeps stylesheets+images loading —
        some anti-bot challenges gate their clearance signal on layout
        completion, and blocking images can keep the page stuck on
        "Just a moment...". The ``navigator.webdriver`` shim masks the
        CDP automation surface that Cloudflare looks at.
        """
        try:
            from parsers.url._browser import parse_session
            from playwright.async_api import Error as PlaywrightError
        except ImportError:
            return ParseResult(content="", title="")

        ua = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        )
        anti_webdriver = (
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        try:
            async with parse_session(
                user_agent=ua,
                block_heavy=False,
                viewport={"width": 1280, "height": 800},
                locale="en-US",
                init_script=anti_webdriver,
            ) as ctx:
                page = await ctx.new_page()
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                except PlaywrightError as exc:
                    cert_code = _chromium_cert_error_code(exc)
                    if cert_code is not None:
                        raise PermanentParseError(
                            f"TLS certificate invalid ({cert_code}) at {url}; "
                            "site cert is expired or misconfigured — not retryable"
                        ) from exc
                    raise

                # Poll until Cloudflare/JS challenge clears. ``page.title()``
                # / ``page.content()`` race with in-flight client-side
                # navigation (ad-tracker redirects on `?gclid=` URLs, SPA
                # route changes) — the JS execution context gets torn down
                # mid-call and playwright surfaces "Execution context was
                # destroyed". Swallow that one error so the loop retries
                # on the next tick instead of bubbling out and burning all
                # three worker retries.
                html = ""
                title = ""
                for _ in range(20):
                    await page.wait_for_timeout(1000)
                    try:
                        title = await page.title()
                        html = await page.content()
                    except PlaywrightError as exc:
                        if "Execution context was destroyed" not in str(exc):
                            raise
                        try:
                            await page.wait_for_load_state(
                                "domcontentloaded", timeout=5000,
                            )
                        except PlaywrightError:
                            pass
                        continue
                    lower_head = html[:5000].lower()
                    stub = (
                        "Just a moment" in title
                        or "javascript is disabled" in lower_head
                        or "enable javascript" in lower_head
                    )
                    if not stub:
                        break

                final_url = page.url
                if not title:
                    title = urlparse(final_url).netloc

                text = extract_with_trafilatura(html)
                img_urls = extract_image_urls(html, final_url)

                # Iframe-wrapped content fallback. Some "publish your HTML"
                # services (html2web.com, certain embedded Notion views)
                # render an empty shell at the top frame and load the real
                # article inside an ``<iframe>``. ``page.content()`` only
                # returns the top frame, so trafilatura sees nothing.
                # Walk sub-frames once and pick the first one whose body
                # extracts. Bounded to the top-empty case so ad/tracker
                # iframes on otherwise-healthy pages don't get scraped.
                if not text.strip():
                    for frame in page.frames:
                        if frame is page.main_frame:
                            continue
                        try:
                            frame_html = await frame.content()
                        except PlaywrightError:
                            continue
                        frame_text = extract_with_trafilatura(frame_html)
                        if not frame_text.strip():
                            continue
                        text = frame_text
                        # Images in the iframe are relative to the iframe's
                        # own URL, not the top page's. Replace the empty
                        # top-frame list outright; mixing would attribute
                        # wrong base URLs.
                        img_urls = extract_image_urls(frame_html, frame.url)
                        logger.info(
                            "url-generic: extracted content from sub-frame "
                            "%s (top-frame body was empty)", frame.url,
                        )
                        break
        except ModuleNotFoundError:
            # ``_browser.get_browser`` imports ``playwright.async_api`` on
            # first use; absent package surfaces here, not at the outer
            # try/except above.
            return ParseResult(content="", title="")

        return ParseResult(content=text, title=title, image_urls=img_urls)
