"""Generic URL handler — httpx fetch with playwright fallback."""
from __future__ import annotations

from urllib.parse import urlparse

import httpx

from parsers.base import ParseResult
from parsers.url._handlers import (
    extract_html_title,
    extract_image_urls,
    extract_with_trafilatura,
    needs_browser_fallback,
)

_BROWSER_ONLY_DOMAINS = {"threads.net", "instagram.com"}


def _browser_only(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return any(host == d or host.endswith("." + d) for d in _BROWSER_ONLY_DOMAINS)


class GenericHandler:
    def match(self, url: str) -> bool:
        return True

    def download_headers(self, url: str) -> dict[str, str] | None:
        return None

    async def parse(self, url: str) -> ParseResult:
        if _browser_only(url):
            return await self._parse_with_playwright(url)

        result = await self._parse_httpx(url)
        if needs_browser_fallback(result):
            return await self._parse_with_playwright(url)
        return result

    async def _parse_httpx(self, url: str) -> ParseResult:
        """Async HTTP fetch. Previously dispatched sync ``httpx.get``
        via ``run_in_executor(None, ...)`` which shared the default
        thread pool with ``asyncio.to_thread`` hot paths elsewhere
        (image_stream, vision calls). Native async removes that
        coupling and stops generic-URL fetches from fighting for
        thread slots under concurrent load.
        """
        try:
            async with httpx.AsyncClient(
                timeout=15, follow_redirects=True,
            ) as client:
                resp = await client.get(url)
            resp.raise_for_status()
            downloaded = resp.text
            final_url = str(resp.url)
        except Exception:
            return ParseResult(content="", images={}, title="")

        text = extract_with_trafilatura(downloaded)
        title = extract_html_title(downloaded, final_url)
        img_urls = extract_image_urls(downloaded, final_url)
        return ParseResult(content=text, images={}, title=title, image_urls=img_urls)

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
        except ImportError:
            return ParseResult(content="", images={}, title="")

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
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)

                # Poll until Cloudflare/JS challenge clears
                html = ""
                title = ""
                for _ in range(20):
                    await page.wait_for_timeout(1000)
                    title = await page.title()
                    html = await page.content()
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
        except ModuleNotFoundError:
            # ``_browser.get_browser`` imports ``playwright.async_api`` on
            # first use; absent package surfaces here, not at the outer
            # try/except above.
            return ParseResult(content="", images={}, title="")

        text = extract_with_trafilatura(html)
        img_urls = extract_image_urls(html, final_url)
        return ParseResult(content=text, images={}, title=title, image_urls=img_urls)
