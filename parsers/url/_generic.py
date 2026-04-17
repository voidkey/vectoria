"""Generic URL handler — httpx fetch with playwright fallback."""
from __future__ import annotations

import asyncio
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

        result = await asyncio.get_running_loop().run_in_executor(
            None, self._parse_sync, url,
        )
        if needs_browser_fallback(result):
            return await self._parse_with_playwright(url)
        return result

    def _parse_sync(self, url: str) -> ParseResult:
        try:
            resp = httpx.get(url, timeout=15, follow_redirects=True)
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
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            return ParseResult(content="", images={}, title="")

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-blink-features=AutomationControlled",
                ],
            )
            try:
                ctx = await browser.new_context(
                    user_agent=(
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
                    ),
                    viewport={"width": 1280, "height": 800},
                    locale="en-US",
                )
                await ctx.add_init_script(
                    "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
                )
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
            finally:
                await browser.close()

        text = extract_with_trafilatura(html)
        img_urls = extract_image_urls(html, final_url)
        return ParseResult(content=text, images={}, title=title, image_urls=img_urls)
