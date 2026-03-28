import asyncio
import re
from urllib.parse import urljoin, urlparse

import httpx
import trafilatura

try:
    from playwright.async_api import async_playwright
except ImportError:
    async_playwright = None

from parsers.base import BaseParser, ParseResult

_IMG_TAG = re.compile(r'<img[^>]+(?:data-src|src)=["\']([^"\']+)["\']', re.IGNORECASE)
_WECHAT_HOSTS = {"mp.weixin.qq.com"}
WECHAT_UA = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 "
    "MicroMessenger/8.0.43 NetType/WIFI Language/zh_CN"
)


def is_wechat_url(url: str) -> bool:
    return urlparse(url).hostname in _WECHAT_HOSTS


def get_wechat_headers(url: str) -> dict[str, str] | None:
    """Return download headers for WeChat CDN, or None for non-WeChat URLs."""
    if not is_wechat_url(url):
        return None
    return {
        "Referer": "https://mp.weixin.qq.com/",
        "User-Agent": WECHAT_UA,
    }


def _extract_image_urls(html: str, base_url: str) -> list[str]:
    """Extract image URLs from HTML, resolve relative URLs, cap at 20."""
    urls: list[str] = []
    for src in _IMG_TAG.findall(html):
        abs_url = urljoin(base_url, src)
        if not abs_url.startswith("data:"):
            urls.append(abs_url)
        if len(urls) >= 20:
            break
    return urls


class UrlParser(BaseParser):
    engine_name = "url"
    supported_types = ["url"]

    async def parse(self, source: bytes | str, filename: str = "", **kwargs) -> ParseResult:
        url = source.decode() if isinstance(source, bytes) else source
        if is_wechat_url(url):
            return await self._parse_with_playwright(url)
        return await asyncio.get_running_loop().run_in_executor(None, self._parse_sync, url)

    async def _parse_with_playwright(self, url: str) -> ParseResult:
        if async_playwright is None:
            return await asyncio.get_running_loop().run_in_executor(None, self._parse_sync, url)

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox"],
            )
            page = await browser.new_page(
                user_agent=WECHAT_UA,
            )
            await page.goto(url, wait_until="networkidle", timeout=30000)

            # Scroll to trigger lazy-loaded images
            await page.evaluate("""
                async () => {
                    const delay = ms => new Promise(r => setTimeout(r, ms));
                    for (let y = 0; y < document.body.scrollHeight; y += 400) {
                        window.scrollTo(0, y);
                        await delay(200);
                    }
                }
            """)
            await page.wait_for_timeout(1000)

            # Extract title: prefer #activity-name over <title>
            title = await page.evaluate("""
                () => {
                    const el = document.querySelector('#activity-name');
                    return el ? el.textContent.trim() : '';
                }
            """) or ""

            # Extract content HTML: prefer #js_content over full body
            content_html = await page.evaluate("""
                () => {
                    const el = document.querySelector('#js_content');
                    return el ? el.innerHTML : document.body.innerHTML;
                }
            """)

            # Extract image URLs from article body
            img_urls = await page.evaluate("""
                () => Array.from(document.querySelectorAll('#js_content img'))
                    .map(img => img.getAttribute('data-src') || img.src)
                    .filter(src => src && !src.startsWith('data:'))
            """)

            if not title:
                html = await page.content()
                title_match = re.search(r"<title[^>]*>([^<]+)</title>", html, re.IGNORECASE)
                title = title_match.group(1).strip() if title_match else urlparse(url).netloc

            await browser.close()

        text = trafilatura.extract(
            f"<html><body>{content_html}</body></html>",
            include_images=True,
            include_links=False,
            output_format="markdown",
        ) or ""

        return ParseResult(
            content=text, images={}, title=title,
            image_urls=img_urls[:20],
        )

    def _parse_sync(self, url: str) -> ParseResult:
        downloaded = trafilatura.fetch_url(url)
        if not downloaded:
            return ParseResult(content="", images={}, title="")

        text = trafilatura.extract(
            downloaded,
            include_images=True,
            include_links=False,
            output_format="markdown",
        ) or ""

        title_match = re.search(r"<title[^>]*>([^<]+)</title>", downloaded, re.IGNORECASE)
        title = title_match.group(1).strip() if title_match else urlparse(url).netloc

        img_urls = _extract_image_urls(downloaded, url)
        return ParseResult(content=text, images={}, title=title, image_urls=img_urls)


def download_images(
    urls: list[str],
    headers: dict[str, str] | None = None,
) -> dict[str, bytes]:
    """Download images from URL list (sync). Returns {filename: bytes}."""
    images: dict[str, bytes] = {}
    for src in urls[:20]:
        try:
            resp = httpx.get(src, timeout=10, follow_redirects=True, headers=headers or {})
            if resp.status_code == 200 and resp.content:
                fname = src.rsplit("/", 1)[-1].split("?")[0] or "image.jpg"
                if fname in images:
                    stem, _, ext = fname.rpartition(".")
                    fname = f"{stem}_{len(images)}.{ext}" if ext else f"{fname}_{len(images)}"
                images[fname] = resp.content
        except Exception:
            continue
    return images
