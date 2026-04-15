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
_WECHAT_HOSTS = {"mp.weixin.qq.com", "weixin.qq.com", "channels.weixin.qq.com"}
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


_JS_CHALLENGE_MARKERS = (
    "javascript is disabled",
    "enable javascript",
    "just a moment",
    "please enable cookies",
    "please turn on javascript",
    "javascript required",
)

# Domains that fingerprint bare httpx clients and then also flag subsequent
# browser requests from the same IP. Must go straight to playwright.
_BROWSER_ONLY_DOMAINS = {
    "threads.net",
    "instagram.com",
}


def _browser_only(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return any(host == d or host.endswith("." + d) for d in _BROWSER_ONLY_DOMAINS)


_X_HOSTS = {"x.com", "twitter.com"}
_X_STATUS_RE = re.compile(r"/status(?:es)?/(\d+)")


def _extract_x_tweet_id(url: str) -> str | None:
    host = (urlparse(url).hostname or "").lower()
    if not any(host == d or host.endswith("." + d) for d in _X_HOSTS):
        return None
    m = _X_STATUS_RE.search(urlparse(url).path)
    return m.group(1) if m else None


def _needs_browser_fallback(result: "ParseResult") -> bool:
    """httpx returned nothing useful — likely a JS-challenge or pure SPA page."""
    content = (result.content or "").strip()
    if len(content) < 300:
        return True
    lower = content[:2000].lower()
    return any(m in lower for m in _JS_CHALLENGE_MARKERS)


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
        tweet_id = _extract_x_tweet_id(url)
        if tweet_id is not None:
            return await self._parse_x_tweet(tweet_id, url)
        if _browser_only(url):
            return await self._parse_generic_with_playwright(url)
        result = await asyncio.get_running_loop().run_in_executor(None, self._parse_sync, url)
        # Sites behind Cloudflare/JS-challenge (e.g. Medium, Notion) return
        # empty or near-empty content via httpx. Fall back to a real browser.
        if _needs_browser_fallback(result):
            return await self._parse_generic_with_playwright(url)
        return result

    async def _parse_x_tweet(self, tweet_id: str, source_url: str) -> ParseResult:
        """Fetch tweet content via X's public syndication API (used by embed widgets)."""
        api = (
            f"https://cdn.syndication.twimg.com/tweet-result"
            f"?id={tweet_id}&lang=en&token=1"
        )
        try:
            async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
                resp = await client.get(api, headers={"User-Agent": "Mozilla/5.0"})
                resp.raise_for_status()
                data = resp.json()
        except Exception:
            return ParseResult(content="", images={}, title="")

        user = data.get("user") or {}
        user_name = user.get("name") or ""
        handle = user.get("screen_name") or ""
        text = (data.get("text") or "").strip()
        article = data.get("article") or {}
        article_title = (article.get("title") or "").strip()
        article_preview = (article.get("preview_text") or "").strip()

        parts: list[str] = []
        header = f"# {user_name}".strip()
        if handle:
            header += f" (@{handle})"
        if header != "#":
            parts.append(header)
        if text:
            parts.append(text)
        if article_title:
            parts.append(f"## {article_title}")
        if article_preview:
            parts.append(article_preview)

        img_urls: list[str] = []
        cover = article.get("cover_media") or {}
        cover_img = (cover.get("media_info") or {}).get("original_img_url")
        if cover_img:
            img_urls.append(cover_img)
        for m in data.get("mediaDetails") or []:
            u = m.get("media_url_https")
            if u and u not in img_urls:
                img_urls.append(u)

        title = article_title or (text[:80] if text else f"Tweet by {user_name or handle}")
        return ParseResult(
            content="\n\n".join(parts),
            images={},
            title=title or source_url,
            image_urls=img_urls[:20],
        )

    async def _parse_generic_with_playwright(self, url: str) -> ParseResult:
        if async_playwright is None:
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
                # Poll up to 20s: wait for Cloudflare to clear AND for SPA
                # content to render past the "enable JavaScript" stub.
                html = ""
                for _ in range(20):
                    await page.wait_for_timeout(1000)
                    t = await page.title()
                    html = await page.content()
                    stub = (
                        "Just a moment" in t
                        or "javascript is disabled" in html.lower()[:5000]
                        or "enable javascript" in html.lower()[:5000]
                    )
                    if not stub:
                        break

                final_url = page.url
                title = await page.title() or urlparse(final_url).netloc
            finally:
                await browser.close()

        text = trafilatura.extract(
            html, include_images=True, include_links=False, output_format="markdown",
        ) or ""
        if not text.strip():
            from trafilatura.utils import load_html
            from trafilatura.core import baseline
            tree = load_html(html)
            if tree is not None:
                _, raw_text, _ = baseline(tree)
                text = raw_text or ""

        img_urls = _extract_image_urls(html, final_url)
        return ParseResult(content=text, images={}, title=title, image_urls=img_urls)

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

            # Detect article type and extract content from appropriate DOM selectors
            article_info = await page.evaluate("""
                () => {
                    // Try image message (#js_image_content): title in h1, content in #js_image_desc
                    const imgContent = document.querySelector('#js_image_content');
                    if (imgContent) {
                        const titleEl = imgContent.querySelector('h1.rich_media_title, .rich_media_title');
                        const descEl = document.querySelector('#js_image_desc');
                        return {
                            type: 'image_msg',
                            title: titleEl ? titleEl.textContent.trim() : '',
                            contentHtml: descEl ? descEl.innerHTML : '',
                        };
                    }
                    // Regular article: title in #activity-name, content in #js_content
                    const contentEl = document.querySelector('#js_content');
                    if (contentEl) {
                        const titleEl = document.querySelector('#activity-name');
                        return {
                            type: 'article',
                            title: titleEl ? titleEl.textContent.trim() : '',
                            contentHtml: contentEl.innerHTML,
                        };
                    }
                    // Fallback for other WeChat pages (e.g. 视频号 channels.weixin.qq.com):
                    // extract all visible text from the rendered page
                    const title = document.title || '';
                    return {
                        type: 'generic',
                        title: title,
                        contentHtml: document.body.innerHTML,
                    };
                }
            """)

            title = article_info.get("title") or ""
            content_html = article_info.get("contentHtml") or ""
            article_type = article_info.get("type", "article")

            # Extract image URLs based on article type
            if article_type == "image_msg":
                img_selector = "#js_image_desc img"
            elif article_type == "article":
                img_selector = "#js_content img"
            else:
                img_selector = "body img"
            img_urls = await page.evaluate(f"""
                () => Array.from(document.querySelectorAll('{img_selector}'))
                    .map(img => img.getAttribute('data-src') || img.src)
                    .filter(src => src && !src.startsWith('data:'))
            """)

            # For image messages, also capture the main card image (author avatar / cover)
            if article_type == "image_msg":
                avatar_img = await page.evaluate("""
                    () => {
                        const avatar = document.querySelector('.reward-avatar img, #js_image_desc + div img, #js_image_content img');
                        return avatar ? (avatar.getAttribute('data-src') || avatar.src) : '';
                    }
                """)
                if avatar_img and avatar_img not in img_urls and not avatar_img.startswith("data:"):
                    img_urls = [avatar_img] + img_urls

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

        # Fallback: extract all visible text when trafilatura finds no article content
        if not text.strip():
            from trafilatura.utils import load_html
            from trafilatura.core import baseline

            tree = load_html(f"<html><body>{content_html}</body></html>")
            if tree is not None:
                _, raw_text, _ = baseline(tree)
                text = raw_text or ""

        return ParseResult(
            content=text, images={}, title=title,
            image_urls=img_urls[:20],
        )

    def _parse_sync(self, url: str) -> ParseResult:
        # Use httpx instead of trafilatura.fetch_url to follow HTTP redirects
        # (trafilatura.fetch_url can't handle JS redirects like baidu.com)
        try:
            resp = httpx.get(url, timeout=15, follow_redirects=True)
            resp.raise_for_status()
            downloaded = resp.text
            final_url = str(resp.url)
        except Exception:
            return ParseResult(content="", images={}, title="")

        if not downloaded:
            return ParseResult(content="", images={}, title="")

        text = trafilatura.extract(
            downloaded,
            include_images=True,
            include_links=False,
            output_format="markdown",
        ) or ""

        # Fallback: extract all visible text when trafilatura finds no article content
        if not text.strip():
            from trafilatura.utils import load_html
            from trafilatura.core import baseline

            tree = load_html(downloaded)
            if tree is not None:
                _, raw_text, _ = baseline(tree)
                text = raw_text or ""

        title_match = re.search(r"<title[^>]*>([^<]+)</title>", downloaded, re.IGNORECASE)
        title = title_match.group(1).strip() if title_match else urlparse(final_url).netloc

        img_urls = _extract_image_urls(downloaded, final_url)
        return ParseResult(content=text, images={}, title=title, image_urls=img_urls)


def download_images(
    urls: list[str],
    headers: dict[str, str] | None = None,
) -> dict[str, bytes]:
    """Download images from URL list (sync). Returns {url: bytes}."""
    images: dict[str, bytes] = {}
    for src in urls[:20]:
        try:
            resp = httpx.get(src, timeout=10, follow_redirects=True, headers=headers or {})
            if resp.status_code == 200 and resp.content:
                images[src] = resp.content
        except Exception:
            continue
    return images
