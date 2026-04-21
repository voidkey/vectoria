"""WeChat site handler.

WeChat embeds full article content in the initial HTML response
(inside #js_content or #js_image_desc), hidden via CSS until
client-side JS verifies the environment.  The primary path extracts
directly from raw HTML, bypassing the JS anti-bot check.  Playwright
is kept as a fallback for edge cases where raw HTML has no content.
"""
from __future__ import annotations

import logging
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

import httpx
import lxml.html

from parsers.base import ParseResult
from parsers.url._handlers import extract_html_title, extract_with_trafilatura

log = logging.getLogger(__name__)

_WECHAT_HOSTS = {"mp.weixin.qq.com", "weixin.qq.com", "channels.weixin.qq.com"}
# WeChat's article image CDN. Not identical to the article host —
# article comes from mp.weixin.qq.com, its images from mmbiz.qpic.cn.
_WECHAT_IMG_HOST = "mmbiz.qpic.cn"
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


def extract_datasrc_urls(container: lxml.html.HtmlElement) -> list[str]:
    """Extract image URLs from data-src attributes within a DOM container."""
    urls: list[str] = []
    for img in container.findall(".//img[@data-src]"):
        src = img.get("data-src", "")
        if src and not src.startswith("data:") and src not in urls:
            urls.append(src)
        if len(urls) >= 20:
            break
    return urls


def extract_wechat_title(doc: lxml.html.HtmlElement) -> str:
    """Extract title from a WeChat article page."""
    activity = doc.get_element_by_id("activity-name", None)
    if activity is None:
        return ""
    inner = activity.find('.//*[@class="js_title_inner"]')
    if inner is not None:
        return (inner.text_content() or "").strip()
    return (activity.text_content() or "").strip()


def canonicalize_wechat_image_url(url: str) -> str:
    """Rewrite a WeChat image URL to force the JPEG rendering variant.

    Without this, mmbiz.qpic.cn may return WebP to callers that advertise
    support for it (many libraries do by default via Accept headers);
    downstream tools like PIL, the vision LLM, and some browser image
    tags prefer JPEG. ``wx_fmt=jpeg`` is the stable knob the CDN honors.

    No-op for non-WeChat image hosts, or URLs that already pinned a
    format. The original string is returned on any parse failure — this
    runs inline in the image-download loop and must never raise.
    """
    try:
        parsed = urlparse(url)
    except Exception:
        return url
    if (parsed.hostname or "").lower() != _WECHAT_IMG_HOST:
        return url

    q = dict(parse_qsl(parsed.query, keep_blank_values=True))
    if "wx_fmt" in q:
        return url
    q["wx_fmt"] = "jpeg"
    return urlunparse(parsed._replace(query=urlencode(q)))


class WechatHandler:
    def match(self, url: str) -> bool:
        return is_wechat_url(url)

    def download_headers(self, url: str) -> dict[str, str] | None:
        return get_wechat_headers(url)

    def canonicalize_image_url(self, url: str) -> str:
        return canonicalize_wechat_image_url(url)

    async def parse(self, url: str) -> ParseResult:
        result = await self._parse_raw(url)
        if result.content.strip():
            return result
        log.warning("WeChat raw extraction empty, falling back to playwright: %s", url)
        return await self._parse_with_playwright(url)

    async def _parse_raw(self, url: str) -> ParseResult:
        """Fetch raw HTML via async httpx. Previously this dispatched the
        sync ``httpx.get`` through ``run_in_executor(None, ...)``, which
        stole from the default thread-pool that ``to_thread`` calls
        elsewhere (notably the image-stream hot path) also share. Under
        load the two sides competed for the same 40 threads and p95
        wobbled. Native async fetch removes that coupling.
        """
        try:
            async with httpx.AsyncClient(
                timeout=15,
                follow_redirects=True,
                headers=get_wechat_headers(url) or {},
            ) as client:
                resp = await client.get(url)
            resp.raise_for_status()
            html = resp.text
        except Exception:
            log.debug("WeChat httpx fetch failed: %s", url, exc_info=True)
            return ParseResult(content="", images={}, title="")

        doc = lxml.html.fromstring(html)

        content_html = ""
        title = ""
        img_urls: list[str] = []

        image_content = doc.get_element_by_id("js_image_content", None)
        js_content = doc.get_element_by_id("js_content", None)

        if image_content is not None:
            title_el = image_content.find('.//h1[@class]')
            if title_el is not None:
                title = (title_el.text_content() or "").strip()
            desc_el = doc.get_element_by_id("js_image_desc", None)
            if desc_el is not None:
                content_html = lxml.html.tostring(desc_el, encoding="unicode")
                img_urls = extract_datasrc_urls(desc_el)
            for img in image_content.findall('.//div[@class]//img[@data-src]'):
                src = img.get("data-src", "")
                if src and src not in img_urls and not src.startswith("data:"):
                    img_urls.insert(0, src)
                    break

        elif js_content is not None:
            title = extract_wechat_title(doc)
            content_html = lxml.html.tostring(js_content, encoding="unicode")
            img_urls = extract_datasrc_urls(js_content)

        if not content_html:
            return ParseResult(content="", images={}, title="")

        wrapped = f"<html><body>{content_html}</body></html>"
        text = extract_with_trafilatura(wrapped)

        if not title:
            title = extract_html_title(html, url)

        return ParseResult(
            content=text, images={}, title=title,
            image_urls=img_urls[:20],
        )

    async def _parse_with_playwright(self, url: str) -> ParseResult:
        try:
            from parsers.url._browser import parse_session
        except ImportError:
            return ParseResult(content="", images={}, title="")

        # Browser pool: one Chromium per worker process, fresh context
        # per call. ``block_heavy=False`` — WeChat pages sometimes
        # delay #js_content hydration until stylesheet / image assets
        # resolve, and the raw-HTML fast path is what we save Chromium
        # for anyway (this slow path is the edge case).
        async with parse_session(user_agent=WECHAT_UA, block_heavy=False) as ctx:
            page = await ctx.new_page()
            await page.goto(url, wait_until="networkidle", timeout=30000)

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

            article_info = await page.evaluate("""
                () => {
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
                    const contentEl = document.querySelector('#js_content');
                    if (contentEl) {
                        const titleEl = document.querySelector('#activity-name');
                        return {
                            type: 'article',
                            title: titleEl ? titleEl.textContent.trim() : '',
                            contentHtml: contentEl.innerHTML,
                        };
                    }
                    return {
                        type: 'generic',
                        title: document.title || '',
                        contentHtml: document.body.innerHTML,
                    };
                }
            """)

            title = article_info.get("title") or ""
            content_html = article_info.get("contentHtml") or ""
            article_type = article_info.get("type", "article")

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
                title = extract_html_title(html, url)

            # Context closes automatically at async-with exit.

        wrapped = f"<html><body>{content_html}</body></html>"
        text = extract_with_trafilatura(wrapped)

        return ParseResult(
            content=text, images={}, title=title,
            image_urls=img_urls[:20],
        )
