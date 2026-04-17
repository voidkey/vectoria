"""WeChat site handler.

WeChat embeds full article content in the initial HTML response
(inside #js_content or #js_image_desc), hidden via CSS until
client-side JS verifies the environment.  The primary path extracts
directly from raw HTML, bypassing the JS anti-bot check.  Playwright
is kept as a fallback for edge cases where raw HTML has no content.
"""
from __future__ import annotations

import asyncio
import logging
from urllib.parse import urlparse

import httpx
import lxml.html

from parsers.base import ParseResult
from parsers.url._handlers import extract_html_title, extract_with_trafilatura

log = logging.getLogger(__name__)

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


class WechatHandler:
    def match(self, url: str) -> bool:
        return is_wechat_url(url)

    def download_headers(self, url: str) -> dict[str, str] | None:
        return get_wechat_headers(url)

    async def parse(self, url: str) -> ParseResult:
        result = await asyncio.get_running_loop().run_in_executor(
            None, self._parse_raw, url,
        )
        if result.content.strip():
            return result
        log.warning("WeChat raw extraction empty, falling back to playwright: %s", url)
        return await self._parse_with_playwright(url)

    def _parse_raw(self, url: str) -> ParseResult:
        try:
            resp = httpx.get(
                url,
                timeout=15,
                follow_redirects=True,
                headers=get_wechat_headers(url) or {},
            )
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
            from playwright.async_api import async_playwright
        except ImportError:
            return ParseResult(content="", images={}, title="")

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox"],
            )
            page = await browser.new_page(user_agent=WECHAT_UA)
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

            await browser.close()

        wrapped = f"<html><body>{content_html}</body></html>"
        text = extract_with_trafilatura(wrapped)

        return ParseResult(
            content=text, images={}, title=title,
            image_urls=img_urls[:20],
        )
