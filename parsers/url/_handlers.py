from __future__ import annotations

import re
from typing import Protocol, runtime_checkable
from urllib.parse import urljoin

import httpx
import trafilatura

from parsers.base import ParseResult

_IMG_TAG = re.compile(r'<img[^>]+(?:data-src|src)=["\']([^"\']+)["\']', re.IGNORECASE)

_JS_CHALLENGE_MARKERS = (
    "javascript is disabled",
    "enable javascript",
    "just a moment",
    "please enable cookies",
    "please turn on javascript",
    "javascript required",
)


@runtime_checkable
class SiteHandler(Protocol):
    def match(self, url: str) -> bool: ...
    async def parse(self, url: str) -> ParseResult: ...
    def download_headers(self, url: str) -> dict[str, str] | None: ...


_handlers: list[SiteHandler] = []


def register_handler(handler: SiteHandler) -> None:
    _handlers.append(handler)


def find_handler(url: str) -> SiteHandler | None:
    for h in _handlers:
        if h.match(url):
            return h
    return None


def extract_image_urls(html: str, base_url: str) -> list[str]:
    """Extract image URLs from HTML, resolve relative URLs, cap at 20."""
    urls: list[str] = []
    for src in _IMG_TAG.findall(html):
        abs_url = urljoin(base_url, src)
        if not abs_url.startswith("data:"):
            urls.append(abs_url)
        if len(urls) >= 20:
            break
    return urls


def extract_with_trafilatura(html: str) -> str:
    """Extract main text content from HTML as markdown via trafilatura."""
    text = trafilatura.extract(
        html,
        include_images=True,
        include_links=False,
        output_format="markdown",
    ) or ""

    if not text.strip():
        from trafilatura.utils import load_html
        from trafilatura.core import baseline
        tree = load_html(html)
        if tree is not None:
            _, raw_text, _ = baseline(tree)
            text = raw_text or ""

    return text


def needs_browser_fallback(result: ParseResult) -> bool:
    """httpx returned nothing useful — likely a JS-challenge or pure SPA page."""
    content = (result.content or "").strip()
    if len(content) < 300:
        return True
    lower = content[:2000].lower()
    return any(m in lower for m in _JS_CHALLENGE_MARKERS)


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
