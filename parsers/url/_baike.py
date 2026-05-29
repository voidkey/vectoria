"""baike.baidu.com handler.

baike gates on the TLS/JA3 fingerprint, so we fetch via curl_cffi
(parsers.url._fetch). Article body is server-rendered in the HTML.
On total fetch failure, fall back to the public lemma-card API (summary
only), then to a permanent AntiBotBlockedError if even that fails.
"""
from __future__ import annotations

import logging
import re
from urllib.parse import urlparse

from lxml import html as _lh

from parsers.base import AntiBotBlockedError, ParseResult
from parsers.url._fetch import fetch_impersonated
from parsers.url._handlers import extract_html_title, extract_image_urls


def _strip_to_text(html: str) -> str:
    t = re.sub(r"(?s)<!--.*?-->", " ", html)
    t = re.sub(r"(?is)<(script|style)\b.*?</\1>", " ", t)
    t = re.sub(r"(?s)<[^>]+>", " ", t)
    return re.sub(r"\s+", " ", t).strip()

logger = logging.getLogger(__name__)


class BaikeHandler:
    def match(self, url: str) -> bool:
        return (urlparse(url).hostname or "").lower() == "baike.baidu.com"

    def download_headers(self, url: str) -> dict[str, str] | None:
        return None  # baike image CDN needs no Referer (verified)

    async def parse(self, url: str) -> ParseResult:
        html = await fetch_impersonated(url)
        if html is not None:
            result = self._extract(html, url)
            if result.content.strip() or result.image_urls:
                return result
        summary = await self._openapi_fallback(url)
        if summary is not None:
            return summary
        raise AntiBotBlockedError(f"baike fetch failed (anti-bot) at {url}")

    def _extract(self, html: str, url: str) -> ParseResult:
        title = extract_html_title(html, url).removesuffix("_百度百科").strip()
        img_urls = extract_image_urls(html, url)
        body = ""
        try:
            tree = _lh.fromstring(html)
            # Primary path: target article paragraph nodes (para_SkYG9 is the
            # server-rendered body class; covers summary, content, and list
            # paragraphs).  Catalog nodes (catalogWrapper_*, catalog_*) live
            # in a separate DOM branch and are not included in para_ results.
            nodes = tree.xpath('//*[contains(@class,"para_SkYG9")]')
            parts = [(n.text_content() or "").strip() for n in nodes]
            body = "\n".join(p for p in parts if p)
        except Exception:
            body = ""
        if len(body) < 200:
            # Fallback: strip all tags and denoise
            text = _strip_to_text(html)
            text = re.sub(r"目录\s*(?:\d+\s+\S+\s*)+", " ", text)
            text = text.replace("播报", " ").replace("编辑", " ")
            body = re.sub(r"\s+", " ", text).strip()
        return ParseResult(content=body, title=title, image_urls=img_urls)

    @staticmethod
    async def _openapi_fallback(url: str) -> "ParseResult | None":
        # filled in T6
        return None
