"""baike.baidu.com handler.

baike gates on the TLS/JA3 fingerprint, so we fetch via curl_cffi
(parsers.url._fetch). Article body is server-rendered in the HTML.
On total fetch failure, fall back to the public lemma-card API (summary
only), then to a permanent AntiBotBlockedError if even that fails.
"""
from __future__ import annotations

import logging
from urllib.parse import urlparse

from parsers.base import AntiBotBlockedError, ParseResult
from parsers.url._fetch import fetch_impersonated

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
        # filled in T4
        return ParseResult(content="", title="")

    @staticmethod
    async def _openapi_fallback(url: str) -> "ParseResult | None":
        # filled in T6
        return None
