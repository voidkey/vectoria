"""baike.baidu.com handler.

baike gates on the TLS/JA3 fingerprint, so we fetch via curl_cffi
(parsers.url._fetch). Article body is server-rendered in the HTML.
On total fetch failure, fall back to the public lemma-card API (summary
only), then to a permanent AntiBotBlockedError if even that fails.
"""
from __future__ import annotations

import logging
import re
from urllib.parse import unquote, urlparse

import httpx
from lxml import html as _lh

from parsers.base import AntiBotBlockedError, ParseResult
from parsers.url._fetch import fetch_impersonated
from parsers.url._handlers import _visible_text, extract_html_title, extract_image_urls


# Public appid used by baike's embeddable lemma-card widget (not a private key).
_LEMMA_CARD_API = "https://baike.baidu.com/api/openapi/BaikeLemmaCardApi"
_LEMMA_CARD_APPID = "379020"


def _url_lemma_id(url: str) -> int | None:
    m = re.search(r"/item/[^/]+/(\d+)", url)
    return int(m.group(1)) if m else None


def _url_lemma_key(url: str) -> str:
    m = re.search(r"/item/([^/?#]+)", url)
    return unquote(m.group(1)) if m else ""


async def _baike_lemma_card(key: str) -> dict | None:
    """One shot, no retry (weak fallback). Returns parsed JSON dict or None."""
    params = {"scope": "103", "format": "json", "appid": _LEMMA_CARD_APPID,
              "bk_length": "600", "bk_key": key}
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(_LEMMA_CARD_API, params=params)
        return r.json()
    except Exception:
        return None


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
            # Primary path: target article paragraph nodes.  Baike uses CSS
            # modules whose class names are "<semantic>_<hash>" (e.g.
            # "para_SkYG9").  Matching on the hash-free prefix "para_" captures
            # all body-paragraph variants regardless of webpack rebuild.
            # False-positive risk is low: sibling prefixes (paraTitle_*,
            # paraList_*, paragraph_*) all have a letter immediately after
            # "para", not "_", so they are not matched.
            # NOTE: paraTitle_* section headers (形态特征/生活习性/...) are
            # intentionally excluded in P1 — body prose only.  Capturing
            # structured headers requires cleaning up embedded 播报/编辑
            # control-text and is deferred to a later enhancement.
            # Catalog nodes (catalogWrapper_*, catalog_*) live in a separate
            # DOM branch and are also not matched.
            nodes = tree.xpath('//*[contains(@class,"para_")]')
            parts = [(n.text_content() or "").strip() for n in nodes]
            body = "\n".join(p for p in parts if p)
        except Exception:
            body = ""
        if len(body) < 200:
            # Fallback: strip all tags (reuse shared _visible_text) and denoise.
            # The denoise replacements below can re-introduce multi-space runs,
            # so the final whitespace collapse is intentional, not redundant.
            text = _visible_text(html)
            text = re.sub(r"目录\s*(?:\d+\s+\S+\s*)+", " ", text)
            text = text.replace("播报", " ").replace("编辑", " ")
            body = re.sub(r"\s+", " ", text).strip()
        return ParseResult(content=body, title=title, image_urls=img_urls)

    @staticmethod
    async def _openapi_fallback(url: str) -> "ParseResult | None":
        key = _url_lemma_key(url)
        if not key:
            return None
        card = await _baike_lemma_card(key)
        if not card or "errno" in card:
            return None
        url_id = _url_lemma_id(url)
        if url_id is not None and card.get("newLemmaId") != url_id:
            logger.info("baike openapi lemma mismatch url_id=%s card=%s; rejecting",
                        url_id, card.get("newLemmaId"))
            return None
        abstract = (card.get("abstract") or "").strip()
        if not abstract:
            return None
        title = (card.get("title") or key).strip()
        desc = (card.get("desc") or "").strip()
        content = (desc + "\n\n" + abstract).strip() if desc else abstract
        img = card.get("image")
        return ParseResult(content=content, title=title,
                           image_urls=[img] if img else None)
