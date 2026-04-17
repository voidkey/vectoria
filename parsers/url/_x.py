"""X / Twitter site handler.

Uses the public syndication API (same as embed widgets) to fetch tweet
content without authentication.
"""
from __future__ import annotations

import re
from urllib.parse import urlparse

import httpx

from parsers.base import ParseResult

_X_HOSTS = {"x.com", "twitter.com"}
_X_STATUS_RE = re.compile(r"/status(?:es)?/(\d+)")


def _extract_tweet_id(url: str) -> str | None:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if not any(host == d or host.endswith("." + d) for d in _X_HOSTS):
        return None
    m = _X_STATUS_RE.search(parsed.path)
    return m.group(1) if m else None


class XHandler:
    def match(self, url: str) -> bool:
        return _extract_tweet_id(url) is not None

    def download_headers(self, url: str) -> dict[str, str] | None:
        return None

    async def parse(self, url: str) -> ParseResult:
        tweet_id = _extract_tweet_id(url)
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
            title=title or url,
            image_urls=img_urls[:20],
        )
