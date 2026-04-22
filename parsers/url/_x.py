"""X / Twitter site handler.

Uses the public syndication API (same as embed widgets) to fetch tweet
content without authentication.
"""
from __future__ import annotations

import re
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import httpx

from parsers.base import ParseResult

_X_HOSTS = {"x.com", "twitter.com"}
_X_STATUS_RE = re.compile(r"/status(?:es)?/(\d+)")

# Twitter image CDN. ``?name=<size>`` picks the variant (``thumb``,
# ``small``, ``medium``, ``large``, ``orig``). For ingest we want
# ``orig`` — full resolution for vision models and embeddings.
_X_IMG_HOST = "pbs.twimg.com"

_X_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
)


def _extract_tweet_id(url: str) -> str | None:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if not any(host == d or host.endswith("." + d) for d in _X_HOSTS):
        return None
    m = _X_STATUS_RE.search(parsed.path)
    return m.group(1) if m else None


def canonicalize_x_image_url(url: str) -> str:
    """Rewrite a pbs.twimg.com image URL to its original-resolution JPEG.

    Twitter serves downsized variants by default (``small``, ``medium``,
    ``large``). Ingest should hit ``orig`` instead — vision models and
    perceptual-hash dedup both want the full-resolution frame, and
    ``name=orig`` is a stable, documented parameter.

    Adds ``format=jpg`` when the URL declares no format — the CDN
    defaults otherwise depend on the ``Accept`` header and can land on
    WebP for some clients.

    No-op for non-twimg hosts; tolerant of malformed URLs.
    """
    try:
        parsed = urlparse(url)
    except Exception:
        return url
    if (parsed.hostname or "").lower() != _X_IMG_HOST:
        return url

    q = dict(parse_qsl(parsed.query, keep_blank_values=True))
    q["name"] = "orig"
    q.setdefault("format", "jpg")
    return urlunparse(parsed._replace(query=urlencode(q)))


def get_x_headers(article_url: str) -> dict[str, str] | None:
    """Headers for downloading images referenced by a tweet.

    pbs.twimg.com serves most images publicly but desktop-browser UA
    and twitter.com Referer are cheap and future-proof against any
    hotlink enforcement rollout.
    """
    if not _extract_tweet_id(article_url):
        return None
    return {"User-Agent": _X_UA, "Referer": "https://twitter.com/"}


class XHandler:
    def match(self, url: str) -> bool:
        return _extract_tweet_id(url) is not None

    def download_headers(self, url: str) -> dict[str, str] | None:
        return get_x_headers(url)

    def canonicalize_image_url(self, url: str) -> str:
        return canonicalize_x_image_url(url)

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
            return ParseResult(content="", title="")

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
                        title=title or url,
            image_urls=img_urls[:20],
        )
