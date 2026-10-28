"""medium.com handler.

Medium sits behind Cloudflare, which hard-blocks (HTTP 403 "Attention
Required") whole datacenter IP ranges — no amount of waiting in a browser
clears it. Medium's public RSS feeds are served without that challenge and
carry the full post HTML in ``content:encoded``, so an article URL is
resolved by locating its post id in the author's / publication's feed.

Feeds only list the ~10 most recent posts. Anything not found there (older
posts, ``/p/<id>`` links, the bare homepage) falls through to the generic
fetch path, which still works from networks Medium doesn't block.
"""
from __future__ import annotations

import logging
import re
from urllib.parse import urlparse

from lxml import etree

from parsers.base import AntiBotBlockedError, ParseResult
from parsers.url._generic import GenericHandler
from parsers.url._handlers import _visible_text, extract_image_urls, extract_with_trafilatura

logger = logging.getLogger(__name__)

_CONTENT_NS = "http://purl.org/rss/1.0/modules/content/"

# Post ids are a hex hash, either the whole last segment (/p/<id>) or the
# tail of the slug (/my-title-<id>).
_POST_ID = re.compile(r"(?:^|-)([0-9a-f]{8,16})$")

# First path segments on medium.com that are site pages, not publications.
_RESERVED_SEGMENTS = frozenset({
    "p", "m", "me", "search", "tag", "topic", "topics", "plans", "membership",
    "about", "creators", "jobs-at-medium", "policy", "new-story", "signin",
})

# *.medium.com hosts that are infrastructure, not user subdomains.
_NON_USER_SUBDOMAINS = frozenset({"miro", "help", "policy", "link", "blog"})

_XML_PARSER = etree.XMLParser(resolve_entities=False, no_network=True)


def _is_medium_host(host: str) -> bool:
    if host in ("medium.com", "www.medium.com"):
        return True
    if not host.endswith(".medium.com"):
        return False
    sub = host.removesuffix(".medium.com")
    return "." not in sub and sub not in _NON_USER_SUBDOMAINS and not sub.startswith("cdn-")


def _post_id(segments: list[str]) -> str | None:
    if not segments:
        return None
    m = _POST_ID.search(segments[-1].lower())
    return m.group(1) if m else None


def feed_and_post(url: str) -> tuple[str | None, str | None]:
    """Map a Medium URL to ``(feed_url, post_id)``; either may be None."""
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    segments = [s for s in parsed.path.split("/") if s]

    if host not in ("medium.com", "www.medium.com"):
        # <user>.medium.com/<slug>-<id>
        return f"https://{host}/feed", _post_id(segments)

    if not segments:
        return None, None
    first = segments[0]
    if first.startswith("@"):
        feed = f"https://medium.com/feed/{first}"
    elif first.lower() in _RESERVED_SEGMENTS:
        feed = None
    else:
        feed = f"https://medium.com/feed/{first}"
    post = _post_id(segments) if len(segments) >= 2 else None
    return feed, post


async def _fetch_feed(feed_url: str) -> etree._Element | None:
    from parsers.url._http import fetch_capped, make_async_client
    try:
        async with make_async_client() as c:
            _resp, body = await fetch_capped(c, feed_url)
        return etree.fromstring(body, _XML_PARSER)
    except Exception as exc:
        logger.info("medium feed fetch failed for %s: %s", feed_url, exc)
        return None


def _find_item(root: etree._Element, post_id: str) -> etree._Element | None:
    for item in root.iter("item"):
        guid = (item.findtext("guid") or "").strip()
        if guid.rstrip("/").endswith(f"/p/{post_id}"):
            return item
        link = urlparse(item.findtext("link") or "").path
        if link.lower().endswith(post_id):
            return item
    return None


def _item_to_result(item: etree._Element) -> ParseResult:
    body_html = item.findtext(f"{{{_CONTENT_NS}}}encoded") or ""
    link = (item.findtext("link") or "").strip()
    # content:encoded is a bare fragment; give trafilatura a document shell.
    text = extract_with_trafilatura(f"<html><body><article>{body_html}</article></body></html>")
    if not text.strip():
        text = _visible_text(body_html)
    return ParseResult(
        content=text,
        title=(item.findtext("title") or "").strip(),
        image_urls=extract_image_urls(body_html, link),
    )


class MediumHandler:
    # Medium's block is per-IP, but the feed path below is unaffected by it.
    # Tripping the domain cooldown on a fallback block would skip that feed
    # path for every other Medium URL for 15 min.
    trips_cooldown = False

    def __init__(self) -> None:
        self._generic = GenericHandler()

    def match(self, url: str) -> bool:
        return _is_medium_host((urlparse(url).hostname or "").lower())

    def download_headers(self, url: str) -> dict[str, str] | None:
        return None

    async def parse(self, url: str) -> ParseResult:
        feed_url, post_id = feed_and_post(url)
        if feed_url and post_id:
            root = await _fetch_feed(feed_url)
            if root is not None:
                item = _find_item(root, post_id)
                if item is not None:
                    result = _item_to_result(item)
                    if result.content.strip():
                        return result
        try:
            return await self._generic.parse(url)
        except AntiBotBlockedError as exc:
            raise AntiBotBlockedError(
                f"{exc} (Medium RSS feeds only carry the latest posts; "
                "this one was not found there)"
            ) from exc
