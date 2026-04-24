from __future__ import annotations

import asyncio
import logging
import re
from typing import Protocol, runtime_checkable
from urllib.parse import urljoin, urlparse

import httpx
import trafilatura

from config import get_settings
from infra.metrics import URL_IMAGES_TRUNCATED_TOTAL
from infra.ratelimit import acquire as rl_acquire
from parsers.base import ParseResult

logger = logging.getLogger(__name__)

_IMG_TAG = re.compile(r'<img[^>]+(?:data-src|src)=["\']([^"\']+)["\']', re.IGNORECASE)

# Image CDN suffix → (rate, per_seconds). Checked via host.endswith so
# subdomains fall under the same bucket — e.g. ``qpic.cn`` matches
# both ``mmbiz.qpic.cn`` (WeChat Mp) and ``mmecoa.qpic.cn`` (newer
# WeChat variant that shipped in early 2024).
# Buckets are global: the rate limiter shares them across every worker
# pointing at the same Redis.
#
# Numbers are conservative guesses from public reports of platform
# ban thresholds; tune when real blocked-rate metrics come in.
#
# Ordering doesn't matter for the current suffix check — only one
# entry can match a given host under strict suffix rules. If we ever
# need overlapping rules, switch to longest-prefix selection here.
_DOMAIN_RATES: tuple[tuple[str, int, int], ...] = (
    ("qpic.cn",        10, 1),   # WeChat Mp article CDN (mmbiz / mmecoa / ...)
    ("xhscdn.com",      3, 1),   # Xiaohongshu image CDN (strict ban risk)
    ("xiaohongshu.com", 3, 1),   # Xiaohongshu static assets (picasso-static)
    ("sinaimg.cn",      5, 1),   # Weibo
    ("pbs.twimg.com",   2, 1),   # Twitter / X media
    ("pic-bj.bcebos.com", 5, 1), # Baidu BCE storage sometimes used by zhihu
    ("zhimg.com",       5, 1),   # Zhihu image CDN
)

# Anything that doesn't match a specific CDN uses this. 10/s is loose
# enough for well-behaved public sites (news, blogs) and tight enough
# that a buggy batch of 1000 URLs can't fire 1000 requests in a burst.
_DEFAULT_IMAGE_RATE = (10, 1)


def _rate_for_host(host: str) -> tuple[int, int]:
    """Return (rate, per_seconds) for an image CDN host."""
    for suffix, rate, per in _DOMAIN_RATES:
        if host == suffix or host.endswith("." + suffix):
            return (rate, per)
    return _DEFAULT_IMAGE_RATE


async def _gate(url: str, *, retries: int = 2) -> bool:
    """Wait-or-give-up wrapper around the distributed rate limiter.

    Tries up to ``retries`` extra times with linear back-off when
    blocked. Returns True when a token was acquired, False when we
    should give up and skip this image.
    """
    host = urlparse(url).hostname or ""
    if not host:
        return False
    rate, per = _rate_for_host(host)
    for attempt in range(retries + 1):
        if await rl_acquire(host, rate=rate, per_seconds=per):
            return True
        if attempt < retries:
            await asyncio.sleep(0.25 * (attempt + 1))
    logger.info(
        "rate limit exhausted for %s after %d retries; skipping image",
        host, retries,
    )
    return False

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
    # ``canonicalize_image_url`` is **optional**. Handlers that want
    # tighter control over the exact URL we fetch (force best-quality
    # variant, strip watermark params, swap a thumbnail prefix for the
    # original, etc.) implement it; callers use ``canonicalize_via``
    # below which tolerates its absence. Keeping it off the Protocol
    # means handlers that don't care don't need to carry a no-op.


def canonicalize_via(handler: SiteHandler | None, url: str) -> str:
    """Apply a handler's image-URL canonicalization if it declared one.

    Returns the original URL unchanged when the handler has no
    ``canonicalize_image_url`` method or when no handler is supplied.
    """
    if handler is None:
        return url
    fn = getattr(handler, "canonicalize_image_url", None)
    if fn is None:
        return url
    try:
        return fn(url)
    except Exception:
        # Canonicalization is opportunistic — never let a handler bug
        # bring down the whole image-download batch.
        logger.exception("canonicalize_image_url raised for %s; using original", url)
        return url


_handlers: list[SiteHandler] = []


def register_handler(handler: SiteHandler) -> None:
    _handlers.append(handler)


def find_handler(url: str) -> SiteHandler | None:
    for h in _handlers:
        if h.match(url):
            return h
    return None


def extract_image_urls(html: str, base_url: str) -> list[str]:
    """Extract image URLs from HTML, resolve relative URLs, cap at settings.url_image_cap.

    Generic-handler path. Emits URL_IMAGES_TRUNCATED_TOTAL{handler="generic"}
    once if the cap is hit (not once per dropped image).
    """
    cap = get_settings().url_image_cap
    urls: list[str] = []
    truncated = False
    for src in _IMG_TAG.findall(html):
        abs_url = urljoin(base_url, src)
        if not abs_url.startswith("data:"):
            urls.append(abs_url)
        if len(urls) >= cap:
            truncated = True
            break
    if truncated:
        URL_IMAGES_TRUNCATED_TOTAL.labels(handler="generic").inc()
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


def extract_html_title(html: str, fallback_url: str) -> str:
    """Extract <title> from HTML, falling back to the URL's hostname."""
    m = re.search(r"<title[^>]*>([^<]+)</title>", html, re.IGNORECASE)
    return m.group(1).strip() if m else urlparse(fallback_url).netloc


def needs_browser_fallback(result: ParseResult) -> bool:
    """httpx returned nothing useful — likely a JS-challenge or pure SPA page."""
    content = (result.content or "").strip()
    if len(content) < 300:
        return True
    lower = content[:2000].lower()
    return any(m in lower for m in _JS_CHALLENGE_MARKERS)


async def download_images(
    urls: list[str],
    headers: dict[str, str] | None = None,
    canonicalize: "callable | None" = None,
) -> dict[str, bytes]:
    """Async download with per-domain distributed rate limiting.

    Each URL is gated by :func:`infra.ratelimit.acquire` keyed on its
    hostname; if the bucket is empty after a brief back-off, we skip
    the image rather than block indefinitely. Buckets are shared
    across all workers connecting to the same Redis — one flat rate
    per CDN domain no matter how many pods are running.

    ``canonicalize`` is an optional ``(url) -> url`` hook that runs
    before the rate gate and fetch. Handlers use it to force a higher-
    quality variant (e.g. WeChat ``wx_fmt=jpeg``, Twitter
    ``?name=orig``). The returned dict is keyed by the *original* URL
    the caller passed in, so downstream markdown-position matching
    still works even when the fetch URL was rewritten.

    SSRF: each image URL is re-validated against the private-address
    blocklist right before fetch. Image URLs come from parsed HTML so
    an attacker embedding ``<img src="http://169.254.169.254/...">``
    would otherwise have worker fetch internal metadata. Blocked URLs
    are silently skipped (image loss is acceptable; SSRF isn't).
    """
    from api.url_validation import reresolve_and_check_ssrf
    from api.errors import AppError

    images: dict[str, bytes] = {}
    cap = get_settings().url_image_cap
    async with httpx.AsyncClient(
        timeout=10, follow_redirects=True, headers=headers or {},
    ) as client:
        for original in urls[:cap]:
            fetch_url = canonicalize(original) if canonicalize else original
            try:
                await reresolve_and_check_ssrf(fetch_url)
            except AppError as exc:
                logger.warning(
                    "image SSRF check rejected %s: %s", fetch_url, exc.detail,
                )
                continue
            if not await _gate(fetch_url):
                continue
            try:
                resp = await client.get(fetch_url)
                if resp.status_code == 200 and resp.content:
                    # Key by the URL as it appeared in markdown so
                    # ``extract_metadata_into_refs`` can still match.
                    images[original] = resp.content
            except Exception:
                logger.debug("image fetch failed for %s", fetch_url, exc_info=True)
                continue
    return images
