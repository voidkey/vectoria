"""Xiaohongshu (小红书) site handler.

Xiaohongshu note pages are a single-page React app: the initial HTML
response is an empty shell, content arrives via JS after DOMContentLoaded.
There's no server-side-rendered path we can scrape with httpx, so this
handler always goes through Playwright.

Image CDN (xhscdn.com) quirks addressed here:
  * The CDN returns WebP by default when the client advertises support.
    ``canonicalize_image_url`` rewrites ``format/webp`` → ``format/jpg``
    for the same downstream-compatibility reason as WeChat.
  * Referer is checked on some asset paths; we set it to the note's
    own article URL via ``download_headers``.

Extraction strategy (2026-05-25 live capture):
  xhs ships UI refactors often. The current video-note layout uses a
  ``reds-text`` class system under ``.new-note-view-container``, with
  the note body in ``.author-desc``; og:title / og:description are
  written empty for video posts. We therefore parse the post-hydration
  HTML returned by ``page.content()`` in this priority:

    Title  : <title> (strip " - 小红书") → og:title → #detail-title → <h1>
    Body   : <meta name="description"> → .author-desc (new DOM) →
             #detail-desc (legacy image-note) → og:description
    Images : <img data-src|src> filtered to xhs CDN hosts, avatars and
             icon assets dropped, deduped, capped at url_image_cap

Title and body fall through together so a layout drift on one selector
doesn't kill the whole extract — empty-result still goes through the
``allow_image_only`` branch downstream.
"""
from __future__ import annotations

import logging
import re
from urllib.parse import parse_qs, urlparse

import lxml.html

from config import get_settings
from infra.metrics import URL_IMAGES_TRUNCATED_TOTAL
from parsers.base import AntiBotBlockedError, ParseResult
from parsers.url._blacklist import UnparseableUrlError
from parsers.url._handlers import detect_block_reason

log = logging.getLogger(__name__)

# xhslink.com is the shortener; xiaohongshu.com is the canonical domain.
# We match both; Playwright's goto() will follow the xhslink redirect.
_XHS_HOSTS = ("xiaohongshu.com", "xhslink.com")

# Image CDN host — note subdomains like ``sns-img-qc.xhscdn.com``,
# ``ci.xiaohongshu.com``, ``sns-img-bd.xhscdn.com`` all serve images.
_XHS_IMG_HOST_SUFFIXES = ("xhscdn.com", "xiaohongshu.com")

# Mobile UA used for the article fetch. Mobile renders a simpler DOM
# (less client-side dynamic composition) so selectors stay stable.
XHS_UA = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 "
    "Mobile/15E148 Safari/604.1"
)


def _is_xhs_article_host(host: str | None) -> bool:
    if not host:
        return False
    host = host.lower()
    return any(host == d or host.endswith("." + d) for d in _XHS_HOSTS)


def _is_xhs_image_host(host: str | None) -> bool:
    if not host:
        return False
    host = host.lower()
    return any(host == s or host.endswith("." + s) for s in _XHS_IMG_HOST_SUFFIXES)


def is_xhs_url(url: str) -> bool:
    return _is_xhs_article_host(urlparse(url).hostname)


def is_xhs_video_note(url: str) -> bool:
    """True when a fully-resolved xhs URL points to a video note.

    xhs share links encode the medium in the ``type`` query arg
    (``type=video`` for video posts, absent or other values for image
    notes). Video notes carry only a hashtag-heavy caption — the actual
    content lives in the video itself, which we can't currently ingest
    (no ASR / frame OCR pipeline yet). Until that lands we'd rather skip
    these cleanly than land them as ``image_only`` docs with title
    "xhslink.com" and noise images.

    Match the ``type`` param exactly so substrings like ``xsec_source``
    can't false-trigger.
    """
    try:
        qs = parse_qs(urlparse(url).query)
    except Exception:
        return False
    return qs.get("type", [""])[0] == "video"


def canonicalize_xhs_image_url(url: str) -> str:
    """Force JPEG delivery from xhs image CDN.

    The xhs CDN uses Qiniu-style ``?imageView2/...`` directives where
    ``format/webp`` is the default modern browsers trigger. Swapping
    to ``format/jpg`` keeps the bytes decodable by any stock
    JPEG/PIL/vision pipeline without reaching for a WebP codec.

    No-op for:
      - hosts that aren't xhs CDNs (e.g. the article host itself)
      - URLs that already pin a non-webp format
      - malformed URLs that urlparse cannot handle
    """
    try:
        parsed = urlparse(url)
    except Exception:
        return url
    if not _is_xhs_image_host(parsed.hostname):
        return url

    # Qiniu-style directives live inside the query string as slash-
    # separated tokens, not standard k=v pairs. Rewrite only the
    # format/webp occurrence; leave everything else (mode, width,
    # quality, watermark directives) untouched so signed URLs keep
    # verifying.
    if "format/webp" in parsed.query:
        new_query = parsed.query.replace("format/webp", "format/jpg")
        return parsed._replace(query=new_query).geturl()
    return url


def get_xhs_headers(article_url: str) -> dict[str, str] | None:
    """Headers to send when fetching images referenced by an xhs note.

    Referer is set to the note's own URL — most xhs CDN paths tolerate
    missing Referer, but hotlink protection is occasionally enabled.
    Matching ours to the caller's own page avoids all ambiguity.
    """
    if not is_xhs_url(article_url):
        return None
    return {"Referer": article_url, "User-Agent": XHS_UA}


# Match xhs CDN hosts in image src/data-src URLs.
_XHS_IMG_URL_RE = re.compile(r"xhscdn\.com|xiaohongshu\.com")
# Avatars / qrcodes / icon assets carry these tokens in the URL and add
# only noise downstream (vision, phash); drop them.
_NOISE_IMG_RE = re.compile(r"avatar|qrcode|icon", re.IGNORECASE)
# xhs appends " - 小红书" to every page <title>. Strip the suffix.
_XHS_TITLE_SUFFIX_RE = re.compile(r"\s*-\s*小红书\s*$")


def _text(el) -> str:
    """text_content() stripped, with None safety."""
    if el is None:
        return ""
    return (el.text_content() or "").strip()


def _meta(doc, **attrs) -> str:
    """First meta tag matching all attrs, returning its content or ''."""
    parts = [f'@{k}="{v}"' for k, v in attrs.items()]
    xp = f'//meta[{" and ".join(parts)}]/@content'
    hits = doc.xpath(xp)
    return hits[0].strip() if hits else ""


def extract_xhs_from_html(html: str, url: str, image_cap: int) -> dict:
    """Pull title + body + image URLs out of a rendered xhs note page.

    Layered fallbacks (see module docstring) protect against the
    frequent xhs DOM refactors. ``url`` is currently unused but kept in
    the signature so callers (and future selectors that need it for
    relative-URL resolution) don't need to be touched again.

    Returns a dict with keys: ``title``, ``body``, ``imgs``.
    """
    del url  # unused for now; kept for signature stability
    try:
        doc = lxml.html.fromstring(html)
    except Exception:
        return {"title": "", "body": "", "imgs": []}

    # ----- Title -------------------------------------------------------
    title = ""
    title_el = doc.find(".//title")
    if title_el is not None and title_el.text:
        title = _XHS_TITLE_SUFFIX_RE.sub("", title_el.text.strip())
    if not title:
        title = _meta(doc, property="og:title")
    if not title:
        legacy_t = doc.xpath('.//*[@id="detail-title"]')
        if legacy_t:
            title = _text(legacy_t[0])
    if not title:
        title = _text(doc.find(".//h1"))

    # ----- Body --------------------------------------------------------
    body = _meta(doc, name="description")
    if not body:
        # New video-note DOM: note caption lives in .author-desc. Match
        # only the exact class token to avoid sweeping in .author-desc-wrapper
        # or .comment-text-box siblings.
        author_descs = doc.xpath(
            './/*[contains(concat(" ", normalize-space(@class), " "), " author-desc ")]'
        )
        for el in author_descs:
            # Drop UI hints like "展开" (the in-line trigger) before
            # extracting text. Setting .text='' (instead of removing the
            # element) preserves the trigger's tail — i.e. the note body
            # that follows the closing </i> — which Element.remove would
            # also delete.
            for trigger in el.xpath('.//*[contains(@class, "author-desc-trigger")]'):
                trigger.text = ""
            t = _text(el)
            if t:
                body = t
                break
    if not body:
        # Legacy image-note layout
        legacy = doc.xpath('.//*[@id="detail-desc"]')
        if legacy:
            body = _text(legacy[0])
    if not body:
        body = _meta(doc, property="og:description")

    # ----- Images ------------------------------------------------------
    imgs: list[str] = []
    seen: set[str] = set()
    for img in doc.iter("img"):
        src = img.get("data-src") or img.get("src") or ""
        if not src or src.startswith("data:"):
            continue
        if not _XHS_IMG_URL_RE.search(src):
            continue
        if _NOISE_IMG_RE.search(src):
            continue
        if src in seen:
            continue
        seen.add(src)
        imgs.append(src)
        if len(imgs) >= image_cap:
            break

    return {"title": title, "body": body, "imgs": imgs}


class XhsHandler:
    def match(self, url: str) -> bool:
        return is_xhs_url(url)

    def download_headers(self, url: str) -> dict[str, str] | None:
        return get_xhs_headers(url)

    def canonicalize_image_url(self, url: str) -> str:
        return canonicalize_xhs_image_url(url)

    async def parse(self, url: str) -> ParseResult:
        """Render the note via Playwright and extract title + text + image URLs.

        See ``extract_xhs_from_html`` for the selector priority. The
        handler fails open (empty ParseResult) on any browser-level
        error so the upstream caller can still use ``image_urls`` for
        the image-only rescue path.
        """
        try:
            from parsers.url._browser import parse_session
        except ImportError:
            log.warning("playwright not installed; xhs parse returning empty")
            return ParseResult(content="", title="")

        cap = get_settings().url_image_cap
        try:
            # Browser pool: single Chromium per worker, fresh context
            # per URL. block_heavy drops image / font / media requests
            # at the network layer — we only need DOM for extraction,
            # and xhs feeds dozens of image assets to each note.
            async with parse_session(user_agent=XHS_UA, block_heavy=True) as ctx:
                page = await ctx.new_page()
                # networkidle is brittle on SPAs; 30 s cap then
                # proceed regardless — extractor tolerates a partially
                # hydrated page via its meta-tag fallbacks.
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(2000)  # soft hydration wait
                final_url = page.url
                if is_xhs_video_note(final_url):
                    # Skip BEFORE reading content / scheduling images —
                    # video-note caption is too noisy to index, and the
                    # cover-image CDN paths 403 anyway. Worker handler
                    # marks status=failed with the message below.
                    raise UnparseableUrlError(
                        "URL pattern not supported: xiaohongshu video "
                        "note (text caption only; video content not yet "
                        "ingestable). "
                    )
                html = await page.content()
        except UnparseableUrlError:
            # PermanentParseError subclass — must propagate so the worker
            # short-circuits cleanly (no retry, no image pipeline).
            raise
        except Exception:
            log.exception("xhs playwright parse failed for %s", url)
            return ParseResult(content="", title="")

        extract = extract_xhs_from_html(html, url, cap)
        title = extract["title"]
        body = extract["body"]

        reason = detect_block_reason(html, title)
        if reason:
            raise AntiBotBlockedError(f"{reason} at {url}")

        raw_imgs = extract["imgs"]
        if len(raw_imgs) > cap:
            URL_IMAGES_TRUNCATED_TOTAL.labels(handler="xhs").inc()
        img_urls = raw_imgs[:cap]

        content = f"# {title}\n\n{body}" if title and body else (body or title)

        return ParseResult(
            content=content,
            title=title or urlparse(url).netloc,
            image_urls=img_urls,
            allow_image_only=True,
        )
