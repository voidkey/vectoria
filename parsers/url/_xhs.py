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

Parse selectors are based on publicly visible DOM at time of writing —
xhs ships UI refactors often enough that this may drift. The handler
fails open: empty content is returned rather than raised, so ingest
completes with a bare title and the upstream caller can still store
the ``image_urls`` for downstream consumption.

Known limitation (2026-04-21 smoke): on shared-link URLs with
``xsec_token`` query args, body extraction returns empty even with the
expanded selector set and ``og:description`` fallback. Title, figures,
and phash all still populate correctly. Fixing the body path requires a
live DOM sample from the current xhs build — tracked, not blocking. If
downstream consumers need the note body (as opposed to title + images)
the handler will need attention.
"""
from __future__ import annotations

import logging
from urllib.parse import urlparse

from config import get_settings
from infra.metrics import URL_IMAGES_TRUNCATED_TOTAL
from parsers.base import ParseResult

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


class XhsHandler:
    def match(self, url: str) -> bool:
        return is_xhs_url(url)

    def download_headers(self, url: str) -> dict[str, str] | None:
        return get_xhs_headers(url)

    def canonicalize_image_url(self, url: str) -> str:
        return canonicalize_xhs_image_url(url)

    async def parse(self, url: str) -> ParseResult:
        """Render the note via Playwright and extract title + text + image URLs.

        Selectors here are based on the current public DOM layout and
        may drift over time — the handler fails open (returns whatever
        was extractable, even if partial) rather than raising, because
        an empty ParseResult at least preserves the ``image_urls`` and
        lets the upstream caller decide next steps.
        """
        try:
            from parsers.url._browser import parse_session
        except ImportError:
            log.warning("playwright not installed; xhs parse returning empty")
            return ParseResult(content="", title="")

        try:
            # Browser pool: single Chromium per worker, fresh context
            # per URL. block_heavy drops image / font / media requests
            # at the network layer — we only need DOM for extraction,
            # and xhs feeds dozens of image assets to each note.
            async with parse_session(user_agent=XHS_UA, block_heavy=True) as ctx:
                page = await ctx.new_page()
                # networkidle is brittle on SPAs; 30 s cap then
                # proceed regardless — selectors below tolerate a
                # partially-hydrated page.
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(2000)  # soft hydration wait

                cap = get_settings().url_image_cap
                js = """
                        () => {
                            const pickText = sels => {
                                for (const s of sels) {
                                    const el = document.querySelector(s);
                                    if (el && el.textContent.trim()) {
                                        return el.textContent.trim();
                                    }
                                }
                                return "";
                            };
                            // Title: note-detail title, generic h1, or og tag.
                            const title = pickText([
                                '#detail-title',
                                '.title',
                                'h1',
                            ]) || (document.querySelector('meta[property="og:title"]')?.content ?? '');

                            // Body: try several known-good selectors, then
                            // a broader attribute match for any xhs-layout
                            // shift that moves ``note-content`` / ``note-text``
                            // onto a new wrapper. og:description is a
                            // last-resort fallback that survives most
                            // refactors because the SSR meta tag is generic.
                            let body = pickText([
                                '#detail-desc',
                                '.note-content .desc',
                                '.note-detail-content',
                                '[class*="note-text"]',
                                '[class*="noteContent"]',
                                '[class*="note-content"]',
                                '.content',
                                'article',
                            ]);
                            if (!body) {
                                body = document.querySelector(
                                    'meta[property="og:description"]'
                                )?.content || '';
                            }

                            // Images: hero carousel + inline. Cap matches
                            // ``settings.url_image_cap`` (injected from Python).
                            // Exclude avatars / qrcodes / icons — they live on
                            // sns-avatar-qc.* or carry those words in
                            // the URL, and only add noise to vision /
                            // phash downstream.
                            const imgs = Array.from(
                                document.querySelectorAll('img')
                            )
                              .map(i => i.getAttribute('data-src') || i.src)
                              .filter(s => s && !s.startsWith('data:'))
                              .filter(s => /xhscdn\\.com|xiaohongshu\\.com/.test(s))
                              .filter(s => !/avatar|qrcode|icon/i.test(s));
                            const seen = new Set();
                            const uniq = [];
                            for (const u of imgs) {
                                if (!seen.has(u)) { seen.add(u); uniq.push(u); }
                                if (uniq.length >= __IMAGE_CAP__) break;
                            }
                            return { title, body, imgs: uniq };
                        }
                        """.replace("__IMAGE_CAP__", str(cap))
                extract = await page.evaluate(js)
                # Context closes at async-with exit.
        except Exception:
            log.exception("xhs playwright parse failed for %s", url)
            return ParseResult(content="", title="")

        title = (extract.get("title") or "").strip()
        body = (extract.get("body") or "").strip()
        raw_imgs = list(extract.get("imgs") or [])
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
