"""Feishu docx URL handler.

Public 飞书 docx / wiki pages are SPAs — initial HTML is a React shell,
real content is rendered after JS executes. Bare httpx is redirected to
``accounts.feishu.cn`` by the anti-bot layer even for public docs.

Image URLs (``internal-api-drive-stream.feishu.cn/...``) carry no
signature token; they require the anonymous session cookie that the
docx page sets on first navigation. Bytes therefore have to be fetched
inside the same playwright ``BrowserContext`` and shipped back as
``image_refs`` so the worker takes the inline path and skips the
deferred ``download_and_store_images`` task (which uses bare httpx and
would 401).
"""
from __future__ import annotations

import logging
import re
from urllib.parse import urlparse

from parsers.base import ParseResult

logger = logging.getLogger(__name__)


_FEISHU_HOST_SUFFIX = ".feishu.cn"
_DOCX_PATH_PREFIXES = ("/docx/", "/wiki/")

# Doc images live on this single host; chrome assets (avatars, emoji,
# UI icons) live on ``*.feishucdn.com`` and are filtered out so the
# downstream pipeline doesn't ingest UI noise as document figures.
_FEISHU_IMG_HOST = "internal-api-drive-stream.feishu.cn"
_IMG_SRC_RE = re.compile(r'<img\b[^>]*?\bsrc=["\']([^"\']+)["\']', re.IGNORECASE)


def is_feishu_docx_url(url: str) -> bool:
    """True iff *url* is a 飞书 docx or wiki page on ``*.feishu.cn``.

    Other path prefixes (``/sheets/``, ``/drive/``, ``/file/`` ...) are
    not docx-shaped content and are explicitly rejected.
    """
    try:
        p = urlparse(url)
    except Exception:
        return False
    host = (p.hostname or "").lower()
    if not (host == "feishu.cn" or host.endswith(_FEISHU_HOST_SUFFIX)):
        return False
    return any(p.path.startswith(prefix) for prefix in _DOCX_PATH_PREFIXES)


def extract_feishu_image_urls(html: str) -> list[str]:
    """Extract doc-image URLs (``internal-api-drive-stream.feishu.cn``)
    from rendered HTML, in document order, deduped, no data: URIs.

    Cap is applied at the call site (``_parse_with_playwright``) — this
    function returns the full list so callers can emit a single
    truncation metric when the cap actually trims something.
    """
    seen: set[str] = set()
    out: list[str] = []
    for src in _IMG_SRC_RE.findall(html):
        if src.startswith("data:"):
            continue
        try:
            host = (urlparse(src).hostname or "").lower()
        except Exception:
            continue
        if host != _FEISHU_IMG_HOST:
            continue
        if src in seen:
            continue
        seen.add(src)
        out.append(src)
    return out


def replace_image_urls_with_names(
    markdown: str, urls: list[str], names: list[str],
) -> str:
    """Rewrite ``![alt](<url>)`` → ``![alt](<name>)`` for each (url, name) pair.

    URL-to-name mapping is positional: ``urls[i]`` maps to ``names[i]``.
    URLs that don't appear in ``markdown`` (trafilatura sometimes drops
    figure-caption images) are silently skipped — no stray placeholder.

    String replacement is done one URL at a time with ``str.replace`` to
    keep alt text intact. URLs are unique within a doc (extractor
    dedupes), so no risk of cross-replacement.
    """
    out = markdown
    for url, name in zip(urls, names, strict=True):
        out = out.replace(url, name)
    return out


async def download_images_in_context(
    context, urls: list[str], *, doc_url: str,
) -> dict[str, bytes]:
    """Download image bytes through the browser's APIRequestContext so
    the anonymous session cookie set on the docx page applies. Sequential —
    飞书 image CDN tolerates light bursts but parallelism gives no win
    on the 1-50 images per doc range and risks anti-abuse signals.

    Non-200 / exception → URL silently dropped from the result dict
    (image-loss is acceptable; the doc still ingests with text).
    """
    headers = {"Referer": doc_url}
    out: dict[str, bytes] = {}
    for url in urls:
        try:
            resp = await context.request.get(url, headers=headers)
        except Exception:
            logger.debug("feishu image fetch raised: %s", url, exc_info=True)
            continue
        if not resp.ok:
            logger.info("feishu image %s returned %s", url, resp.status)
            try:
                await resp.dispose()
            except Exception:
                pass
            continue
        try:
            data = await resp.body()
        except Exception:
            logger.debug("feishu image body() raised: %s", url, exc_info=True)
            continue
        if data:
            out[url] = data
    return out


_MIME_EXT = {
    "image/png": ".png",
    "image/jpeg": ".jpg",
    "image/webp": ".webp",
    "image/gif": ".gif",
}


def sniff_image_mime(data: bytes) -> str:
    """Return content-type by magic bytes. Default ``image/jpeg`` —
    feishu covers are JPEG by default and giving an extension keeps
    ``ImageRef.name`` looking sane even on malformed bytes.
    """
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if data.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    if data.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    return "image/jpeg"


def _ext_for_mime(mime: str) -> str:
    return _MIME_EXT.get(mime, ".jpg")


class FeishuHandler:
    def match(self, url: str) -> bool:
        return is_feishu_docx_url(url)

    def download_headers(self, url: str) -> dict[str, str] | None:
        # Inline image_refs path — there is no deferred httpx download
        # to inject Referer/UA into. Returning None is the documented
        # "I have no opinion" branch in the SiteHandler protocol.
        return None

    async def parse(self, url: str) -> ParseResult:
        return await self._parse_with_playwright(url)

    async def _parse_with_playwright(self, url: str) -> ParseResult:
        # Implemented in Task 7.
        return ParseResult(content="", title="")
