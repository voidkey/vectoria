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

import config
from infra.metrics import URL_IMAGES_TRUNCATED_TOTAL
from parsers.base import ParseResult
from parsers.image_ref import BytesFactory, ImageRef
from parsers.url._browser import parse_session
from parsers.url._handlers import extract_html_title, extract_with_trafilatura

logger = logging.getLogger(__name__)


_FEISHU_HOST_SUFFIX = ".feishu.cn"
_DOCX_PATH_PREFIXES = ("/docx/", "/wiki/")

# Doc images live on this single host; chrome assets (avatars, emoji,
# UI icons) live on ``*.feishucdn.com`` and are filtered out so the
# downstream pipeline doesn't ingest UI noise as document figures.
_FEISHU_IMG_HOST = "internal-api-drive-stream.feishu.cn"
_IMG_SRC_RE = re.compile(r'<img\b[^>]*?\bsrc=["\']([^"\']+)["\']', re.IGNORECASE)

_LOGIN_HOST = "accounts.feishu.cn"
# Match the desktop Chromium that the worker pool launches. Stating it
# explicitly keeps Feishu's bot detector from sniffing a stale UA when
# we eventually upgrade Chromium in the deps.
_FEISHU_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/130.0 Safari/537.36"
)


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
        # block_heavy=False — image responses set cookies the doc page
        # also relies on; aborting them sometimes leaves the BrowserContext
        # without the session cookie that image fetches in this method
        # depend on. We pay the bandwidth cost (~few MB per doc) for
        # session correctness.
        async with parse_session(
            user_agent=_FEISHU_UA, block_heavy=False,
        ) as ctx:
            page = await ctx.new_page()
            try:
                await page.goto(url, wait_until="networkidle", timeout=30000)
            except Exception:
                logger.warning("feishu page.goto failed: %s", url, exc_info=True)
                return ParseResult(content="", title="")

            # Login wall: feishu redirects unauthorized requests to
            # accounts.feishu.cn. Anything not on the original feishu.cn
            # subdomain, or now on accounts.feishu.cn, means the doc is
            # not anonymously accessible.
            current = (page.url or "").lower()
            if _LOGIN_HOST in current:
                logger.info("feishu doc requires login: %s", url)
                return ParseResult(content="", title="")

            # Trigger lazy-loaded blocks (long docs paginate images on
            # scroll). Single-shot scroll-to-bottom; networkidle above
            # already covered the initial render.
            try:
                await page.evaluate(
                    "() => window.scrollTo(0, document.body.scrollHeight)"
                )
                await page.wait_for_timeout(800)
            except Exception:
                pass

            html = await page.content()
            image_urls_all = extract_feishu_image_urls(html)

            cap = config.get_settings().url_image_cap
            truncated = len(image_urls_all) > cap
            image_urls = image_urls_all[:cap]
            if truncated:
                URL_IMAGES_TRUNCATED_TOTAL.labels(handler="feishu").inc()

            blob_by_url = await download_images_in_context(
                ctx, image_urls, doc_url=url,
            )

        # Build refs in the order the URLs appeared in the doc, skipping
        # ones whose download failed.
        refs: list[ImageRef] = []
        names_for_md: list[str] = []
        urls_for_md: list[str] = []
        next_idx = 1
        for u in image_urls:
            data = blob_by_url.get(u)
            if not data:
                continue
            mime = sniff_image_mime(data)
            ext = _ext_for_mime(mime)
            name = f"image_{next_idx:04d}{ext}"
            refs.append(ImageRef(name=name, mime=mime, _factory=BytesFactory(data)))
            names_for_md.append(name)
            urls_for_md.append(u)
            next_idx += 1

        markdown = extract_with_trafilatura(html)
        markdown = replace_image_urls_with_names(markdown, urls_for_md, names_for_md)

        # trafilatura drops feishu image URLs (no file extension on the
        # CDN paths, so its image filter strips them). Append a tail
        # block of ``![](name)`` references for any downloaded image
        # whose placeholder name didn't make it into the markdown — so
        # ``image_metadata.extract_metadata_into_refs`` can still locate
        # every ref by name.
        missing = [n for n in names_for_md if n not in markdown]
        if missing:
            tail = "\n".join(f"![]({n})" for n in missing)
            markdown = (markdown + "\n\n" + tail) if markdown else tail

        title = extract_html_title(html, url)

        return ParseResult(
            content=markdown,
            title=title,
            image_refs=refs,
        )
