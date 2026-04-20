"""URL parser with site-specific handler dispatch.

Handlers are registered in order of specificity — the first handler
whose ``match(url)`` returns True wins.  ``GenericHandler`` is always
last (catch-all).

External code should import from this package:
    from parsers.url import UrlParser, download_images, get_wechat_headers
"""
from __future__ import annotations

from parsers.base import BaseParser, ParseResult
from parsers.url._handlers import (
    canonicalize_via,
    download_images,
    find_handler,
    register_handler,
)

# --- Register site handlers (order = priority) ---
from parsers.url._wechat import WechatHandler, get_wechat_headers
register_handler(WechatHandler())

from parsers.url._xhs import XhsHandler
register_handler(XhsHandler())

from parsers.url._x import XHandler
register_handler(XHandler())

from parsers.url._generic import GenericHandler
register_handler(GenericHandler())  # catch-all, must be last


class UrlParser(BaseParser):
    engine_name = "url"
    supported_types = ["url"]

    async def parse(self, source: bytes | str, filename: str = "", **kwargs) -> ParseResult:
        url = source.decode() if isinstance(source, bytes) else source
        handler = find_handler(url)
        if handler is None:
            return ParseResult(content="", images={}, title="")
        return await handler.parse(url)


async def download_images_for_url(
    source_url: str, image_urls: list[str],
) -> dict[str, bytes]:
    """Fetch images, automatically threading the source URL's handler's
    headers + image canonicalization.

    The single entry point callers in worker / analyze should prefer
    over raw ``download_images``: one line, correct platform quirks
    applied (Referer, WeChat ``wx_fmt=jpeg``, etc.).
    """
    handler = find_handler(source_url)
    headers = handler.download_headers(source_url) if handler else None
    # Bind ``handler`` into the closure so each URL reuses the same
    # canonicalizer without re-lookup per image.
    canonicalize = (lambda u: canonicalize_via(handler, u)) if handler else None
    return await download_images(
        image_urls, headers=headers, canonicalize=canonicalize,
    )


__all__ = [
    "UrlParser",
    "download_images",
    "download_images_for_url",
    "get_wechat_headers",
    "find_handler",
]
