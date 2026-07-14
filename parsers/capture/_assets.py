"""Asset URL selection + SSRF-checked download + ImageRef construction."""
from __future__ import annotations

import logging

from api.url_validation import reresolve_and_check_ssrf
from parsers.image_ref import BytesFactory, ImageRef
from parsers.url._http import fetch_capped, make_async_client

logger = logging.getLogger(__name__)


async def fetch_asset_bytes(url: str, *, max_bytes: int) -> tuple[bytes, str] | None:
    """SSRF-check then fetch (capped). Returns (data, content_type) or None on
    any failure (rejected URL, network error, over cap)."""
    try:
        await reresolve_and_check_ssrf(url)
    except Exception:
        logger.info("capture asset SSRF/URL rejected: %s", url)
        return None
    try:
        async with make_async_client() as client:
            resp, data = await fetch_capped(client, url, max_bytes=max_bytes)
        ctype = (resp.headers.get("content-type", "") or "").split(";")[0].strip()
        return data, ctype
    except Exception:
        logger.info("capture asset fetch failed: %s", url, exc_info=True)
        return None


def image_ref_from_bytes(data: bytes, *, filename: str, mime: str,
                         width: int | None = None, height: int | None = None,
                         alt: str = "") -> ImageRef:
    return ImageRef(name=filename, mime=mime, _factory=BytesFactory(data),
                    width=width, height=height, alt=alt)
