"""Shared httpx client + capped reader for URL fetching.

Every raw ``httpx`` client in ``parsers/url`` should be built here so none is
left with an uncapped redirect chain or an unbounded response body. The size
cap protects the worker (and API memory) from a hostile or accidental huge
response; the redirect cap stops redirect-chain amplification.
"""
from __future__ import annotations

import httpx

from config import get_settings
from parsers.url._handlers import DEFAULT_BROWSER_UA


class ResponseTooLargeError(Exception):
    """Raised when a response body exceeds ``max_url_response_bytes``."""


def make_async_client(**kw) -> httpx.AsyncClient:
    """httpx.AsyncClient with redirect cap + sane defaults. Caller ``kw``
    (e.g. ``headers``, ``timeout``, ``transport``) overrides the defaults."""
    s = get_settings()
    opts: dict = dict(
        follow_redirects=True,
        max_redirects=s.url_max_redirects,
        timeout=15,
        headers={"User-Agent": DEFAULT_BROWSER_UA},
    )
    opts.update(kw)
    return httpx.AsyncClient(**opts)


async def fetch_capped(
    client: httpx.AsyncClient, url: str, *, max_bytes: int | None = None, **req,
) -> tuple[httpx.Response, bytes]:
    """GET ``url`` and return ``(response, body_bytes)``, aborting with
    :class:`ResponseTooLargeError` if the body exceeds the cap. Streams so an
    oversized body is rejected before it is fully buffered. The returned
    response carries headers/url/encoding (read after the stream closes)."""
    s = get_settings()
    cap = s.max_url_response_bytes if max_bytes is None else max_bytes
    async with client.stream("GET", url, **req) as resp:
        resp.raise_for_status()
        cl = resp.headers.get("content-length")
        if cl is not None and cl.isdigit() and int(cl) > cap:
            raise ResponseTooLargeError(f"{url}: content-length {cl} > {cap}")
        total = 0
        chunks: list[bytes] = []
        async for chunk in resp.aiter_bytes():
            total += len(chunk)
            if total > cap:
                raise ResponseTooLargeError(f"{url}: body exceeded {cap} bytes")
            chunks.append(chunk)
    return resp, b"".join(chunks)
