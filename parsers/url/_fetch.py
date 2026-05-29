"""Shared JA3-impersonated fetch (curl_cffi).

Some sites (e.g. baike.baidu.com) gate on the TLS/JA3 fingerprint, not the
IP: plain httpx is rejected regardless of UA/cookies, but a request whose
TLS handshake matches a real Chrome passes. curl_cffi replays Chrome's
JA3/HTTP2 fingerprint. We retry with backoff on anti-bot pages and rate-
limit per host so a single IP stays polite (no proxy in use).
"""
from __future__ import annotations

import asyncio
import logging
from urllib.parse import urlparse

from curl_cffi import requests as _cc
from infra.ratelimit import acquire as _rl_acquire
from parsers.url._handlers import detect_block_reason, extract_html_title

logger = logging.getLogger(__name__)

_DEFAULT_RATE, _DEFAULT_PER = 1, 2  # ~0.5/s, polite single-IP
_BACKOFFS = (0.5, 1.0, 2.0)


def _cc_get(url: str, **kw):  # wrapper so tests can monkeypatch
    return _cc.get(url, impersonate="chrome", timeout=30, **kw)


async def _ratelimit(host: str) -> bool:
    return await _rl_acquire(host, rate=_DEFAULT_RATE, per_seconds=_DEFAULT_PER)


async def _sleep(seconds: float) -> None:
    await asyncio.sleep(seconds)


async def fetch_impersonated(
    url: str, *, retries: int = 3, proxy: str | None = None,
) -> str | None:
    """Fetch HTML via curl_cffi (Chrome JA3). Returns HTML, or None if every
    attempt was blocked / errored. Retries with backoff on anti-bot pages.
    ``proxy`` is reserved for future use (kuaidaili); unused in P1.
    """
    host = (urlparse(url).hostname or "").lower()
    for attempt in range(retries):
        if not await _ratelimit(host):
            await _sleep(_BACKOFFS[min(attempt, len(_BACKOFFS) - 1)])
            continue
        try:
            kw = {"proxies": {"http": proxy, "https": proxy}} if proxy else {}
            resp = await asyncio.to_thread(_cc_get, url, **kw)
            html = resp.text or ""
        except Exception:
            logger.warning("fetch_impersonated error on %s (attempt %d)", url, attempt + 1, exc_info=True)
            html = ""
        if html and detect_block_reason(html, extract_html_title(html, url)) is None:
            return html
        if attempt < retries - 1:
            await _sleep(_BACKOFFS[min(attempt, len(_BACKOFFS) - 1)])
    logger.info("fetch_impersonated gave up on %s after %d attempts", url, retries)
    return None
