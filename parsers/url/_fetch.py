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

from config import get_settings
from curl_cffi import requests as _cc
from infra.ratelimit import acquire as _rl_acquire
from parsers.url._handlers import detect_block_reason, extract_html_title, raise_if_gone

logger = logging.getLogger(__name__)

_BACKOFFS = (0.5, 1.0, 2.0)


def _cc_get(url: str, **kw):  # wrapper so tests can monkeypatch
    return _cc.get(url, impersonate="chrome", timeout=30, **kw)


async def _ratelimit(host: str) -> bool:
    s = get_settings()
    return await _rl_acquire(host, rate=s.url_page_fetch_rate, per_seconds=s.url_page_fetch_per)


async def _sleep(seconds: float) -> None:
    await asyncio.sleep(seconds)


async def acquire_page_token(host: str, *, max_wait: float = 6.0) -> None:
    """Per-host politeness gate for any page-fetch tier (curl_cffi, httpx,
    Playwright). Waits until a rate token is available, bounded by max_wait
    so we never hang forever. Acquiring a token must NOT consume a fetch
    attempt — it is the per-IP politeness gate, separate from block-retry.
    """
    waited = 0.0
    while not await _ratelimit(host):
        await _sleep(0.5)
        waited += 0.5
        if waited >= max_wait:
            return  # proceed anyway rather than starve


async def fetch_impersonated(
    url: str, *, retries: int = 3, proxy: str | None = None,
    retry_on_block: bool = False,
) -> str | None:
    """Fetch HTML via curl_cffi (Chrome JA3). Returns HTML, or None on a
    confirmed anti-bot block or after exhausting retries. Retries with backoff
    on transient errors (exception / empty body).

    A *confirmed* block is by default terminal — for sites that hard-ban by IP,
    re-hitting a challenge just escalates the ban. Pass ``retry_on_block=True``
    for sites whose anti-bot is *per-request and probabilistic* (e.g. baike,
    which serves a verification page ~38% of the time): there a paced retry
    usually lands a clean page and does not stick a ban. Retries stay rate-
    limited per host, so retrying is still polite.
    ``proxy`` is reserved for future use (kuaidaili); unused in P1.
    """
    host = (urlparse(url).hostname or "").lower()
    for attempt in range(retries):
        await acquire_page_token(host)    # politeness gate, does NOT consume the attempt
        try:
            kw = {"proxies": {"http": proxy, "https": proxy}} if proxy else {}
            resp = await asyncio.to_thread(_cc_get, url, **kw)
        except Exception:
            logger.warning("fetch_impersonated error on %s (attempt %d)", url, attempt + 1, exc_info=True)
            html = ""
        else:
            # 404/410 → permanent (propagates, no retry); the body would just be
            # the site's error page. 5xx/429 are server-side/transient, so drop
            # the body and let the loop retry with backoff.
            raise_if_gone(resp.status_code, url)
            html = resp.text or ""
            if resp.status_code >= 500 or resp.status_code == 429:
                html = ""
        if html:
            if detect_block_reason(html, extract_html_title(html, url)) is None:
                return html
            if not retry_on_block:
                logger.info("fetch_impersonated: confirmed block on %s; not retrying", url)
                return None
            logger.info("fetch_impersonated: block on %s (attempt %d/%d); retrying", url, attempt + 1, retries)
        if attempt < retries - 1:
            await _sleep(_BACKOFFS[min(attempt, len(_BACKOFFS) - 1)])
    logger.info("fetch_impersonated gave up on %s after %d attempts", url, retries)
    return None
