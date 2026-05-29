"""Blacklist handler for known-unparseable URL patterns.

Some sites either:
  * actively block bots so hard that playwright fails repeatedly (412
    + "Execution context was destroyed" — the bilibili pattern we hit
    in production)
  * expose only JS-rendered video players (no text body to extract)
  * gate content behind login walls that we can't reasonably bypass

For these we'd rather fail fast (~ms) with a clear reason than burn
a worker slot on 30s of playwright startup and three retries — both
of which we already pay before discovering the URL is hopeless.

This handler is registered *before* ``GenericHandler`` so it short-
circuits the catch-all path. The blacklist is intentionally small
and conservative — only sites we have direct evidence of being
hopeless. False positives degrade UX (legitimate URL rejected); false
negatives just waste a few seconds on retry, so the bias is toward
narrow patterns.
"""
from __future__ import annotations

import logging
import re
from urllib.parse import urlparse

from config import get_settings
from parsers.base import ParseResult, PermanentParseError

logger = logging.getLogger(__name__)


# (host_suffix, path_regex, reason). host_suffix matches via endswith
# so subdomains are covered (e.g. ``b23.tv`` is bilibili's shortener).
# path_regex anchored with re.search; None means "any path on this host".
_BLACKLIST: tuple[tuple[str, "re.Pattern[str] | None", str], ...] = (
    # Bilibili video player pages — strict anti-bot, returns 412 to
    # plain httpx and aggressively navigates away under playwright.
    ("bilibili.com", re.compile(r"/video/"), "bilibili video page (anti-bot)"),
    ("b23.tv",       None,                   "bilibili short link (anti-bot)"),
    # Douyin / TikTok video player pages — same story.
    ("douyin.com",   re.compile(r"/video/"), "douyin video page (anti-bot)"),
    ("v.douyin.com", None,                   "douyin short link (anti-bot)"),
    ("tiktok.com",   re.compile(r"/@[^/]+/video/"), "tiktok video page (anti-bot)"),
    # iQiyi / Youku / Xigua video — JS-rendered player, no scrapable body.
    ("iqiyi.com",    re.compile(r"/v_"),     "iqiyi video page (player only)"),
    ("youku.com",    re.compile(r"/v_show"), "youku video page (player only)"),
    ("ixigua.com",   re.compile(r"/\d+"),    "xigua video page (player only)"),
    # Larksuite (overseas Lark) — same login-wall behavior as feishu.cn,
    # but the FeishuHandler is scoped to ``*.feishu.cn`` only. Without
    # an entry here, larksuite URLs fall through to GenericHandler and
    # silently fail on the login redirect after a 30s playwright run.
    ("larksuite.com", None,                  "larksuite (overseas Lark) — separate handler not implemented; please open an issue if needed"),
)


def _unreachable_suffixes() -> tuple[str, ...]:
    """Return the operator-configured domain suffixes treated as region-unreachable.

    Reads ``UNREACHABLE_DOMAINS`` env var (no prefix) via pydantic-settings.
    Returns an empty tuple when the config is unset — no effect on matching.
    """
    raw = get_settings().unreachable_domains or ""
    return tuple(s.strip().lower() for s in raw.split(",") if s.strip())


def _host_matches(host: str, suffix: str) -> bool:
    """Return True if ``host`` equals ``suffix`` or is a subdomain of it."""
    return host == suffix or host.endswith("." + suffix)


class UnparseableUrlError(PermanentParseError):
    """Raised when a URL is on the blacklist. Subclass of
    PermanentParseError so the worker handler short-circuits — marks
    the doc failed and *succeeds* the task to the queue, no retry, no
    dead-task alert. Carries a human-readable reason so the failed-doc
    record explains why we didn't try.
    """


class BlacklistHandler:
    """Registered before GenericHandler so it short-circuits the
    catch-all playwright path for known-hopeless URLs.
    """

    def match(self, url: str) -> bool:
        if _matched_entry(url) is not None:
            return True
        host = (urlparse(url).hostname or "").lower()
        return any(_host_matches(host, s) for s in _unreachable_suffixes())

    async def parse(self, url: str) -> ParseResult:
        entry = _matched_entry(url)
        if entry is not None:
            # Static blacklist hit — use the pre-defined reason.
            reason = entry[2]
            logger.info("url_blacklist: rejecting %s (%s)", url, reason)
            raise UnparseableUrlError(
                f"URL pattern not supported: {reason}. "
                "Video / login-gated / hard-anti-bot pages aren't crawlable; "
                "upload the source content directly instead."
            )
        else:
            # Region-unreachable domain configured via UNREACHABLE_DOMAINS.
            reason = "该区域网络不可达(region unreachable),请直接上传内容"
            logger.info("url_blacklist: rejecting %s (%s)", url, reason)
            raise UnparseableUrlError(
                f"URL pattern not supported: {reason}. "
                "Video / login-gated / hard-anti-bot / region-blocked pages "
                "aren't crawlable; upload the source content directly instead."
            )

    def download_headers(self, url: str) -> dict[str, str] | None:
        return None


def _matched_entry(
    url: str,
) -> tuple[str, "re.Pattern[str] | None", str] | None:
    host = (urlparse(url).hostname or "").lower()
    if not host:
        return None
    path = urlparse(url).path or ""
    for host_suffix, path_re, reason in _BLACKLIST:
        if host == host_suffix or host.endswith("." + host_suffix):
            if path_re is None or path_re.search(path):
                return (host_suffix, path_re, reason)
    return None
