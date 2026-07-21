"""Asset URL selection + SSRF-checked download + ImageRef construction."""
from __future__ import annotations

import logging
import re
from urllib.parse import urlsplit

from api.url_validation import reresolve_and_check_ssrf
from parsers.image_ref import BytesFactory, ImageRef
from parsers.url._http import fetch_capped, make_async_client

logger = logging.getLogger(__name__)

_UTILITY_CLASS_RE = re.compile(r"^(w-|h-|p-|m-|flex|grid|block)")
_HEXHASH_RE = re.compile(r"^[a-f0-9]{8,}$", re.I)
_DIGITS_RE = re.compile(r"^\d+$")


def _slugify(text: str) -> str:
    """Lowercase, collapse non-alphanumerics to single dashes, trim, cap at 40
    chars. Mirrors hyperframes assetDownloader.ts::slugify."""
    s = re.sub(r"[^a-z0-9]+", "-", text.lower())
    return s.strip("-")[:40]


def derive_asset_name(cat: dict, used: set[str]) -> str:
    """Human-readable slug for a downloaded catalog image, from its context.

    Port of hyperframes assetDownloader.ts::deriveAssetName. Priority:
    alt/description -> nearestHeading -> meaningful URL-path segment ->
    sectionClasses. Prefix: ``poster`` (video[poster]), ``hero`` (aboveFold),
    else ``image``. Falls back to ``<prefix>-<idx>`` and numeric-dedups against
    ``used``. Pure — no I/O. ``used`` is read (never mutated); caller adds the
    returned name after storing."""
    cat = cat or {}
    idx = len(used)
    candidates: list[str] = []

    # 1. Alt text / description
    desc = re.sub(r"[^a-zA-Z0-9 -]", "", (cat.get("description") or "")).strip()
    if 3 < len(desc) < 80:
        candidates.append(desc)

    # 2. Nearest heading
    heading = re.sub(r"[^a-zA-Z0-9 -]", "", (cat.get("nearestHeading") or "")).strip()
    if 3 < len(heading) < 60:
        candidates.append(heading)

    # 3. Meaningful URL path segment (extension stripped)
    path = urlsplit(cat.get("url", "")).path
    raw_name = re.sub(r"\.[^.]+$", "", path.rsplit("/", 1)[-1])
    if (2 < len(raw_name) < 50 and not _HEXHASH_RE.match(raw_name)
            and not _DIGITS_RE.match(raw_name)
            and "_next" not in raw_name and "?" not in raw_name):
        candidates.append(raw_name)

    # 4. Section classes as context (skip utility/layout classes; first two)
    section_classes = cat.get("sectionClasses") or ""
    if section_classes:
        meaningful = [c for c in section_classes.split()
                      if 3 < len(c) < 30 and not _UTILITY_CLASS_RE.match(c)]
        classes = "-".join(meaningful[:2])
        if len(classes) > 3:
            candidates.append(classes)

    contexts = cat.get("contexts") or []
    is_poster = "video[poster]" in contexts
    prefix = "poster" if is_poster else ("hero" if cat.get("aboveFold") else "image")

    slug = ""
    for c in candidates:
        slug = _slugify(c)
        if len(slug) > 3 and slug not in used:
            break

    if not slug or len(slug) <= 3 or slug in used:
        slug = f"{prefix}-{idx}"

    final = slug
    suffix = 2
    while final in used:
        final = f"{slug}-{suffix}"
        suffix += 1
    return final


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
