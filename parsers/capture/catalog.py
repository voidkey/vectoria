"""Font catalog matching. Maps a captured font-family to a downstream
CDN entry so we reference it instead of re-storing the WOFF2. The catalog
file is deployment-provided (private); path from config, empty default =
matching off (every font is a miss -> downloaded to our S3)."""
from __future__ import annotations

import json
import logging
from functools import lru_cache

from parsers.capture.profile import CatalogMatch

logger = logging.getLogger(__name__)

_DROP_WORDS = {"thin", "light", "regular", "medium", "semibold", "demibold",
               "bold", "black", "italic", "oblique", "display", "text"}


def _settings_path() -> str:
    from config import get_settings
    return get_settings().font_catalog_path


def _normalize(family: str) -> str:
    """Lowercase, take first stack entry, strip quotes + trailing weight/style
    words so 'Inter Display Bold' and '"Inter", sans' compare equal-ish."""
    first = family.split(",")[0].strip().strip('"').strip("'").lower()
    parts = first.split()
    while len(parts) > 1 and parts[-1] in _DROP_WORDS:
        parts.pop()
    return " ".join(parts)


@lru_cache(maxsize=1)
def _load_catalog() -> dict[str, dict]:
    """family(normalized) -> {slug, css_url, weights}. Empty on no/broken file."""
    path = _settings_path()
    if not path:
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            entries = json.load(f)
        out: dict[str, dict] = {}
        for e in entries:
            fam = _normalize(e.get("family", ""))
            if fam:
                out[fam] = {
                    "slug": e.get("slug"),
                    "css_url": e.get("css_url"),
                    "weights": e.get("weights", []),
                }
        return out
    except Exception:
        logger.exception("failed to load font catalog at %s", path)
        return {}


def match_font(family: str) -> CatalogMatch:
    entry = _load_catalog().get(_normalize(family))
    if entry is None:
        return CatalogMatch(matched=False)
    return CatalogMatch(
        matched=True, slug=entry["slug"], css_url=entry["css_url"], source="catalog",
    )
