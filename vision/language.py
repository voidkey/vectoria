"""Resolve a per-request target language for vision output.

The value originates from an end-user-influenced API field and is
interpolated into an LLM prompt, so it MUST be constrained: only
BCP-47-shaped locale tokens are accepted; anything else is treated as
absent and falls back to the deployment default. This closes the
prompt-injection vector while still letting the product pass real
locales like ``pt-BR``.
"""
from __future__ import annotations

import re

from config import get_settings

# ``\Z`` (not ``$``) anchors the end so a trailing newline can't slip
# through even if the ``.strip()`` below is ever removed: Python's ``$``
# matches just before a final ``\n`` (so "en\n" would pass), ``\Z`` does
# not. Defense-in-depth — ``.strip()`` is the first layer, this is the second.
_LOCALE_RE = re.compile(r"^[A-Za-z]{2,3}(-[A-Za-z0-9]{2,8})*\Z")

_NAMES = {
    "en": "English", "zh": "Chinese", "pt": "Portuguese", "es": "Spanish",
    "fr": "French", "de": "German", "ja": "Japanese", "ko": "Korean",
    "it": "Italian", "ru": "Russian",
}


def resolve_language(raw: str | None) -> str:
    """Return a clean language string safe to inject into a vision prompt.

    Absent/invalid input falls back to ``settings.vision_default_language``.
    """
    candidate = (raw or "").strip()
    if not _LOCALE_RE.match(candidate):
        candidate = get_settings().vision_default_language.strip()
    if not _LOCALE_RE.match(candidate):
        # Misconfigured default (not a locale code) — never let arbitrary
        # config text reach the prompt; settle on a safe constant.
        candidate = "en"
    primary = candidate.split("-", 1)[0].lower()
    return _NAMES.get(primary, candidate)
