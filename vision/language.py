"""Resolve the deployment's vision output language for prompts.

The language is fixed per deployment via ``vision_default_language``
(env ``VISION_DEFAULT_LANGUAGE``): ``zh`` domestic, ``en`` overseas. It is
operator-controlled, but since it gets interpolated into an LLM prompt it is
still validated as a BCP-47-shaped locale token — a misconfigured (non-locale)
value can't leak arbitrary text into the prompt and falls back to a safe
constant.
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


def resolve_language() -> str:
    """Return the configured vision output language as a prompt-ready name.

    Reads ``settings.vision_default_language`` and maps it to an English
    language name (e.g. ``en`` → ``English``). A misconfigured (non-locale)
    value never reaches the prompt — it falls back to a safe constant.
    """
    candidate = get_settings().vision_default_language.strip()
    if not _LOCALE_RE.match(candidate):
        # Misconfigured default (not a locale code) — never let arbitrary
        # config text reach the prompt; settle on a safe constant.
        candidate = "en"
    primary = candidate.split("-", 1)[0].lower()
    return _NAMES.get(primary, candidate)
