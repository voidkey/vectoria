"""Capture-quality assessment — gate high-fidelity artifacts on real content.

A "completed" render is not the same as a usable capture: anti-bot challenge
pages (Cloudflare "Just a moment"), login walls, and un-hydrated SPAs all render
"successfully" but carry no real content. Rebuilding a page-card from such a page
reproduces the CHALLENGE PAGE, which is worse than no structural reference at all.

assess_quality() classifies the extracted DOM into:
  - "full"    : rich real content — page.html / design-styles.json are trustworthy.
  - "partial" : thin / likely-blocked body, but head-level brand (theme-color,
                og:image, favicon, title) was salvaged — emit brand only, NO rebuild.
  - "blocked" : challenge / near-empty and no head brand — almost nothing usable.

Pure function over the run_extract() dict; no I/O.
"""
from __future__ import annotations

# Lowercased substrings that fingerprint a bot-challenge / interstitial page.
_CHALLENGE_MARKERS = (
    "just a moment",
    "attention required",
    "cf-browser-verification",
    "checking your browser",
    "verify you are human",
    "verifying you are human",
    "please turn javascript on",
    "enable javascript to continue",
    "access denied",
    "ddos protection",
    "ray id",  # Cloudflare error footer
    "captcha",
    "are you a robot",
)

# Below this many chars of visible text a body is "thin" (blocked / unhydrated SPA).
_MIN_FULL_TEXT = 200
# Below this it is effectively empty.
_MIN_ANY_TEXT = 40


def _has_head_brand(raw: dict) -> bool:
    """True if the <head>/meta yielded *something* brandable even when the body
    is blocked: theme-color, og:image (hero), a logo/favicon, or a title."""
    colors = raw.get("colors", {}) or {}
    if colors.get("theme_color"):
        return True
    assets = raw.get("assets", {}) or {}
    if any(assets.get(k) for k in ("hero", "og_image", "logo", "favicon")):
        return True
    text = raw.get("text", {}) or {}
    return bool((text.get("headline") or "").strip())


def assess_quality(raw: dict) -> tuple[str, str | None]:
    """Return (quality, blocked_reason). quality ∈ {full, partial, blocked}."""
    text = raw.get("text", {}) or {}
    full_text = (text.get("full_text") or "").strip()
    headline = (text.get("headline") or "").strip()
    sections = raw.get("sections", []) or []
    color_samples = (raw.get("colors", {}) or {}).get("samples", []) or []
    head_brand = _has_head_brand(raw)

    hay = (headline + "\n" + full_text[:2000]).lower()
    marker = next((m for m in _CHALLENGE_MARKERS if m in hay), None)

    # Hard block signal: a challenge fingerprint, or an essentially empty body.
    if marker or len(full_text) < _MIN_ANY_TEXT:
        reason = (f"challenge fingerprint: {marker!r}" if marker
                  else f"near-empty body ({len(full_text)} chars)")
        return ("partial" if head_brand else "blocked", reason)

    # Thin body (blocked-lite / un-hydrated / minimal SPA): brand OK, no rebuild.
    if len(full_text) < _MIN_FULL_TEXT or (not sections and not color_samples):
        return ("partial", f"thin content ({len(full_text)} chars, "
                            f"{len(sections)} sections)")

    return ("full", None)
