"""Font-metadata extraction via fonttools — the fontkit port.

Ported from hyperframes `fontMetadataExtractor.ts`. Modern build tools hash-rename
font files (``19cfc7226ec3afaa-s.woff2``), stripping human-readable identity from
the filename. Every OpenType/WOFF/WOFF2 file still embeds a ``name`` table with the
family, subfamily, PostScript name, weight class, and variation axes; subsetting and
hashing don't strip it. This module reads that table from the captured bytes and
produces a manifest in the reference ``types.ts::FontsManifest`` shape.

All functions are pure (bytes/str in, plain dicts out) — no I/O. Parse failures are
non-fatal: ``font_file_metadata`` never raises, returning an ``identified: False``
entry instead.
"""
from __future__ import annotations

import io
import re

# Trailing weight tokens found in family names (e.g. "Inter Medium" -> "Inter")
# mapped to their OS/2 weight equivalent. Conservative: only well-known English
# tokens. Width modifiers ("Tight", "Condensed", "Extended") are intentionally
# NOT here — they denote separate typographic families, not weight variants.
WEIGHT_TOKEN_TO_VALUE: dict[str, int] = {
    "Thin": 100,
    "Hairline": 100,
    "ExtraLight": 200,
    "UltraLight": 200,
    "Light": 300,
    "Book": 400,
    "Regular": 400,
    "Normal": 400,
    "Medium": 500,
    "SemiBold": 600,
    "DemiBold": 600,
    "Bold": 700,
    "ExtraBold": 800,
    "UltraBold": 800,
    "Black": 900,
    "Heavy": 900,
    "ExtraBlack": 950,
    "UltraBlack": 950,
}

_WEIGHT_TOKEN_RE = re.compile(
    r"\s+(" + "|".join(WEIGHT_TOKEN_TO_VALUE) + r")$", re.IGNORECASE)
_ITALIC_SUFFIX_RE = re.compile(r"\s+(Italic|Oblique)$", re.IGNORECASE)
# "Semi Bold" -> "SemiBold" (anchored end-of-string so mid-name words are untouched).
_SPACED_COMPOUND_RE = re.compile(
    r"\s+(Semi|Extra|Ultra|Demi)\s+(Bold|Black|Light)$", re.IGNORECASE)
_ITALIC_WORD_RE = re.compile(r"italic|oblique", re.IGNORECASE)


def _capitalize(s: str) -> str:
    return s[:1].upper() + s[1:].lower() if s else s


def canonicalize_family(raw_family: str) -> tuple[str, int | None]:
    """Strip a trailing weight token from a family name.

    Returns ``(canonical_family, weight_from_token_or_None)``. Strips trailing
    Italic/Oblique first (recovered separately via the style flag), then a trailing
    weight token; normalizes a spaced compound token ("Semi Bold" -> "SemiBold")
    before matching. Width modifiers (Condensed/Tight/Extended) are KEPT.

    "Inter Medium"        -> ("Inter", 500)
    "Inter Tight Medium"  -> ("Inter Tight", 500)
    "Inter Medium Italic" -> ("Inter", 500)
    "Tiempos Headline"    -> ("Tiempos Headline", None)
    """
    if not raw_family:
        return raw_family, None
    result = raw_family.strip()
    result = _ITALIC_SUFFIX_RE.sub("", result).strip()
    result = _SPACED_COMPOUND_RE.sub(
        lambda m: f" {_capitalize(m.group(1))}{_capitalize(m.group(2))}", result)
    match = _WEIGHT_TOKEN_RE.search(result)
    if match:
        token = match.group(1)
        key = next((k for k in WEIGHT_TOKEN_TO_VALUE
                    if k.lower() == token.lower()), None)
        weight = WEIGHT_TOKEN_TO_VALUE[key] if key else None
        result = result[: match.start()].strip()
        return result, weight
    return result, None


def infer_weight_from_subfamily(subfamily: str) -> int | None:
    """Guess weight from a subfamily string ("Bold", "Light", ...) when OS/2 is
    absent. Spaces and hyphens are collapsed so "Extra Light"/"Extra-Light" match
    "ExtraLight". Returns 400 as the default (never None) — mirrors the reference."""
    s = re.sub(r"[\s-]+", "", subfamily.lower())
    if "thin" in s:
        return 100
    if "extralight" in s or "ultralight" in s:
        return 200
    if "light" in s:
        return 300
    if "medium" in s:
        return 500
    if "semibold" in s or "demibold" in s:
        return 600
    if "extrabold" in s or "ultrabold" in s:
        return 800
    if "black" in s or "heavy" in s:
        return 900
    if "bold" in s:
        return 700
    return 400


def is_icon_charset(cmap_codepoints: set[int]) -> bool:
    """Detect an ICON font from glyph coverage. BOTH must hold:
      1. it lacks a real Latin alphabet (<26 of A-Za-z present); and
      2. >50% of its glyphs live in a Unicode Private Use Area.
    The Latin gate is essential: some text fonts (SF Pro, Booton) pack thousands of
    PUA glyphs yet ship a full alphabet — flagging those by PUA ratio alone would
    strip a brand's real typeface."""
    if not cmap_codepoints:
        return False
    latin = sum(1 for cp in cmap_codepoints
                if 0x41 <= cp <= 0x5A or 0x61 <= cp <= 0x7A)
    if latin >= 26:
        return False
    in_pua = sum(1 for cp in cmap_codepoints
                 if 0xE000 <= cp <= 0xF8FF
                 or 0xF0000 <= cp <= 0xFFFFD
                 or 0x100000 <= cp <= 0x10FFFD)
    return in_pua / len(cmap_codepoints) > 0.5


def _derive_family_from_postscript(postscript: str) -> str:
    """PostScript names follow ``Family-Style``; recover the family portion."""
    if not postscript:
        return ""
    idx = postscript.find("-")
    return (postscript[:idx] if idx > 0 else postscript).strip()


def _empty_entry(filename: str) -> dict:
    return {
        "file": filename,
        "family": "",
        "rawFamily": "",
        "subfamily": "",
        "postscript": "",
        "weight": 0,
        "style": "normal",
        "variationAxes": [],
        "identified": False,
        "isIcon": False,
    }


def font_file_metadata(data: bytes, filename: str) -> dict:
    """Extract a ``FontFileMetadata`` dict from font bytes (woff/woff2/ttf/otf).

    Reads the ``name`` table (family = nameID 16 or 1, subfamily = 17 or 2,
    postscript = 6), OS/2 ``usWeightClass``/``fsSelection`` (italic), ``fvar`` axes,
    and ``cmap`` (icon detection). Weight precedence: OS/2 usWeightClass ->
    weight inferred from a family-name token -> inferred from the subfamily.
    Handles TTC/collections (first font). Never raises — a parse failure or an
    empty name table returns an ``identified: False`` entry."""
    from fontTools.ttLib import TTFont

    try:
        font = TTFont(io.BytesIO(data), fontNumber=0, lazy=True)
    except Exception:
        return _empty_entry(filename)

    try:
        name = font["name"]

        def _nm(*ids: int) -> str:
            for nid in ids:
                val = name.getDebugName(nid)
                if val:
                    return val.strip()
            return ""

        raw_family = _nm(16, 1)
        subfamily = _nm(17, 2)
        postscript = _nm(6)

        if not raw_family and not postscript:
            return _empty_entry(filename)

        os2 = font["OS/2"] if "OS/2" in font else None
        fs_selection = getattr(os2, "fsSelection", 0) if os2 is not None else 0
        italic_bit = bool(fs_selection & 0x01)          # ITALIC
        style = ("italic" if italic_bit or _ITALIC_WORD_RE.search(subfamily)
                 else "normal")

        variation_axes: list[str] = []
        if "fvar" in font:
            variation_axes = [ax.axisTag for ax in font["fvar"].axes]

        family_for_canon = raw_family or _derive_family_from_postscript(postscript)
        canonical, inferred = canonicalize_family(family_for_canon)

        if os2 is not None and getattr(os2, "usWeightClass", None):
            weight = int(os2.usWeightClass)
        elif inferred is not None:
            weight = inferred
        else:
            weight = infer_weight_from_subfamily(subfamily)

        cmap_cps: set[int] = set()
        try:
            best = font.getBestCmap()
            if best:
                cmap_cps = set(best.keys())
        except Exception:
            cmap_cps = set()

        return {
            "file": filename,
            "family": canonical or family_for_canon,
            "rawFamily": family_for_canon,
            "subfamily": subfamily,
            "postscript": postscript,
            "weight": weight,
            "style": style,
            "variationAxes": variation_axes,
            "identified": True,
            "isIcon": is_icon_charset(cmap_cps),
        }
    except Exception:
        return _empty_entry(filename)
    finally:
        try:
            font.close()
        except Exception:
            pass


def build_fonts_manifest(files: list[dict], generated_at: str) -> dict:
    """Aggregate per-file entries into a ``FontsManifest``.

    ``files`` is the list of ``FontFileMetadata`` dicts (from ``font_file_metadata``).
    Builds per-family ``FontFamilySummary`` (weights sorted+deduped, variable flag,
    file count, sorted file list), collects ``unidentified`` (files where
    ``identified`` is false), and stamps ``meta``. ``generated_at`` is passed in
    (from the profile's ``captured_at``) — never call datetime.now() here."""
    by_family: dict[str, dict] = {}
    unidentified: list[str] = []
    for f in files:
        if not f.get("identified"):
            unidentified.append(f.get("file", ""))
        fam = f.get("family") or ""
        if not fam:
            continue
        entry = by_family.get(fam)
        if entry is None:
            entry = {"family": fam, "weights": [], "variable": False,
                     "fileCount": 0, "files": []}
            by_family[fam] = entry
        entry["fileCount"] += 1
        entry["files"].append(f.get("file", ""))
        if f.get("variationAxes"):
            entry["variable"] = True
        w = f.get("weight")
        if w and w not in entry["weights"]:
            entry["weights"].append(w)

    for entry in by_family.values():
        entry["weights"].sort()
        entry["files"].sort()
    families = sorted(by_family.values(), key=lambda e: e["family"])

    return {
        "files": files,
        "families": families,
        "unidentified": unidentified,
        "meta": {"generatedAt": generated_at, "tool": "fonttools"},
    }
