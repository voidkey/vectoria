"""Unit tests for the fonttools-backed font-metadata extractor (Phase 4)."""
import io

import pytest


# ---- pure helpers ----

def test_canonicalize_family_strips_trailing_weight_token():
    from parsers.capture._font_metadata import canonicalize_family
    assert canonicalize_family("Inter Medium") == ("Inter", 500)
    assert canonicalize_family("Funnel Display Light") == ("Funnel Display", 300)


def test_canonicalize_family_keeps_width_modifiers():
    from parsers.capture._font_metadata import canonicalize_family
    # "Tight" is a WIDTH modifier and must be kept; only "Medium" is stripped.
    assert canonicalize_family("Inter Tight Medium") == ("Inter Tight", 500)
    # A family that is only a width modifier keeps its full name (no weight token).
    assert canonicalize_family("Tiempos Headline") == ("Tiempos Headline", None)


def test_canonicalize_family_strips_italic_first():
    from parsers.capture._font_metadata import canonicalize_family
    assert canonicalize_family("Inter Italic") == ("Inter", None)
    assert canonicalize_family("Inter Medium Italic") == ("Inter", 500)


def test_canonicalize_family_normalizes_spaced_compound_weight():
    from parsers.capture._font_metadata import canonicalize_family
    # "Semi Bold" -> "SemiBold" (600); family stripped cleanly.
    assert canonicalize_family("Inter Semi Bold") == ("Inter", 600)
    assert canonicalize_family("Inter Extra Bold") == ("Inter", 800)


def test_canonicalize_family_extrablack_out_of_range():
    from parsers.capture._font_metadata import canonicalize_family
    assert canonicalize_family("Acme ExtraBlack") == ("Acme", 950)


def test_infer_weight_from_subfamily():
    from parsers.capture._font_metadata import infer_weight_from_subfamily
    assert infer_weight_from_subfamily("Regular") == 400
    assert infer_weight_from_subfamily("Bold") == 700
    assert infer_weight_from_subfamily("Extra Light") == 200   # spaced
    assert infer_weight_from_subfamily("Semi-Bold") == 600     # hyphenated
    assert infer_weight_from_subfamily("Black Italic") == 900
    assert infer_weight_from_subfamily("") == 400


def test_is_icon_charset_true_for_pua_only():
    from parsers.capture._font_metadata import is_icon_charset
    # No Latin letters, all PUA -> icon.
    assert is_icon_charset({0xE000, 0xE001, 0xE002, 0xF001}) is True


def test_is_icon_charset_false_when_latin_alphabet_present():
    from parsers.capture._font_metadata import is_icon_charset
    # Full A-Z present (26 letters) even with lots of PUA -> NOT an icon font.
    latin = set(range(0x41, 0x5B))                 # A-Z = 26 letters
    pua = set(range(0xE000, 0xE000 + 200))         # heavy PUA
    assert is_icon_charset(latin | pua) is False


def test_is_icon_charset_false_when_pua_minority():
    from parsers.capture._font_metadata import is_icon_charset
    # <26 latin letters but PUA is a minority -> not an icon font.
    cps = {0x41, 0x42, 0x43} | {0x30, 0x31, 0x32, 0x33} | {0xE000}
    assert is_icon_charset(cps) is False


def test_is_icon_charset_empty():
    from parsers.capture._font_metadata import is_icon_charset
    assert is_icon_charset(set()) is False


# ---- synthetic fonts via FontBuilder ----

def _build_font(family, subfamily, *, weight=None, italic=False,
                codepoints=None, variable=False) -> bytes:
    """Build a tiny in-memory TTF and return its bytes."""
    from fontTools.fontBuilder import FontBuilder

    codepoints = codepoints if codepoints is not None else [ord("A"), ord("B")]
    glyph_order = [".notdef"] + [f"g{i}" for i in range(len(codepoints))]
    cmap = {cp: f"g{i}" for i, cp in enumerate(codepoints)}

    fb = FontBuilder(1000, isTTF=True)
    fb.setupGlyphOrder(glyph_order)
    fb.setupCharacterMap(cmap)
    advances = {name: 500 for name in glyph_order}
    from fontTools.ttLib.tables._g_l_y_f import Glyph
    fb.setupGlyf({name: Glyph() for name in glyph_order})
    fb.setupHorizontalMetrics({name: (advances[name], 0) for name in glyph_order})
    fb.setupHorizontalHeader(ascent=800, descent=-200)
    name_strings = {
        "familyName": family,
        "styleName": subfamily,
        "psName": f"{family.replace(' ', '')}-{subfamily.replace(' ', '')}",
    }
    fb.setupNameTable(name_strings)
    fb.setupOS2(usWeightClass=weight or 400,
                fsSelection=(0x01 if italic else 0x40))  # ITALIC vs REGULAR
    fb.setupPost()
    if variable:
        # (tag, min, default, max, nameID) — FontBuilder builds the fvar table.
        fb.setupFvar(axes=[("wght", 100, weight or 400, 900, "Weight")], instances=[])
    buf = io.BytesIO()
    fb.save(buf)
    return buf.getvalue()


def test_font_file_metadata_basic():
    from parsers.capture._font_metadata import font_file_metadata
    data = _build_font("Inter", "Medium", weight=500)
    meta = font_file_metadata(data, "inter-med.woff2")
    assert meta["identified"] is True
    assert meta["file"] == "inter-med.woff2"
    assert meta["family"] == "Inter"
    assert meta["weight"] == 500
    assert meta["style"] == "normal"
    assert meta["subfamily"] == "Medium"
    assert meta["isIcon"] is False
    assert meta["variationAxes"] == []
    # Verbatim key set.
    assert set(meta) == {"file", "family", "rawFamily", "subfamily", "postscript",
                         "weight", "style", "variationAxes", "identified", "isIcon"}


def test_font_file_metadata_italic():
    from parsers.capture._font_metadata import font_file_metadata
    data = _build_font("Inter", "Italic", weight=400, italic=True)
    meta = font_file_metadata(data, "inter-it.woff2")
    assert meta["style"] == "italic"


def test_font_file_metadata_variable_axes():
    from parsers.capture._font_metadata import font_file_metadata
    data = _build_font("Inter", "Regular", weight=400, variable=True)
    meta = font_file_metadata(data, "inter-vf.woff2")
    assert "wght" in meta["variationAxes"]


def test_font_file_metadata_icon_font():
    from parsers.capture._font_metadata import font_file_metadata
    pua = list(range(0xE000, 0xE000 + 8))
    data = _build_font("swiper-icons", "Regular", weight=400, codepoints=pua)
    meta = font_file_metadata(data, "icons.woff2")
    assert meta["isIcon"] is True


def test_font_file_metadata_parse_failure_unidentified():
    from parsers.capture._font_metadata import font_file_metadata
    meta = font_file_metadata(b"not a font", "junk.woff2")
    assert meta["identified"] is False
    assert meta["file"] == "junk.woff2"
    assert meta["family"] == ""
    assert meta["weight"] == 0


def test_build_fonts_manifest_shape():
    from parsers.capture._font_metadata import build_fonts_manifest, font_file_metadata
    files = [
        font_file_metadata(_build_font("Inter", "Regular", weight=400), "inter-reg.woff2"),
        font_file_metadata(_build_font("Inter", "Bold", weight=700), "inter-bold.woff2"),
        font_file_metadata(b"junk", "broken.woff2"),
    ]
    manifest = build_fonts_manifest(files, "2026-07-21T00:00:00+00:00")
    assert set(manifest) == {"files", "families", "unidentified", "meta"}
    assert manifest["meta"] == {"generatedAt": "2026-07-21T00:00:00+00:00",
                                "tool": "fonttools"}
    assert manifest["unidentified"] == ["broken.woff2"]
    fam = next(f for f in manifest["families"] if f["family"] == "Inter")
    assert set(fam) == {"family", "weights", "variable", "fileCount", "files"}
    assert fam["weights"] == [400, 700]     # sorted, deduped
    assert fam["fileCount"] == 2
    assert fam["variable"] is False
    assert fam["files"] == ["inter-bold.woff2", "inter-reg.woff2"]  # sorted


def test_build_fonts_manifest_marks_variable_family():
    from parsers.capture._font_metadata import build_fonts_manifest, font_file_metadata
    files = [font_file_metadata(
        _build_font("Roboto Flex", "Regular", weight=400, variable=True), "roboto-vf.woff2")]
    manifest = build_fonts_manifest(files, "2026-07-21T00:00:00+00:00")
    fam = manifest["families"][0]
    assert fam["variable"] is True
