"""CSS color parsing + brand-color extraction (pure Python, no deps).

Input is a raw samples dict from the JS extractor; output is a small list
of ColorToken with roles, coverage, confidence, and sources."""
from __future__ import annotations

import colorsys
import math
import re

from parsers.capture.profile import ColorToken

# Minimal CSS named-color subset (common brand/UI names). Extend as needed.
_NAMED = {
    "white": (255, 255, 255), "black": (0, 0, 0), "red": (255, 0, 0),
    "green": (0, 128, 0), "blue": (0, 0, 255), "gray": (128, 128, 128),
    "grey": (128, 128, 128), "silver": (192, 192, 192), "navy": (0, 0, 128),
    "orange": (255, 165, 0), "yellow": (255, 255, 0), "purple": (128, 0, 128),
    "teal": (0, 128, 128), "transparent": None,
}


def parse_css_color(value: str) -> tuple[int, int, int] | None:
    """Return (r,g,b) for a supported CSS color, else None (transparent,
    gradients, var(), color()/oklch() literals, unknown names)."""
    if not value:
        return None
    v = value.strip().lower()
    if v in _NAMED:
        return _NAMED[v]
    if v.startswith("#"):
        h = v[1:]
        if len(h) in (3, 4):
            h = "".join(c * 2 for c in h[:3])
        elif len(h) in (6, 8):
            h = h[:6]
        else:
            return None
        try:
            return (int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16))
        except ValueError:
            return None
    m = re.match(r"rgba?\(([^)]+)\)", v)
    if m:
        parts = re.split(r"[,\s/]+", m.group(1).strip())
        try:
            r, g, b = (int(round(float(p))) for p in parts[:3])
            return (max(0, min(255, r)), max(0, min(255, g)), max(0, min(255, b)))
        except (ValueError, TypeError):
            return None
    m = re.match(r"hsla?\(([^)]+)\)", v)
    if m:
        parts = re.split(r"[,\s/]+", m.group(1).strip())
        try:
            h = float(parts[0].replace("deg", "")) / 360.0
            s = float(parts[1].rstrip("%")) / 100.0
            ll = float(parts[2].rstrip("%")) / 100.0
            r, g, b = colorsys.hls_to_rgb(h, ll, s)
            return (round(r * 255), round(g * 255), round(b * 255))
        except (ValueError, IndexError):
            return None
    return None


def _srgb_to_linear(c: float) -> float:
    c /= 255.0
    return c / 12.92 if c <= 0.04045 else ((c + 0.055) / 1.055) ** 2.4


def rgb_to_lab(rgb: tuple[int, int, int]) -> list[float]:
    r, g, b = (_srgb_to_linear(x) for x in rgb)
    # linear sRGB -> XYZ (D65)
    x = r * 0.4124 + g * 0.3576 + b * 0.1805
    y = r * 0.2126 + g * 0.7152 + b * 0.0722
    z = r * 0.0193 + g * 0.1192 + b * 0.9505
    xn, yn, zn = 0.95047, 1.0, 1.08883

    def f(t: float) -> float:
        return t ** (1 / 3) if t > 0.008856 else (7.787 * t + 16 / 116)

    fx, fy, fz = f(x / xn), f(y / yn), f(z / zn)
    return [116 * fy - 16, 500 * (fx - fy), 200 * (fy - fz)]


def rgb_to_oklch(rgb: tuple[int, int, int]) -> str:
    r, g, b = (_srgb_to_linear(x) for x in rgb)
    l_ = 0.4122214708 * r + 0.5363325363 * g + 0.0514459929 * b
    m_ = 0.2119034982 * r + 0.6806995451 * g + 0.1073969566 * b
    s_ = 0.0883024619 * r + 0.2817188376 * g + 0.6299787005 * b
    l_, m_, s_ = (max(v, 0.0) ** (1 / 3) for v in (l_, m_, s_))
    ll = 0.2104542553 * l_ + 0.7936177850 * m_ - 0.0040720468 * s_
    a = 1.9779984951 * l_ - 2.4285922050 * m_ + 0.4505937099 * s_
    bb = 0.0259040371 * l_ + 0.7827717662 * m_ - 0.8086757660 * s_
    c = math.hypot(a, bb)
    h = math.degrees(math.atan2(bb, a)) % 360
    return f"oklch({ll:.3f} {c:.3f} {h:.1f})"


def delta_e(lab1: list[float], lab2: list[float]) -> float:
    return math.sqrt(sum((a - b) ** 2 for a, b in zip(lab1, lab2)))


def _to_hex(rgb: tuple[int, int, int]) -> str:
    return "#{:02x}{:02x}{:02x}".format(*rgb)


def _saturation(rgb: tuple[int, int, int]) -> float:
    r, g, b = (x / 255 for x in rgb)
    return colorsys.rgb_to_hls(r, g, b)[2]


def dominant_screenshot_hex(png_bytes: bytes) -> str | None:
    """Most common color in a downscaled screenshot — the pixel-sampling half
    of reference-style extraction, used to cross-check the computed-style
    background (and to catch gradient/image backgrounds the DOM misses).
    Returns None if PIL can't decode the bytes."""
    try:
        import io

        from PIL import Image

        with Image.open(io.BytesIO(png_bytes)) as im:
            small = im.convert("RGB").resize((64, 64))
            colors = small.getcolors(64 * 64)  # [(count, (r,g,b)), ...]
        if not colors:
            return None
        _, rgb = max(colors, key=lambda c: c[0])
        return _to_hex(rgb)
    except Exception:
        return None


# A color is a "primary/accent candidate" only if it's chromatic enough — below
# this HLS saturation it's a neutral (gray/near-black/near-white), which should
# never win the primary role on saturation alone. A strong explicit brand signal
# (theme-color / --*primary var) still overrides this via _brand_prior.
_CHROMATIC_SAT = 0.2


class _Cluster:
    __slots__ = ("rgb", "lab", "area", "text_area", "bg_area", "interactive_area", "sources")

    def __init__(self, rgb, lab, area, text, interactive):
        self.rgb = rgb
        self.lab = lab
        self.area = area
        self.text_area = area if text else 0.0
        self.bg_area = 0.0 if text else area
        # Fill area that landed on an interactive element (a/button/[role]/btn-cta
        # class) — the reference `interactiveBg`, the strongest usage signal that a
        # color is the brand action color.
        self.interactive_area = area if (interactive and not text) else 0.0
        self.sources = {"computed"}


def _brand_prior(sources: set[str]) -> int:
    """Declared-brand-color strength from the signals already captured in
    ``sources``: a ``<meta theme-color>`` match or a CSS var whose NAME says
    primary/brand/main is an authoritative "this is THE primary" signal (2);
    an accent-named var is NOT rewarded for the primary role (0)."""
    if "theme-color" in sources:
        return 2
    names = [s[len("css-var:"):].lower() for s in sources if s.startswith("css-var:")]
    if any(re.search(r"primary|brand|main", n) for n in names):
        return 2
    return 0


def process_colors(raw: dict, *, delta_e_threshold: float = 10.0,
                   screenshot_bg_hex: str | None = None) -> list[ColorToken]:
    samples = raw.get("samples", [])
    clusters: list[_Cluster] = []
    for s in samples:
        rgb = parse_css_color(s.get("color", ""))
        if rgb is None:
            continue
        lab = rgb_to_lab(rgb)
        area = float(s.get("area", 0) or 0)
        text = bool(s.get("text"))
        interactive = bool(s.get("interactive"))
        for c in clusters:
            if delta_e(c.lab, lab) < delta_e_threshold:
                c.area += area
                if text:
                    c.text_area += area
                else:
                    c.bg_area += area
                    if interactive:
                        c.interactive_area += area
                break
        else:
            clusters.append(_Cluster(rgb, lab, area, text, interactive))
    if not clusters:
        return []

    total = sum(c.area for c in clusters) or 1.0

    # explicit brand signals
    brand_rgbs: list[tuple[tuple[int, int, int], str]] = []
    for name, val in (raw.get("css_vars") or {}).items():
        rgb = parse_css_color(val)
        if rgb:
            brand_rgbs.append((rgb, f"css-var:{name}"))
    theme_rgb = parse_css_color(raw.get("theme_color") or "")
    if theme_rgb:
        brand_rgbs.append((theme_rgb, "theme-color"))
    for rgb, src in brand_rgbs:
        lab = rgb_to_lab(rgb)
        for c in clusters:
            if delta_e(c.lab, lab) < delta_e_threshold:
                c.sources.add(src)
                break
    if screenshot_bg_hex:
        sb = parse_css_color(screenshot_bg_hex)
        if sb:
            labb = rgb_to_lab(sb)
            bg_like = max(clusters, key=lambda c: c.area)
            if delta_e(bg_like.lab, labb) < delta_e_threshold * 1.5:
                bg_like.sources.add("screenshot")

    # _Cluster has no __eq__/__hash__, so it hashes by identity — safe to use
    # the cluster objects directly as dict keys (clearer than id()-keying).
    background = max(clusters, key=lambda c: c.area)
    text_c = max(clusters, key=lambda c: c.text_area)
    assigned: dict[_Cluster, str] = {background: "background"}
    # Only assign a "text" role when text was actually observed — otherwise
    # max() picks an arbitrary cluster and mislabels it (image-only pages).
    if text_c.text_area > 0 and text_c not in assigned:
        assigned[text_c] = "text"

    remaining = [c for c in clusters if c not in assigned]

    # Sequential role picks (mirrors the reference pickAccent(exclude=...) chaining),
    # replacing the old saturation-dominated sort that mislabeled a bright speck as
    # primary over a muted, heavily-used brand color:
    #   1. PRIMARY — strongest *declared* brand signal (theme-color / --*primary var)
    #      first; ties broken by usage (chromatic → interactive fill → coverage),
    #      with raw saturation demoted to the final tiebreaker.
    #   2. ACCENT  — the best remaining *chromatic* color by usage (role diversity →
    #      coverage → saturation); a neutral can't outrank a real chromatic accent.
    #   3. MUTED   — most prominent leftover by area.
    def _role_div(c: _Cluster) -> int:
        return (c.interactive_area > 0) + (c.text_area > 0) + (c.bg_area > 0)

    def _primary_key(c: _Cluster):
        return (
            _brand_prior(c.sources),
            1 if _saturation(c.rgb) >= _CHROMATIC_SAT else 0,
            c.interactive_area,
            c.area,
            _saturation(c.rgb),
        )

    def _accent_key(c: _Cluster):
        return (
            1 if _saturation(c.rgb) >= _CHROMATIC_SAT else 0,
            _role_div(c),
            c.area,
            _saturation(c.rgb),
        )

    for role, key in (("primary", _primary_key), ("accent", _accent_key),
                      ("muted", lambda c: c.area)):
        if not remaining:
            break
        pick = max(remaining, key=key)
        assigned[pick] = role
        remaining.remove(pick)

    out: list[ColorToken] = []
    for c in clusters:
        role = assigned.get(c)
        if role is None:
            continue
        coverage = c.area / total
        n_extra = len(c.sources - {"computed"})
        confidence = min(1.0, 0.4 + 0.4 * min(coverage * 2, 1.0) + 0.1 * n_extra)
        out.append(ColorToken(
            hex=_to_hex(c.rgb), oklch=rgb_to_oklch(c.rgb), lab=c.lab, role=role,
            coverage=round(coverage, 4), confidence=round(confidence, 3),
            sources=sorted(c.sources),
        ))
    order = {"background": 0, "primary": 1, "accent": 2, "text": 3, "muted": 4}
    out.sort(key=lambda t: order.get(t.role, 9))
    return out[:5]
