"""Post-processing for fonts, spacing, and section typing (pure Python)."""
from __future__ import annotations

from parsers.capture.catalog import match_font
from parsers.capture.profile import FontRole

_TYPE_KEYWORDS = [
    ("pricing", ("pric", "plan")),
    ("features", ("feature",)),
    ("testimonial", ("testimonial", "review", "quote")),
    ("cta", ("cta", "get started", "sign up", "try ", "start free")),
    ("footer", ("footer",)),
]


def section_type(heading: str, class_names: list[str], index: int, total: int) -> str:
    if index == 0:
        return "hero"
    hay = " ".join([heading.lower(), " ".join(class_names).lower()])
    for label, kws in _TYPE_KEYWORDS:
        if any(k in hay for k in kws):
            return label
    if total and index == total - 1:
        return "footer"
    return "generic"


def cluster_spacing(values: list[float], tol: int = 3) -> list[int]:
    """Round + merge nearby px values into a sorted unique scale."""
    rounded = sorted({int(round(v)) for v in values if v and v > 0})
    out: list[int] = []
    for v in rounded:
        if out and v - out[-1] <= tol:
            continue
        out.append(v)
    return out


def build_font_role(info: dict, *, weights: list[int]) -> FontRole:
    stack = info.get("family", "") or ""
    family = stack.split(",")[0].strip().strip('"').strip("'") or "sans-serif"
    cm = match_font(stack)
    return FontRole(
        family=family, stack=stack, weights=sorted(set(w for w in weights if w)),
        sample_selector=info.get("selector", ""),
        catalog_match=cm, renderable=cm.matched, files=[],
    )
