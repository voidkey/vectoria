"""Post-processing for fonts, spacing, and section typing (pure Python)."""
from __future__ import annotations

from parsers.capture.catalog import match_font
from parsers.capture.profile import FontRole

_TYPE_KEYWORDS = [
    ("pricing", ("pric", "plan", "定价", "价格", "方案", "套餐", "订阅")),
    ("features", ("feature", "功能", "特性", "能力")),
    ("testimonial", ("testimonial", "review", "quote", "评价", "用户说",
                     "怎么说", "口碑", "好评", "案例", "创作者")),
    ("cta", ("cta", "get started", "sign up", "try ", "start free",
             "免费开始", "开始创作", "立即", "马上", "试用", "开始使用")),
    ("footer", ("footer", "页脚")),
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


def cluster_spacing(values: list[float], tol: int = 3,
                    max_val: int | None = None) -> list[int]:
    """Round + merge nearby px values into a sorted unique scale. ``max_val``
    drops absurd outliers (e.g. a 33554400px border-radius from a pill/circle
    element) that aren't meaningful design tokens."""
    rounded = sorted({int(round(v)) for v in values
                      if v and v > 0 and (max_val is None or v <= max_val)})
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
