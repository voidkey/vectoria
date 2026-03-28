import re

_HEADING_RE = re.compile(r"^(#{1,6})\s+(.+)$", re.MULTILINE)
_CODE_FENCE_RE = re.compile(r"^```", re.MULTILINE)


def extract_outline(markdown: str) -> list[dict]:
    """Extract heading hierarchy from markdown text.

    Returns list of {"level": int, "title": str} dicts.
    Ignores headings inside fenced code blocks.
    """
    if not markdown:
        return []

    # Find code block ranges to exclude
    fences = [m.start() for m in _CODE_FENCE_RE.finditer(markdown)]
    code_ranges: list[tuple[int, int]] = []
    for i in range(0, len(fences) - 1, 2):
        code_ranges.append((fences[i], fences[i + 1]))

    def _in_code_block(pos: int) -> bool:
        return any(start <= pos <= end for start, end in code_ranges)

    result = []
    for m in _HEADING_RE.finditer(markdown):
        if _in_code_block(m.start()):
            continue
        level = len(m.group(1))
        title = m.group(2).strip()
        result.append({"level": level, "title": title})
    return result
