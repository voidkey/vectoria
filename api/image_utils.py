"""API-layer image helpers.

As of W1 Task 3 the image upload paths live in ``api/image_stream.py``
(streaming) and ``parsers/image_metadata.py`` (MIME detection). This
module retained only the small formatting helper the read-side route
still uses — kept here rather than moved so the import path on the
existing caller in ``api/routes/images.py`` stays stable.
"""
import math


def compute_aspect_ratio(w: int, h: int) -> str:
    """Compute a human-friendly aspect ratio string like '16:9' or '3:2'."""
    if not w or not h:
        return ""
    g = math.gcd(w, h)
    rw, rh = w // g, h // g
    # Snap to common ratios if close
    common = [(16, 9), (4, 3), (3, 2), (1, 1), (9, 16), (3, 4), (2, 3)]
    for cw, ch in common:
        if abs(rw / rh - cw / ch) < 0.05:
            return f"{cw}:{ch}"
    # If reduced ratio is too large, approximate
    if rw > 20 or rh > 20:
        ratio = w / h
        for cw, ch in common:
            if abs(ratio - cw / ch) < 0.1:
                return f"{cw}:{ch}"
        return f"{rw}:{rh}"
    return f"{rw}:{rh}"
