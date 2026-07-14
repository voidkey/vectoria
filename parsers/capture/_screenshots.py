"""Screenshot capture: above-fold, full-page (height-clamped), sections."""
from __future__ import annotations


async def capture_screenshots(page, sections: list[dict], *, max_screenshots: int,
                              max_height: int) -> list[dict]:
    shots: list[dict] = []
    vp = page.viewport_size or {"width": 1280, "height": 800}
    # above-the-fold
    shots.append({"kind": "above_fold", "bytes": await page.screenshot(),
                  "width": vp["width"], "height": vp["height"], "section_index": None})
    # full page (fall back to viewport shot if full_page fails)
    try:
        full = await page.screenshot(full_page=True)
    except Exception:
        full = await page.screenshot()
        full_h = vp["height"]
    else:
        # Record the *real* rendered height so the stored dimension isn't
        # misleadingly clamped to the viewport. Best-effort — fall back to
        # the viewport height if the measure fails.
        try:
            full_h = int(await page.evaluate("document.documentElement.scrollHeight"))
        except Exception:
            full_h = vp["height"]
    shots.append({"kind": "full_page", "bytes": full, "width": vp["width"],
                  "height": min(max_height, full_h) if full_h > 0 else vp["height"],
                  "section_index": None})
    # per-section, until the cap
    for sec in sections:
        if len(shots) >= max_screenshots:
            break
        rect = sec.get("rect") or {}
        y, h = rect.get("y", 0), min(rect.get("height", 0), max_height)
        if h <= 0:
            continue
        try:
            b = await page.screenshot(
                clip={"x": 0, "y": y, "width": vp["width"], "height": h})
        except Exception:
            continue
        shots.append({"kind": "section", "bytes": b, "width": vp["width"],
                      "height": int(h), "section_index": sec.get("index")})
    return shots[:max_screenshots]
