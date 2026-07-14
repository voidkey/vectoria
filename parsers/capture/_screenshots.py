"""Screenshot capture: above-fold, full-page (height-clamped), sections."""
from __future__ import annotations


async def autoscroll_page(page, *, step_frac: float, step_ms: int,
                          max_steps: int) -> None:
    """Walk the page top→bottom then back to top before capture.

    Modern marketing sites reveal below-fold content on scroll (IntersectionObserver
    animations, ``loading="lazy"`` images). Without walking the page first, a
    ``full_page`` screenshot renders those sections in their initial hidden state —
    a solid-colour background with no content. We step by a viewport fraction,
    pausing each step so reveals fire and lazy assets fetch, re-reading the height
    each step since it can grow as content loads. Best-effort: any failure just
    leaves the page as-is. Returns with the page scrolled back to the top so
    downstream extraction sees a consistent origin.
    """
    if max_steps <= 0:
        return
    try:
        vp_h = (page.viewport_size or {}).get("height", 800)
        step = max(1, int(vp_h * step_frac))
        y = 0
        for _ in range(max_steps):
            await page.evaluate("(y) => window.scrollTo(0, y)", y)
            await page.wait_for_timeout(step_ms)
            total = int(await page.evaluate("document.documentElement.scrollHeight"))
            y += step
            if y >= total:
                break
        await page.evaluate("() => window.scrollTo(0, 0)")
        await page.wait_for_timeout(step_ms)
    except Exception:
        pass


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
