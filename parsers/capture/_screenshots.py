"""Screenshot capture aligned with the mainstream headless-capture playbook.

Modern marketing sites reveal below-fold content on scroll (IntersectionObserver
/ scroll-linked animations, ``loading="lazy"`` images), so a single static
full-page paint captures those sections in their initial hidden state — a solid
background with no content. Following what Puppeteer/Playwright screenshot tools
(and HyperFrames' own capture) do, we:

1. inject CSS that collapses animation/transition timing (final frame, no fade);
2. *prepare* the page — walk it top→bottom to fire reveals + lazy loads, wait for
   fonts and images, hide sticky/fixed chrome (cookie bars, chat widgets);
3. screenshot each section **while it is scrolled into view**, so scroll-linked
   reveals are captured revealed rather than blank.
"""
from __future__ import annotations

# Injected after load, before we walk the page. Collapses animation/transition
# timing so reveal effects settle to their final frame instantly. This does NOT
# stop the reveal from happening — the JS that adds the "visible" class still
# runs; we just drop the tween so a static screenshot is deterministic.
NEUTRALIZE_ANIMATION_CSS = (
    "*,*::before,*::after{"
    "animation-duration:0s!important;animation-delay:0s!important;"
    "transition-duration:0s!important;transition-delay:0s!important;"
    "scroll-behavior:auto!important}"
)

# Runs in-page: walk to fire reveals + lazy loads, await fonts, wait for images
# (capped), then hide sticky/fixed chrome that would otherwise repeat across the
# per-section shots. Real <header>/<nav> is preserved. All best-effort.
_PREPARE_JS = """
async ({stepFrac, stepMs, maxSteps, imgWaitMs}) => {
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  const vh = window.innerHeight || 800;
  const step = Math.max(1, Math.floor(vh * stepFrac));
  let y = 0;
  for (let i = 0; i < maxSteps; i++) {
    window.scrollTo(0, y);
    await sleep(stepMs);
    const h = document.documentElement.scrollHeight;
    y += step;
    if (y >= h) break;
  }
  window.scrollTo(0, document.documentElement.scrollHeight);
  await sleep(stepMs);
  try { if (document.fonts && document.fonts.ready) await document.fonts.ready; } catch (e) {}
  const imgs = Array.from(document.querySelectorAll('img')).filter((im) => !im.complete);
  if (imgs.length) {
    await Promise.race([
      Promise.all(imgs.map((im) => new Promise((r) => { im.onload = r; im.onerror = r; }))),
      sleep(imgWaitMs),
    ]);
  }
  try {
    document.querySelectorAll('*').forEach((el) => {
      const cs = getComputedStyle(el);
      if (cs.position !== 'fixed' && cs.position !== 'sticky') return;
      const tag = el.tagName;
      if (tag === 'HEADER' || tag === 'NAV' || el.closest('header') || el.closest('nav')) return;
      const r = el.getBoundingClientRect();
      if (r.height <= 80 || r.width <= 300) return;
      const z = parseInt(cs.zIndex) || 0;
      if (z > 100 || cs.position === 'fixed') el.style.setProperty('display', 'none', 'important');
    });
  } catch (e) {}
  window.scrollTo(0, 0);
  await sleep(stepMs);
}
"""


async def prepare_page(page, *, step_frac: float, step_ms: int, max_steps: int,
                       img_wait_ms: int) -> None:
    """Ready a freshly-loaded page for capture (see module docstring).

    Best-effort: any failure just leaves the page as-is. Ends scrolled back to
    the top so downstream extraction sees a consistent origin.
    """
    if max_steps <= 0:
        return
    try:
        await page.evaluate(_PREPARE_JS, {"stepFrac": step_frac, "stepMs": step_ms,
                                          "maxSteps": max_steps, "imgWaitMs": img_wait_ms})
    except Exception:
        pass


async def capture_screenshots(page, sections: list[dict], *, max_screenshots: int,
                              max_height: int, section_settle_ms: int = 350) -> list[dict]:
    shots: list[dict] = []
    vp = page.viewport_size or {"width": 1280, "height": 800}
    # above-the-fold (page is at top after prepare)
    try:
        await page.evaluate("() => window.scrollTo(0, 0)")
    except Exception:
        pass
    shots.append({"kind": "above_fold", "bytes": await page.screenshot(),
                  "width": vp["width"], "height": vp["height"], "section_index": None})
    # full page — kept for the dominant-colour cross-check (may still show a band
    # on scroll-linked sites, but it's only sampled for colour). Fall back to a
    # viewport shot if full_page fails.
    try:
        full = await page.screenshot(full_page=True)
    except Exception:
        full = await page.screenshot()
        full_h = vp["height"]
    else:
        try:
            full_h = int(await page.evaluate("document.documentElement.scrollHeight"))
        except Exception:
            full_h = vp["height"]
    shots.append({"kind": "full_page", "bytes": full, "width": vp["width"],
                  "height": min(max_height, full_h) if full_h > 0 else vp["height"],
                  "section_index": None})
    # per-section: scroll each section to the top of the viewport so scroll-linked
    # / reveal-on-scroll content is in its revealed state, then capture the
    # viewport it fills. Viewport-tile capture (vs. clipping a static full-page
    # paint) is what makes below-fold sections render instead of coming out blank.
    for sec in sections:
        if len(shots) >= max_screenshots:
            break
        rect = sec.get("rect") or {}
        y = int(rect.get("y", 0) or 0)
        if int(rect.get("height", 0) or 0) <= 0:
            continue
        try:
            await page.evaluate("(y) => window.scrollTo(0, y)", max(0, y))
            await page.wait_for_timeout(section_settle_ms)
            b = await page.screenshot()
        except Exception:
            continue
        shots.append({"kind": "section", "bytes": b, "width": vp["width"],
                      "height": vp["height"], "section_index": sec.get("index")})
    try:
        await page.evaluate("() => window.scrollTo(0, 0)")
    except Exception:
        pass
    return shots[:max_screenshots]
