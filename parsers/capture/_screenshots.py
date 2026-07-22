"""Screenshot capture — 1:1 with hyperframes ``screenshotCapture.ts``.

Modern marketing sites reveal below-fold content on scroll (IntersectionObserver
/ scroll-linked animations, ``loading="lazy"`` images), so a single static
full-page paint captures those sections in their initial hidden state. Matching
the reference ``captureScrollScreenshots``, we:

1. inject CSS that collapses animation/transition timing (final frame, no fade);
2. *prepare* the page — walk it top→bottom to fire reveals + lazy loads, wait for
   fonts and images, dismiss cookie/consent, hide sticky/fixed chrome (z-index>100);
3. step down the page in 70%-of-viewport increments and screenshot the viewport at
   each stop, naming each by its scroll PERCENTAGE (``scroll-000.png`` …
   ``scroll-100.png``) — natural browsing state with reveal animations fired.

One extra full-page screenshot is taken for the dominant-colour cross-check only;
it is NOT exported (the reference writes no full-page.png).
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

# Runs in-page: dismiss cookie/consent/GDPR banners before we screenshot, so a
# consent modal can't block the shot. Scoped to cookie/consent/gdpr *containers*
# (ported from hyperframes screenshotCapture.ts) and only clicks a VISIBLE
# accept/agree button inside such a container — never reject/manage/random
# buttons elsewhere on the page. Accept-text set = the reference English terms
# plus common zh terms (同意 / 接受 / 同意并继续) as a documented vectoria
# extension. Best-effort: everything is guarded so a JS error can't abort the
# capture (this runs inside a bare page.evaluate the caller does not wrap).
DISMISS_CONSENT_JS = r"""
() => {
  try {
    // Cookie/consent/gdpr container selectors (scope so we never touch an
    // unrelated "Accept invitation"/"Accept terms" button on the page).
    const containerSel = [
      '[id*="cookie" i]', '[class*="cookie" i]',
      '[id*="consent" i]', '[class*="consent" i]',
      '[id*="gdpr" i]', '[class*="gdpr" i]',
      '[aria-label*="cookie" i]', '[aria-label*="consent" i]',
    ].join(',');
    // Accept-text: reference English set + zh extension (同意/接受/同意并继续).
    const acceptRe = /accept|agree|got it|allow|consent|同意|接受|同意并继续/i;
    // Never click these even if the text also matches.
    const rejectRe = /reject|decline|manage|settings|preference/i;
    const isVisible = (el) => {
      try {
        const cs = getComputedStyle(el);
        if (cs.display === 'none' || cs.visibility === 'hidden') return false;
        if (parseFloat(cs.opacity || '1') === 0) return false;
        const r = el.getBoundingClientRect();
        return r.width > 0 && r.height > 0;
      } catch (e) { return false; }
    };
    const containers = Array.from(document.querySelectorAll(containerSel)).slice(0, 50);
    for (const container of containers) {
      let clicked = false;
      const btns = container.querySelectorAll('button,[role="button"],a');
      for (const btn of btns) {
        try {
          const txt = (btn.textContent || '').trim();
          if (!txt || txt.length > 40) continue;
          if (rejectRe.test(txt)) continue;
          if (!acceptRe.test(txt)) continue;
          if (!isVisible(btn)) continue;
          btn.click();
          clicked = true;
          break;
        } catch (e) { /* ignore this button */ }
      }
      if (clicked) break;  // one accept per pass is enough
    }
  } catch (e) { /* best-effort: never throw */ }
}
"""


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
  // Hide fixed/sticky overlays (cookie bars, chat widgets) that aren't header/
  // nav, matching hyperframes screenshotCapture.ts: a bounded TreeWalker
  // (<=5000 nodes) with a cheap getBoundingClientRect size prefilter before the
  // expensive getComputedStyle, hiding only z-index>100 fixed/sticky elements.
  try {
    const SCAN_CAP = 5000;
    const minWidth = (window.innerWidth || 1280) * 0.3;
    const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_ELEMENT);
    let visited = 0;
    let node = walker.nextNode();
    while (node && visited < SCAN_CAP) {
      visited++;
      const el = node;
      const r = el.getBoundingClientRect();
      // Cheap viewport-size filter first — skips tiny/hidden/off-screen elements
      // without touching getComputedStyle.
      if (r.height > 80 && r.width > minWidth) {
        const tag = el.tagName;
        if (tag !== 'HEADER' && tag !== 'NAV' && !el.closest('header') && !el.closest('nav')) {
          const cs = getComputedStyle(el);
          if ((cs.position === 'fixed' || cs.position === 'sticky') &&
              cs.zIndex !== 'auto' && parseInt(cs.zIndex) > 100) {
            el.style.setProperty('display', 'none', 'important');
          }
        }
      }
      node = walker.nextNode();
    }
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
    # Best-effort consent dismissal first, before the scroll-walk, so a cookie /
    # GDPR modal can't block the shots. Isolated try/except so a failure here
    # never aborts the walk (or the capture).
    try:
        await page.evaluate(DISMISS_CONSENT_JS)
    except Exception:
        pass
    try:
        await page.evaluate(_PREPARE_JS, {"stepFrac": step_frac, "stepMs": step_ms,
                                          "maxSteps": max_steps, "imgWaitMs": img_wait_ms})
    except Exception:
        pass


async def capture_screenshots(page, *, max_screenshots: int, max_height: int,
                              settle_ms: int = 400) -> list[dict]:
    """Reference scroll-position capture (screenshotCapture.ts::captureScrollScreenshots).

    Step down the page in 70%-of-viewport increments (30% overlap), screenshot the
    viewport at each stop, and label each by its scroll PERCENTAGE — so the exported
    files are ``scroll-000.png`` (top) … ``scroll-100.png`` (bottom), exactly as
    hyperframes. Always includes the top and the bottom; downsamples to
    ``max_screenshots`` positions on very long pages (keeping first + last, striding
    the middle); collapses positions that round to the same percentage to one shot
    (the reference overwrites the same filename). Sticky/fixed overlays were already
    hidden and cookie/consent dismissed by ``prepare_page``, so shots show the page in
    its natural scrolled state with reveal animations fired.

    ALSO captures one ``kind="full_page"`` shot — NOT exported (the reference writes no
    full-page.png), retained only for the dominant-colour cross-check in
    ``_build_layout_tokens``. Scroll shots carry ``kind="scroll"`` + ``pct``. Every
    step is best-effort; a failure at one position just skips it."""
    vp = page.viewport_size or {"width": 1280, "height": 800}
    vw, vh = vp["width"], vp["height"]
    shots: list[dict] = []

    if max_screenshots <= 0:
        return shots        # screenshots disabled — capture nothing (not even full_page)

    try:
        scroll_h = int(await page.evaluate(
            "Math.max(document.body.scrollHeight, document.documentElement.scrollHeight)"))
    except Exception:
        scroll_h = vh
    try:
        vph = int(await page.evaluate("window.innerHeight")) or vh
    except Exception:
        vph = vh

    # Scroll positions: 0, then +70% viewport until the bottom, always ending at the
    # true bottom (scroll_h - viewport).
    step = max(1, int(vph * 0.7))
    positions = [0]
    y = step
    while y < scroll_h - vph:
        positions.append(y)
        y += step
    last = max(0, scroll_h - vph)
    if positions[-1] != last:
        positions.append(last)

    # Downsample to the cap (keep first + last, stride the middle).
    cap = max(1, max_screenshots)
    if len(positions) > cap:
        sampled = [positions[0]]
        stride = (len(positions) - 1) / (cap - 1) if cap > 1 else 1.0
        for i in range(1, cap - 1):
            sampled.append(positions[round(i * stride)])
        sampled.append(positions[-1])
        positions = sampled

    span = max(1, scroll_h - vph)
    last_pct = None
    for pos in positions:
        pct = min(100, round(pos / span * 100))
        if pct == last_pct:          # same percentage -> one file (reference overwrites)
            continue
        last_pct = pct
        try:
            await page.evaluate(f"window.scrollTo(0, {pos})")
            await page.wait_for_timeout(settle_ms)
            b = await page.screenshot()
        except Exception:
            continue
        shots.append({"kind": "scroll", "bytes": b, "pct": pct,
                      "width": vw, "height": vph, "section_index": None})

    try:
        await page.evaluate("window.scrollTo(0, 0)")
    except Exception:
        pass

    # Internal full_page shot for the dominant-colour cross-check only (never exported).
    try:
        full = await page.screenshot(full_page=True)
        try:
            full_h = int(await page.evaluate("document.documentElement.scrollHeight"))
        except Exception:
            full_h = vh
    except Exception:
        full, full_h = None, vh
    if full:
        shots.append({"kind": "full_page", "bytes": full, "pct": None, "width": vw,
                      "height": min(max_height, full_h) if full_h > 0 else vh,
                      "section_index": None})
    return shots
