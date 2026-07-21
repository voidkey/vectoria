"""Animation catalog + WebGL shader capture (ported from hyperframes'
animationCataloger.ts + index.ts GLSL hook + contentExtractor.ts::detectLibraries).

Two pieces run PRE-navigation (injected via page.add_init_script in the
orchestrator, before page.goto):
  - SHADER_CAPTURE_JS: wraps HTMLCanvasElement.getContext + WebGL shaderSource to
    push captured GLSL into window.__capturedShaders.
  - IO_CAPTURE_JS: monkey-patches IntersectionObserver to record observed target
    selectors/rects into window.__hf_io_targets (scroll-trigger fingerprint).

One piece runs AFTER the page has settled (but before the DOM-mutating page.html
pass): COLLECT_ANIMATIONS_JS reads the Web Animations API, scans computed CSS
animation/transition, reads the IO targets, and counts canvases.

CDP is OPTIONAL and best-effort: start_cdp_animation_capture returns (None, [])
on any failure so a fake/degraded page never aborts the capture.

All in-page JS bodies are guarded (try/catch → empty structures) because a throw
inside page.evaluate would abort the whole capture.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


# ── Pre-nav init scripts ─────────────────────────────────────────────────────

# Wrap getContext + shaderSource to capture GLSL. Guarded — a throw here would
# break every canvas on the page. Dedup by content happens in collect_shaders.
SHADER_CAPTURE_JS = r"""
(() => {
  try {
    if (window.__capturedShaders) return;
    var origGetContext = HTMLCanvasElement.prototype.getContext;
    window.__capturedShaders = [];
    HTMLCanvasElement.prototype.getContext = function(type, attrs) {
      var ctx = origGetContext.call(this, type, attrs);
      try {
        if (ctx && (type === 'webgl' || type === 'webgl2' || type === 'experimental-webgl')) {
          if (ctx.shaderSource && !ctx.__hfHooked) {
            var origShaderSource = ctx.shaderSource.bind(ctx);
            ctx.shaderSource = function(shader, source) {
              try {
                var shaderType = ctx.getShaderParameter(shader, ctx.SHADER_TYPE);
                window.__capturedShaders.push({
                  type: shaderType === ctx.VERTEX_SHADER ? 'vertex' : 'fragment',
                  source: (source || '').slice(0, 5000)
                });
              } catch(e) {}
              return origShaderSource(shader, source);
            };
            ctx.__hfHooked = true;
          }
        }
      } catch(e) {}
      return ctx;
    };
  } catch(e) {}
})();
"""

# Monkey-patch IntersectionObserver to record observed targets (scroll triggers).
IO_CAPTURE_JS = r"""
(() => {
  try {
    if (window.__hf_io_patched) return;
    window.__hf_io_patched = true;
    window.__hf_io_targets = [];
    var OrigIO = window.IntersectionObserver;
    if (!OrigIO) return;
    window.IntersectionObserver = function(callback, options) {
      var observer = new OrigIO(callback, options);
      try {
        var origObserve = observer.observe.bind(observer);
        observer.observe = function(target) {
          try {
            var sel = target.id ? '#' + target.id : target.tagName.toLowerCase();
            if (target.className && typeof target.className === 'string') {
              var cls = Array.from(target.classList).slice(0, 2).join('.');
              if (cls) sel += '.' + cls;
            }
            var rect = target.getBoundingClientRect();
            window.__hf_io_targets.push({
              selector: sel,
              rect: { top: Math.round(rect.top + window.scrollY),
                      height: Math.round(rect.height), width: Math.round(rect.width) }
            });
          } catch(e) {}
          return origObserve(target);
        };
      } catch(e) {}
      return observer;
    };
    window.IntersectionObserver.prototype = OrigIO.prototype;
  } catch(e) {}
})();
"""

# Read the live DOM's animations. Fully guarded: returns empty structures rather
# than throwing (a throw here would abort the capture). Bounded to ≤5000 elements.
COLLECT_ANIMATIONS_JS = r"""
(() => {
  var webAnimations = [];
  var cssDeclarations = [];
  var scrollTargets = [];
  var canvasCount = 0;
  try {
    // 1. Web Animations API
    try {
      var anims = document.getAnimations();
      webAnimations = anims.map(function(anim) {
        var r = { type: anim.constructor.name, playState: anim.playState,
                  animationName: anim.animationName || null };
        var effect = anim.effect;
        if (effect && effect.target) {
          var t = effect.target;
          r.targetSelector = t.id ? '#' + t.id : t.tagName.toLowerCase();
          if (t.className && typeof t.className === 'string') {
            var cls = Array.from(t.classList).slice(0, 3).join('.');
            if (cls) r.targetSelector += '.' + cls;
          }
          try { r.targetRect = t.getBoundingClientRect().toJSON(); } catch(e) {}
        }
        if (effect && typeof effect.getKeyframes === 'function') {
          try { r.keyframes = effect.getKeyframes(); } catch(e) {}
        }
        if (effect && typeof effect.getComputedTiming === 'function') {
          try {
            var timing = effect.getComputedTiming();
            r.timing = { duration: timing.duration, delay: timing.delay,
                         iterations: timing.iterations, easing: timing.easing,
                         direction: timing.direction };
          } catch(e) {}
        }
        return r;
      });
    } catch(e) {}

    // 2. CSS animation/transition scan (≤5000 els)
    try {
      var allEls = document.querySelectorAll('*');
      for (var i = 0; i < allEls.length && i < 5000; i++) {
        var el = allEls[i];
        try {
          var cs = getComputedStyle(el);
          var hasAnim = cs.animationName && cs.animationName !== 'none';
          var hasTrans = cs.transitionProperty && cs.transitionProperty !== 'all' &&
                         cs.transitionProperty !== 'none' && cs.transitionDuration !== '0s';
          if (hasAnim || hasTrans) {
            var sel = el.id ? '#' + el.id : el.tagName.toLowerCase();
            if (el.className && typeof el.className === 'string') {
              var c2 = Array.from(el.classList).slice(0, 2).join('.');
              if (c2) sel += '.' + c2;
            }
            var entry = { selector: sel };
            if (hasAnim) entry.animation = { name: cs.animationName,
              duration: cs.animationDuration, easing: cs.animationTimingFunction };
            if (hasTrans) entry.transition = { property: cs.transitionProperty,
              duration: cs.transitionDuration };
            cssDeclarations.push(entry);
          }
        } catch(e) {}
      }
    } catch(e) {}

    // 3. IO targets (collected by the pre-nav monkey-patch)
    try {
      scrollTargets = (window.__hf_io_targets || []).map(function(t) {
        return { selector: t.selector, rect: t.rect };
      });
    } catch(e) {}

    // 4. Canvas summary
    try { canvasCount = document.querySelectorAll('canvas').length; } catch(e) {}
  } catch(e) {}
  return { webAnimations: webAnimations, cssDeclarations: cssDeclarations,
           scrollTargets: scrollTargets, canvasCount: canvasCount };
})()
"""


async def collect_animation_catalog(page, cdp_entries: list) -> dict:
    """Run COLLECT_ANIMATIONS_JS, attach the CDP entries, and build the summary.

    Returns an AnimationCatalog dict (verbatim field names from types.ts):
    ``webAnimations``, ``cssDeclarations``, ``scrollTargets``, ``cdpAnimations``,
    ``summary``. The in-page JS is guarded, so this is a plain evaluate."""
    result = await page.evaluate(COLLECT_ANIMATIONS_JS) or {}
    web = result.get("webAnimations") or []
    css = result.get("cssDeclarations") or []
    scroll = result.get("scrollTargets") or []
    cdp = list(cdp_entries or [])
    return {
        "webAnimations": web,
        "cssDeclarations": css,
        "scrollTargets": scroll,
        "cdpAnimations": cdp,
        "summary": {
            "webAnimations": len(web),
            "cssDeclarations": len(css),
            "scrollTargets": len(scroll),
            "cdpAnimations": len(cdp),
            "canvases": int(result.get("canvasCount") or 0),
        },
    }


async def collect_shaders(page) -> list[dict]:
    """Read window.__capturedShaders and dedupe by source (matches index.ts)."""
    shaders = await page.evaluate("window.__capturedShaders || []")
    if not isinstance(shaders, list):
        return []
    seen: set = set()
    unique: list[dict] = []
    for s in shaders:
        if not isinstance(s, dict):
            continue
        src = s.get("source")
        if src in seen:
            continue
        seen.add(src)
        unique.append(s)
    return unique


async def start_cdp_animation_capture(page):
    """Best-effort CDP Animation-domain capture.

    Returns ``(session, entries)`` where ``entries`` is a mutable list that the
    ``Animation.animationStarted`` handler appends CdpAnimationEntry dicts to. On
    ANY failure (no real CDP, e.g. a fake test page) returns ``(None, [])`` so the
    caller degrades cleanly to ``cdpAnimations: []`` — never raises, never hangs."""
    entries: list[dict] = []
    try:
        session = await page.context.new_cdp_session(page)
    except Exception:
        logger.info("capture: CDP animation session unavailable — degrading", exc_info=True)
        return None, entries

    def _on_started(event: Any) -> None:
        try:
            anim = (event or {}).get("animation") or {}
            source = anim.get("source") or {}
            entries.append({
                "id": anim.get("id", ""),
                "name": anim.get("name") or "",
                "type": anim.get("type", ""),
                "duration": source.get("duration"),
                "delay": source.get("delay"),
            })
        except Exception:
            pass

    try:
        session.on("Animation.animationStarted", _on_started)
        await session.send("Animation.enable")
    except Exception:
        logger.info("capture: CDP Animation.enable failed — degrading", exc_info=True)
        try:
            await session.detach()
        except Exception:
            pass
        return None, entries
    return session, entries


# ── Library detection (ported from contentExtractor.ts::detectLibraries) ──────

def detect_libraries(raw_libs: list, shaders: list, dom_fingerprints: dict) -> list[str]:
    """Merge script-src library sniff + DOM fingerprints + WebGL shader
    fingerprints into a deduplicated library-name list.

    ``raw_libs``: the extractor's cheap script-src hit list (passed through).
    ``dom_fingerprints``: booleans collected in-page (window globals + DOM probes).
    ``shaders``: captured GLSL dicts ({type, source}); their combined source is
    fingerprinted for Three.js / PixiJS / Babylon.js (uniforms survive bundling)."""
    libs: list[str] = []

    def add(name: str) -> None:
        if name and name not in libs:
            libs.append(name)

    # 1. Cheap script-src hits from the extractor, passed through verbatim.
    for lib in raw_libs or []:
        add(lib)

    # 2. Window globals + DOM fingerprints (collected in-page as booleans).
    fp = dom_fingerprints or {}
    if fp.get("gsap"):
        add("GSAP")
    if fp.get("scrollTrigger"):
        add("GSAP ScrollTrigger")
    if fp.get("three"):
        add("Three.js")
    if fp.get("pixi"):
        add("PixiJS")
    if fp.get("babylon"):
        add("Babylon.js")
    if fp.get("lottie"):
        add("Lottie")
    if fp.get("nextData") or fp.get("nextRoot"):
        add("Next.js")
    if fp.get("nuxt") or fp.get("nuxtRoot"):
        add("Nuxt")
    if fp.get("webflow"):
        add("Webflow")
    if fp.get("rive"):
        add("Rive")
    if fp.get("react"):
        add("React")
    if fp.get("svelte"):
        add("Svelte")
    if fp.get("tailwind"):
        add("Tailwind CSS")
    if fp.get("framerMotion"):
        add("Framer Motion")

    # 3. Shader fingerprinting — infer the WebGL framework from captured GLSL.
    try:
        shader_list = shaders or []
        if shader_list:
            all_source = "\n".join((s.get("source") or "") for s in shader_list
                                   if isinstance(s, dict))
            add("WebGL")
            if "modelViewMatrix" in all_source and "projectionMatrix" in all_source:
                add("Three.js (confirmed via shaders)")
            elif ("vTextureCoord" in all_source and "uSampler" in all_source
                  and "modelViewMatrix" not in all_source):
                add("PixiJS (confirmed via shaders)")
            elif "viewProjection" in all_source and "world" in all_source:
                add("Babylon.js (confirmed via shaders)")
    except Exception:
        logger.info("capture: shader fingerprinting failed", exc_info=True)

    return libs
