"""Site media catalog — images / videos / backgrounds / icons / fonts as URL-only refs.

Ported from the reference asset cataloger
and its DOM video descriptor scan so the OUTPUT SHAPE matches
the reference (zero schema drift): each catalog item is a ``CatalogedAsset`` dict, each
video a ``VideoDescriptor`` dict.

URL-only by design — nothing is downloaded here. Downloading (and preview frames,
network-intercepted streaming URLs, GIF header parsing) is downstream's call, mirroring
the reference split between the catalog pass and the download pass. Absolutizing,
tracking-pixel filtering, dedup and srcset-variant collapse match the reference exactly.
"""
from __future__ import annotations

import io
import json
import logging
import zipfile
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

logger = logging.getLogger(__name__)

# A dotLottie file is a ZIP archive — same local-file-header magic bytes.
_ZIP_MAGIC = b"PK\x03\x04"
# Lottie JSON structure keys (the reference validates by these). We require the
# core set so a random JSON download isn't mistaken for an animation.
_LOTTIE_KEYS = ("v", "ip", "op", "layers", "w", "h", "fr")

# Default per-member uncompressed cap for a single dotLottie animation JSON, used
# when the caller doesn't thread one in. Matches capture_max_lottie_bytes (2MB) —
# a generous bound for one lottie JSON. Guards a zip-decompression bomb: the
# COMPRESSED body is already capped upstream (fetch_asset_bytes), but a crafted
# archive could inflate a ≤25MB body to gigabytes at zf.read() time.
_DEFAULT_MAX_LOTTIE_UNCOMPRESSED = 2 * 1024 * 1024


def _extract_dotlottie_json(
    data: bytes, *, max_uncompressed: int = _DEFAULT_MAX_LOTTIE_UNCOMPRESSED
) -> bytes | None:
    """Return the first animation JSON out of a dotLottie ZIP, or None.

    dotLottie is a ZIP; the animation JSON lives under ``animations/`` (v1) or
    ``a/`` (v2). Stdlib ``zipfile`` only — no adm-zip / third-party dep. Pure.

    Guards against a zip-decompression bomb: each candidate member's declared
    uncompressed size (``ZipInfo.file_size``, read from the central directory
    without decompressing) is checked against ``max_uncompressed`` and SKIPPED
    when it exceeds the cap — so a ≤25MB fetched body can't inflate to gigabytes
    at ``zf.read()`` time. Returns None if no member is both valid and within the
    cap (existing skip behavior)."""
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            infos = [i for i in zf.infolist()
                     if (i.filename.startswith("animations/")
                         or i.filename.startswith("a/"))
                     and i.filename.endswith(".json")]
            for info in sorted(infos, key=lambda i: i.filename):
                if info.file_size > max_uncompressed:
                    logger.info(
                        "capture: dotlottie member %s over uncompressed cap "
                        "(%d > %d), skipped", info.filename, info.file_size,
                        max_uncompressed)
                    continue
                try:
                    return zf.read(info)
                except Exception:
                    continue
    except Exception:
        return None
    return None


def _valid_lottie(data: bytes) -> dict | None:
    """Parse ``data`` as JSON and return it only when it has the core lottie keys
    (v/ip/op/layers/w/h/fr). Returns the parsed dict, or None. Pure."""
    try:
        parsed = json.loads(data)
    except Exception:
        return None
    if not isinstance(parsed, dict):
        return None
    if not all(k in parsed for k in _LOTTIE_KEYS):
        return None
    return parsed


def lottie_json_from_bytes(
    url: str, data: bytes, *,
    max_uncompressed: int = _DEFAULT_MAX_LOTTIE_UNCOMPRESSED,
) -> tuple[bytes, dict] | None:
    """Turn a fetched lottie body into (json_bytes, parsed_dict).

    Unzips a dotLottie ZIP (magic ``PK\\x03\\x04`` or a ``.lottie`` URL) to its
    animation JSON first, then validates lottie structure. Returns None when the
    body isn't a valid lottie. ``max_uncompressed`` bounds a dotLottie member's
    decompressed size (zip-bomb guard; see ``_extract_dotlottie_json``). Pure (no
    I/O — the fetch happens upstream)."""
    body = data
    if data[:4] == _ZIP_MAGIC or (url or "").lower().endswith(".lottie"):
        extracted = _extract_dotlottie_json(data, max_uncompressed=max_uncompressed)
        if extracted is None:
            return None
        body = extracted
    parsed = _valid_lottie(body)
    if parsed is None:
        return None
    return body, parsed


def lottie_manifest_entry(name: str, url: str, parsed: dict) -> dict:
    """Build one lottie-manifest entry from a parsed lottie dict.

    Reference manifest shape (the reference lottie-preview renderer) is exactly
    ``{file, preview, name, width, height, duration, frameRate, layers}`` — no
    ``url``. ``preview`` is added later by the preview pass. ``url`` stays a
    parameter (the caller passes the source URL for AssetRef provenance) but is
    intentionally NOT serialized into the manifest, to stay 1:1. Pure."""
    fr = parsed.get("fr") or 30
    ip = parsed.get("ip") or 0
    op = parsed.get("op") or 0
    try:
        duration = round(((op - ip) / fr) * 10) / 10 if fr else 0
    except Exception:
        duration = 0
    return {
        "file": f"assets/lottie/{name}",
        "name": parsed.get("nm") or name,
        "width": parsed.get("w") or 0,
        "height": parsed.get("h") or 0,
        "duration": duration,
        "frameRate": fr,
        "layers": len(parsed.get("layers") or []),
    }


# Real browser JS (an IIFE returning CatalogedAsset[]). Ported verbatim from
# the reference asset cataloger's page.evaluate body with the TS-template escaping
# unwound (\\s -> \s, \\/ -> /, \\( -> \( ...) so it runs unchanged in Playwright.
# The GIF byte-header annotation (a ranged fetch in the reference) is intentionally
# dropped — it is a download, which is out of scope here; ``notes`` stays null.
ASSET_CATALOG_JS = r"""(() => {
  var assetMap = {};

  function getElementContext(el) {
    var ctx = {};
    var desc = el.alt || el.getAttribute('aria-label') || el.getAttribute('title') || '';
    var fig = el.closest('figure');
    if (fig) {
      var cap = fig.querySelector('figcaption');
      if (cap) desc = desc || cap.textContent.trim().slice(0, 100);
    }
    var ariaBy = el.getAttribute('aria-describedby');
    if (ariaBy) {
      var descEl = document.getElementById(ariaBy);
      if (descEl) desc = desc || descEl.textContent.trim().slice(0, 100);
    }
    if (desc) ctx.description = desc.slice(0, 150);
    var section = el.closest('section, article, header, footer, main, [class*="hero"], [class*="banner"], [class*="feature"]');
    if (section) {
      var heading = section.querySelector('h1, h2, h3, h4');
      if (heading) ctx.nearestHeading = heading.textContent.trim().slice(0, 80);
      ctx.sectionClasses = (section.className || '').toString().slice(0, 120);
    }
    try {
      var rect = el.getBoundingClientRect();
      ctx.aboveFold = rect.top < window.innerHeight;
    } catch(e) {}
    ctx.inBanner = el.closest('header, nav, [role="banner"]') !== null;
    var homeAnchor = el.closest('a[href]');
    if (homeAnchor) {
      var aHref = homeAnchor.getAttribute('href') || '';
      ctx.inHomeLink = aHref === '/' || aHref === '#' || aHref === './' ||
                       /^https?:\/\/[^/]+\/?$/.test(aHref);
    }
    var titleParts = (document.title || '').split(/[-|—:]/);
    if (desc) {
      for (var ti = 0; ti < titleParts.length; ti++) {
        var part = titleParts[ti].trim();
        if (part.length > 1 && part.length < 30 &&
            desc.toLowerCase().indexOf(part.toLowerCase()) !== -1) {
          ctx.matchesTitleBrand = true;
          break;
        }
      }
    }
    return ctx;
  }

  function add(url, type, context, notes, richCtx) {
    if (!url || url === '' || url.startsWith('data:') || url.startsWith('blob:') || url === 'about:blank') return;
    try { url = new URL(url, document.baseURI).href; } catch(e) { return; }
    if (url.length > 50000) return;
    var lurl = url.toLowerCase();
    if (lurl.indexOf('analytics.') > -1 || lurl.indexOf('adsct') > -1 || lurl.indexOf('pixel.') > -1 || lurl.indexOf('tracking.') > -1 || lurl.indexOf('pdscrb.') > -1 || lurl.indexOf('doubleclick') > -1 || lurl.indexOf('googlesyndication') > -1 || lurl.indexOf('facebook.com/tr') > -1 || lurl.indexOf('bat.bing') > -1 || lurl.indexOf('clarity.ms') > -1) return;
    if (lurl.indexOf('bci=') > -1 && lurl.indexOf('twpid=') > -1) return;
    if (lurl.indexOf('cachebust=') > -1 || lurl.indexOf('event_id=') > -1) return;
    if (url.indexOf('.css#') > -1) return;
    if (url.indexOf('.css%23') > -1) return;
    try { var parsed = new URL(url); if (parsed.hash && parsed.pathname.length <= 1) return; } catch(e2) {}

    if (!assetMap[url]) {
      assetMap[url] = { url: url, type: type, contexts: [], notes: null };
    }
    var entry = assetMap[url];
    if (entry.contexts.indexOf(context) === -1) {
      entry.contexts.push(context);
    }
    if (notes && !entry.notes) {
      entry.notes = notes;
    }
    if (richCtx) {
      if (richCtx.description && !entry.description) entry.description = richCtx.description;
      if (richCtx.nearestHeading && !entry.nearestHeading) entry.nearestHeading = richCtx.nearestHeading;
      if (richCtx.sectionClasses && !entry.sectionClasses) entry.sectionClasses = richCtx.sectionClasses;
      if (richCtx.aboveFold !== undefined && entry.aboveFold === undefined) entry.aboveFold = richCtx.aboveFold;
      if (richCtx.inBanner) entry.inBanner = true;
      if (richCtx.inHomeLink) entry.inHomeLink = true;
      if (richCtx.matchesTitleBrand) entry.matchesTitleBrand = true;
    }
  }

  document.querySelectorAll('img[src]').forEach(function(img) {
    var notes = img.alt || img.getAttribute('aria-label') || null;
    var ctx = getElementContext(img);
    add(img.src, 'Image', 'img[src]', notes, ctx);
    if (img.srcset) {
      img.srcset.split(',').forEach(function(entry) {
        var u = entry.trim().split(/\s+/)[0];
        if (u) add(u, 'Image', 'img[srcset]', notes, ctx);
      });
    }
  });

  document.querySelectorAll('img[data-src], img[data-lazy-src], img[data-original], [data-background-image]').forEach(function(el) {
    var dataSrc = el.getAttribute('data-src') || el.getAttribute('data-lazy-src') || el.getAttribute('data-original') || el.getAttribute('data-background-image');
    if (dataSrc) add(dataSrc, 'Image', 'data-src', el.alt || el.getAttribute('aria-label') || null, getElementContext(el));
  });

  document.querySelectorAll('div, section, [class*="hero"], [class*="card"], [class*="image"], [data-framer-background]').forEach(function(el) {
    var bg = getComputedStyle(el).backgroundImage;
    if (bg && bg !== 'none') {
      var match = bg.match(/url\(["']?(https?:\/\/[^"')]+)["']?\)/);
      if (match && match[1]) {
        add(match[1], 'Background', 'css url()', el.getAttribute('aria-label') || null, getElementContext(el));
      }
    }
  });

  document.querySelectorAll('source[srcset]').forEach(function(src) {
    src.srcset.split(',').forEach(function(entry) {
      var u = entry.trim().split(/\s+/)[0];
      if (u) add(u, 'Image', 'source[srcset]', null);
    });
  });

  document.querySelectorAll('video[src]').forEach(function(v) {
    add(v.src, 'Video', 'video[src]', null);
  });
  document.querySelectorAll('video source[src]').forEach(function(s) {
    add(s.src, 'Video', 'video source[src]', null);
  });
  document.querySelectorAll('video[poster]').forEach(function(v) {
    add(v.poster, 'Image', 'video[poster]', null);
  });

  document.querySelectorAll('link[rel]').forEach(function(link) {
    var rel = link.rel.toLowerCase();
    var href = link.href;
    if (!href) return;
    if (rel.includes('preload')) {
      var asType = link.getAttribute('as') || '';
      if (asType === 'font') add(href, 'Font', 'link[rel="preload"]', null);
      else if (asType === 'image') add(href, 'Image', 'link[rel="preload"]', null);
      else if (asType === 'video') add(href, 'Video', 'link[rel="preload"]', null);
      else if (asType === 'style') add(href, 'Other', 'link[rel="preload"]', null);
      else add(href, 'Other', 'link[rel="preload"]', null);
    }
    if (rel.includes('icon')) add(href, 'Icon', 'link[rel="' + rel + '"]', null);
    if (rel === 'apple-touch-icon') add(href, 'Icon', 'link[rel="apple-touch-icon"]', null);
  });

  document.querySelectorAll('meta[property="og:image"], meta[content][name="twitter:image"]').forEach(function(m) {
    var content = m.getAttribute('content');
    if (content) {
      var prop = m.getAttribute('property') || m.getAttribute('name') || '';
      add(content, 'Image', 'meta[' + prop + ']', null);
    }
  });

  try {
    for (var i = 0; i < document.styleSheets.length; i++) {
      try {
        var sheet = document.styleSheets[i];
        var rules = sheet.cssRules || sheet.rules;
        if (!rules) continue;
        for (var j = 0; j < rules.length; j++) {
          var rule = rules[j];
          var cssText = rule.cssText || '';
          var urlMatches = cssText.match(/url\(["']?([^"')]+)["']?\)/g);
          if (urlMatches) {
            urlMatches.forEach(function(m) {
              var u = m.replace(/url\(["']?/, '').replace(/["']?\)/, '');
              if (u.startsWith('data:')) return;
              if (/\.(woff2?|ttf|otf|eot)$/i.test(u)) {
                add(u, 'Font', 'css url()', null);
              } else if (/\.(png|jpg|jpeg|gif|webp|avif|svg)$/i.test(u)) {
                add(u, 'Background', 'css url()', null);
              } else {
                add(u, 'Other', 'css url()', null);
              }
            });
          }
        }
      } catch(e) { /* cross-origin stylesheet */ }
    }
  } catch(e) {}

  document.querySelectorAll('[style]').forEach(function(el) {
    var style = el.getAttribute('style') || '';
    var urlMatches = style.match(/url\(["']?([^"')]+)["']?\)/g);
    if (urlMatches) {
      urlMatches.forEach(function(m) {
        var u = m.replace(/url\(["']?/, '').replace(/["']?\)/, '');
        if (u.startsWith('data:')) return;
        if (/\.(woff2?|ttf|otf|eot)$/i.test(u)) {
          add(u, 'Font', 'html inline style url()', null);
        } else {
          add(u, 'Other', 'html inline style url()', null);
        }
      });
    }
  });

  return Object.values(assetMap);
})()"""


# DOM-only video descriptors (VideoDescriptor shape). No network interception, no
# preview frames, no download — those are the reference's download-pass concerns.
VIDEO_DESCRIPTORS_JS = r"""(() => {
  function absUrl(u) { try { return new URL(u, document.baseURI).href; } catch(e) { return ''; } }
  function nearestHeading(el) {
    var cur = el, hops = 0;
    while (cur && hops < 8) {
      var h = cur.querySelector ? cur.querySelector('h1, h2, h3, h4') : null;
      if (h && h.textContent) return h.textContent.trim().slice(0, 100);
      cur = cur.parentElement; hops++;
    }
    return '';
  }
  function nearestCaption(el) {
    var cur = el, hops = 0;
    while (cur && hops < 5) {
      var c = cur.querySelector ? cur.querySelector('figcaption, [class*="caption"], p') : null;
      if (c && c.textContent) return c.textContent.trim().slice(0, 200);
      cur = cur.parentElement; hops++;
    }
    return '';
  }
  var out = [];
  var seen = {};
  Array.prototype.forEach.call(document.querySelectorAll('video'), function(v) {
    var src = v.src || v.currentSrc || '';
    if (!src) { var s = v.querySelector('source'); if (s) src = s.src || s.getAttribute('src') || ''; }
    if (!src) return;  // guard before absUrl: '' would resolve to the page URL
    src = absUrl(src);
    if (!src || !src.startsWith('http')) return;
    if (seen[src]) return;
    var rect = v.getBoundingClientRect();
    if (rect.width < 10 || rect.height < 10) return;  // skip hidden/decorative videos
    seen[src] = true;
    var wrap = v.closest('figure, section, article, div') || v.parentElement || v;
    var filename = '';
    try { var p = new URL(src).pathname.split('/'); filename = p[p.length - 1] || ''; } catch(e) {}
    out.push({
      src: src,
      width: Math.round(rect.width),
      height: Math.round(rect.height),
      sourceWidth: v.videoWidth || 0,
      sourceHeight: v.videoHeight || 0,
      top: Math.round(rect.top),
      left: Math.round(rect.left),
      heading: nearestHeading(wrap),
      caption: nearestCaption(wrap),
      ariaLabel: v.getAttribute('aria-label') || v.getAttribute('title') ||
                 (wrap.getAttribute ? (wrap.getAttribute('aria-label') || '') : ''),
      filename: filename
    });
  });
  return out;
})()"""


# lottie-web from a CDN — the render page injects this to rasterize a mid-frame.
# CDN may be blocked in the server env; the whole preview pass is best-effort and
# LOGS on failure (never aborts the capture). Mirrors renderLottiePreviews.
_LOTTIE_WEB_CDN = "https://cdnjs.cloudflare.com/ajax/libs/lottie-web/5.12.2/lottie.min.js"
_LOTTIE_PREVIEW_HTML = (
    "<!DOCTYPE html><html><head>"
    f'<script src="{_LOTTIE_WEB_CDN}"></script>'
    "<style>*{margin:0;padding:0;background:transparent}"
    "#c{width:400px;height:400px}</style>"
    "</head><body><div id=\"c\"></div></body></html>")

# In-page loader: lottie.loadAnimation(animationData) + goToAndStop(midFrame).
# String expression (no arrow-fn __name injection). Sets window.__READY on load.
_LOTTIE_LOAD_JS = """(args) => {
  var data = args[0], frame = args[1];
  window.__READY = false;
  try {
    var a = window.lottie.loadAnimation({
      container: document.getElementById('c'), renderer: 'svg',
      loop: false, autoplay: false, animationData: data });
    a.addEventListener('DOMLoaded', function() {
      try { a.goToAndStop(frame, true); } catch(e) {}
      window.__READY = true; });
  } catch(e) { window.__READY = true; }
}"""


async def render_lottie_previews(page, entries: list, *, max_bytes: int) -> dict:
    """Best-effort mid-frame preview PNGs for parsed lottie entries.

    Port of the reference lottie-preview renderer: for each entry (carrying its
    parsed lottie under ``_parsed``), inject lottie-web into a shell page, load the
    animation data, seek to ~30% of (op-ip), and screenshot a transparent PNG.
    Returns ``{entry_name: png_bytes}``. Skips a lottie whose JSON is larger than
    ``max_bytes`` (CDP message-limit guard). The WHOLE pass is wrapped so a blocked
    CDN / render failure LOGS ("lottie preview render skipped: …") and leaves the
    result empty — a failure never aborts the capture. Requires a live browser page
    with set_content/evaluate/screenshot; a fake/limited page degrades cleanly."""
    out: dict = {}
    if not entries:
        return out
    try:
        await page.set_content(_LOTTIE_PREVIEW_HTML, wait_until="load", timeout=10000)
    except Exception:
        logger.info("lottie preview render skipped: shell page load failed", exc_info=True)
        return out
    for entry in entries:
        parsed = entry.get("_parsed")
        name = entry.get("file", "").rsplit("/", 1)[-1] or entry.get("name", "")
        if not parsed or not name:
            continue
        try:
            raw_len = len(json.dumps(parsed).encode())
        except Exception:
            continue
        if raw_len > max_bytes:
            logger.info("lottie preview render skipped: %s over %d bytes", name, max_bytes)
            continue
        ip = parsed.get("ip") or 0
        op = parsed.get("op") or 0
        mid_frame = int((op - ip) * 0.3)
        try:
            await page.evaluate(_LOTTIE_LOAD_JS, [parsed, mid_frame])
            try:
                await page.wait_for_function("() => window.__READY === true", timeout=5000)
            except Exception:
                pass
            png = await page.screenshot(omit_background=True)
        except Exception:
            logger.info("lottie preview render skipped: %s render failed", name, exc_info=True)
            continue
        if png:
            out[name] = png
    return out


# In-page SVG rasterizer: inject the markup, wait for an <img> load, read a
# 200×200 PNG data-URL off a canvas. String expression (no arrow __name injection).
# Returns the base64 PNG (sans data-URL prefix) or "" on any failure. Runs entirely
# in-page so a tainted-canvas / load failure degrades to "" rather than throwing.
_SVG_RASTER_JS = """async (args) => {
  var markup = args[0], size = args[1];
  try {
    var blob = new Blob([markup], {type: 'image/svg+xml'});
    var url = URL.createObjectURL(blob);
    var img = new Image();
    var loaded = new Promise(function(res) {
      img.onload = function(){ res(true); };
      img.onerror = function(){ res(false); };
    });
    img.src = url;
    var ok = await Promise.race([loaded,
      new Promise(function(r){ setTimeout(function(){ r(false); }, 3000); })]);
    if (!ok) { URL.revokeObjectURL(url); return ''; }
    var cvs = document.createElement('canvas');
    cvs.width = size; cvs.height = size;
    var ctx = cvs.getContext('2d');
    ctx.fillStyle = '#f5f5f5';
    ctx.fillRect(0, 0, size, size);
    var iw = img.naturalWidth || img.width || size;
    var ih = img.naturalHeight || img.height || size;
    var scale = Math.min(size / iw, size / ih);
    var dw = Math.max(1, iw * scale), dh = Math.max(1, ih * scale);
    ctx.drawImage(img, (size - dw) / 2, (size - dh) / 2, dw, dh);
    URL.revokeObjectURL(url);
    return cvs.toDataURL('image/png').split(',')[1] || '';
  } catch(e) { return ''; }
}"""


async def rasterize_svgs(page, svgs: list, *, cap: int, thumb: int = 200) -> list:
    """Best-effort rasterize captured SVG markup to PNG thumbnails via the browser.

    Port of the reference SVG contact-sheet's per-SVG raster step, done in the
    live page (Pillow has no SVG rasterizer + we add no cairosvg): each ``svgs[i]``
    (an extractor SVG dict with ``outerHTML``) is drawn onto a ``thumb``×``thumb``
    canvas and read back as a PNG. Deduped by a basename derived from the SVG's label/
    logo flag/index. Returns ``[(png_bytes, basename)]`` for the sheet builder. The
    WHOLE pass is guarded so a blocked/tainted-canvas raster LOGS ("svg contact sheet
    skipped: …") and returns [] — never aborts. Bounded by ``cap``."""
    import base64

    out: list = []
    seen: set[str] = set()
    for i, svg in enumerate((svgs or [])[:cap]):
        markup = (svg or {}).get("outerHTML") or ""
        if not markup:
            continue
        label = (svg.get("label") or "").strip()
        stem = label or (f"logo-{i}" if svg.get("isLogo") else f"svg-{i}")
        base = "".join(c if c.isalnum() or c in "-_" else "-" for c in stem)[:40] or f"svg-{i}"
        if base in seen:
            continue
        seen.add(base)
        try:
            b64 = await page.evaluate(_SVG_RASTER_JS, [markup, thumb])
        except Exception:
            logger.info("svg contact sheet skipped: raster unavailable", exc_info=True)
            return []          # page can't rasterize at all — omit the whole sheet
        if not b64:
            continue
        try:
            out.append((base64.b64decode(b64), base))
        except Exception:
            continue
    return out


def _width_param(url: str) -> int:
    """Return the ``w=`` query value (Next.js image size), 0 if absent/invalid."""
    try:
        for k, v in parse_qsl(urlsplit(url).query, keep_blank_values=True):
            if k == "w":
                return int(v)
    except (ValueError, TypeError):
        pass
    return 0


def dedupe_srcset_variants(items: list[dict]) -> list[dict]:
    """Collapse srcset / Next.js ``_next/image`` size variants of the same image.

    Python port of the reference srcset-variant dedup: group by base URL (with
    ``w=`` / ``q=`` stripped), merge contexts + boolean signals, keep the highest-``w=``
    URL. First-seen order is preserved.
    """
    by_base: dict[str, dict] = {}
    for a in items:
        url = a.get("url", "")
        base_key = url
        try:
            parts = urlsplit(url)
            qs = parse_qsl(parts.query, keep_blank_values=True)
            # Exact key match (mirrors the reference searchParams.has("w")) — a raw
            # "w=" substring would false-positive on params like ?show=/?flow=.
            if "_next/image" in parts.path or any(k == "w" for k, _ in qs):
                kept = [(k, v) for (k, v) in qs if k not in ("w", "q")]
                base_key = urlunsplit(
                    (parts.scheme, parts.netloc, parts.path, urlencode(kept), parts.fragment))
        except (ValueError, TypeError):
            pass

        existing = by_base.get(base_key)
        if existing is None:
            copy = dict(a)
            copy["contexts"] = list(a.get("contexts", []))
            by_base[base_key] = copy
            continue
        for ctx in a.get("contexts", []):
            if ctx not in existing["contexts"]:
                existing["contexts"].append(ctx)
        if a.get("notes") and not existing.get("notes"):
            existing["notes"] = a["notes"]
        for flag in ("inBanner", "inHomeLink", "matchesTitleBrand"):
            if a.get(flag):
                existing[flag] = True
        if _width_param(a.get("url", "")) > _width_param(existing.get("url", "")):
            existing["url"] = a["url"]
    return list(by_base.values())


def cap_items(items: list[dict], cap: int) -> tuple[list[dict], bool]:
    """Truncate to ``cap`` items. Returns (kept, truncated?). No cap when cap < 0."""
    if cap is not None and cap >= 0 and len(items) > cap:
        return items[:cap], True
    return items, False


async def catalog_assets(page, *, cap: int = 200) -> list[dict]:
    """Catalog every referenced asset on the rendered page as CatalogedAsset dicts.

    URL-only; matches the reference catalog output shape. ``page`` is a
    Playwright page. Non-mutating (safe to run before the DOM-mutating page.html pass).
    """
    raw = await page.evaluate(ASSET_CATALOG_JS)
    items = dedupe_srcset_variants(raw or [])
    kept, truncated = cap_items(items, cap)
    if truncated:
        logger.warning("capture: asset_catalog truncated %d -> %d", len(items), cap)
    return kept


async def video_descriptors(page, *, cap: int = 20) -> list[dict]:
    """DOM video descriptors (VideoDescriptor shape), URL-only. Non-mutating."""
    raw = await page.evaluate(VIDEO_DESCRIPTORS_JS)
    items = raw or []
    kept, truncated = cap_items(items, cap)
    if truncated:
        logger.warning("capture: videos truncated %d -> %d", len(items), cap)
    return kept


# Direct-file video extensions we're willing to download. Streaming manifests
# (HLS .m3u8 / DASH .mpd) and blob:/data: pseudo-URLs are NEVER downloaded — they
# aren't a single fetchable body (mirrors the reference DOWNLOADABLE_VIDEO_EXTS).
DOWNLOADABLE_VIDEO_EXTS = (".mp4", ".webm", ".mov", ".m4v")
# Streaming-manifest extensions: DISCOVERED (recorded in the manifest so downstream
# knows the site streams video) but flagged download=False — not a fetchable body.
STREAMING_VIDEO_EXTS = (".m3u8", ".mpd")
# All video-ish exts we surface in the manifest (direct + streaming).
_DISCOVERABLE_VIDEO_EXTS = DOWNLOADABLE_VIDEO_EXTS + STREAMING_VIDEO_EXTS


def is_downloadable_video_url(url: str) -> bool:
    """True only for a direct-file video URL (http[s] with a downloadable ext).

    False for HLS (.m3u8), DASH (.mpd), and blob:/data: pseudo-URLs — those are
    streaming manifests / in-memory blobs, not a single fetchable body. Pure."""
    if not url or url.startswith(("blob:", "data:")):
        return False
    try:
        path = urlsplit(url).path.lower()
    except (ValueError, TypeError):
        return False
    return path.endswith(DOWNLOADABLE_VIDEO_EXTS)


# HLS/DASH manifest content-types — surfaced in the manifest (download=False),
# never fetched. The ``video/`` prefix wouldn't match these on its own.
_STREAMING_CONTENT_TYPES = ("application/vnd.apple.mpegurl", "application/x-mpegurl",
                            "application/dash+xml")


def _looks_like_video_response(url: str, content_type: str) -> bool:
    """Layer-1 filter: a network response is video-ish when its URL path ends in a
    direct OR streaming-manifest video ext, its Content-Type is ``video/*``, or its
    Content-Type is an HLS/DASH manifest type. Streaming manifests are recorded so
    they surface in the manifest (flagged download=False downstream); direct bodies
    are the download candidates. Pure + cheap."""
    ct = (content_type or "").lower()
    if ct.startswith("video/") or any(ct.startswith(s) for s in _STREAMING_CONTENT_TYPES):
        return True
    if not url:
        return False
    try:
        return urlsplit(url).path.lower().endswith(_DISCOVERABLE_VIDEO_EXTS)
    except (ValueError, TypeError):
        return False


def make_video_response_handler(discovered: set):
    """Build a ``page.on("response")`` handler that records direct-video URLs into
    ``discovered`` (a live set the merge step reads AFTER the page settles).

    The handler is SYNC (Playwright fires response listeners synchronously),
    exception-safe (a malformed response object can never bubble into the page's
    event loop), and cheap (header read + string checks, no body fetch). A tiny
    response (<100 bytes when the length is known) is skipped as a likely error/
    tracking blob. Mirrors the reference network-URL Set."""
    def _handler(response) -> None:
        try:
            url = getattr(response, "url", "") or ""
            if not url.startswith("http"):
                return
            headers = {}
            try:
                headers = response.headers or {}
            except Exception:
                headers = {}
            ct = (headers.get("content-type") or headers.get("Content-Type") or "")
            if not _looks_like_video_response(url, ct):
                return
            # Skip a known-tiny body (error page / tracking beacon mislabeled).
            try:
                clen = int(headers.get("content-length") or headers.get("Content-Length") or 0)
            except (ValueError, TypeError):
                clen = 0
            if 0 < clen < 100:
                return
            discovered.add(url)
        except Exception:
            # Never let a listener exception escape into the page event loop.
            pass
    return _handler


def _video_url_basename(url: str) -> str:
    """Reference filename derivation for a network-only video (src.split('/').pop()
    .split('?')[0])."""
    try:
        return (urlsplit(url).path.rsplit("/", 1)[-1] or "").split("?")[0]
    except Exception:
        return ""


def merge_video_manifest(network_urls: set, dom_videos: list, cap: int) -> list[dict]:
    """Merge two-layer video discovery into capped manifest entries.

    Port of the reference video-manifest merge step. Layer 2 (DOM, rich
    — carries dims + nearby heading/caption/aria) is kept ahead of Layer 1 (network-
    only, thin) so a clip seen in both lands once as the richer DOM entry. Deduped by
    URL, then capped (DOM entries first so the cap never drops a rich entry).

    Each entry carries the reference manifest keys ``{url, filename, width, height,
    sourceWidth, sourceHeight, heading, caption, ariaLabel}`` PLUS two underscore-
    prefixed internal control fields the orchestrator consumes and then strips before
    serialization: ``_source`` ("dom"|"network", drives which entries get a preview
    screenshot) and ``_download`` (direct-ext body the orchestrator may fetch — False
    for HLS/DASH/blob/data). ``index``/``preview``/``localPath`` are added by the
    orchestrator's per-video pass (reference assigns them in the download loop), NOT
    here. Pure — no I/O."""
    by_url: dict[str, dict] = {}
    for d in dom_videos or []:
        url = d.get("src") or ""
        if not url or url in by_url:
            continue
        by_url[url] = {
            "url": url,
            "filename": d.get("filename") or _video_url_basename(url),
            "width": d.get("width", 0) or 0,
            "height": d.get("height", 0) or 0,
            "sourceWidth": d.get("sourceWidth", 0) or 0,
            "sourceHeight": d.get("sourceHeight", 0) or 0,
            "heading": d.get("heading") or "",
            "caption": d.get("caption") or "",
            "ariaLabel": d.get("ariaLabel") or "",
            "_source": "dom",
            "_download": is_downloadable_video_url(url),
        }
    for url in network_urls or set():
        if not url or url in by_url:
            continue
        by_url[url] = {
            "url": url,
            "filename": _video_url_basename(url),
            "width": 0, "height": 0, "sourceWidth": 0, "sourceHeight": 0,
            "heading": "", "caption": "", "ariaLabel": "",
            "_source": "network",
            "_download": is_downloadable_video_url(url),
        }
    items = list(by_url.values())
    kept, truncated = cap_items(items, cap)
    if truncated:
        logger.info("capture: video manifest cap dropped %d of %d discovered videos",
                    len(items) - len(kept), len(items))
    return kept
