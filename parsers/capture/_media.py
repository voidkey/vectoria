"""Site media catalog — images / videos / backgrounds / icons / fonts as URL-only refs.

Ported from hyperframes' asset cataloger (packages/cli/src/capture/assetCataloger.ts)
and its DOM video descriptor scan (mediaCapture.ts) so the OUTPUT SHAPE matches
hyperframes (zero schema drift): each catalog item is a ``CatalogedAsset`` dict, each
video a ``VideoDescriptor`` dict.

URL-only by design — nothing is downloaded here. Downloading (and preview frames,
network-intercepted streaming URLs, GIF header parsing) is downstream's call, mirroring
hyperframes' split between the catalog pass and the download pass. Absolutizing,
tracking-pixel filtering, dedup and srcset-variant collapse match hyperframes exactly.
"""
from __future__ import annotations

import logging
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

logger = logging.getLogger(__name__)


# Real browser JS (an IIFE returning CatalogedAsset[]). Ported verbatim from
# hyperframes assetCataloger.ts's page.evaluate body with the TS-template escaping
# unwound (\\s -> \s, \\/ -> /, \\( -> \( ...) so it runs unchanged in Playwright.
# The GIF byte-header annotation (a ranged fetch in hyperframes) is intentionally
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
# preview frames, no download — those are hyperframes' download-pass concerns.
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

    Python port of hyperframes' deduplicateSrcsetVariants: group by base URL (with
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
            # Exact key match (mirrors hyperframes' searchParams.has("w")) — a raw
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

    URL-only; matches hyperframes' ``catalogAssets`` output shape. ``page`` is a
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
# aren't a single fetchable body (mirrors mediaCapture.ts DOWNLOADABLE_VIDEO_EXTS).
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
    tracking blob. Mirrors mediaCapture.ts's network-URL Set."""
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


def merge_video_manifest(network_urls: set, dom_videos: list, cap: int) -> list[dict]:
    """Merge two-layer video discovery into capped manifest entries.

    Port of mediaCapture.ts::captureVideoManifest's merge step, mapped to
    vectoria's manifest entry shape. Layer 2 (DOM, rich — carries width/height/
    poster) is kept ahead of Layer 1 (network-only, thin) so a clip seen in both
    lands once as the richer DOM entry. Deduped by URL, then capped (DOM entries
    first so the cap never drops a rich entry in favour of a thin one).

    Each entry leaves this function with the key set ``{url, source, width,
    height, poster, download, preview}``: ``source`` is "dom" or "network";
    ``download`` marks a direct-ext body the orchestrator may fetch (False for
    HLS/DASH/blob/data); ``preview`` starts None and is filled by the
    orchestrator's screenshot pass. The orchestrator ALSO annotates each entry it
    downloads in place with ``local_key`` and ``downloaded`` (both serialized into
    video-manifest.json) — those two keys are added downstream, not here.
    Pure — no I/O."""
    by_url: dict[str, dict] = {}
    for d in dom_videos or []:
        url = d.get("src") or ""
        if not url or url in by_url:
            continue
        by_url[url] = {
            "url": url,
            "source": "dom",
            "width": d.get("width", 0) or 0,
            "height": d.get("height", 0) or 0,
            "poster": d.get("poster") or "",
            "download": is_downloadable_video_url(url),
            "preview": None,
        }
    for url in network_urls or set():
        if not url or url in by_url:
            continue
        by_url[url] = {
            "url": url,
            "source": "network",
            "width": 0,
            "height": 0,
            "poster": "",
            "download": is_downloadable_video_url(url),
            "preview": None,
        }
    items = list(by_url.values())
    kept, truncated = cap_items(items, cap)
    if truncated:
        logger.info("capture: video manifest cap dropped %d of %d discovered videos",
                    len(items) - len(kept), len(items))
    return kept
