"""Reusable website-capture orchestration: URL -> SiteProfile.

Extracted from worker/handlers.py::_capture_core so the capture pipeline is
testable without a queue/browser/DB and reusable outside the worker. All side
effects (browser page, object storage, image upload + image-id hydration) are
injected via CaptureDeps; the worker supplies a real implementation and does the
final update_doc + enqueue based on the returned CaptureOutcome."""
from __future__ import annotations

import hashlib
import logging
import re
import time
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from parsers.capture.profile import SiteProfile

logger = logging.getLogger(__name__)

# Monotonic clock for the video-download wall-clock budget. Module-level so tests
# can patch it deterministically (no real sleeps); production uses time.monotonic.
_monotonic = time.monotonic

# Content-type -> ext for a downloaded direct video body (falls back to the URL ext).
_VIDEO_CT_EXT = {"video/mp4": ".mp4", "video/webm": ".webm",
                 "video/quicktime": ".mov", "video/x-m4v": ".m4v"}

# Only these image-asset kinds are worth a vision description — AND only when
# the bytes are a raster format the vision model can actually decode. A logo/
# favicon is frequently SVG or .ico (vector/icon), which the vision API rejects;
# those are stored but never described (avoids a guaranteed-failing, billed call).
_VISION_ASSET_KINDS = ("logo", "hero", "og_image")
_VISION_RASTER_EXT = ("png", "jpg", "jpeg", "webp", "gif")
# Content-type -> extension maps (moved verbatim from worker/handlers.py).
_IMG_EXT = {"image/png": ".png", "image/jpeg": ".jpg", "image/webp": ".webp",
            "image/svg+xml": ".svg", "image/gif": ".gif",
            "image/x-icon": ".ico", "image/vnd.microsoft.icon": ".ico"}
_BIN_EXT = {"video/mp4": ".mp4", "video/webm": ".webm", "application/json": ".json"}
# Catalog-image download gate (ported from the reference asset downloader): only
# real content images (Image/Background) reached through a standard image context,
# minus obvious tracking/junk URLs and favicons (handled separately above).
_CATALOG_IMAGE_TYPES = ("Image", "Background")
_CATALOG_GOOD_CONTEXTS = frozenset(
    ("img[src]", "img[srcset]", "video[poster]", "source[srcset]", "data-src", "css url()"))
_CATALOG_JUNK_SUBSTR = ("pixel", "beacon", "analytics")


def _catalog_image_ext(url: str) -> str:
    """Extension from the URL path (``.jpg`` fallback), matching the reference's
    ``pathExt && pathExt.length <= 5 ? pathExt : ".jpg"``."""
    from urllib.parse import urlsplit
    tail = urlsplit(url).path.rsplit("/", 1)[-1]
    ext = ("." + tail.rsplit(".", 1)[-1]) if "." in tail else ""
    return ext if 0 < len(ext) <= 5 else ".jpg"


def _asset_gets_vision(kind: str, fname: str) -> bool:
    """True only for vision-worthy kinds whose stored format is a raster the
    vision model can decode (not svg/ico)."""
    return kind in _VISION_ASSET_KINDS and fname.rsplit(".", 1)[-1] in _VISION_RASTER_EXT


# Non-latin unicode-range subset tokens that a font URL/path may name; a face so
# labelled sorts AFTER a latin/hashed face (explicitly de-prioritized).
_NON_LATIN_SUBSETS = ("cyrillic", "greek", "vietnamese", "cjk", "korean",
                      "japanese", "arabic", "thai", "hebrew")
_HASHED_BASENAME_RE = re.compile(r"[A-Za-z0-9]{8,}")


def _is_latin_subset(url: str) -> bool:
    """True when a font URL looks like a Latin subset or an opaque hashed-filename
    face, so it sorts ahead of explicitly non-latin (CJK/Arabic/...) unicode-range
    subsets. Mirrors the reference sort key in the asset downloader. A ``latin``
    token in the path wins; a hashed/opaque
    basename (e.g. ``19cfc7226ec3afaa-s.woff2``) is treated as NEUTRAL — still
    True so it outranks a named non-latin subset — because we can't tell its
    coverage from the name alone. Pure helper."""
    from urllib.parse import urlsplit
    path = urlsplit(url or "").path.lower()
    if "latin" in path:
        return True
    basename = path.rsplit("/", 1)[-1]
    stem = basename.rsplit(".", 1)[0] if "." in basename else basename
    # A named non-latin subset is de-prioritized; anything else (including a long
    # opaque hash) is treated as latin-neutral and kept ahead of those.
    if any(tok in stem for tok in _NON_LATIN_SUBSETS):
        return False
    return bool(_HASHED_BASENAME_RE.search(stem))


def _motion_hints(MotionHints, motion: dict, shaders: list):
    """Build MotionHints from the raw motion dict, replacing the cheap script-src
    `libraries` with the upgraded detect_libraries (script-src + DOM fingerprints
    + shader fingerprints). The extractor's `fingerprints` block is consumed here
    and NOT persisted on MotionHints (which only carries libraries + the two
    boolean flags)."""
    from parsers.capture._animations import detect_libraries
    m = dict(motion or {})
    fingerprints = m.pop("fingerprints", {}) or {}
    raw_libs = m.pop("libraries", []) or []
    libraries = detect_libraries(raw_libs, shaders or [], fingerprints)
    return MotionHints(libraries=libraries,
                       has_video_background=bool(m.get("has_video_background")),
                       has_canvas=bool(m.get("has_canvas")))


async def _capture_video_previews(page, entries: list, out: dict) -> None:
    """Best-effort per-DOM-video preview frame -> ``out[url] = png_bytes``.

    Screenshots each on-page <video> element (element_handle.screenshot()) and maps
    it back to its manifest entry by src. A network-only entry has no element, so it
    gets no preview; an unscreenshotable video is simply omitted. One failure never
    aborts — mirrors the reference per-video preview guard. Non-mutating (runs
    before the DOM-mutating page.html pass)."""
    by_url = {e["url"]: e for e in entries if e.get("_source") == "dom"}
    if not by_url:
        return
    try:
        handles = await page.query_selector_all("video")
    except Exception:
        return
    for h in handles or []:
        try:
            src = await h.evaluate("v => v.src || v.currentSrc || "
                                   "(v.querySelector('source') ? v.querySelector('source').src : '')")
        except Exception:
            continue
        if not src or src not in by_url or src in out:
            continue
        try:
            data = await h.screenshot()
        except Exception:
            continue
        if data:                       # skip empty (unscreenshotable) frames
            out[src] = data


async def _save_lotties(urls: list, kb_id: str, doc_id: str, cfg, storage) -> tuple[list, list]:
    """Download + validate discovered lottie sources; store the animation JSON.

    Port of the reference lottie save step: for up to ``capture_max_lotties``
    sources, fetch (SSRF/size-capped via fetch_asset_bytes), unzip a dotLottie ZIP to
    its animation JSON, validate lottie structure (v/ip/op/layers/w/h/fr), dedup by
    content hash, and store to ``captures/{kb}/{doc}/assets/lottie/animation-N.json``.
    Returns (asset_refs, manifest_entries). Best-effort per lottie — one failure never
    aborts. The parsed dict is carried on each manifest entry (``_parsed``, popped by
    the preview pass) so previews need no re-parse."""
    from parsers.capture._assets import fetch_asset_bytes
    from parsers.capture._media import lottie_json_from_bytes, lottie_manifest_entry
    from parsers.capture.profile import AssetRef

    refs: list = []
    entries: list = []
    seen_hashes: set[str] = set()
    saved = 0
    for u in (urls or [])[: cfg.capture_max_lotties]:
        if not u or not u.startswith("http"):
            continue
        got = await fetch_asset_bytes(u, max_bytes=cfg.capture_max_asset_bytes)
        if got is None:
            continue
        data, _ctype = got
        try:
            result = lottie_json_from_bytes(
                u, data, max_uncompressed=cfg.capture_max_lottie_bytes)
        except Exception:
            logger.info("capture: lottie parse failed for %s", u, exc_info=True)
            continue
        if result is None:
            continue
        json_bytes, parsed = result
        digest = hashlib.sha1(json_bytes).hexdigest()
        if digest in seen_hashes:
            continue
        seen_hashes.add(digest)
        name = f"animation-{saved}.json"
        key = f"captures/{kb_id}/{doc_id}/assets/lottie/{name}"
        try:
            await storage.put(key, json_bytes, content_type="application/json")
        except Exception:
            logger.info("capture: lottie store failed for %s", key, exc_info=True)
            continue
        saved += 1
        refs.append(AssetRef(kind="lottie_json", storage_key=key, url=u, format="json"))
        entry = lottie_manifest_entry(name, u, parsed)
        entry["_parsed"] = parsed          # popped by the preview pass; never serialized
        entries.append(entry)
    if len(urls or []) > cfg.capture_max_lotties:
        logger.info("capture: lottie cap (%d) dropped %d of %d discovered sources",
                    cfg.capture_max_lotties,
                    len(urls) - cfg.capture_max_lotties, len(urls))
    return refs, entries


def _safe_hex(css: str) -> str:
    from parsers.capture._colors import _to_hex, parse_css_color
    rgb = parse_css_color(css or "")
    return _to_hex(rgb) if rgb else ""


async def _store_page_svgs(raw: dict, kb_id: str, doc_id: str, cfg, storage) -> list:
    """Store extracted page SVG markup under ``assets/svgs/`` (content-hash names).

    SVGs come from the already-extracted markup (``raw["svgs"][*]["outerHTML"]``) —
    no fetch needed, so no SSRF check applies; we store the markup bytes directly.
    Named by sha1-of-markup (``logo-<hash>`` when isLogo, else ``svg-<hash>``) so the
    filename can't drift from content and duplicate SVGs dedupe on the same hash.
    Returns the stored AssetRefs (best-effort per SVG — one store failure logs + skips)."""
    from parsers.capture.profile import AssetRef

    svg_asset_refs: list = []
    seen_svg_hashes: set[str] = set()
    for svg in (raw.get("svgs") or [])[: cfg.capture_max_svgs]:
        markup = svg.get("outerHTML") or ""
        if not markup:
            continue
        markup_bytes = markup.encode("utf-8")
        if len(markup_bytes) < cfg.capture_min_svg_bytes:
            continue
        digest = hashlib.sha1(markup_bytes).hexdigest()[:8]
        if digest in seen_svg_hashes:
            continue
        seen_svg_hashes.add(digest)
        is_logo = bool(svg.get("isLogo"))
        name = f"{'logo' if is_logo else 'svg'}-{digest}"
        key = f"captures/{kb_id}/{doc_id}/assets/svgs/{name}.svg"
        try:
            await storage.put(key, markup_bytes, content_type="image/svg+xml")
            svg_asset_refs.append(AssetRef(
                kind=("logo" if is_logo else "svg"), storage_key=key, format="svg"))
        except Exception:
            logger.info("capture: svg store failed for %s", key, exc_info=True)
    return svg_asset_refs


async def _fetch_named_assets(raw_assets: dict, cfg) -> tuple[list, list, dict, set]:
    """Fetch the 4 named image assets (logo/hero/og_image/favicon) -> ImageRefs.

    Returns ``(image_refs, novision_asset_refs, filename_kind, seen_urls)``:
    vision-worthy raster assets (logo/hero/og_image) go to ``image_refs``; favicon and
    non-raster (svg/ico) logos/heros go to ``novision_asset_refs``. ``filename_kind``
    maps each stored filename -> kind for the post-hydration profile join; ``seen_urls``
    carries the fetched URLs so the later catalog-image pass skips duplicates."""
    from parsers.capture._assets import fetch_asset_bytes, image_ref_from_bytes

    image_refs: list = []            # vision-worthy assets (logo/hero/og_image)
    novision_asset_refs: list = []   # favicon etc. — stored, never described
    filename_kind: dict[str, str] = {}
    seen_urls: set[str] = set()
    for kind in ("logo", "hero", "og_image", "favicon"):
        a_url = raw_assets.get(kind)
        if not a_url or a_url in seen_urls:
            continue
        seen_urls.add(a_url)
        got = await fetch_asset_bytes(a_url, max_bytes=cfg.capture_max_asset_bytes)
        if got is None:
            continue
        data, ctype = got
        ext = _IMG_EXT.get(ctype, ".png")
        fname = f"{kind}{ext}"
        filename_kind[fname] = kind
        ref = image_ref_from_bytes(data, filename=fname, mime=ctype or "image/png", alt=kind)
        (image_refs if _asset_gets_vision(kind, fname) else novision_asset_refs).append(ref)
    return image_refs, novision_asset_refs, filename_kind, seen_urls


async def _store_binaries(raw_assets: dict, kb_id: str, doc_id: str, cfg, storage) -> list:
    """Fetch + store the non-image named binary (background video).

    Port of the original inline block: for the ``video`` raw-asset URL, fetch
    (SSRF/size-capped) and store under ``assets/{kind}{ext}``. Returns the stored
    AssetRefs. Best-effort per binary — a fetch/store failure logs + skips, matching
    the other store helpers, so it can never abort an otherwise-degrading capture.
    Note: the legacy single ``lottie`` raw asset is intentionally NOT handled here —
    it is captured once by ``_save_lotties`` (validated JSON + manifest) via the
    ``_legacy_lottie`` insert into the lottie list."""
    from parsers.capture._assets import fetch_asset_bytes
    from parsers.capture.profile import AssetRef

    refs: list = []
    for kind, a_url in (("background_video", raw_assets.get("video")),):
        if not a_url:
            continue
        try:
            got = await fetch_asset_bytes(a_url, max_bytes=cfg.capture_max_asset_bytes)
            if got is None:
                continue
            data, ctype = got
            ext = _BIN_EXT.get(ctype, ".bin")
            key = f"captures/{kb_id}/{doc_id}/assets/{kind}{ext}"
            await storage.put(key, data, content_type=ctype or "application/octet-stream")
            refs.append(AssetRef(kind=kind, storage_key=key, format=ext.lstrip(".")))
        except Exception:
            logger.info("capture: binary store failed for %s", kind, exc_info=True)
            continue
    return refs


async def _store_lottie_previews(lottie_entries: list, lottie_refs: list,
                                 lottie_preview_pngs: dict, kb_id: str, doc_id: str,
                                 storage) -> tuple[list, dict | None]:
    """Attach downloaded lottie refs + store preview PNGs + assemble the manifest.

    The download/validate/store + best-effort preview render already happened inside
    the page block (``lottie_refs`` / ``lottie_entries`` / ``lottie_preview_pngs``).
    Here we store the preview PNGs (annotating each entry's ``preview`` in place),
    collect the AssetRefs (downloaded refs first, then any preview refs, matching the
    original append order), and build the manifest. Returns ``(profile_asset_refs,
    lottie_manifest_or_None)``. Best-effort per preview."""
    from parsers.capture.profile import AssetRef

    if not lottie_entries:
        return [], None
    refs: list = list(lottie_refs)
    preview_count = 0
    for idx, entry in enumerate(lottie_entries):
        name = entry.get("file", "").rsplit("/", 1)[-1]
        png = lottie_preview_pngs.get(name)
        if png:
            pname = name.rsplit(".", 1)[0] + "-preview.png"
            pkey = f"captures/{kb_id}/{doc_id}/assets/lottie/previews/{pname}"
            try:
                await storage.put(pkey, png, content_type="image/png")
                entry["preview"] = f"assets/lottie/previews/{pname}"
                refs.append(AssetRef(kind="lottie_preview", storage_key=pkey, format="png"))
                preview_count += 1
            except Exception:
                logger.info("capture: lottie preview store failed for %s", pkey,
                            exc_info=True)
    manifest = {
        "lotties": lottie_entries,
        "meta": {"discovered": len(lottie_entries), "previews": preview_count},
    }
    return refs, manifest


async def _store_videos(video_entries: list, video_previews: dict, capture_quality: str,
                        kb_id: str, doc_id: str, cfg, storage) -> tuple[list, list | None]:
    """Store per-video preview frames + bounded/budgeted direct-ext body downloads.

    Two-layer discovery already merged into ``video_entries`` (each carrying the
    reference keys + internal ``_source``/``_download``). Stores preview frames
    (captured while the page was open) as ``kind="video_preview"`` AssetRefs, and
    DOWNLOADS direct-ext bodies (skip HLS/DASH/blob/data) only when ``capture_quality
    == "full"``, bounded by ``capture_max_video_downloads`` AND a cumulative wall-clock
    budget.

    Returns ``(profile_asset_refs, video_manifest_list_or_None)`` — the manifest is
    the reference BARE ARRAY: entries ``{index, url, filename, width, height,
    sourceWidth, sourceHeight, heading, caption, ariaLabel}`` plus optional
    ``preview``/``localPath`` (relative paths). Mirroring the reference, an entry
    that yields NEITHER a preview NOR a downloaded body is dropped (it carries nothing
    usable downstream), and the internal ``_``-prefixed control keys never serialize.
    Best-effort per video."""
    from parsers.capture._assets import fetch_asset_bytes
    from parsers.capture.profile import AssetRef

    refs: list = []
    kept: list = []
    video_dl_count = 0
    video_dl_start = _monotonic()
    video_budget_hit = False
    allow_download = capture_quality == "full"
    for idx, entry in enumerate(video_entries):
        v_url = entry["url"]
        preview: str | None = None
        local_path: str | None = None
        # Preview frame (DOM videos only) -> assets/videos/previews/.
        png = video_previews.get(v_url)
        if png:
            pkey = f"captures/{kb_id}/{doc_id}/assets/videos/previews/video-{idx}-preview.png"
            try:
                await storage.put(pkey, png, content_type="image/png")
                preview = f"assets/videos/previews/video-{idx}-preview.png"
                refs.append(AssetRef(
                    kind="video_preview", storage_key=pkey, url=v_url, format="png"))
            except Exception:
                logger.info("capture: video preview store failed for %s", pkey, exc_info=True)
        # Direct-ext body download (guarded, bounded, budgeted). Skipping the download
        # must NOT skip the entry — a preview-only entry is still listed.
        if allow_download and entry.get("_download"):
            if video_dl_count >= cfg.capture_max_video_downloads:
                logger.info("capture: video download cap (%d) reached — %s not fetched",
                            cfg.capture_max_video_downloads, v_url)
            elif _monotonic() - video_dl_start >= cfg.capture_video_download_budget_s:
                if not video_budget_hit:
                    logger.info("capture: video download budget (%.0fs) exceeded — "
                                "remaining bodies not fetched",
                                cfg.capture_video_download_budget_s)
                    video_budget_hit = True
            else:
                got = await fetch_asset_bytes(v_url, max_bytes=cfg.capture_max_video_bytes)
                if got is not None:
                    data, ctype = got
                    # Ext from content-type, else the URL. Safe to reuse the image-ext
                    # helper: we only get here when _download is True (is_downloadable_
                    # video_url confirmed a direct .mp4/.webm/.mov/.m4v path), so the
                    # helper's .jpg fallback is unreachable.
                    ext = _VIDEO_CT_EXT.get((ctype or "").lower()) or _catalog_image_ext(v_url)
                    vkey = f"captures/{kb_id}/{doc_id}/assets/videos/video-{idx}{ext}"
                    try:
                        await storage.put(
                            vkey, data, content_type=ctype or "application/octet-stream")
                        video_dl_count += 1
                        local_path = f"assets/videos/video-{idx}{ext}"
                        refs.append(AssetRef(
                            kind="video", storage_key=vkey, url=v_url, format=ext.lstrip(".")))
                    except Exception:
                        logger.info("capture: video body store failed for %s", vkey,
                                    exc_info=True)
        # A video with neither a preview nor a body is a dead reference — drop it.
        if preview is None and local_path is None:
            continue
        out: dict = {
            "index": idx,
            "url": v_url,
            "filename": entry.get("filename", ""),
            "width": entry.get("width", 0),
            "height": entry.get("height", 0),
            "sourceWidth": entry.get("sourceWidth", 0),
            "sourceHeight": entry.get("sourceHeight", 0),
            "heading": entry.get("heading", ""),
            "caption": entry.get("caption", ""),
            "ariaLabel": entry.get("ariaLabel", ""),
        }
        if preview:
            out["preview"] = preview
        if local_path:
            out["localPath"] = local_path
        kept.append(out)

    return refs, (kept or None)


async def _download_catalog_images(asset_catalog: list, seen_urls: set, kb_id: str,
                                   doc_id: str, cfg, storage) -> tuple[list, list]:
    """Download good-context catalog images (beyond the 4 named kinds).

    Port of the reference catalog-image pass: keep only real
    content images reached through a standard image context, drop tracking/junk +
    favicons, fetch (SSRF+size-capped) and gate on a min-size threshold, then name by
    page context (derive_asset_name). Dedups by URL against ``seen_urls`` (mutated in
    place as it consumes, matching the original). Returns ``(profile_asset_refs,
    asset_sheet_items)`` where the latter is ``(bytes, filename)`` of downloaded RASTER
    images for the asset contact sheet (SVGs excluded — Pillow can't decode markup).
    Best-effort per image.

    NON-GOAL (explicit): these bulk catalog images get NO vision description —
    vision_status="skipped". Vision stays scoped to the named logo/hero/og assets."""
    from parsers.capture._assets import derive_asset_name, fetch_asset_bytes
    from parsers.capture.profile import AssetRef

    refs: list = []
    # Seed with the reserved named-asset stems so a catalog image whose derived slug
    # collides with one gets suffix-deduped (hero -> hero-2); without this a catalog
    # "hero.jpg" would silently clobber the named hero in the export ZIP.
    used_names: set[str] = {"logo", "hero", "og_image", "favicon",
                            "background_video", "lottie"}
    asset_sheet_items: list[tuple[bytes, str]] = []
    good_catalog = []
    for cat in asset_catalog:
        c_url = cat.get("url", "")
        if not c_url.startswith("http") or c_url in seen_urls:
            continue
        if cat.get("type") not in _CATALOG_IMAGE_TYPES:
            continue
        lurl = c_url.lower()
        if any(j in lurl for j in _CATALOG_JUNK_SUBSTR) or "/favicon" in lurl:
            continue
        if not (_CATALOG_GOOD_CONTEXTS & set(cat.get("contexts") or [])):
            continue
        good_catalog.append(cat)
    capped = good_catalog[: cfg.capture_max_catalog_images]
    if len(good_catalog) > len(capped):
        logger.info("capture: catalog image cap dropped %d of %d good-context images",
                    len(good_catalog) - len(capped), len(good_catalog))
    for cat in capped:
        c_url = cat["url"]
        # Load-bearing (not the earlier filter-loop check): dedups the same URL
        # appearing twice within asset_catalog itself, since this loop also ADDs.
        if c_url in seen_urls:
            continue
        seen_urls.add(c_url)
        got = await fetch_asset_bytes(c_url, max_bytes=cfg.capture_max_asset_bytes)
        if got is None:
            continue
        data, ctype = got
        ext = _catalog_image_ext(c_url)
        is_svg = ext == ".svg" or ".svg" in c_url.lower()
        min_size = cfg.capture_min_svg_bytes if is_svg else cfg.capture_min_image_bytes
        if len(data) < min_size:
            continue
        name = derive_asset_name(cat, used_names)
        used_names.add(name)
        key = f"captures/{kb_id}/{doc_id}/assets/{name}{ext}"
        try:
            await storage.put(key, data, content_type=ctype or "application/octet-stream")
        except Exception:
            logger.info("capture: catalog image store failed for %s", key, exc_info=True)
            continue
        refs.append(AssetRef(
            kind="image", storage_key=key, url=c_url, format=ext.lstrip("."),
            description="", vision_status="skipped"))
        if not is_svg:
            asset_sheet_items.append((data, f"{name}{ext}"))
    return refs, asset_sheet_items


async def _build_contact_sheets(shots: list, asset_sheet_items: list, svg_raster_items: list,
                                kb_id: str, doc_id: str, url: str, storage) -> list:
    """Build + store the scroll / asset / SVG contact sheets (pure Pillow).

    Port of the reference contact-sheet builders. Built
    from bytes already in hand (screenshot ``shots`` + downloaded raster catalog images
    + in-page SVG rasters), so no browser + no re-fetch. Scroll: 3 cols, 9/page,
    kind/section labels. Asset: 4 cols, 12/page, filename labels. SVG: 5 cols, 15/page,
    basename labels. Returns the stored ``contact_sheet`` AssetRefs in order.
    Best-effort — a Pillow/store failure logs + skips, never aborts."""
    from parsers.capture._contact_sheet import build_contact_sheet
    from parsers.capture.profile import AssetRef

    refs: list = []

    async def _store_sheets(pages: list, prefix: str) -> None:
        for n, page_bytes in enumerate(pages, 1):
            skey = f"captures/{kb_id}/{doc_id}/{prefix}/contact-sheet-{n}.jpg"
            try:
                await storage.put(skey, page_bytes, content_type="image/jpeg")
                refs.append(AssetRef(kind="contact_sheet", storage_key=skey, format="jpg"))
            except Exception:
                logger.info("capture: contact sheet store failed for %s", skey, exc_info=True)

    # Reference scroll sheet: only the scroll strips, labelled "N% scroll"
    # (the reference scroll contact-sheet builder). The internal full_page shot is
    # excluded.
    scroll_items = [(s["bytes"], f"{s['pct']}% scroll")
                    for s in shots
                    if s.get("bytes") and s.get("kind") == "scroll"]
    try:
        scroll_pages = build_contact_sheet(scroll_items, cols=3, per_page=9, thumb_w=600)
        await _store_sheets(scroll_pages, "screenshots")
    except Exception:
        logger.info("capture: scroll contact sheet failed for %s", url, exc_info=True)
    try:
        asset_pages = build_contact_sheet(asset_sheet_items, cols=4, per_page=12, thumb_w=480)
        await _store_sheets(asset_pages, "assets")
    except Exception:
        logger.info("capture: asset contact sheet failed for %s", url, exc_info=True)
    # SVG contact sheet from the best-effort in-page rasters captured above. Empty
    # when rasterization degraded (logged).
    try:
        svg_pages = build_contact_sheet(svg_raster_items, cols=5, per_page=15, thumb_w=200)
        await _store_sheets(svg_pages, "assets/svgs")
    except Exception:
        logger.info("capture: svg contact sheet failed for %s", url, exc_info=True)
    return refs


async def _build_fonts(raw: dict, kb_id: str, doc_id: str, cfg, storage):
    """Assemble the display/body font roles + bounded site face set.

    Catalog-matches each role (build_font_role); on a non-renderable role, fetches the
    first @font-face src and stores it. Then downloads the page's @font-face woff2 URLs
    (Latin subsets first), capped per-family and in total, best-effort per face. Role
    faces already stored are counted (content-hash dedup) so the manifest covers
    everything without double-storing. Returns ``(Fonts, font_files)`` where font_files
    is the list of raw FontFileMetadata dicts (+ storage_key)."""
    from parsers.capture._assets import fetch_asset_bytes
    from parsers.capture._font_metadata import font_file_metadata
    from parsers.capture._fonts import build_font_role
    from parsers.capture.profile import FontFile, Fonts

    face_srcs = raw.get("fonts", {}).get("face_srcs", {})
    font_files: list[dict] = []             # raw FontFileMetadata dicts (+ storage_key)
    seen_font_hashes: set[str] = set()      # content-hash dedup across role + face set
    seen_font_urls: set[str] = set()        # url-level dedup so we never refetch a face

    async def _store_face(data: bytes) -> str:
        """Store one woff2 face under assets/fonts/ (content-hash name) and append
        its fonttools metadata to the accumulator. Deduped by content hash."""
        digest = hashlib.sha1(data).hexdigest()[:8]
        key = f"captures/{kb_id}/{doc_id}/assets/fonts/{digest}.woff2"
        if digest not in seen_font_hashes:
            seen_font_hashes.add(digest)
            await storage.put(key, data, content_type="font/woff2")
            meta = font_file_metadata(data, f"{digest}.woff2")
            meta["storage_key"] = key
            font_files.append(meta)
        return key

    async def _font_role(info: dict):
        role = build_font_role(info, weights=[info.get("weight", 400)])
        if not role.renderable:
            srcs = face_srcs.get(role.family.lower(), [])
            if srcs:
                got = await fetch_asset_bytes(srcs[0], max_bytes=cfg.capture_max_asset_bytes)
                seen_font_urls.add(srcs[0])
                if got:
                    data, _ = got
                    key = await _store_face(data)
                    role.files = [FontFile(url=key, weight=info.get("weight"), source="captured")]
        return role

    raw_fonts = raw.get("fonts", {})
    fonts = Fonts(display=await _font_role(raw_fonts.get("display", {})),
                  body=await _font_role(raw_fonts.get("body", {})))

    # Bounded site face set (port of the reference font downloader): download
    # the page's @font-face woff2 URLs, Latin subsets first, capped per-family and
    # in total, best-effort per face. Role fonts already stored above are counted
    # (content-hash dedup) so the manifest covers everything without double-storing.
    total_fonts = len(font_files)
    truncated = 0
    for urls in face_srcs.values():
        fam_count = 0
        # Latin-subset priority: a URL whose path names a "latin" subset (or an
        # opaque hashed filename) sorts before CJK/Arabic/etc unicode-range subsets
        # (mirrors the reference sort key).
        for f_url in sorted(urls, key=lambda u: 0 if _is_latin_subset(u) else 1):
            if f_url in seen_font_urls:      # already fetched by the role-font pass
                continue
            if total_fonts >= cfg.capture_max_total_fonts:
                truncated += 1
                continue
            if fam_count >= cfg.capture_max_fonts_per_family:
                continue
            seen_font_urls.add(f_url)
            got = await fetch_asset_bytes(f_url, max_bytes=cfg.capture_max_asset_bytes)
            if got is None:
                continue
            data, _ = got
            before = len(font_files)
            await _store_face(data)
            if len(font_files) > before:     # newly stored (not a content dup)
                fam_count += 1
                total_fonts += 1
    if truncated:
        logger.info("capture: total-font cap (%d) truncated %d face(s)",
                    cfg.capture_max_total_fonts, truncated)
    return fonts, font_files


def _build_layout_tokens(raw: dict, shots: list, profile_shots: list, cfg):
    """Derive the pure profile tokens: colors / spacing / sections / headings / svgs /
    page geometry. No side effects — only reads ``raw`` (+ the full-page screenshot for
    dominant-color cross-check) and the already-hydrated ``profile_shots``. Returns
    ``(colors, spacing, sections, headings, svgs, page)``.

    camelCase (raw) -> snake_case (profile); SVG outerHTML is DROPPED here so the raw
    markup never lands in the persisted profile (DB-bloat guard)."""
    from parsers.capture._colors import dominant_screenshot_hex, process_colors
    from parsers.capture._fonts import cluster_spacing, section_type
    from parsers.capture.profile import (
        Heading, PageGeom, SectionInfo, Spacing, SvgInfo)

    # Pixel-sample the full-page screenshot's dominant color to cross-check the
    # computed-style background (raises confidence + catches gradient/image bgs).
    full_png = next((s["bytes"] for s in shots if s["kind"] == "full_page"), None)
    screenshot_bg = dominant_screenshot_hex(full_png) if full_png else None
    colors = process_colors(raw.get("colors", {}),
                            delta_e_threshold=cfg.capture_color_delta_e,
                            screenshot_bg_hex=screenshot_bg)
    sp = raw.get("spacing", {})
    spacing = Spacing(
        scale=cluster_spacing(sp.get("margins", []) + sp.get("paddings", [])),
        radii=cluster_spacing(sp.get("radii", []), max_val=500),
        container_max_width=sp.get("container_max_width"),
        section_gap=(round(min(sp["section_gaps"])) if sp.get("section_gaps") else None))
    raw_sections = raw.get("sections", [])
    shot_by_section = {s.section_index: s.image_id
                       for s in profile_shots if s.section_index is not None}
    sections = [SectionInfo(
        index=s["index"], heading=s.get("heading", ""),
        # Prefer the reference's inline classification (vocabulary:
        # hero/footer/cta/logos/testimonials/features/content) so the downstream
        # renderer recognizes it; fall back to the zh-aware section_type
        # heuristic for old profiles captured before EXTRACT_JS emitted `type`.
        type=s.get("type") or section_type(
            s.get("heading", ""), s.get("classNames", []), s["index"], len(raw_sections)),
        bg_color=_safe_hex(s.get("bg", "")),
        screenshot_image_id=shot_by_section.get(s["index"]),
        layout=s.get("layout", ""),
        background_image=s.get("backgroundImage", ""),
        cta_texts=s.get("callsToAction", []),
        asset_urls=s.get("assetUrls", []),
        text=s.get("text", ""),
        selector=s.get("selector", ""),
        x=(s.get("rect") or {}).get("x", 0),
        y=(s.get("rect") or {}).get("y", 0),
        width=(s.get("rect") or {}).get("width", 0),
        height=(s.get("rect") or {}).get("height", 0),
    ) for s in raw_sections]

    # Phase 1 tokens: headings / svgs / page geometry (reference parity).
    headings = [Heading(level=h.get("level", 1), text=h.get("text", ""),
                        font_size=h.get("fontSize", ""),
                        font_weight=h.get("fontWeight", ""),
                        color=h.get("color", ""))
                for h in raw.get("headings", [])]
    svgs = [SvgInfo(label=s.get("label", ""), view_box=s.get("viewBox", ""),
                    width=s.get("width", 0) or 0, height=s.get("height", 0) or 0,
                    is_logo=bool(s.get("isLogo", False)))
            for s in raw.get("svgs", [])]
    raw_page = raw.get("page") or {}
    page = None
    if raw_page:
        vp = raw_page.get("viewport", {}) or {}
        page = PageGeom(width=raw_page.get("width", 0) or 0,
                        height=raw_page.get("height", 0) or 0,
                        viewport_width=vp.get("width", 0) or 0,
                        viewport_height=vp.get("height", 0) or 0)
    return colors, spacing, sections, headings, svgs, page


@dataclass
class CaptureOutcome:
    profile: SiteProfile
    title: str
    has_images: bool            # -> image_status "completed" vs "none"
    enqueue_image_analysis: bool


class CaptureDeps(Protocol):
    """Injected side-effects. Real impl: worker.handlers._HandlerCaptureDeps."""
    storage: Any  # object storage exposing `async put(key, data, content_type=...)`

    def open_page(self) -> AbstractAsyncContextManager:
        """Async context manager yielding a ready Playwright Page."""
        ...

    async def upload_image_refs(self, refs: list, *, vision_configured: bool) -> int:
        """Upload a batch of ImageRefs (screenshots / raster assets)."""
        ...

    async def hydrate_image_ids(self) -> dict[str, tuple[str, str]]:
        """After uploads: return {filename: (image_id, storage_key)}."""
        ...


async def run_capture(url: str, kb_id: str, doc_id: str, cfg, deps: CaptureDeps) -> CaptureOutcome:
    """Render `url`, extract a SiteProfile, store assets + screenshots via `deps`,
    and return it. Caller (worker) does SSRF pre-check, update_doc, and enqueue.

    Imports are function-local (matches the original _capture_core) so tests that
    patch parsers.capture._assets.fetch_asset_bytes keep biting at call time."""
    from parsers.capture._animations import (
        IO_CAPTURE_JS, SHADER_CAPTURE_JS, collect_animation_catalog,
        collect_shaders, start_cdp_animation_capture)
    from parsers.capture._assets import image_ref_from_bytes
    from parsers.capture._design_styles import extract_design_styles
    from parsers.capture._extract import run_extract
    from parsers.capture._html import extract_page_html
    from parsers.capture._media import (
        catalog_assets, make_video_response_handler, merge_video_manifest,
        rasterize_svgs, render_lottie_previews, video_descriptors)
    from parsers.capture._quality import assess_quality
    from parsers.capture._screenshots import (
        NEUTRALIZE_ANIMATION_CSS, capture_screenshots, prepare_page)
    from parsers.capture.profile import (
        AssetRef, MotionHints, ScreenshotRef, TextInfo)

    async with deps.open_page() as page:
        # Pre-nav hooks (must run BEFORE goto): capture WebGL shaders + record
        # IntersectionObserver targets. add_init_script applies to the next
        # navigation. Best-effort — a real failure degrades, never aborts.
        for _script in (SHADER_CAPTURE_JS, IO_CAPTURE_JS):
            try:
                await page.add_init_script(_script)
            except Exception:
                logger.info("capture: init-script injection failed for %s", url, exc_info=True)
        # Layer-1 video discovery: a pre-nav response listener records every
        # direct-video URL the page fetches (load / scroll / carousel rotation),
        # independent of DOM presence, into a live set read after the page settles.
        # GUARDED — a page whose .on is unsupported (CDP-style) or a fake test page
        # degrades to DOM-only discovery, never aborts. Handler is sync + exception-safe.
        discovered_videos: set[str] = set()
        try:
            page.on("response", make_video_response_handler(discovered_videos))
        except Exception:
            logger.info("capture: video response listener unavailable for %s", url,
                        exc_info=True)
        # Best-effort CDP Animation-domain capture. Returns (None, []) when no
        # real CDP is available (e.g. a fake page) — degrades to cdpAnimations: [].
        cdp_session = None
        cdp_entries: list = []
        try:
            cdp_session, cdp_entries = await start_cdp_animation_capture(page)
        except Exception:
            logger.info("capture: CDP animation start failed for %s", url, exc_info=True)
        animation_catalog = None
        shaders: list = []
        video_entries: list = []          # merged video manifest entries
        video_previews: dict = {}         # url -> preview PNG bytes (DOM videos)
        lottie_refs: list = []            # kind="lottie_json" AssetRefs
        lottie_entries: list = []         # lottie-manifest entries (+ transient _parsed)
        lottie_preview_pngs: dict = {}    # entry basename -> preview PNG bytes
        svg_raster_items: list = []       # [(png_bytes, basename)] for the SVG sheet
        try:
            try:
                await page.goto(url, wait_until="load",
                                timeout=int(cfg.capture_render_timeout * 1000))
            except Exception:
                logger.info("capture goto timed out/failed, using rendered DOM: %s", url)
            # Mainstream capture waits for networkidle, not just `load`. Do it as a
            # best-effort wait on top of `load`, capped so long-polling sites don't
            # burn the whole budget.
            try:
                await page.wait_for_load_state(
                    "networkidle", timeout=int(cfg.capture_networkidle_timeout * 1000))
            except Exception:
                pass
            await page.wait_for_timeout(cfg.capture_settle_ms)
            # Collapse animation/transition timing so scroll-reveal effects are
            # captured at their final frame, not mid-fade.
            try:
                await page.add_style_tag(content=NEUTRALIZE_ANIMATION_CSS)
            except Exception:
                pass
            # Ready the page (walk it to fire reveals + lazy loads, wait for fonts +
            # images, hide sticky chrome) before extract + per-section screenshots.
            await prepare_page(
                page, step_frac=cfg.capture_scroll_step_frac,
                step_ms=cfg.capture_scroll_step_ms, max_steps=cfg.capture_scroll_max_steps,
                img_wait_ms=cfg.capture_img_wait_ms)
            raw = await run_extract(page)
            shots = await capture_screenshots(
                page,
                max_screenshots=cfg.capture_max_screenshots,
                max_height=cfg.capture_max_screenshot_height,
                settle_ms=cfg.capture_section_settle_ms)
            # Classify capture quality (real content vs anti-bot challenge / thin SPA).
            # High-fidelity artifacts (design-styles, page.html) are GATED on "full":
            # rebuilding from a challenge/login page would reproduce the block page.
            capture_quality, blocked_reason = assess_quality(raw)
            if capture_quality != "full":
                logger.warning("capture: quality=%s (%s) for %s — brand-only, no page.html/"
                               "design-styles", capture_quality, blocked_reason, url)
            # URL-only site media catalog (images/videos/backgrounds/icons/fonts),
            # aligned with the reference asset cataloger. Collected for ALL quality
            # levels (even a partial page carries real media) and BEFORE the page.html
            # pass mutates the DOM. Best-effort: a failure here never fails the capture.
            asset_catalog = []
            page_videos = []
            try:
                asset_catalog = await catalog_assets(page, cap=cfg.capture_asset_catalog_cap)
            except Exception:
                logger.info("capture: asset catalog failed for %s", url, exc_info=True)
            try:
                page_videos = await video_descriptors(page, cap=cfg.capture_video_cap)
            except Exception:
                logger.info("capture: video descriptors failed for %s", url, exc_info=True)
            # Two-layer video manifest: merge the pre-nav network URLs (Layer 1) with
            # the DOM descriptors (Layer 2), deduped + capped. Built for ALL quality
            # levels (media exists even on a partial page). Preview frames are captured
            # here (non-mutating, before page.html) as PNG bytes; downloads happen after
            # the page closes (need storage). Best-effort — never aborts the capture.
            video_entries = merge_video_manifest(
                discovered_videos, page_videos, cap=cfg.capture_max_videos)
            if video_entries:
                try:
                    await _capture_video_previews(page, video_entries, video_previews)
                except Exception:
                    logger.info("capture: video previews failed for %s", url, exc_info=True)
            design_styles = None
            page_html = None
            if capture_quality == "full":
                # Animation catalog + captured shaders. Non-mutating, so BEFORE
                # page.html. Best-effort: any failure degrades (None / []) and
                # never aborts. CDP entries were accumulated live since goto.
                try:
                    animation_catalog = await collect_animation_catalog(page, cdp_entries)
                except Exception:
                    logger.info("capture: animation catalog failed for %s", url, exc_info=True)
                try:
                    shaders = await collect_shaders(page)
                except Exception:
                    logger.info("capture: shader collection failed for %s", url, exc_info=True)
                try:
                    design_styles = await extract_design_styles(page)  # non-mutating: before html
                except Exception:
                    logger.info("capture: design-styles extract failed for %s", url, exc_info=True)
                try:
                    page_html = await extract_page_html(page)  # MUTATES DOM — must be last
                except Exception:
                    logger.info("capture: page.html extract failed for %s", url, exc_info=True)
            # SVG contact-sheet rasterization: draw each captured SVG's markup to a
            # 200px PNG via the live page (best-effort — Pillow has no SVG rasterizer;
            # we add no cairosvg). page.evaluate only (no set_content), so it does NOT
            # mutate the visible DOM, but it MUST precede the lottie preview's
            # set_content below (which wipes the page). Any failure LOGS + yields []
            # (no SVG sheet), never aborts.
            try:
                svg_raster_items = await rasterize_svgs(
                    page, raw.get("svgs") or [], cap=cfg.capture_max_svg_sheet)
            except Exception:
                logger.info("svg contact sheet skipped: %s", url, exc_info=True)
            # Lotties: multi-source discovery (extractor: raw["assets"]["lotties"] +
            # legacy single `lottie`) -> download + dotLottie unzip + validate + store
            # the animation JSON, then a BEST-EFFORT in-page mid-frame preview render
            # (lottie-web via CDN). Runs at ALL quality levels (lotties exist on partial
            # pages). Placed dead-last in the page block because render_lottie_previews
            # does page.set_content, which DESTROYS the DOM — nothing page-dependent may
            # follow. Fully guarded: any failure logs + degrades (no manifest / no
            # previews), never aborts. Store/manifest assembly happens after close.
            lottie_urls: list = list(raw.get("assets", {}).get("lotties") or [])
            _legacy_lottie = raw.get("assets", {}).get("lottie")
            if _legacy_lottie and _legacy_lottie not in lottie_urls:
                lottie_urls.insert(0, _legacy_lottie)
            if lottie_urls:
                try:
                    lottie_refs, lottie_entries = await _save_lotties(
                        lottie_urls, kb_id, doc_id, cfg, deps.storage)
                except Exception:
                    logger.info("capture: lottie save failed for %s", url, exc_info=True)
                if lottie_entries:
                    try:
                        lottie_preview_pngs = await render_lottie_previews(
                            page, lottie_entries, max_bytes=cfg.capture_max_lottie_bytes)
                    except Exception:
                        logger.info("lottie preview render skipped: %s", url, exc_info=True)
        finally:
            # Detach the CDP session (if any) regardless of what happened above.
            if cdp_session is not None:
                try:
                    await cdp_session.detach()
                except Exception:
                    logger.info("capture: CDP detach failed for %s", url, exc_info=True)

    final_url = raw.get("final_url", url)
    vision_configured = bool(cfg.vision_base_url)
    storage = deps.storage
    raw_assets = raw.get("assets", {})

    # ---- page SVGs -> assets/svgs/ (content-hash names) ----
    svg_asset_refs = await _store_page_svgs(raw, kb_id, doc_id, cfg, storage)

    # ---- page.html (self-contained structural reference; only when quality==full) ----
    page_html_key = None
    if page_html:
        page_html_key = f"captures/{kb_id}/{doc_id}/extracted/page.html"
        await storage.put(page_html_key, page_html.encode("utf-8"),
                          content_type="text/html; charset=utf-8")

    # ---- image assets (logo/hero/og/favicon) -> fetch -> ImageRef ----
    image_refs, novision_asset_refs, filename_kind, seen_urls = \
        await _fetch_named_assets(raw_assets, cfg)

    # ---- screenshots -> ImageRef ----
    # Only the reference scroll-position strips (kind=="scroll") are uploaded/exported;
    # the internal full_page shot is used solely for the colour cross-check below.
    shot_refs: list = []
    shot_meta: list[tuple[str, dict]] = []
    for s in shots:
        if s["kind"] != "scroll":
            continue
        fname = f"screenshot-scroll-{s['pct']:03d}.png"
        shot_meta.append((fname, s))
        shot_refs.append(image_ref_from_bytes(s["bytes"], filename=fname, mime="image/png",
                                              width=s["width"], height=s["height"]))

    # Two upload calls split by whether the image should get a vision
    # description: logo/hero/og_image do; favicon + screenshots don't. The
    # post-upload re-query joins on filename, which is safe because filenames
    # are unique by construction: asset names are ``{kind}.{ext}`` for distinct
    # kinds (duplicate URLs dropped above), screenshot names are ``screenshot-
    # {label}.png`` for distinct labels — so name_picker never renames.
    novision_refs = novision_asset_refs + shot_refs
    if image_refs:
        await deps.upload_image_refs(image_refs, vision_configured=vision_configured)
    if novision_refs:
        await deps.upload_image_refs(novision_refs, vision_configured=False)

    fn_to_row = await deps.hydrate_image_ids()

    # ---- image assets -> profile ----
    profile_assets: list = list(svg_asset_refs)
    for fname, kind in filename_kind.items():
        row = fn_to_row.get(fname)
        if not row:
            continue
        img_id, skey = row
        profile_assets.append(AssetRef(
            kind=kind, image_id=img_id, storage_key=skey, format=fname.rsplit(".", 1)[-1],
            vision_status=("pending" if vision_configured and _asset_gets_vision(kind, fname)
                           else "skipped")))

    # ---- non-image binaries: background video / lottie ----
    profile_assets.extend(await _store_binaries(raw_assets, kb_id, doc_id, cfg, storage))

    # ---- lotties: attach downloaded refs + build manifest (previews rendered in-page) ----
    _lottie_refs, lottie_manifest = await _store_lottie_previews(
        lottie_entries, lottie_refs, lottie_preview_pngs, kb_id, doc_id, storage)
    profile_assets.extend(_lottie_refs)

    # ---- video manifest: preview frames + bounded/budgeted direct-ext downloads ----
    _video_refs, video_manifest = await _store_videos(
        video_entries, video_previews, capture_quality, kb_id, doc_id, cfg, storage)
    profile_assets.extend(_video_refs)

    # ---- good-context catalog images (beyond the 4 named kinds) ----
    _catalog_refs, asset_sheet_items = await _download_catalog_images(
        asset_catalog, seen_urls, kb_id, doc_id, cfg, storage)
    profile_assets.extend(_catalog_refs)

    # ---- contact sheets: scroll (screenshots) + asset (catalog images) + svg ----
    profile_assets.extend(await _build_contact_sheets(
        shots, asset_sheet_items, svg_raster_items, kb_id, doc_id, url, storage))

    # ---- fonts (catalog match; download woff2 on miss + bounded site face set) ----
    fonts, font_files = await _build_fonts(raw, kb_id, doc_id, cfg, storage)

    # ---- screenshots -> profile ----
    profile_shots: list = []
    for fname, s in shot_meta:
        row = fn_to_row.get(fname)
        if not row:
            continue
        img_id, _skey = row
        profile_shots.append(ScreenshotRef(kind=s["kind"], image_id=img_id,
                                           width=s["width"], height=s["height"],
                                           section_index=s["section_index"],
                                           pct=s.get("pct")))

    # ---- colors / spacing / sections / headings / svgs / page geometry ----
    colors, spacing, sections, headings, svgs, page = _build_layout_tokens(
        raw, shots, profile_shots, cfg)

    # Drop the transient parsed-lottie dicts (carried only for the preview pass) so
    # the persisted manifest stays lean + JSON-serializable.
    if lottie_manifest:
        for e in lottie_manifest.get("lotties", []):
            e.pop("_parsed", None)

    t = raw.get("text", {})
    profile = SiteProfile(
        url=final_url, captured_at=datetime.now(timezone.utc).isoformat(),
        fetch_tier="playwright",
        capture_quality=capture_quality, blocked_reason=blocked_reason,
        page_html_key=page_html_key, design_styles=design_styles,
        colors=colors, theme_color=raw.get("colors", {}).get("theme_color"),
        og_image=raw.get("assets", {}).get("og_image") or "",
        # Phase 2 — reference-shaped color parity alongside the role tokens:
        # top-20 usage-ranked hexes + top-48 raw stat dicts (build-frame reads
        # these directly; role tokens above stay for vectoria's own downstream).
        colors_ranked=raw.get("colors", {}).get("ranked", []) or [],
        color_stats=raw.get("colors", {}).get("stats", []) or [],
        css_variables=raw.get("colors", {}).get("css_vars", {}) or {},
        headings=headings, svgs=svgs, page=page,
        fonts=fonts, font_files=font_files, spacing=spacing, sections=sections,
        text=TextInfo(headline=t.get("headline", ""), tagline=t.get("tagline", ""),
                      ctas=t.get("ctas", []), cta_links=t.get("cta_links", []),
                      visible_text=t.get("visible_text", ""),
                      full_text=t.get("full_text", "")),
        assets=profile_assets, screenshots=profile_shots,
        motion_hints=_motion_hints(MotionHints, raw.get("motion", {}), shaders),
        asset_catalog=asset_catalog, videos=page_videos,
        animation_catalog=animation_catalog, shaders=shaders,
        video_manifest=video_manifest, lottie_manifest=lottie_manifest)

    return CaptureOutcome(
        profile=profile,
        title=(t.get("headline") or final_url),
        has_images=bool(image_refs or novision_refs),
        enqueue_image_analysis=bool(vision_configured and image_refs))
