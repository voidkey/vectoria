"""Reusable website-capture orchestration: URL -> SiteProfile.

Extracted from worker/handlers.py::_capture_core so the capture pipeline is
testable without a queue/browser/DB and reusable outside the worker. All side
effects (browser page, object storage, image upload + image-id hydration) are
injected via CaptureDeps; the worker supplies a real implementation and does the
final update_doc + enqueue based on the returned CaptureOutcome."""
from __future__ import annotations

import hashlib
import logging
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from parsers.capture.profile import SiteProfile

logger = logging.getLogger(__name__)

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


def _asset_gets_vision(kind: str, fname: str) -> bool:
    """True only for vision-worthy kinds whose stored format is a raster the
    vision model can decode (not svg/ico)."""
    return kind in _VISION_ASSET_KINDS and fname.rsplit(".", 1)[-1] in _VISION_RASTER_EXT


def _safe_hex(css: str) -> str:
    from parsers.capture._colors import _to_hex, parse_css_color
    rgb = parse_css_color(css or "")
    return _to_hex(rgb) if rgb else ""


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
    from parsers.capture._assets import fetch_asset_bytes, image_ref_from_bytes
    from parsers.capture._colors import dominant_screenshot_hex, process_colors
    from parsers.capture._design_styles import extract_design_styles
    from parsers.capture._extract import run_extract
    from parsers.capture._fonts import build_font_role, cluster_spacing, section_type
    from parsers.capture._html import extract_page_html
    from parsers.capture._media import catalog_assets, video_descriptors
    from parsers.capture._quality import assess_quality
    from parsers.capture._screenshots import (
        NEUTRALIZE_ANIMATION_CSS, capture_screenshots, prepare_page)
    from parsers.capture.profile import (
        AssetRef, FontFile, Fonts, Heading, MotionHints, PageGeom, ScreenshotRef,
        SectionInfo, Spacing, SvgInfo, TextInfo)

    async with deps.open_page() as page:
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
            page, raw.get("sections", []),
            max_screenshots=cfg.capture_max_screenshots,
            max_height=cfg.capture_max_screenshot_height,
            section_settle_ms=cfg.capture_section_settle_ms)
        # Classify capture quality (real content vs anti-bot challenge / thin SPA).
        # High-fidelity artifacts (design-styles, page.html) are GATED on "full":
        # rebuilding from a challenge/login page would reproduce the block page.
        capture_quality, blocked_reason = assess_quality(raw)
        if capture_quality != "full":
            logger.warning("capture: quality=%s (%s) for %s — brand-only, no page.html/"
                           "design-styles", capture_quality, blocked_reason, url)
        # URL-only site media catalog (images/videos/backgrounds/icons/fonts),
        # aligned with hyperframes' asset cataloger. Collected for ALL quality
        # levels (even a partial page carries real media) and BEFORE the page.html
        # pass mutates the DOM. Best-effort: a failure here never fails the capture.
        asset_catalog: list = []
        page_videos: list = []
        try:
            asset_catalog = await catalog_assets(page, cap=cfg.capture_asset_catalog_cap)
        except Exception:
            logger.info("capture: asset catalog failed for %s", url, exc_info=True)
        try:
            page_videos = await video_descriptors(page, cap=cfg.capture_video_cap)
        except Exception:
            logger.info("capture: video descriptors failed for %s", url, exc_info=True)
        design_styles = None
        page_html = None
        if capture_quality == "full":
            try:
                design_styles = await extract_design_styles(page)  # non-mutating: before html
            except Exception:
                logger.info("capture: design-styles extract failed for %s", url, exc_info=True)
            try:
                page_html = await extract_page_html(page)  # MUTATES DOM — must be last
            except Exception:
                logger.info("capture: page.html extract failed for %s", url, exc_info=True)

    final_url = raw.get("final_url", url)
    vision_configured = bool(cfg.vision_base_url)
    storage = deps.storage
    raw_assets = raw.get("assets", {})

    # ---- page SVGs -> assets/svgs/ (content-hash names) ----
    # SVGs come from the already-extracted markup (raw["svgs"][*]["outerHTML"]) —
    # no fetch needed, so no SSRF check applies; we store the markup bytes directly.
    # Named by sha1-of-markup ("logo-<hash>" when isLogo, else "svg-<hash>") so the
    # filename can't drift from content and duplicate SVGs dedupe on the same hash.
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

    # ---- page.html (self-contained structural reference; only when quality==full) ----
    page_html_key = None
    if page_html:
        page_html_key = f"captures/{kb_id}/{doc_id}/extracted/page.html"
        await storage.put(page_html_key, page_html.encode("utf-8"),
                          content_type="text/html; charset=utf-8")

    # ---- image assets (logo/hero/og/favicon) -> fetch -> ImageRef ----
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

    # ---- screenshots -> ImageRef ----
    shot_refs: list = []
    shot_meta: list[tuple[str, dict]] = []
    for s in shots:
        label = s["kind"] if s["section_index"] is None else f"section-{s['section_index']:02d}"
        fname = f"screenshot-{label}.png"
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
    for kind, a_url in (("background_video", raw_assets.get("video")),
                        ("lottie", raw_assets.get("lottie"))):
        if not a_url:
            continue
        got = await fetch_asset_bytes(a_url, max_bytes=cfg.capture_max_asset_bytes)
        if got is None:
            continue
        data, ctype = got
        ext = _BIN_EXT.get(ctype, ".bin")
        key = f"captures/{kb_id}/{doc_id}/assets/{kind}{ext}"
        await storage.put(key, data, content_type=ctype or "application/octet-stream")
        profile_assets.append(AssetRef(kind=kind, storage_key=key, format=ext.lstrip(".")))

    # ---- fonts (catalog match; download woff2 on miss) ----
    face_srcs = raw.get("fonts", {}).get("face_srcs", {})

    async def _font_role(info: dict):
        role = build_font_role(info, weights=[info.get("weight", 400)])
        if not role.renderable:
            srcs = face_srcs.get(role.family.lower(), [])
            if srcs:
                got = await fetch_asset_bytes(srcs[0], max_bytes=cfg.capture_max_asset_bytes)
                if got:
                    data, _ = got
                    key = (f"captures/{kb_id}/{doc_id}/fonts/"
                           f"{role.family.replace(' ', '-').lower()}.woff2")
                    await storage.put(key, data, content_type="font/woff2")
                    role.files = [FontFile(url=key, weight=info.get("weight"), source="captured")]
        return role

    raw_fonts = raw.get("fonts", {})
    fonts = Fonts(display=await _font_role(raw_fonts.get("display", {})),
                  body=await _font_role(raw_fonts.get("body", {})))

    # ---- screenshots -> profile ----
    profile_shots: list = []
    for fname, s in shot_meta:
        row = fn_to_row.get(fname)
        if not row:
            continue
        img_id, _skey = row
        profile_shots.append(ScreenshotRef(kind=s["kind"], image_id=img_id,
                                           width=s["width"], height=s["height"],
                                           section_index=s["section_index"]))

    # ---- colors / spacing / sections ----
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
        type=section_type(s.get("heading", ""), s.get("classNames", []),
                          s["index"], len(raw_sections)),
        bg_color=_safe_hex(s.get("bg", "")),
        screenshot_image_id=shot_by_section.get(s["index"]),
        layout=s.get("layout", ""),
        background_image=s.get("backgroundImage", ""),
        cta_texts=s.get("callsToAction", []),
        asset_urls=s.get("assetUrls", []),
        text=s.get("text", ""),
    ) for s in raw_sections]

    # ---- Phase 1 tokens: headings / svgs / page geometry (hyperframes parity) ----
    # camelCase (raw) -> snake_case (profile); SVG outerHTML is DROPPED here so the
    # raw markup never lands in the persisted profile (DB-bloat guard).
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

    t = raw.get("text", {})
    profile = SiteProfile(
        url=final_url, captured_at=datetime.now(timezone.utc).isoformat(),
        fetch_tier="playwright",
        capture_quality=capture_quality, blocked_reason=blocked_reason,
        page_html_key=page_html_key, design_styles=design_styles,
        colors=colors, theme_color=raw.get("colors", {}).get("theme_color"),
        # Phase 2 — reference-shaped color parity alongside the role tokens:
        # top-20 usage-ranked hexes + top-48 raw stat dicts (build-frame reads
        # these directly; role tokens above stay for vectoria's own downstream).
        colors_ranked=raw.get("colors", {}).get("ranked", []) or [],
        color_stats=raw.get("colors", {}).get("stats", []) or [],
        css_variables=raw.get("colors", {}).get("css_vars", {}) or {},
        headings=headings, svgs=svgs, page=page,
        fonts=fonts, spacing=spacing, sections=sections,
        text=TextInfo(headline=t.get("headline", ""), tagline=t.get("tagline", ""),
                      ctas=t.get("ctas", []), full_text=t.get("full_text", "")),
        assets=profile_assets, screenshots=profile_shots,
        motion_hints=MotionHints(**raw.get("motion", {})),
        asset_catalog=asset_catalog, videos=page_videos)

    return CaptureOutcome(
        profile=profile,
        title=(t.get("headline") or final_url),
        has_images=bool(image_refs or novision_refs),
        enqueue_image_analysis=bool(vision_configured and image_refs))
