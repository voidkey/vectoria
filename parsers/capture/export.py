"""Build a hyperframes-compatible capture/ zip from a stored SiteProfile."""
from __future__ import annotations

import asyncio
import io
import json
import re
import zipfile
from urllib.parse import urlparse

from sqlalchemy import select

from db.base import get_session
from db.models import DocumentImage
from parsers.capture._font_metadata import build_fonts_manifest
from storage import get_storage


async def _image_keys(doc_id: str) -> dict[str, str]:
    async with get_session() as session:
        rows = (await session.execute(
            select(DocumentImage.id, DocumentImage.storage_key)
            .where(DocumentImage.doc_id == doc_id))).all()
    return {i: k for i, k in rows}


def _fonts_array(fonts: dict) -> list[dict]:
    """Flatten the role-keyed Fonts object ({display, body}) into the official
    hyperframes tokens.json `fonts` array [{family, weights, ...}], deduped by
    family (display first so it wins the display role in build-frame)."""
    out: list[dict] = []
    seen: set[str] = set()
    for role in ("display", "body"):
        fr = fonts.get(role) or {}
        fam = (fr.get("family") or "").strip()
        if not fam or fam.lower() in seen:
            continue
        seen.add(fam.lower())
        entry = {"family": fam, "weights": fr.get("weights", [])}
        cm = fr.get("catalog_match") or {}
        if cm.get("css_url"):
            entry["css_url"] = cm["css_url"]  # renderable brand font served from a CDN
        out.append(entry)
    return out


def _ranked_colors(profile: dict) -> list[str]:
    """The top-N usage-ranked hex list. Falls back to the role tokens' hexes for
    profiles captured before the ranking pass (empty ``colors_ranked``) so
    downstream never gets an empty color list."""
    return profile.get("colors_ranked") or [
        c["hex"] for c in (profile.get("colors") or []) if c.get("hex")]


def _official_tokens(profile: dict) -> dict:
    """tokens.json in the official hyperframes shape build-frame.mjs reads:
    {title, description, colors, fonts[], colorStats, spacing}. `colors` is the
    top-20 usage-ranked hex STRING list (reference shape) and `colorStats` the
    REAL top-48 per-hex stats (Phase 2 — was synthetic object-colors + a stats
    projection). title/description come from the captured page text; fonts are
    projected from vectoria's role-keyed Fonts. If a stored profile predates the
    ranking pass (empty colors_ranked), fall back to the role tokens' hexes so
    downstream never gets an empty `colors`; colorStats then falls back to []."""
    text = profile.get("text", {}) or {}
    ranked = _ranked_colors(profile)
    out = {
        "title": text.get("headline", ""),
        "description": text.get("tagline", ""),
        "ctas": text.get("ctas", []),  # extra (official schema ignores unknown keys); handy for downstream summaries
        "colors": ranked,
        "fonts": _fonts_array(profile.get("fonts", {}) or {}),
        "colorStats": profile.get("color_stats") or [],
        "spacing": profile.get("spacing", {}),
        # Phase 1 — hyperframes DesignTokens parity. Vectoria's profile is
        # snake_case; project back to the verbatim camelCase keys build-frame reads.
        "cssVariables": profile.get("css_variables", {}) or {},
        "headings": _headings_out(profile.get("headings", []) or []),
        "svgs": _svgs_out(profile.get("svgs", []) or []),
        "sections": _sections_out(profile.get("sections", []) or []),
        # extra: lets downstream (go-figlens) gate structural rebuild on fidelity.
        "capture_quality": profile.get("capture_quality", "full"),
    }
    page = profile.get("page")
    if page:
        out["page"] = {"width": page.get("width", 0), "height": page.get("height", 0),
                       "viewport": {"width": page.get("viewport_width", 0),
                                    "height": page.get("viewport_height", 0)}}
    return out


def _headings_out(headings: list[dict]) -> list[dict]:
    """snake_case Heading -> hyperframes camelCase (fontSize/fontWeight)."""
    return [{"level": h["level"], "text": h.get("text", ""),
             "fontSize": h.get("font_size", ""), "fontWeight": h.get("font_weight", ""),
             "color": h.get("color", "")} for h in headings]


def _svgs_out(svgs: list[dict]) -> list[dict]:
    """snake_case SvgInfo -> hyperframes camelCase (viewBox/isLogo). Metadata only —
    outerHTML was never persisted to the profile (DB-bloat guard)."""
    return [{"label": s.get("label", ""), "viewBox": s.get("view_box", ""),
             "width": s.get("width", 0), "height": s.get("height", 0),
             "isLogo": bool(s.get("is_logo", False))} for s in svgs]


def _sections_out(sections: list[dict]) -> list[dict]:
    """snake_case SectionInfo -> hyperframes DesignTokens `sections` shape
    (backgroundColor/backgroundImage/callsToAction/assetUrls)."""
    return [{"type": s.get("type", "generic"), "heading": s.get("heading", ""),
             "backgroundColor": s.get("bg_color", ""),
             "backgroundImage": s.get("background_image", ""),
             "callsToAction": s.get("cta_texts", []) or [],
             "assetUrls": s.get("asset_urls", []) or [],
             "layout": s.get("layout", ""), "text": s.get("text", "")}
            for s in sections]


def _lean_animations(catalog: dict) -> dict:
    """Project the raw AnimationCatalog into the LEAN animations.json shape the
    agent reads (ported from index.ts): summary + the unique named CSS animations
    + a scroll-trigger count + ≤10 keyframed Web Animations (the entries most
    useful for recreation). Avoids dumping hundreds of raw CSS declarations."""
    named: list[str] = []
    for d in catalog.get("cssDeclarations") or []:
        name = ((d.get("animation") or {}).get("name") or "").strip()
        if name and name not in named:
            named.append(name)
    representative = [a for a in (catalog.get("webAnimations") or [])
                      if a.get("keyframes")][:10]
    return {
        "summary": catalog.get("summary", {}),
        "namedAnimations": named,
        "scrollTriggeredElements": len(catalog.get("scrollTargets") or []),
        "representativeAnimations": representative,
    }


# Cap on synthesized @font-face rules (mirrors hyperframes MAX_TOTAL_FONTS).
_MAX_TOTAL_FONTS = 30


def _font_face_block(family: str, weight, style: str, basename: str) -> str:
    return (
        "@font-face {\n"
        f"  font-family: \"{family}\";\n"
        f"  font-weight: {weight or 400};\n"
        f"  font-style: {style or 'normal'};\n"
        f"  src: url(\"./{basename}\") format(\"woff2\");\n"
        "}")


def _fonts_css(profile: dict) -> str:
    """Synthesize an @font-face stylesheet for every captured face.

    Phase 4: emits one @font-face per entry in ``profile["font_files"]`` (the
    bounded site face set + role fonts, with fonttools-derived family/weight/style),
    each referencing its woff2 locally as ``./<basename>`` (staged alongside this
    CSS at capture/assets/fonts/). Falls back to the role-font ``files`` for older
    profiles that predate ``font_files``. Only captured files (stored under
    ``captures/``) are included; CDN/catalog fonts have no local file. Faces capped
    at _MAX_TOTAL_FONTS. Returns "" when there are no captured files."""
    blocks: list[str] = []
    font_files = profile.get("font_files") or []
    if font_files:
        for m in font_files:
            if len(blocks) >= _MAX_TOTAL_FONTS:
                break
            key = m.get("storage_key") or ""
            family = (m.get("family") or "").strip()
            if not key.startswith("captures/") or not family:
                continue
            blocks.append(_font_face_block(
                family, m.get("weight"), m.get("style"), key.rsplit("/", 1)[-1]))
        return "\n".join(blocks)

    # Fallback: old profiles without font_files — synthesize from role-font files.
    fonts = profile.get("fonts", {}) or {}
    for role in ("display", "body"):
        fr = fonts.get(role) or {}
        family = (fr.get("family") or "").strip()
        if not family:
            continue
        for f in fr.get("files", []) or []:
            if len(blocks) >= _MAX_TOTAL_FONTS:
                break
            key = f.get("url") or ""
            if not key.startswith("captures/"):
                continue  # CDN/catalog font — no local file to reference
            blocks.append(_font_face_block(
                family, f.get("weight"), f.get("style"), key.rsplit("/", 1)[-1]))
    return "\n".join(blocks)


def _asset_zip_path(a: dict, storage_key: str) -> str:
    """ZIP path for one AssetRef. Downloaded assets (SVGs under assets/svgs/,
    bulk catalog images with kind=="image") are keyed by the BASENAME of their
    already-unique storage_key (content-hash / derived slug) so many of them
    can't collide on ``{kind}.{format}`` (every svg -> svg.svg, every image ->
    image.jpg). Named assets (logo/hero/og_image/favicon/background_video/lottie)
    keep the stable ``{kind}.{format}`` name."""
    basename = storage_key.rsplit("/", 1)[-1]
    if "/assets/svgs/" in storage_key:
        return f"capture/assets/svgs/{basename}"
    # Phase 7 video manifest: downloaded bodies + preview frames are keyed by their
    # already-unique storage_key basename (video-N.ext / video-N-preview.png) under
    # the videos/ tree — many of them can't collapse on {kind}.{format}.
    if a.get("kind") == "video_preview" or "/assets/videos/previews/" in storage_key:
        return f"capture/assets/videos/previews/{basename}"
    if a.get("kind") == "video" or "/assets/videos/" in storage_key:
        return f"capture/assets/videos/{basename}"
    if a.get("kind") == "image":
        return f"capture/assets/{basename}"
    # Phase 8 contact sheets: JPEG grid pages route to the reference paths by their
    # storage_key subdir — screenshots/ (scroll sheet), assets/svgs/ (svg sheet), or
    # assets/ (asset sheet). Keyed by the unique contact-sheet-N.jpg basename.
    if a.get("kind") == "contact_sheet":
        if "/screenshots/" in storage_key:
            return f"capture/screenshots/{basename}"
        if "/assets/svgs/" in storage_key:
            return f"capture/assets/svgs/{basename}"
        return f"capture/assets/{basename}"
    # Phase 8 lottie: the animation JSON (dotLottie-unzipped) + best-effort mid-frame
    # previews are keyed by their already-unique storage_key basename (animation-N.json
    # / animation-N-preview.png) under the lottie/ tree.
    if a.get("kind") == "lottie_preview" or "/assets/lottie/previews/" in storage_key:
        return f"capture/assets/lottie/previews/{basename}"
    if a.get("kind") == "lottie_json" or "/assets/lottie/" in storage_key:
        return f"capture/assets/lottie/{basename}"
    return f"capture/assets/{a.get('kind')}.{a.get('format', 'bin')}"


def infer_color_role(hex_str: str) -> str:
    """Human-readable role hint from a hex color via luminance + saturation.
    Ported verbatim (thresholds) from agentPromptGenerator.ts::inferColorRole —
    just orients an agent scanning the brand summary; not a substitute for real
    design analysis. Returns "color" for anything that can't be parsed as #RRGGBB."""
    try:
        r = int(hex_str[1:3], 16) / 255
        g = int(hex_str[3:5], 16) / 255
        b = int(hex_str[5:7], 16) / 255
    except (ValueError, IndexError, TypeError):
        # TypeError guards non-str input (None / an int slipping through the
        # ranked-color list) so one bad entry can't abort the whole export.
        return "color"
    mx, mn = max(r, g, b), min(r, g, b)
    luminance = 0.2126 * r + 0.7152 * g + 0.0722 * b
    saturation = 0.0 if mx == 0 else (mx - mn) / mx
    if luminance < 0.04:
        return "bg-dark"
    if luminance > 0.9:
        return "bg-light"
    if saturation > 0.4 and 0.05 < luminance < 0.7:
        return "accent"
    if luminance < 0.2:
        return "surface-dark"
    if luminance > 0.7:
        return "surface-light"
    return "neutral"


def _hostname(url: str) -> str:
    """Bare hostname (www. stripped), the reference's title/id fallback."""
    try:
        host = urlparse(url).hostname or ""
    except ValueError:
        host = ""
    return host[4:] if host.startswith("www.") else host


def _meta_json(profile: dict) -> dict:
    """Project metadata (capture/meta.json). The reference scaffolding.ts writes a
    minimal {id, name}; we keep those for parity and add the richer vectoria fields
    (url/capturedAt/captureQuality/counts/generatedBy) the export surfaces. Counts
    are derived from the assembled profile so downstream can size the capture at a
    glance without unzipping every artifact."""
    url = profile.get("url", "")
    host = _hostname(url)
    title = (profile.get("text", {}) or {}).get("headline", "") or host
    video_manifest = profile.get("video_manifest") or {}
    lottie_manifest = profile.get("lottie_manifest") or {}
    ranked = _ranked_colors(profile)
    counts = {
        "screenshots": len(profile.get("screenshots") or []),
        "assets": len(profile.get("assets") or []),
        "fonts": len(_fonts_array(profile.get("fonts", {}) or {})),
        "videos": len(video_manifest.get("videos") or []),
        "lotties": len(lottie_manifest.get("lotties") or []),
        "colors": len(ranked),
    }
    return {
        "id": (host + "-video") if host else "capture-video",
        "name": title,
        "url": url,
        "title": title,
        "capturedAt": profile.get("captured_at", ""),
        "captureQuality": profile.get("capture_quality", "full"),
        "counts": counts,
        "generatedBy": "vectoria",
    }


# Contact-sheet rows: match `contact-sheet.jpg` + digit-suffixed paginated pages
# (`contact-sheet-2.jpg`) under a dir, but NOT the `contact-sheet-svgs.jpg` fallback
# sheet — the "-N" suffix is digits only (ported from agentPromptGenerator.ts).
_PAGINATED_RE = re.compile(r"^contact-sheet(?:-(\d+))?\.jpg$")


def _contact_sheet_rows(written: set[str], dir_: str, label: str) -> list[str]:
    """Reference contactSheetRows port: enumerate the paginated contact-sheet pages
    ACTUALLY present under capture/<dir>/, page-numbered so 10 sorts after 2."""
    prefix = f"capture/{dir_}/"
    pages: list[tuple[int, str]] = []
    for path in written:
        if not path.startswith(prefix):
            continue
        base = path[len(prefix):]
        m = _PAGINATED_RE.match(base)
        if m:
            pages.append((int(m.group(1) or 0), base))
    pages.sort()
    if not pages:
        return []
    if len(pages) == 1:
        return [f"| `{dir_}/{pages[0][1]}` | {label} |"]
    n = len(pages)
    return [f"| `{dir_}/{b}` | {label} — page {i + 1} of {n} |"
            for i, (_p, b) in enumerate(pages)]


def _agent_prompt(profile: dict, written: set[str]) -> str:
    """Build the AGENTS.md / CLAUDE.md / .cursorrules body (identical content) from
    the assembled profile + the set of capture/ paths ACTUALLY written to this zip.
    Ported from agentPromptGenerator.ts::buildPrompt: a data-inventory table listing
    only the artifacts present (incl. paginated contact sheets), a brand summary with
    infer_color_role hints + fonts, and a pointer to the product-launch-video skill."""
    url = profile.get("url", "")
    text = profile.get("text", {}) or {}
    title = text.get("headline", "") or _hostname(url)
    ranked = _ranked_colors(profile)

    color_summary = ", ".join(
        f"{hex_} ({infer_color_role(hex_)})" for hex_ in ranked[:10])
    fonts = profile.get("fonts", {}) or {}
    font_parts: list[str] = []
    seen_fam: set[str] = set()
    for role in ("display", "body"):
        fr = fonts.get(role) or {}
        fam = (fr.get("family") or "").strip()
        if not fam or fam.lower() in seen_fam:
            continue
        seen_fam.add(fam.lower())
        weights = fr.get("weights") or []
        font_parts.append(f"{fam} ({','.join(map(str, weights))})" if weights else fam)
    font_summary = ", ".join(font_parts) or "none detected"

    def present(path: str) -> bool:
        return f"capture/{path}" in written

    rows: list[str] = []
    # Screenshots — contact sheet(s) first, then the individual frames.
    ss_rows = _contact_sheet_rows(
        written, "screenshots",
        "**View this first.** All scroll screenshots in a labeled grid — see the "
        "entire page at a glance")
    if ss_rows:
        rows += ss_rows
    if any(p.startswith("capture/screenshots/") and p.endswith(".png") for p in written):
        rows.append("| `screenshots/*.png` | Individual viewport screenshots for "
                    "detail on a specific section. |")
    # Core extracted artifacts — always present.
    rows.append(
        f"| `extracted/tokens.json` | Design tokens: {len(ranked)} colors, "
        f"{len(_fonts_array(fonts))} fonts, {len(profile.get('headings') or [])} "
        f"headings, {len(text.get('ctas') or [])} CTAs |")
    rows.append("| `extracted/fonts.json` | Role-keyed display/body font families. |")
    if present("extracted/fonts-manifest.json"):
        rows.append("| `extracted/fonts-manifest.json` | Captured font faces "
                    "(family/weights) from fonttools. |")
    if present("extracted/design-styles.json"):
        rows.append("| `extracted/design-styles.json` | Computed styles from the live "
                    "DOM: typography, buttons/cards/nav, spacing, radii, shadows. "
                    "Primary source for DESIGN.md. |")
    rows.append("| `extracted/asset-descriptions.md` | One-line description of every "
                "downloaded asset. Read before opening individual files. |")
    rows.append("| `extracted/visible-text.txt` | Page text in DOM order. Use as "
                "context — rephrase freely. |")
    if present("extracted/page.html"):
        rows.append("| `extracted/page.html` | Self-contained structural recreation "
                    "of the page. |")
    if present("extracted/animations.json"):
        rows.append("| `extracted/animations.json` | Captured animation catalog "
                    "(named CSS + representative keyframes). |")
    if present("extracted/video-manifest.json"):
        rows.append("| `extracted/video-manifest.json` | Discovered videos with local "
                    "bodies at `assets/videos/` and previews at "
                    "`assets/videos/previews/`. |")
    if present("extracted/lottie-manifest.json"):
        rows.append("| `extracted/lottie-manifest.json` | Lottie animations with "
                    "previews at `assets/lottie/previews/`. |")
    if present("extracted/shaders.json"):
        rows.append("| `extracted/shaders.json` | WebGL shader source (GLSL). |")
    # Asset + SVG contact sheets (paginated).
    rows += _contact_sheet_rows(
        written, "assets",
        "Downloaded images in a labeled grid — view before opening individual files")
    rows += _contact_sheet_rows(
        written, "assets/svgs", "SVGs rendered as thumbnails in a labeled grid")
    if any(p.startswith("capture/assets/fonts/") for p in written):
        rows.append("| `assets/fonts/` | Captured woff2 faces + a `fonts.css` with "
                    "local @font-face rules. |")
    rows.append("| `assets/` | Individual downloaded images, SVGs, and font files. |")

    brand = [f"- **Colors**: {color_summary or 'see tokens.json'}",
             f"- **Fonts**: {font_summary}"]
    if title:
        brand.append(f"- **Title**: {title}")
    if text.get("tagline"):
        brand.append(f"- **Tagline**: {text['tagline']}")

    table = "\n".join(rows)
    brand_block = "\n".join(brand)
    return (
        f"# {title}\n\n"
        f"Source: {url}\n\n"
        "To create a video from this capture, use the `product-launch-video` skill.\n\n"
        "## What's in This Capture\n\n"
        "| File | Contents |\n|------|----------|\n"
        f"{table}\n\n"
        "## Brand Summary\n\n"
        f"{brand_block}\n")


async def build_hyperframes_zip(doc) -> bytes:
    profile = doc.profile or {}
    storage = await get_storage()
    keys = await _image_keys(doc.id)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        # extracted/tokens.json — official hyperframes shape (title/description/
        # colors[str]/fonts[]/colorStats/spacing) so build-frame.mjs remixes
        # brand colors AND fonts onto the preset. `colors` is the top-20 ranked
        # hex list and `colorStats` the REAL per-hex stats that drive role
        # detection (no longer projected from role tokens/luminance fallback).
        zf.writestr("capture/extracted/tokens.json",
                    json.dumps(_official_tokens(profile), ensure_ascii=False, indent=2))
        # extracted/fonts.json — the role-keyed Fonts object (legacy shape).
        zf.writestr("capture/extracted/fonts.json",
                    json.dumps(profile.get("fonts", {}), ensure_ascii=False, indent=2))
        # extracted/fonts-manifest.json — the REAL types.ts::FontsManifest built from
        # captured font bytes (fonttools). Fallback for old profiles without
        # font_files: an empty-but-well-formed manifest so downstream never breaks.
        font_files = profile.get("font_files") or []
        manifest = build_fonts_manifest(font_files, profile.get("captured_at", ""))
        zf.writestr("capture/extracted/fonts-manifest.json",
                    json.dumps(manifest, ensure_ascii=False, indent=2))
        # extracted/visible-text.txt
        t = profile.get("text", {})
        text_body = "\n\n".join(filter(None, [
            t.get("headline", ""), t.get("tagline", ""),
            "\n".join(t.get("ctas", [])), t.get("full_text", "")]))
        zf.writestr("capture/extracted/visible-text.txt", text_body)
        # extracted/asset-descriptions.md — under extracted/ (the path the skills
        # read: capture/extracted/asset-descriptions.md), not top-level capture/.
        # Skip blank-description assets (Phase 3 adds many svg/logo/image refs with
        # description=""): a media-heavy page would otherwise emit dozens of noise
        # lines with nothing after the colon. Fallback stays "(no descriptions)".
        desc = "\n".join(f"- **{a.get('kind')}**: {a.get('description', '')}"
                         for a in profile.get("assets", []) if a.get("description"))
        zf.writestr("capture/extracted/asset-descriptions.md", desc or "(no descriptions)")
        # extracted/design-styles.json — computed design system (only present when
        # capture_quality == full). Inlined in the profile, so write directly.
        if profile.get("design_styles"):
            zf.writestr("capture/extracted/design-styles.json",
                        json.dumps(profile["design_styles"], ensure_ascii=False, indent=2))
        # extracted/animations.json — LEAN animation catalog (summary + named +
        # ≤10 keyframed Web Animations). Only when a catalog was collected
        # (capture_quality == full); absent -> file omitted.
        anim_catalog = profile.get("animation_catalog")
        if anim_catalog:
            zf.writestr("capture/extracted/animations.json",
                        json.dumps(_lean_animations(anim_catalog),
                                   ensure_ascii=False, indent=2))
        # extracted/shaders.json — the deduped captured GLSL. Omitted when empty.
        shaders = profile.get("shaders") or []
        if shaders:
            zf.writestr("capture/extracted/shaders.json",
                        json.dumps(shaders, ensure_ascii=False, indent=2))
        # extracted/video-manifest.json — two-layer (network + DOM) video manifest
        # (Phase 7). Carries per-video metadata + preview/local-body paths; the
        # binaries themselves are routed below (kind=="video"/"video_preview").
        # Omitted for old profiles / pages with no discovered videos.
        video_manifest = profile.get("video_manifest")
        if video_manifest:
            zf.writestr("capture/extracted/video-manifest.json",
                        json.dumps(video_manifest, ensure_ascii=False, indent=2))
        # extracted/lottie-manifest.json — Phase 8 lottie manifest. Written as the
        # bare `lotties` array (mediaCapture.ts renderLottiePreviews parity: file/url/
        # name/width/height/duration/frameRate/layers/preview?). The animation JSON +
        # preview binaries are routed below (kind=="lottie_json"/"lottie_preview").
        # Omitted for old profiles / pages with no discovered lotties.
        lottie_manifest = profile.get("lottie_manifest")
        if lottie_manifest and lottie_manifest.get("lotties"):
            zf.writestr("capture/extracted/lottie-manifest.json",
                        json.dumps(lottie_manifest["lotties"], ensure_ascii=False, indent=2))

        # assets/fonts/fonts.css — synthesized @font-face stylesheet pointing at
        # the captured woff2 files (staged alongside at capture/assets/fonts/), so
        # build-frame can register the faces locally. Only emitted when there are
        # captured font files. Generated from profile["fonts"] (no S3 needed).
        fonts_css = _fonts_css(profile)
        if fonts_css:
            zf.writestr("capture/assets/fonts/fonts.css", fonts_css)

        # Collect every binary member as (zip_path, storage_key), then fetch
        # them from S3 concurrently — a capture can have ~17 objects and the
        # export is a synchronous response, so sequential GETs cost seconds.
        members: list[tuple[str, str]] = []
        # page.html — self-contained structural reference (only when quality==full).
        if profile.get("page_html_key"):
            members.append(("capture/extracted/page.html", profile["page_html_key"]))
        for a in profile.get("assets", []):
            skey = a.get("storage_key")
            if not skey:
                continue
            members.append((_asset_zip_path(a, skey), skey))
        for s in profile.get("screenshots", []):
            key = keys.get(s.get("image_id"))
            if key:
                label = (s.get("kind") if s.get("section_index") is None
                         else f"section-{s['section_index']:02d}")
                members.append((f"capture/screenshots/{label}.png", key))
        # Captured woff2 faces -> capture/assets/fonts/ (where build-frame.mjs globs
        # to stage @font-face faces). Phase 4: the bounded face set (font_files) plus
        # the role-font files; deduped by storage key so a role font that's also in
        # font_files isn't written twice. Old profiles carry only role-font files.
        seen_font_keys: set[str] = set()
        font_keys = [m.get("storage_key") for m in (profile.get("font_files") or [])]
        for role in ("display", "body"):
            for f in profile.get("fonts", {}).get(role, {}).get("files", []):
                font_keys.append(f.get("url"))
        for key in font_keys:
            if not key or not key.startswith("captures/") or key in seen_font_keys:
                continue
            seen_font_keys.add(key)
            members.append((f"capture/assets/fonts/{key.rsplit('/', 1)[-1]}", key))

        datas = await asyncio.gather(*(_safe_get(storage, k) for _, k in members))
        for (path, _key), data in zip(members, datas):
            if data is not None:
                zf.writestr(path, data)

        # Phase 9 — agent scaffolding. Emit from the assembled profile + the set
        # of capture/ paths ACTUALLY written above (so the data inventory reflects
        # exactly what's in THIS zip: paginated contact sheets, present-vs-absent
        # artifacts). Deliberately NO index.html (reference omits it to avoid a
        # composition-discovery double-audio bug). `written` is assembled from the
        # zip's members here — a single source of truth — BEFORE meta.json/AGENTS.md
        # are written, so scaffolding sees every data artifact but not itself.
        written = {i.filename for i in zf.infolist()}
        zf.writestr("capture/meta.json",
                    json.dumps(_meta_json(profile), ensure_ascii=False, indent=2))
        # AGENTS.md / CLAUDE.md / .cursorrules — build the doc string ONCE from the
        # written-member set + profile, write it verbatim to all three paths so any
        # agent (Claude Code, Cursor, Codex, ...) auto-discovers identical guidance.
        prompt = _agent_prompt(profile, written)
        for name in ("capture/AGENTS.md", "capture/CLAUDE.md", "capture/.cursorrules"):
            zf.writestr(name, prompt)
    return buf.getvalue()


async def _safe_get(storage, key: str) -> bytes | None:
    """Fetch one object; None on any failure so one missing object doesn't
    abort the whole export."""
    try:
        return await storage.get(key)
    except Exception:
        return None
