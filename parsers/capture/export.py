"""Build a hyperframes-compatible capture/ zip from a stored SiteProfile."""
from __future__ import annotations

import asyncio
import io
import json
import zipfile

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
    ranked = profile.get("colors_ranked") or []
    if not ranked:
        ranked = [c["hex"] for c in (profile.get("colors") or []) if c.get("hex")]
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
    if a.get("kind") == "image":
        return f"capture/assets/{basename}"
    return f"capture/assets/{a.get('kind')}.{a.get('format', 'bin')}"


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
    return buf.getvalue()


async def _safe_get(storage, key: str) -> bytes | None:
    """Fetch one object; None on any failure so one missing object doesn't
    abort the whole export."""
    try:
        return await storage.get(key)
    except Exception:
        return None
