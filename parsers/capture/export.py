"""Build a hyperframes-compatible capture/ zip from a stored SiteProfile."""
from __future__ import annotations

import asyncio
import io
import json
import zipfile

from sqlalchemy import select

from db.base import get_session
from db.models import DocumentImage
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


def _color_stats(colors: list[dict]) -> list[dict]:
    """Synthesize the official tokens.json `colorStats` from vectoria's richer
    role-tagged ColorTokens, so build-frame's brandRolesFromStats() picks brand
    roles by FUNCTION (canvas = largest background, accent = interactive, ink =
    text) instead of falling back to luminance/chroma guessing. Vectoria already
    resolved `role` (background|primary|accent|text|muted) + `coverage`(0..1);
    we project those onto the areaBg/interactiveBg/textCount fields the script ranks."""
    stats: list[dict] = []
    for c in colors:
        hexv = c.get("hex")
        if not hexv:
            continue
        role = (c.get("role") or "").lower()
        cov = float(c.get("coverage") or 0.0)
        s = {"hex": hexv, "areaBg": 0, "interactiveBg": 0, "textCount": 0,
             "bgCount": 0, "maxArea": 0, "count": max(1, round(cov * 100))}
        if role == "background":
            area = max(1, round(cov * 10000))
            s.update(areaBg=area, maxArea=area, bgCount=1)
        elif role in ("accent", "primary"):
            s["interactiveBg"] = max(1, round(cov * 10000))
        elif role == "text":
            s["textCount"] = max(1, round(cov * 1000))
        stats.append(s)
    return stats


def _official_tokens(profile: dict) -> dict:
    """tokens.json in the official hyperframes shape build-frame.mjs reads:
    {title, description, colors, fonts[], colorStats, spacing}. Colors stay as the
    rich ColorTokens (build-frame reads `.hex`); title/description come from the
    captured page text; fonts/colorStats are projected from vectoria's own fields."""
    text = profile.get("text", {}) or {}
    return {
        "title": text.get("headline", ""),
        "description": text.get("tagline", ""),
        "ctas": text.get("ctas", []),  # extra (official schema ignores unknown keys); handy for downstream summaries
        "colors": profile.get("colors", []),
        "fonts": _fonts_array(profile.get("fonts", {}) or {}),
        "colorStats": _color_stats(profile.get("colors", []) or []),
        "spacing": profile.get("spacing", {}),
        # extra: lets downstream (go-figlens) gate structural rebuild on fidelity.
        "capture_quality": profile.get("capture_quality", "full"),
    }


async def build_hyperframes_zip(doc) -> bytes:
    profile = doc.profile or {}
    storage = await get_storage()
    keys = await _image_keys(doc.id)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        # extracted/tokens.json — official hyperframes shape (title/description/
        # colors/fonts[]/colorStats/spacing) so build-frame.mjs remixes brand
        # colors AND fonts onto the preset (was {colors, spacing}-only, which
        # dropped brand typography + made role detection fall back to luminance).
        zf.writestr("capture/extracted/tokens.json",
                    json.dumps(_official_tokens(profile), ensure_ascii=False, indent=2))
        # extracted/fonts.json + fonts-manifest.json — the role-keyed Fonts object.
        # Emitted under both names: fonts.json (legacy) and fonts-manifest.json
        # (the name the website-to-video font-identification step reads).
        fonts_obj = json.dumps(profile.get("fonts", {}), ensure_ascii=False, indent=2)
        zf.writestr("capture/extracted/fonts.json", fonts_obj)
        zf.writestr("capture/extracted/fonts-manifest.json", fonts_obj)
        # extracted/visible-text.txt
        t = profile.get("text", {})
        text_body = "\n\n".join(filter(None, [
            t.get("headline", ""), t.get("tagline", ""),
            "\n".join(t.get("ctas", [])), t.get("full_text", "")]))
        zf.writestr("capture/extracted/visible-text.txt", text_body)
        # extracted/asset-descriptions.md — under extracted/ (the path the skills
        # read: capture/extracted/asset-descriptions.md), not top-level capture/.
        desc = "\n".join(f"- **{a.get('kind')}**: {a.get('description', '')}"
                         for a in profile.get("assets", []))
        zf.writestr("capture/extracted/asset-descriptions.md", desc or "(no descriptions)")
        # extracted/design-styles.json — computed design system (only present when
        # capture_quality == full). Inlined in the profile, so write directly.
        if profile.get("design_styles"):
            zf.writestr("capture/extracted/design-styles.json",
                        json.dumps(profile["design_styles"], ensure_ascii=False, indent=2))

        # Collect every binary member as (zip_path, storage_key), then fetch
        # them from S3 concurrently — a capture can have ~17 objects and the
        # export is a synchronous response, so sequential GETs cost seconds.
        members: list[tuple[str, str]] = []
        # page.html — self-contained structural reference (only when quality==full).
        if profile.get("page_html_key"):
            members.append(("capture/extracted/page.html", profile["page_html_key"]))
        for a in profile.get("assets", []):
            if a.get("storage_key"):
                members.append((f"capture/assets/{a.get('kind')}.{a.get('format', 'bin')}",
                                a["storage_key"]))
        for s in profile.get("screenshots", []):
            key = keys.get(s.get("image_id"))
            if key:
                label = (s.get("kind") if s.get("section_index") is None
                         else f"section-{s['section_index']:02d}")
                members.append((f"capture/screenshots/{label}.png", key))
        for role in ("display", "body"):
            for f in profile.get("fonts", {}).get(role, {}).get("files", []):
                key = f.get("url")  # stored key for captured (unmatched) fonts
                if key and key.startswith("captures/"):
                    # capture/assets/fonts/ — where build-frame.mjs looks to stage
                    # @font-face faces (it globs capture/assets/fonts, not capture/fonts).
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
