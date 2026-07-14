"""Build a hyperframes-compatible capture/ zip from a stored SiteProfile."""
from __future__ import annotations

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


async def build_hyperframes_zip(doc) -> bytes:
    profile = doc.profile or {}
    storage = await get_storage()
    keys = await _image_keys(doc.id)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        # extracted/tokens.json (colors + spacing)
        zf.writestr("capture/extracted/tokens.json", json.dumps(
            {"colors": profile.get("colors", []), "spacing": profile.get("spacing", {})},
            ensure_ascii=False, indent=2))
        # extracted/fonts.json
        zf.writestr("capture/extracted/fonts.json",
                    json.dumps(profile.get("fonts", {}), ensure_ascii=False, indent=2))
        # extracted/visible-text.txt
        t = profile.get("text", {})
        text_body = "\n\n".join(filter(None, [
            t.get("headline", ""), t.get("tagline", ""),
            "\n".join(t.get("ctas", [])), t.get("full_text", "")]))
        zf.writestr("capture/extracted/visible-text.txt", text_body)
        # asset-descriptions.md
        desc = "\n".join(f"- **{a.get('kind')}**: {a.get('description', '')}"
                         for a in profile.get("assets", []))
        zf.writestr("capture/asset-descriptions.md", desc or "(no descriptions)")
        # assets
        for a in profile.get("assets", []):
            key = a.get("storage_key")
            if not key:
                continue
            try:
                zf.writestr(f"capture/assets/{a.get('kind')}.{a.get('format', 'bin')}",
                            await storage.get(key))
            except Exception:
                continue
        # screenshots
        for s in profile.get("screenshots", []):
            key = keys.get(s.get("image_id"))
            if not key:
                continue
            label = (s.get("kind") if s.get("section_index") is None
                     else f"section-{s['section_index']:02d}")
            try:
                zf.writestr(f"capture/screenshots/{label}.png", await storage.get(key))
            except Exception:
                continue
        # captured font files (matched fonts have no file — only css_url in fonts.json)
        for role in ("display", "body"):
            for f in profile.get("fonts", {}).get(role, {}).get("files", []):
                key = f.get("url")  # stored key for captured fonts
                if not key or not key.startswith("captures/"):
                    continue
                try:
                    zf.writestr(f"capture/fonts/{key.rsplit('/', 1)[-1]}",
                                await storage.get(key))
                except Exception:
                    continue
    return buf.getvalue()
