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

        # Collect every binary member as (zip_path, storage_key), then fetch
        # them from S3 concurrently — a capture can have ~17 objects and the
        # export is a synchronous response, so sequential GETs cost seconds.
        members: list[tuple[str, str]] = []
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
                    members.append((f"capture/fonts/{key.rsplit('/', 1)[-1]}", key))

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
