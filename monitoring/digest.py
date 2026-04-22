"""Bad-case digest: aggregate failed ingests for periodic review.

Why
---
Alerts tell ops "something's on fire"; digests tell developers "these
are the failure samples you should look at to improve parsers". The
digest is intentionally non-blocking — we don't page on it — but it
ships to the same wecom group so parsing regressions don't go unnoticed
for days.

Shape of a digest
-----------------
{
    "window_hours": 24,
    "total": 47,
    "by_type": [{"error_type": "parse_error", "count": 18}, ...],
    "by_engine": [{"engine": "pdfium", "count": 20}, ...],
    "samples": [
        {
            "doc_id": "...",
            "kb_id": "...",
            "engine": "pdfium",
            "error_type": "parse_error",
            "error_msg": "Parsing failed: ...",
            "source": "https://... or filename.pdf",
            "storage_key": "upload_files/.../x.pdf",  # None for URLs
            "created_at": "2026-04-23T09:12:34Z",
        },
        ...
    ],
}

Call ``build_digest(hours=24, sample_limit=10)`` for the full dict;
``format_digest_text(d, env="test")`` for a wecom-ready string.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, select

from db.base import get_session
from db.models import Document


async def build_digest(
    hours: int = 24, sample_limit: int = 10,
) -> dict[str, Any]:
    """Query the documents table for failures in the last ``hours``.

    Returns a dict ready to format or serve as JSON. Three aggregations
    + one sample list in separate queries; all run against the
    ``ix_documents_status_created_at`` index so they're O(failed rows
    in window), not O(table).
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    async with get_session() as session:
        # Total count.
        total = (await session.execute(
            select(func.count()).select_from(Document).where(
                Document.status == "failed",
                Document.created_at >= cutoff,
            )
        )).scalar_one()

        # Breakdown by error_type.
        by_type_rows = (await session.execute(
            select(
                func.coalesce(Document.error_type, "unclassified").label("error_type"),
                func.count().label("count"),
            )
            .where(
                Document.status == "failed",
                Document.created_at >= cutoff,
            )
            .group_by(Document.error_type)
            .order_by(func.count().desc())
        )).all()

        # Breakdown by engine. Empty string → 'unknown' so the label is
        # never blank in the digest.
        by_engine_rows = (await session.execute(
            select(
                func.nullif(Document.parse_engine, "").label("engine"),
                func.count().label("count"),
            )
            .where(
                Document.status == "failed",
                Document.created_at >= cutoff,
            )
            .group_by(Document.parse_engine)
            .order_by(func.count().desc())
        )).all()

        # Most-recent samples. Cap error_msg here (not in SQL) so we
        # return the full trace to JSON API callers if they want it, but
        # the digest WeCom formatter only shows the short msg.
        sample_rows = (await session.execute(
            select(Document)
            .where(
                Document.status == "failed",
                Document.created_at >= cutoff,
            )
            .order_by(Document.created_at.desc())
            .limit(sample_limit)
        )).scalars().all()

    return {
        "window_hours": hours,
        "total": int(total),
        "by_type": [
            {"error_type": r.error_type, "count": int(r.count)}
            for r in by_type_rows
        ],
        "by_engine": [
            {"engine": r.engine or "unknown", "count": int(r.count)}
            for r in by_engine_rows
        ],
        "samples": [
            {
                "doc_id": d.id,
                "kb_id": d.kb_id,
                "title": d.title,
                "engine": d.parse_engine or "unknown",
                "error_type": d.error_type or "unclassified",
                "error_msg": d.error_msg or "",
                "source": d.source or "",
                "storage_key": d.storage_key,
                "created_at": d.created_at.isoformat() if d.created_at else None,
            }
            for d in sample_rows
        ],
    }


# Emoji per error_type for quick visual scanning in WeCom. Neutral
# defaults — the digest isn't an alert, we don't want red sirens.
_TYPE_MARKER = {
    "parse_error":    "⚠️",
    "empty_content":  "📭",
    "too_large":      "📦",
    "indexing_error": "💾",
    "url_fetch_error": "🔗",
    "unclassified":   "❓",
}


def _truncate_source(source: str, limit: int = 80) -> str:
    """Keep wecom messages readable when sources are giant signed URLs."""
    source = source or "(empty)"
    return source if len(source) <= limit else source[: limit - 1] + "…"


def format_digest_text(digest: dict[str, Any], env: str = "") -> str:
    """Render a digest dict as a wecom group-bot text body.

    The format mirrors the alert-relay style: env-prefixed header,
    breakdowns as single lines, samples as a bulleted block. Kept under
    2000 chars in practice because we cap sample_limit at 10 and
    truncate sources at 80 chars.
    """
    hours = digest["window_hours"]
    total = digest["total"]
    prefix = f"[{env}] " if env else ""

    if total == 0:
        return f"{prefix}过去 {hours} 小时入库失败样本：0 ✅"

    lines = [f"{prefix}过去 {hours} 小时入库失败样本 {total} 条"]

    # Breakdowns — "18 parse_error · 12 empty_content · ..." fits one
    # line for the normal 3-5 category case.
    by_type_bits = [
        f"{_TYPE_MARKER.get(b['error_type'], '•')} {b['error_type']}×{b['count']}"
        for b in digest["by_type"]
    ]
    if by_type_bits:
        lines.append("按类型：" + "  ".join(by_type_bits))
    by_engine_bits = [f"{b['engine']}×{b['count']}" for b in digest["by_engine"]]
    if by_engine_bits:
        lines.append("按引擎：" + "  ".join(by_engine_bits))

    samples = digest.get("samples") or []
    if samples:
        lines.append("")
        lines.append(f"最近 {len(samples)} 个样本：")
        for s in samples:
            marker = _TYPE_MARKER.get(s["error_type"], "•")
            source = _truncate_source(s["source"])
            lines.append(
                f"{marker} [{s['engine']}] {source}\n"
                f"   {s['error_msg'][:120]}"
            )

    return "\n".join(lines)
