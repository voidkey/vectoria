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
    "total": 47,                          # total failures (all source kinds)
    "by_source_kind": [                   # call-count + failure count split by
        {"kind": "file", "total": 42, "failed": 3},   # input type: storage_key
        {"kind": "url",  "total": 18, "failed": 6},   # present → file upload
    ],                                                # else → URL submission
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

from sqlalchemy import case as sa_case, func, select

from db.base import get_session
from db.models import Document


async def build_digest(
    hours: int = 24, sample_limit: int = 10,
) -> dict[str, Any]:
    """Query the documents table for failures in the last ``hours``.

    Returns a dict ready to format or serve as JSON. Four aggregations
    + one sample list in separate queries; the failure-specific ones
    run against the ``ix_documents_status_created_at`` index. The
    source-kind breakdown walks all rows in the window (not just
    failed), which is O(docs created in window) — cheap at normal
    traffic, and the ``created_at`` lookup is index-eligible.
    """
    # documents.created_at is TIMESTAMP WITHOUT TIME ZONE (schema default);
    # asyncpg refuses to compare it against a tz-aware value. Strip to naive
    # UTC — the server clock is UTC in prod, so "now() - 24h" stays correct.
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).replace(tzinfo=None)

    # SQL: CASE classifies each row as 'file' (has non-empty storage_key)
    # or 'url' (no storage_key). Keep this logic in one place — the
    # discriminator shows up in samples, breakdowns, and eventually the
    # Grafana panel. Treat empty string as NULL since some historic
    # rows may have '' rather than real NULL.
    _SOURCE_KIND = sa_case(
        (Document.storage_key.isnot(None) & (Document.storage_key != ""), "file"),
        else_="url",
    ).label("kind")

    async with get_session() as session:
        # Total failed count.
        total = (await session.execute(
            select(func.count()).select_from(Document).where(
                Document.status == "failed",
                Document.created_at >= cutoff,
            )
        )).scalar_one()

        # NEW: totals + failed-counts split by source kind (url vs file).
        # One query, two aggregations — so we can show "18 URLs (6 failed)"
        # without a second roundtrip.
        source_kind_rows = (await session.execute(
            select(
                _SOURCE_KIND,
                func.count().label("total"),
                func.sum(
                    sa_case((Document.status == "failed", 1), else_=0)
                ).label("failed"),
            )
            .where(Document.created_at >= cutoff)
            .group_by(_SOURCE_KIND)
            .order_by(func.count().desc())
        )).all()

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
        "by_source_kind": [
            {"kind": r.kind, "total": int(r.total), "failed": int(r.failed or 0)}
            for r in source_kind_rows
        ],
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
    "image_only":     "🖼️",
    "too_large":      "📦",
    "indexing_error": "💾",
    "url_fetch_error": "🔗",
    "unclassified":   "❓",
}


def _truncate_source(source: str, limit: int = 80) -> str:
    """Keep wecom messages readable when sources are giant signed URLs."""
    source = source or "(empty)"
    return source if len(source) <= limit else source[: limit - 1] + "…"


# Markers for the per-source-kind summary line. Neutral shapes — this
# section runs on a healthy day too, not just incidents.
_KIND_MARKER = {"file": "📄 文件", "url": "🔗 链接"}


def _format_kind_line(kind: str, total: int, failed: int) -> str:
    """One line per source kind.

    On a healthy day → "✅ 全部成功". When there were failures, include
    the success-rate so the operator can tell '6/18 bad URLs' from
    '6/6000 bad URLs' without doing mental arithmetic.
    """
    label = _KIND_MARKER.get(kind, f"• {kind}")
    if total == 0:
        return f"{label}：0（无新增）"
    if failed == 0:
        return f"{label} {total} 个 ✅ 全部成功"
    rate = (total - failed) / total
    return f"{label} {total} 个（失败 {failed}，成功率 {rate:.0%}）"


def format_digest_text(digest: dict[str, Any], env: str = "") -> str:
    """Render a digest dict as a wecom group-bot text body.

    Section order: header → per-source-kind summary → failure
    breakdowns (only if there were failures) → recent samples.
    Kept under 2000 chars in practice because we cap sample_limit at 10
    and truncate sources at 80 chars.
    """
    hours = digest["window_hours"]
    total = digest["total"]
    kinds = digest.get("by_source_kind") or []
    total_ingests = sum(k["total"] for k in kinds)
    prefix = f"[{env}] " if env else ""

    # Zero traffic: single line, don't pretend we have data.
    if total_ingests == 0:
        return f"{prefix}过去 {hours} 小时入库样本：0（无新增）"

    lines = [f"{prefix}过去 {hours} 小时入库样本"]

    # Always show the per-source-kind line so ops sees call volume even
    # on clean days — that's often the signal they want most.
    for kind in ("file", "url"):
        row = next((k for k in kinds if k["kind"] == kind), None)
        if row:
            lines.append(_format_kind_line(kind, row["total"], row["failed"]))
        else:
            # No rows of this kind in window — skip cleanly (don't fake a 0).
            continue

    # Failure-specific breakdowns: skip entirely on a clean day rather
    # than printing empty sections.
    if total > 0:
        lines.append("")
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
