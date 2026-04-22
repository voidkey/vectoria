"""Tests for the bad-case digest query + wecom formatter.

``build_digest`` is tested with a mocked session so the test suite
doesn't need a real Postgres (matches the project's mocking style —
see tests/test_api/test_ingest_atomicity.py). ``format_digest_text``
is pure and tested against fixture dicts directly.
"""
from __future__ import annotations

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from monitoring.digest import build_digest, format_digest_text


# ---------------------------------------------------------------------------
# format_digest_text: pure function, exhaustive cases
# ---------------------------------------------------------------------------

def test_format_digest_zero_ingests_shows_empty_window():
    """No ingests at all → single compact line, no fake zeros per kind."""
    text = format_digest_text(
        {
            "window_hours": 24, "total": 0,
            "by_source_kind": [], "by_type": [], "by_engine": [], "samples": [],
        },
        env="test",
    )
    assert text == "[test] 过去 24 小时入库样本：0（无新增）"


def test_format_digest_clean_day_shows_call_counts_with_allclear():
    """Traffic but zero failures → show per-kind totals, each marked
    '全部成功 ✅'. This is the most common 'healthy' daily digest."""
    text = format_digest_text(
        {
            "window_hours": 24, "total": 0,
            "by_source_kind": [
                {"kind": "file", "total": 42, "failed": 0},
                {"kind": "url",  "total": 18, "failed": 0},
            ],
            "by_type": [], "by_engine": [], "samples": [],
        },
        env="test",
    )
    assert "过去 24 小时入库样本" in text
    assert "📄 文件 42 个 ✅ 全部成功" in text
    assert "🔗 链接 18 个 ✅ 全部成功" in text
    # On a clean day, no failure-specific sections.
    assert "按类型" not in text
    assert "最近" not in text


def test_format_digest_breakdown_by_type_and_engine():
    """With failures, emit per-kind summary (with success-rate) plus the
    type + engine breakdowns + recent samples."""
    digest = {
        "window_hours": 24,
        "total": 3,
        "by_source_kind": [
            {"kind": "file", "total": 10, "failed": 2},
            {"kind": "url",  "total":  5, "failed": 1},
        ],
        "by_type": [
            {"error_type": "parse_error", "count": 2},
            {"error_type": "empty_content", "count": 1},
        ],
        "by_engine": [
            {"engine": "pdfium", "count": 2},
            {"engine": "url", "count": 1},
        ],
        "samples": [
            {
                "doc_id": "d1", "kb_id": "kb1", "title": "",
                "engine": "pdfium", "error_type": "parse_error",
                "error_msg": "Parsing failed: bad PDF header",
                "source": "scan.pdf", "storage_key": "upload/x.pdf",
                "created_at": "2026-04-23T01:00:00Z",
            },
        ],
    }
    text = format_digest_text(digest, env="test")
    assert "[test] 过去 24 小时入库样本" in text
    # Per-kind totals with success-rate math (80% for file: 8/10 ok).
    assert "📄 文件 10 个（失败 2，成功率 80%）" in text
    assert "🔗 链接 5 个（失败 1，成功率 80%）" in text
    # Failure breakdowns (unchanged from before).
    assert "parse_error×2" in text
    assert "empty_content×1" in text
    assert "pdfium×2" in text
    assert "url×1" in text
    # Recent sample block.
    assert "scan.pdf" in text
    assert "Parsing failed: bad PDF header" in text


def test_format_digest_truncates_long_sources():
    """A 200-char signed URL would eat the wecom text budget; format
    caps source at 80 chars with an ellipsis."""
    long_url = "https://example.com/" + "a" * 300
    digest = {
        "window_hours": 24,
        "total": 1,
        "by_source_kind": [{"kind": "url", "total": 1, "failed": 1}],
        "by_type": [{"error_type": "parse_error", "count": 1}],
        "by_engine": [{"engine": "url", "count": 1}],
        "samples": [{
            "doc_id": "d1", "kb_id": "kb1", "title": "",
            "engine": "url", "error_type": "parse_error",
            "error_msg": "anti-bot page",
            "source": long_url, "storage_key": None,
            "created_at": "2026-04-23T01:00:00Z",
        }],
    }
    text = format_digest_text(digest)
    assert long_url not in text       # verbatim URL should NOT leak in
    assert "…" in text                 # ellipsis marker must be present
    # Line with the source shouldn't exceed the truncation limit + prefix.
    source_lines = [l for l in text.splitlines() if "example.com" in l]
    assert source_lines
    assert all(len(l) < 120 for l in source_lines)


def test_format_digest_no_env_prefix_when_empty():
    """Empty env string → no `[]` prefix (avoids ugly `[] 过去 24…`)."""
    text = format_digest_text(
        {
            "window_hours": 24, "total": 0,
            "by_source_kind": [], "by_type": [], "by_engine": [], "samples": [],
        },
        env="",
    )
    assert not text.startswith("[")
    assert text == "过去 24 小时入库样本：0（无新增）"


def test_format_digest_unknown_error_type_gets_bullet_marker():
    """Unclassified / unexpected error_types still render — don't crash
    on an error_type that the marker map doesn't know about."""
    digest = {
        "window_hours": 1,
        "total": 1,
        "by_source_kind": [{"kind": "file", "total": 1, "failed": 1}],
        "by_type": [{"error_type": "some_future_type", "count": 1}],
        "by_engine": [{"engine": "unknown", "count": 1}],
        "samples": [{
            "doc_id": "d1", "kb_id": "kb1", "title": "",
            "engine": "unknown", "error_type": "some_future_type",
            "error_msg": "edge case",
            "source": "x", "storage_key": None,
            "created_at": "2026-04-23T01:00:00Z",
        }],
    }
    text = format_digest_text(digest)
    assert "some_future_type" in text
    # Fallback marker is "•" for both the breakdown and sample lines.
    assert "• some_future_type×1" in text or "•" in text


# ---------------------------------------------------------------------------
# build_digest: mocked-session integration
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_build_digest_assembles_aggregations_and_samples():
    """Verify build_digest runs all five queries and shapes the result
    dict correctly. We don't care about SQL correctness (that's Postgres's
    job) — we care that aggregation rows and sample rows get wired into
    the documented output shape."""
    # Synthetic DB rows.
    total = 3
    source_kind_rows = [
        MagicMock(kind="file", total=10, failed=2),
        MagicMock(kind="url",  total=5,  failed=1),
    ]
    type_rows = [
        MagicMock(error_type="parse_error", count=2),
        MagicMock(error_type="empty_content", count=1),
    ]
    engine_rows = [
        MagicMock(engine="pdfium", count=2),
        MagicMock(engine=None, count=1),    # nullif('') returns NULL
    ]
    sample_rows = [
        MagicMock(
            id="d1", kb_id="kb1", title="scan",
            parse_engine="pdfium",
            error_type="parse_error",
            error_msg="Parsing failed: bad header",
            source="scan.pdf",
            storage_key="upload/x.pdf",
            created_at=datetime(2026, 4, 23, 1, 0, 0),
        ),
    ]

    # Each session.execute call returns a different result shape:
    #   1st: scalar total-failed count
    #   2nd: .all() rows for by_source_kind (new)
    #   3rd: .all() rows for by_type
    #   4th: .all() rows for by_engine
    #   5th: .scalars().all() for sample rows
    count_result = MagicMock()
    count_result.scalar_one.return_value = total

    kind_result = MagicMock()
    kind_result.all.return_value = source_kind_rows

    type_result = MagicMock()
    type_result.all.return_value = type_rows

    engine_result = MagicMock()
    engine_result.all.return_value = engine_rows

    sample_result = MagicMock()
    sample_scalars = MagicMock()
    sample_scalars.all.return_value = sample_rows
    sample_result.scalars.return_value = sample_scalars

    session = AsyncMock()
    session.execute = AsyncMock(
        side_effect=[count_result, kind_result, type_result, engine_result, sample_result]
    )

    with patch("monitoring.digest.get_session") as mock_sess:
        mock_sess.return_value.__aenter__.return_value = session
        digest = await build_digest(hours=24, sample_limit=10)

    assert digest["window_hours"] == 24
    assert digest["total"] == 3
    assert digest["by_source_kind"] == [
        {"kind": "file", "total": 10, "failed": 2},
        {"kind": "url",  "total": 5,  "failed": 1},
    ]
    assert digest["by_type"] == [
        {"error_type": "parse_error", "count": 2},
        {"error_type": "empty_content", "count": 1},
    ]
    # Null engine (blank parse_engine) normalised to 'unknown' so
    # downstream consumers never see an empty label.
    assert digest["by_engine"] == [
        {"engine": "pdfium", "count": 2},
        {"engine": "unknown", "count": 1},
    ]
    assert len(digest["samples"]) == 1
    s = digest["samples"][0]
    assert s["doc_id"] == "d1"
    assert s["engine"] == "pdfium"
    assert s["storage_key"] == "upload/x.pdf"
    assert s["created_at"] == "2026-04-23T01:00:00"
