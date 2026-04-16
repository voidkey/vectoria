"""Bytes limit at the HTTP entry, and per-KB file-hash dedup.

These two live together because they both gate `ingest_file` *before* the
parser runs — the whole point of this layer is to keep gigantic files and
repeat uploads from reaching the heavy pipeline.
"""
import hashlib
import pytest
from datetime import datetime
from unittest.mock import patch, AsyncMock, MagicMock

from parsers.base import ParseResult


@pytest.mark.asyncio
async def test_upload_exceeding_max_bytes_returns_413(client):
    """A file larger than max_upload_bytes is rejected before parse/storage.

    Why: a 1GB upload would otherwise be buffered in memory by `file.read()`
    — the content_chars check only fires *after* that damage is done.
    """
    from config import get_settings
    limit = get_settings().max_upload_bytes
    oversized = b"a" * (limit + 1)

    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_storage") as mock_storage,
        patch("api.routes.documents.registry") as mock_registry,
    ):
        mock_storage.return_value = AsyncMock()
        mock_registry.auto_select.return_value = "markitdown"

        resp = await client.post(
            "/v1/knowledgebases/kb-x/documents/file",
            files={"file": ("big.bin", oversized, "application/octet-stream")},
        )

    assert resp.status_code == 413, resp.text
    assert resp.json()["code"] == 1204  # UPLOAD_TOO_LARGE
    # Storage must NOT have been written to — we rejected before that.
    mock_storage.return_value.put.assert_not_called()


@pytest.mark.asyncio
async def test_duplicate_file_in_same_kb_returns_existing_doc(client):
    """Uploading the same file twice to the same KB reuses the first doc.

    Why: without this, rapid retries (user clicks upload twice, browser
    retries a 502, client scripts with no idempotency) duplicate all the
    parse + embed work and can OOM the server (this happened in prod).
    """
    raw = b"hello world, same content twice"
    expected_md5 = hashlib.md5(raw).hexdigest()

    existing = MagicMock()
    existing.id = "doc-existing"
    existing.kb_id = "kb-x"
    existing.title = "hello"
    existing.source = "hello.txt"
    existing.chunk_count = 3
    existing.status = "completed"
    existing.error_msg = ""
    existing.content = "hello world"
    existing.created_at = datetime(2026, 4, 15, 21, 0, 0)
    existing.file_hash = expected_md5

    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_storage") as mock_storage,
        patch("api.routes.documents.registry") as mock_registry,
        patch("api.routes.documents.get_session") as mock_sess,
        patch("api.routes.documents.extract_outline", return_value=[]),
        patch("worker.queue.enqueue", new=AsyncMock()) as mock_task,
    ):
        mock_storage.return_value = AsyncMock()
        mock_registry.auto_select.return_value = "markitdown"

        session = AsyncMock()
        lookup_result = MagicMock()
        lookup_result.scalar_one_or_none.return_value = existing
        session.execute = AsyncMock(return_value=lookup_result)
        mock_sess.return_value.__aenter__.return_value = session

        resp = await client.post(
            "/v1/knowledgebases/kb-x/documents/file",
            files={"file": ("hello.txt", raw, "text/plain")},
        )

    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["id"] == "doc-existing"
    # Must NOT have kicked off a new indexing task.
    mock_task.assert_not_called()


@pytest.mark.asyncio
async def test_different_content_does_not_dedup(client):
    """Different bytes → different hash → fresh ingest (no false-positive dedup)."""
    fake_parser = MagicMock()
    fake_parser.parse = AsyncMock(
        return_value=ParseResult(content="parsed text", images={}, title="fresh")
    )

    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_storage") as mock_storage,
        patch("api.routes.documents.registry") as mock_registry,
        patch("api.routes.documents.get_session") as mock_sess,
        patch("api.routes.documents.extract_outline", return_value=[]),
        patch("worker.queue.enqueue", new=AsyncMock()),
    ):
        mock_storage.return_value = AsyncMock()
        mock_registry.auto_select.return_value = "markitdown"
        mock_registry.get_by_engine.return_value = fake_parser

        session = AsyncMock()
        # Dedup lookup misses, then later refresh works.
        miss_result = MagicMock()
        miss_result.scalar_one_or_none.return_value = None
        session.execute = AsyncMock(return_value=miss_result)

        def _refresh(obj):
            obj.id = "doc-new"
            obj.created_at = datetime(2026, 4, 15, 21, 0, 0)

        session.add = MagicMock()
        session.commit = AsyncMock()
        session.refresh = AsyncMock(side_effect=_refresh)
        mock_sess.return_value.__aenter__.return_value = session

        resp = await client.post(
            "/v1/knowledgebases/kb-x/documents/file",
            files={"file": ("fresh.txt", b"totally new content", "text/plain")},
        )

    assert resp.status_code == 201, resp.text
    assert resp.json()["id"] == "doc-new"
    fake_parser.parse.assert_called_once()
