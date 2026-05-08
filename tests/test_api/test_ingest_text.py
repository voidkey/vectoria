"""POST /knowledgebases/{kb_id}/documents/text — text-as-document upload.

Mirrors the file upload path but the payload is a JSON body containing
the text directly. The endpoint encodes the text to UTF-8, stores it as
a .txt object, and runs it through the same parse → embed pipeline.
"""
import hashlib
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _configure_session(session: AsyncMock, *, add_captures: list, dedup_hit=None):
    """Common session plumbing matching test_ingest_wait._configure_session.

    ``dedup_hit`` lets a test inject an existing Document so the dedup
    branch fires on the very first execute() call.
    """
    def _execute(_stmt):
        r = MagicMock()
        if dedup_hit is not None:
            r.scalar_one_or_none.return_value = dedup_hit
        else:
            r.scalar_one_or_none.return_value = (
                add_captures[0] if add_captures else None
            )
        return r

    session.execute = AsyncMock(side_effect=_execute)
    session.add = MagicMock(side_effect=lambda d: add_captures.append(d))
    session.commit = AsyncMock()
    session.get = AsyncMock(
        side_effect=lambda _cls, _doc_id: add_captures[0] if add_captures else None,
    )

    def _refresh(obj):
        obj.created_at = datetime(2026, 5, 8)

    session.refresh = AsyncMock(side_effect=_refresh)
    return session


@pytest.mark.asyncio
async def test_text_upload_happy_path_uses_first_line_as_title(client):
    """Default title comes from the first non-empty line, capped at 80
    chars. Storage key contains the derived filename and the doc is
    enqueued for parsing.
    """
    captures: list = []
    text = "My Knowledge Note\n\nbody line one\nbody line two"

    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_storage") as mock_storage,
        patch("api.routes.documents.get_session") as mock_sess,
        patch("worker.queue.enqueue", new=AsyncMock()),
    ):
        storage = AsyncMock()
        mock_storage.return_value = storage
        session = AsyncMock()
        _configure_session(session, add_captures=captures)
        mock_sess.return_value.__aenter__.return_value = session

        resp = await client.post(
            "/v1/knowledgebases/kb-x/documents/text",
            json={"text": text},
        )

    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["status"] == "queued"
    # User-facing title/source omit the .txt suffix — that's an internal
    # storage detail used to drive parser selection, not something the
    # caller asked for.
    assert body["title"] == "My Knowledge Note"
    assert body["source"] == "My Knowledge Note"

    # The text bytes were uploaded under upload_files/<kb>/<doc>/<title>.txt
    # — the .txt suffix lives only in the storage key.
    storage.put.assert_awaited_once()
    call_args = storage.put.await_args
    storage_key = call_args.args[0]
    payload = call_args.args[1]
    assert storage_key.startswith("upload_files/kb-x/")
    assert storage_key.endswith("/My Knowledge Note.txt")
    assert payload == text.encode("utf-8")
    assert "text/plain" in call_args.kwargs.get("content_type", "")


@pytest.mark.asyncio
async def test_text_upload_explicit_title_wins(client):
    """If the caller supplies a title, we use it instead of the first
    line. Strip whitespace; don't double-suffix .txt.
    """
    captures: list = []

    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_storage") as mock_storage,
        patch("api.routes.documents.get_session") as mock_sess,
        patch("worker.queue.enqueue", new=AsyncMock()),
    ):
        mock_storage.return_value = AsyncMock()
        session = AsyncMock()
        _configure_session(session, add_captures=captures)
        mock_sess.return_value.__aenter__.return_value = session

        resp = await client.post(
            "/v1/knowledgebases/kb-x/documents/text",
            json={"text": "ignored body line", "title": "  custom-title.txt  "},
        )

    assert resp.status_code == 201, resp.text
    body = resp.json()
    # Explicit title kept verbatim (after .strip()), .txt not duplicated.
    assert body["title"] == "custom-title.txt"
    assert body["source"] == "custom-title.txt"


@pytest.mark.asyncio
async def test_text_upload_blank_title_falls_back_to_hash(client):
    """When both the supplied title and every body line are blank, fall
    back to text-{8-char hash}.txt — keeps the doc addressable in the
    UI and ensures the storage key never collides on missing-title.
    """
    captures: list = []
    text = "   \n\t  \n   "
    expected_hash_prefix = hashlib.sha256(text.encode("utf-8")).hexdigest()[:8]

    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_storage") as mock_storage,
        patch("api.routes.documents.get_session") as mock_sess,
        patch("worker.queue.enqueue", new=AsyncMock()),
    ):
        mock_storage.return_value = AsyncMock()
        session = AsyncMock()
        _configure_session(session, add_captures=captures)
        mock_sess.return_value.__aenter__.return_value = session

        resp = await client.post(
            "/v1/knowledgebases/kb-x/documents/text",
            json={"text": text},
        )

    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["title"] == f"text-{expected_hash_prefix}"
    assert body["source"] == f"text-{expected_hash_prefix}"


@pytest.mark.asyncio
async def test_text_upload_long_first_line_truncated(client):
    """Title derived from the body is capped at 80 chars so it stays a
    sane filename and fits typical UI columns.
    """
    captures: list = []
    long_line = "A" * 200
    text = long_line + "\nrest"

    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_storage") as mock_storage,
        patch("api.routes.documents.get_session") as mock_sess,
        patch("worker.queue.enqueue", new=AsyncMock()),
    ):
        mock_storage.return_value = AsyncMock()
        session = AsyncMock()
        _configure_session(session, add_captures=captures)
        mock_sess.return_value.__aenter__.return_value = session

        resp = await client.post(
            "/v1/knowledgebases/kb-x/documents/text",
            json={"text": text},
        )

    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["title"] == "A" * 80
    assert body["source"] == "A" * 80


@pytest.mark.asyncio
async def test_text_upload_exceeding_max_bytes_returns_413(client):
    """Same byte cap as /file — caller can't bypass the upload limit by
    encoding a huge file as a JSON string.
    """
    from config import get_settings
    limit = get_settings().max_upload_bytes
    oversized_text = "a" * (limit + 1)

    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_storage") as mock_storage,
    ):
        storage = AsyncMock()
        mock_storage.return_value = storage
        resp = await client.post(
            "/v1/knowledgebases/kb-x/documents/text",
            json={"text": oversized_text},
        )

    assert resp.status_code == 413, resp.text
    assert resp.json()["code"] == 1204  # UPLOAD_TOO_LARGE
    storage.put.assert_not_called()


@pytest.mark.asyncio
async def test_text_upload_dedup_returns_existing_doc(client):
    """Same text submitted twice into one KB collapses to one doc; no
    new parse is enqueued, and the dedup response strips ``content`` to
    avoid the cross-tenant leak guard documented on _dedup_response.
    """
    text = "duplicate body content"
    sha = hashlib.sha256(text.encode("utf-8")).hexdigest()

    existing = MagicMock()
    existing.id = "doc-existing"
    existing.kb_id = "kb-x"
    existing.title = "duplicate body content"
    existing.source = "duplicate body content.txt"
    existing.chunk_count = 2
    existing.status = "completed"
    existing.error_msg = ""
    existing.content = "duplicate body content"
    existing.created_at = datetime(2026, 5, 8)
    existing.file_hash_sha256 = sha

    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_storage") as mock_storage,
        patch("api.routes.documents.get_session") as mock_sess,
        patch("worker.queue.enqueue", new=AsyncMock()) as mock_task,
    ):
        storage = AsyncMock()
        mock_storage.return_value = storage
        session = AsyncMock()
        _configure_session(session, add_captures=[], dedup_hit=existing)
        mock_sess.return_value.__aenter__.return_value = session

        resp = await client.post(
            "/v1/knowledgebases/kb-x/documents/text",
            json={"text": text},
        )

    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["id"] == "doc-existing"
    # Dedup hit short-circuits before storage.put and before any new
    # parse_document task gets enqueued.
    storage.put.assert_not_called()
    mock_task.assert_not_called()
    assert body["content"] == "", "dedup response leaked existing content"


@pytest.mark.asyncio
async def test_text_upload_empty_string_rejected_by_pydantic(client):
    """``text`` has min_length=1 — empty body is rejected at the schema
    boundary so we don't even hash / store the empty payload.
    """
    resp = await client.post(
        "/v1/knowledgebases/kb-x/documents/text",
        json={"text": ""},
    )
    assert resp.status_code == 422, resp.text
