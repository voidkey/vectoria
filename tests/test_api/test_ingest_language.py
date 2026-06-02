"""Per-request language field is forwarded into the parse_document payload.

Task 5 of the vision-output-language plan: the three ingest endpoints
accept an optional ``language`` and embed it in the ``parse_document``
task row so the worker can pass it down to the vision / OCR layer.

Strategy: intercept session.add so we can inspect the Task row that
_enqueue_ingest stages in the same transaction as the Document.  This
mirrors the approach in test_ingest_atomicity.py and avoids having to
mock deep into the import graph.
"""
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from db.models import Document, Task


def _configure_session(session: AsyncMock) -> list:
    """Wire up session mocks; return the adds list for inspection."""
    adds: list = []

    def _add(obj):
        adds.append(obj)

    def _refresh(obj):
        if isinstance(obj, Document):
            obj.created_at = datetime(2026, 1, 1)

    miss = MagicMock()
    miss.scalar_one_or_none.return_value = None
    session.execute = AsyncMock(return_value=miss)
    session.add = MagicMock(side_effect=_add)
    session.commit = AsyncMock()
    session.refresh = AsyncMock(side_effect=_refresh)
    return adds


# ---------------------------------------------------------------------------
# text endpoint
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ingest_text_puts_language_in_payload(client):
    """language=pt-BR on the text endpoint lands in parse_document payload."""
    session = AsyncMock()
    adds = _configure_session(session)

    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_storage") as mock_storage,
        patch("api.routes.documents.get_session") as mock_sess,
    ):
        mock_storage.return_value = AsyncMock()
        mock_sess.return_value.__aenter__.return_value = session

        resp = await client.post(
            "/v1/knowledgebases/kb-1/documents/text",
            json={"text": "hello body", "language": "pt-BR"},
            headers={"X-API-Key": "alice"},
        )

    assert resp.status_code in (200, 201), resp.text

    task_rows = [o for o in adds if isinstance(o, Task)]
    assert task_rows, "no Task row was staged in session"
    assert task_rows[0].payload.get("language") == "pt-BR"


@pytest.mark.asyncio
async def test_ingest_text_language_defaults_to_none(client):
    """Omitting language on the text endpoint puts None in the payload
    (backward compatible — worker treats None as "auto-detect").
    """
    session = AsyncMock()
    adds = _configure_session(session)

    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_storage") as mock_storage,
        patch("api.routes.documents.get_session") as mock_sess,
    ):
        mock_storage.return_value = AsyncMock()
        mock_sess.return_value.__aenter__.return_value = session

        resp = await client.post(
            "/v1/knowledgebases/kb-1/documents/text",
            json={"text": "hello body"},
            headers={"X-API-Key": "alice"},
        )

    assert resp.status_code in (200, 201), resp.text

    task_rows = [o for o in adds if isinstance(o, Task)]
    assert task_rows, "no Task row was staged in session"
    assert task_rows[0].payload.get("language") is None


# ---------------------------------------------------------------------------
# url endpoint
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ingest_url_puts_language_in_payload(client):
    """language=ja on the url endpoint lands in parse_document payload."""
    session = AsyncMock()
    adds = _configure_session(session)

    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.validate_url", new=AsyncMock()),
        patch("api.routes.documents.get_session") as mock_sess,
    ):
        mock_sess.return_value.__aenter__.return_value = session

        resp = await client.post(
            "/v1/knowledgebases/kb-1/documents/url",
            json={"url": "https://example.com/doc.pdf", "language": "ja"},
            headers={"X-API-Key": "alice"},
        )

    assert resp.status_code in (200, 201), resp.text

    task_rows = [o for o in adds if isinstance(o, Task)]
    assert task_rows, "no Task row was staged in session"
    assert task_rows[0].payload.get("language") == "ja"


# ---------------------------------------------------------------------------
# file endpoint
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ingest_file_puts_language_in_payload(client):
    """language=zh-CN on the file endpoint (query param) lands in payload."""
    session = AsyncMock()
    adds = _configure_session(session)

    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_storage") as mock_storage,
        patch("api.routes.documents.get_session") as mock_sess,
    ):
        mock_storage.return_value = AsyncMock()
        mock_sess.return_value.__aenter__.return_value = session

        resp = await client.post(
            "/v1/knowledgebases/kb-1/documents/file?language=zh-CN",
            files={"file": ("test.txt", b"content here", "text/plain")},
            headers={"X-API-Key": "alice"},
        )

    assert resp.status_code in (200, 201), resp.text

    task_rows = [o for o in adds if isinstance(o, Task)]
    assert task_rows, "no Task row was staged in session"
    assert task_rows[0].payload.get("language") == "zh-CN"
