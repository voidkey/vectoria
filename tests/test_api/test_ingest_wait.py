"""``?wait=true`` compatibility knob.

Default ``wait=false`` returns a ``queued`` response immediately — the
new architecture. ``wait=true`` polls the DB for up to
``ingest_wait_timeout_seconds`` and returns whatever state the doc
reached, so sync-style callers still get content in the body.
"""
import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _configure_session(session: AsyncMock, *, add_captures: list):
    """Common session plumbing: dedup miss, add() captures, refresh()
    stamps id+created_at, execute() returns the captured doc on lookup.
    """
    miss = MagicMock()
    miss.scalar_one_or_none.return_value = None

    def _execute(_stmt):
        r = MagicMock()
        # After the doc is created, subsequent execute calls (re-fetches)
        # should return it. Before that, it's a dedup miss.
        r.scalar_one_or_none.return_value = add_captures[0] if add_captures else None
        return r

    session.execute = AsyncMock(side_effect=_execute)
    session.add = MagicMock(side_effect=lambda d: add_captures.append(d))
    session.commit = AsyncMock()

    def _refresh(obj):
        obj.created_at = datetime(2026, 4, 20)

    session.refresh = AsyncMock(side_effect=_refresh)
    return session


@pytest.mark.asyncio
async def test_default_returns_queued_without_waiting(client):
    """Without ``?wait=true``, the API returns in ~ms with status=queued."""
    captures: list = []

    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_storage") as mock_storage,
        patch("api.routes.documents.registry") as mock_reg,
        patch("api.routes.documents.get_session") as mock_sess,
        patch("worker.queue.enqueue", new=AsyncMock()),
    ):
        mock_storage.return_value = AsyncMock()
        mock_reg.auto_select.return_value = "markitdown"
        session = AsyncMock()
        _configure_session(session, add_captures=captures)
        mock_sess.return_value.__aenter__.return_value = session

        resp = await client.post(
            "/v1/knowledgebases/kb-x/documents/file",
            files={"file": ("x.txt", b"hi", "text/plain")},
        )

    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["status"] == "queued"
    assert body["content"] == ""


@pytest.mark.asyncio
async def test_wait_true_polls_until_parse_reaches_indexing(client, monkeypatch):
    """With ``?wait=true`` the API blocks until the doc reaches a
    parse-terminal status (indexing / completed / failed) and returns
    with populated content.

    We simulate the worker's progress by flipping the captured doc's
    status after a short delay. The polling loop must observe the new
    status and return.
    """
    from config import get_settings
    settings = get_settings()
    monkeypatch.setattr(settings, "ingest_wait_timeout_seconds", 2.0)
    monkeypatch.setattr(settings, "ingest_wait_poll_interval_seconds", 0.02)

    captures: list = []

    async def _flip_after_delay():
        await asyncio.sleep(0.05)
        if captures:
            # Simulate worker: mark parse done, populate content.
            captures[0].status = "indexing"
            captures[0].content = "# Parsed"
            captures[0].images = []

    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_storage") as mock_storage,
        patch("api.routes.documents.registry") as mock_reg,
        patch("api.routes.documents.get_session") as mock_sess,
        patch("worker.queue.enqueue", new=AsyncMock()),
    ):
        mock_storage.return_value = AsyncMock()
        mock_reg.auto_select.return_value = "markitdown"

        session = AsyncMock()
        _configure_session(session, add_captures=captures)
        mock_sess.return_value.__aenter__.return_value = session

        # Kick off the status flip concurrently with the request.
        flip_task = asyncio.create_task(_flip_after_delay())
        resp = await client.post(
            "/v1/knowledgebases/kb-x/documents/file?wait=true",
            files={"file": ("x.txt", b"hi", "text/plain")},
        )
        await flip_task

    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["status"] == "indexing"
    assert body["content"] == "# Parsed"


@pytest.mark.asyncio
async def test_wait_true_times_out_returns_current_state(client, monkeypatch):
    """If the worker never reaches parse-terminal within the timeout,
    the API returns anyway with the current (still queued/parsing)
    state. The client can keep polling GET /documents/{id}.
    """
    from config import get_settings
    settings = get_settings()
    # Tight timeout; doc never flips.
    monkeypatch.setattr(settings, "ingest_wait_timeout_seconds", 0.1)
    monkeypatch.setattr(settings, "ingest_wait_poll_interval_seconds", 0.02)

    captures: list = []

    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_storage") as mock_storage,
        patch("api.routes.documents.registry") as mock_reg,
        patch("api.routes.documents.get_session") as mock_sess,
        patch("worker.queue.enqueue", new=AsyncMock()),
    ):
        mock_storage.return_value = AsyncMock()
        mock_reg.auto_select.return_value = "markitdown"
        session = AsyncMock()
        _configure_session(session, add_captures=captures)
        session.get = AsyncMock(
            side_effect=lambda cls, doc_id: captures[0] if captures else None,
        )
        mock_sess.return_value.__aenter__.return_value = session

        resp = await client.post(
            "/v1/knowledgebases/kb-x/documents/file?wait=true",
            files={"file": ("x.txt", b"hi", "text/plain")},
        )

    assert resp.status_code == 201, resp.text
    # Doc never flipped; status still queued.
    assert resp.json()["status"] == "queued"
