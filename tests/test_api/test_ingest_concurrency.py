"""Concurrent uploads are non-blocking.

Before W1 Task 4 the API parsed in-process behind a concurrency
semaphore and rejected the N+1 concurrent upload with 429 INGEST_BUSY.
That semaphore is gone now: the API only uploads raw bytes to S3 and
enqueues a ``parse_document`` task, and parsing happens in worker.
This file guards the new behaviour — no 429 under fan-in, both requests
land in the queue.
"""
import asyncio
import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from datetime import datetime

from db.models import Task


@pytest.mark.asyncio
async def test_concurrent_uploads_do_not_throttle(client):
    """Fanning in N uploads must not return 429 — there is no semaphore."""
    added_tasks: list[Task] = []

    def _setup_session():
        session = AsyncMock()
        miss = MagicMock()
        miss.scalar_one_or_none.return_value = None
        session.execute = AsyncMock(return_value=miss)

        def _add(obj):
            if isinstance(obj, Task):
                added_tasks.append(obj)

        session.add = MagicMock(side_effect=_add)
        session.commit = AsyncMock()

        def _refresh(obj):
            obj.created_at = datetime(2026, 1, 1)

        session.refresh = AsyncMock(side_effect=_refresh)
        return session

    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_storage") as mock_storage,
        patch("api.routes.documents.registry") as mock_reg,
        patch("api.routes.documents.get_session") as mock_sess,
    ):
        mock_storage.return_value = AsyncMock()
        mock_reg.auto_select.return_value = "markitdown"
        mock_sess.return_value.__aenter__ = AsyncMock(side_effect=lambda: _setup_session())
        mock_sess.return_value.__aexit__ = AsyncMock(return_value=False)

        # Five concurrent uploads — none should 429.
        tasks = [
            asyncio.create_task(client.post(
                "/v1/knowledgebases/kb-x/documents/file",
                files={"file": (f"f{i}.txt", f"content-{i}".encode(), "text/plain")},
            ))
            for i in range(5)
        ]
        responses = await asyncio.gather(*tasks)

    statuses = [r.status_code for r in responses]
    assert all(s == 201 for s in statuses), (
        f"concurrent uploads must not throttle; got {statuses}"
    )
    assert all(r.json()["status"] == "queued" for r in responses)
    # All five got a parse_document Task row staged in-session — no
    # silent drops, and they all share the atomic Document+Task insert.
    parse_tasks = [t for t in added_tasks if t.task_type == "parse_document"]
    assert len(parse_tasks) == 5
