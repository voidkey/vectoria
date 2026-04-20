"""Document and its parse_document Task must land atomically.

Failure mode this guards: before the fix, the API committed the
Document in one transaction and then called ``enqueue()`` which opened
its own transaction for the Task insert. A DB blip between the two
commits could leave a ``queued`` Document with no Task row — the
worker would never pick it up, and the per-hash dedup lookup would
then keep matching the wedged row on every retry.

The fix: both rows are staged in the API's session and a single commit
makes them succeed or fail together.
"""
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from db.models import Document, Task


@pytest.mark.asyncio
async def test_document_and_parse_task_committed_in_single_transaction(client):
    """session.add is called once for the Document and once for the Task,
    but commit() happens once — not twice. A failure of that commit
    rolls back both; a success commits both.
    """
    adds: list = []
    commit_calls = 0

    async def _commit():
        nonlocal commit_calls
        commit_calls += 1

    def _add(obj):
        adds.append(obj)

    def _refresh_side(obj):
        obj.created_at = datetime(2026, 4, 20)

    session = AsyncMock()
    miss = MagicMock()
    miss.scalar_one_or_none.return_value = None
    session.execute = AsyncMock(return_value=miss)
    session.add = MagicMock(side_effect=_add)
    session.commit = AsyncMock(side_effect=_commit)
    session.refresh = AsyncMock(side_effect=_refresh_side)

    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_storage") as mock_storage,
        patch("api.routes.documents.registry") as mock_reg,
        patch("api.routes.documents.get_session") as mock_sess,
    ):
        mock_storage.return_value = AsyncMock()
        mock_reg.auto_select.return_value = "markitdown"
        mock_sess.return_value.__aenter__.return_value = session

        resp = await client.post(
            "/v1/knowledgebases/kb-x/documents/file",
            files={"file": ("x.txt", b"payload", "text/plain")},
        )

    assert resp.status_code == 201

    doc_rows = [o for o in adds if isinstance(o, Document)]
    task_rows = [o for o in adds if isinstance(o, Task)]

    assert len(doc_rows) == 1, "expected exactly one Document added"
    assert len(task_rows) == 1, "expected exactly one Task added"
    assert task_rows[0].task_type == "parse_document"
    assert task_rows[0].payload["doc_id"] == doc_rows[0].id

    # Single commit — both rows in the same transaction.
    assert commit_calls == 1, (
        f"Document and Task must share one commit; saw {commit_calls} commits"
    )
