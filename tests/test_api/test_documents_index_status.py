import pytest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from api.routes.documents import _doc_to_response


def test_doc_to_response_passes_through_index_status():
    doc = SimpleNamespace(
        id="d", kb_id="k", title="t", source="s", chunk_count=0,
        status="completed", index_status="failed", error_msg="",
        created_at=datetime(2026, 1, 1),
    )
    resp = _doc_to_response(doc)
    assert resp.index_status == "failed"


@pytest.mark.asyncio
async def test_ingest_response_includes_index_status(client):
    captures: list = []

    def _configure_session(session):
        def _execute(_stmt):
            r = MagicMock()
            r.scalar_one_or_none.return_value = (
                captures[0] if captures else None
            )
            return r

        session.execute = AsyncMock(side_effect=_execute)
        session.add = MagicMock(side_effect=lambda d: captures.append(d))
        session.commit = AsyncMock()
        session.get = AsyncMock(
            side_effect=lambda _cls, _doc_id: captures[0] if captures else None,
        )

        def _refresh(obj):
            obj.created_at = datetime(2026, 5, 8)

        session.refresh = AsyncMock(side_effect=_refresh)
        return session

    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_storage") as mock_storage,
        patch("api.routes.documents.get_session") as mock_sess,
        patch("worker.queue.enqueue_in_session"),
    ):
        mock_storage.return_value = AsyncMock()
        session = AsyncMock()
        _configure_session(session)
        mock_sess.return_value.__aenter__.return_value = session

        resp = await client.post(
            "/v1/knowledgebases/kb-x/documents/text",
            json={"text": "hello world this is a body"},
        )

    assert resp.status_code == 201, resp.text
    assert "index_status" in resp.json()
