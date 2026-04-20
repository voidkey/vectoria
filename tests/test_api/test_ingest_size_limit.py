"""Upload size gate lives in the API; content-size gate lives in worker.

W1 Task 4 moved parse into the worker, so the "parsed content is too
large" check can't run on the API side any more — the API doesn't even
look at the parse result. This file guards the two remaining invariants:

  * API still rejects oversized raw uploads (413 before storage put)
  * Worker's ``parse_document`` handler marks oversized parsed content
    as ``failed`` instead of proceeding to index and OOM the embedder.
"""
import pytest
from unittest.mock import patch, AsyncMock, MagicMock

from parsers.base import ParseResult


@pytest.mark.asyncio
async def test_ingest_file_rejects_oversized_raw_upload(client):
    """Raw bytes over ``max_upload_bytes`` get 413 before storage write.

    This is the cheap early reject; the parsed-content limit (enforced
    in worker) is a second line of defense for small inputs that expand
    into huge markdown.
    """
    from config import get_settings
    limit = get_settings().max_upload_bytes
    oversized = b"a" * (limit + 1)

    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_storage") as mock_storage,
        patch("api.routes.documents.registry") as mock_reg,
    ):
        mock_storage.return_value = AsyncMock()
        mock_reg.auto_select.return_value = "markitdown"

        resp = await client.post(
            "/v1/knowledgebases/kb-x/documents/file",
            files={"file": ("big.bin", oversized, "application/octet-stream")},
        )

    assert resp.status_code == 413, resp.text
    assert resp.json()["code"] == 1204  # UPLOAD_TOO_LARGE
    mock_storage.return_value.put.assert_not_called()


@pytest.mark.asyncio
async def test_parse_document_marks_oversized_content_as_failed():
    """Worker handler path: after parse, if content exceeds
    ``max_content_chars`` we don't proceed to embedding — that would
    fan out huge chunk lists and OOM the worker. Mark failed, return
    (do not re-raise — retry wouldn't help; the content won't shrink).
    """
    from config import get_settings
    limit = get_settings().max_content_chars
    oversized_content = "a" * (limit + 1)

    fake_parser = MagicMock()
    fake_parser.parse = AsyncMock(
        return_value=ParseResult(content=oversized_content, title="big"),
    )

    fake_doc = MagicMock()
    fake_doc.status = "queued"

    update_calls = []

    async def _update(doc_id, **fields):
        update_calls.append(fields)

    enqueue_calls = []

    async def _enqueue(task_type, payload, *_args, **_kw):
        enqueue_calls.append(task_type)

    session = AsyncMock()
    result = MagicMock()
    result.scalar_one_or_none.return_value = fake_doc
    session.execute = AsyncMock(return_value=result)

    with (
        patch("worker.handlers.get_session") as mock_sess,
        patch("worker.handlers.registry") as mock_reg,
        patch("worker.handlers.get_storage") as mock_storage,
        patch("worker.handlers.update_doc", new=_update),
        patch("worker.queue.enqueue", new=_enqueue),
    ):
        mock_sess.return_value.__aenter__.return_value = session
        mock_reg.get_by_engine.return_value = fake_parser
        mock_storage.return_value = AsyncMock(get=AsyncMock(return_value=b"raw"))

        from worker.handlers import handle_parse_document
        await handle_parse_document({
            "doc_id": "d1", "kb_id": "k1",
            "storage_key": "some/key", "source": "file.pdf",
            "filename": "file.pdf", "selected_engine": "markitdown",
        })

    # Final update must mark failed with CONTENT-too-large reason.
    statuses = [u.get("status") for u in update_calls if "status" in u]
    assert statuses[-1] == "failed", f"final status should be failed; got {statuses}"
    error_msgs = [u.get("error_msg") for u in update_calls if "error_msg" in u]
    assert any(
        "exceeds" in (m or "") for m in error_msgs
    ), f"expected content-too-large error_msg; got {error_msgs}"
    # Must NOT have enqueued index/analyze follow-ups.
    assert "index_document" not in enqueue_calls
    assert "analyze_images" not in enqueue_calls
