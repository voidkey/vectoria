from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from config import get_settings
from parsers.base import ParseResult


def _build_session(doc):
    session = AsyncMock()
    result = MagicMock()
    result.scalar_one_or_none.return_value = doc
    session.execute = AsyncMock(return_value=result)
    return session


async def _run_parse():
    fake_parser = MagicMock()
    fake_parser.parse = AsyncMock(return_value=ParseResult(
        content="# Doc\n\n" + "Enough body text to clear the threshold. " * 3,
        title="t",
    ))
    doc = MagicMock()
    doc.status = "queued"
    update_calls: list[dict] = []
    enqueue_calls: list[str] = []

    async def _update(doc_id, **fields):
        update_calls.append(fields)

    async def _enqueue(task_type, payload, *_a, **_k):
        enqueue_calls.append(task_type)

    with (
        patch("worker.handlers.get_session") as mock_sess,
        patch("worker.handlers.registry") as mock_reg,
        patch("worker.handlers.get_storage") as mock_storage,
        patch("worker.handlers.update_doc", new=_update),
        patch("worker.queue.enqueue", new=_enqueue),
    ):
        mock_sess.return_value.__aenter__.return_value = _build_session(doc)
        mock_reg.get_by_engine.return_value = fake_parser
        mock_storage.return_value = AsyncMock(get=AsyncMock(return_value=b"bytes"))
        from worker.handlers import handle_parse_document
        await handle_parse_document({
            "doc_id": "d1", "kb_id": "k1",
            "storage_key": "upload/k1/d1/x.pdf",
            "source": "x.pdf", "filename": "x.pdf",
            "selected_engine": "markitdown",
        })
    return update_calls, enqueue_calls


@pytest.mark.asyncio
async def test_indexing_enabled_keeps_legacy_sequence(monkeypatch):
    monkeypatch.setattr(get_settings(), "enable_indexing", True)
    update_calls, enqueue_calls = await _run_parse()
    statuses = [u["status"] for u in update_calls if "status" in u]
    assert statuses == ["parsing", "indexing"]
    assert "index_document" in enqueue_calls
    idx = [u["index_status"] for u in update_calls if "index_status" in u]
    assert idx[-1] == "pending"


@pytest.mark.asyncio
async def test_indexing_disabled_completes_and_skips(monkeypatch):
    monkeypatch.setattr(get_settings(), "enable_indexing", False)
    update_calls, enqueue_calls = await _run_parse()
    statuses = [u["status"] for u in update_calls if "status" in u]
    assert statuses == ["parsing", "completed"]
    assert "index_document" not in enqueue_calls
    idx = [u["index_status"] for u in update_calls if "index_status" in u]
    assert idx[-1] == "skipped"


async def _run_index(embed_side_effect=None, embed_return=None):
    fake_doc = MagicMock()
    fake_doc.content = "# Doc\n\nBody text long enough to split into a chunk."
    update_calls: list[dict] = []

    async def _update(doc_id, **fields):
        update_calls.append(fields)

    embedder = MagicMock()
    if embed_side_effect is not None:
        embedder.embed_batch = AsyncMock(side_effect=embed_side_effect)
    else:
        embedder.embed_batch = AsyncMock(return_value=embed_return or [[0.0] * 4])

    store = AsyncMock()
    store_cm = AsyncMock()
    store_cm.__aenter__.return_value = store

    with (
        patch("worker.handlers.load_doc", new=AsyncMock(return_value=fake_doc)),
        patch("worker.handlers.get_embedder", return_value=embedder),
        patch("worker.handlers.update_doc", new=_update),
        patch("worker.handlers.PgVectorStore") as mock_store_cls,
    ):
        mock_store_cls.create = AsyncMock(return_value=store_cm)
        from worker.handlers import handle_index_document
        exc = None
        try:
            await handle_index_document({"doc_id": "d1", "kb_id": "k1"})
        except Exception as e:  # noqa: BLE001
            exc = e
    return update_calls, exc


@pytest.mark.asyncio
async def test_index_success_marks_completed():
    update_calls, exc = await _run_index(embed_return=[[0.1] * 4] * 10)
    assert exc is None
    final = update_calls[-1]
    assert final["status"] == "completed"
    assert final["index_status"] == "completed"


@pytest.mark.asyncio
async def test_index_failure_keeps_doc_usable_and_reraises():
    update_calls, exc = await _run_index(embed_side_effect=RuntimeError("embed down"))
    assert isinstance(exc, RuntimeError)
    failure_update = update_calls[-1]
    assert failure_update["index_status"] == "failed"
    assert failure_update["status"] == "completed"
    assert "error_trace" not in failure_update
    assert "error_type" not in failure_update
