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
