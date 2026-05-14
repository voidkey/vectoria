"""Worker-side persistence of ``ParseResult.page_count``.

The PPTX parser counts slides as a free byproduct of the parse walk and
emits the value on ParseResult so the worker can persist it without a
second pass over the file. This pins:
  * value present → forwarded to update_doc
  * value None → NOT forwarded (don't clobber an upload-time PDF page
    count with a None from a parser that doesn't emit one)
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from parsers.base import ParseResult


def _build_session(doc):
    session = AsyncMock()
    result = MagicMock()
    result.scalar_one_or_none.return_value = doc
    session.execute = AsyncMock(return_value=result)
    return session


def _enough_content() -> str:
    return "# Parsed\n\n" + "Body text with enough chars to pass the threshold. " * 2


@pytest.mark.asyncio
async def test_parse_result_page_count_is_persisted():
    """PptxParser-shaped ParseResult.page_count ends up in update_doc kwargs."""
    fake_parser = MagicMock()
    fake_parser.parse = AsyncMock(
        return_value=ParseResult(
            content=_enough_content(), title="deck", page_count=23,
        )
    )
    doc = MagicMock()
    doc.status = "queued"
    update_calls: list[dict] = []

    async def _update(doc_id, **fields):
        update_calls.append(fields)

    async def _enqueue(task_type, payload, *_args, **_kw):
        return None

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
            "storage_key": "upload/k1/d1/x.pptx",
            "source": "x.pptx", "filename": "x.pptx",
            "selected_engine": "pptx-native",
        })

    pc_updates = [u for u in update_calls if "page_count" in u]
    assert len(pc_updates) == 1, (
        f"expected page_count to be written exactly once; got {update_calls}"
    )
    assert pc_updates[0]["page_count"] == 23


@pytest.mark.asyncio
async def test_parse_result_without_page_count_does_not_clobber():
    """When ParseResult.page_count is None (e.g. PDF parsers that don't
    emit it), update_doc must NOT receive ``page_count=None`` — that
    would overwrite the upload-time count the PDF gate already stored.
    """
    fake_parser = MagicMock()
    fake_parser.parse = AsyncMock(
        return_value=ParseResult(content=_enough_content(), title="paper"),
    )
    doc = MagicMock()
    doc.status = "queued"
    update_calls: list[dict] = []

    async def _update(doc_id, **fields):
        update_calls.append(fields)

    async def _enqueue(task_type, payload, *_args, **_kw):
        return None

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
            "doc_id": "d2", "kb_id": "k1",
            "storage_key": "upload/k1/d2/x.pdf",
            "source": "x.pdf", "filename": "x.pdf",
            "selected_engine": "mineru",
        })

    assert not any("page_count" in u for u in update_calls), (
        f"page_count should not appear in any update call; got {update_calls}"
    )
