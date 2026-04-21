"""``parse_document`` handler — the work the API used to do inline.

Covers the successful path and the two classifier-level branches:
  * empty content → terminal ``failed``, no retry, no downstream enqueues
  * skip when doc is already past parse (idempotent replay)
Content-too-large is in test_api/test_ingest_size_limit.py alongside
the API-side raw-upload gate.
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


@pytest.mark.asyncio
async def test_happy_path_uploads_content_and_enqueues_index():
    """Fetch → parse → persist content → enqueue index_document."""
    fake_parser = MagicMock()
    fake_parser.parse = AsyncMock(
        return_value=ParseResult(content="# Parsed\n\nBody.", title="t")
    )

    doc = MagicMock()
    doc.status = "queued"
    update_calls: list[dict] = []
    enqueue_calls: list[str] = []

    async def _update(doc_id, **fields):
        update_calls.append(fields)

    async def _enqueue(task_type, payload, *_args, **_kw):
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

    statuses = [u.get("status") for u in update_calls if "status" in u]
    assert statuses == ["parsing", "indexing"], (
        f"expected parsing → indexing state machine; got {statuses}"
    )
    # Must have populated content.
    content_updates = [u for u in update_calls if "content" in u]
    assert any("Parsed" in u["content"] for u in content_updates)
    # Fanned out embedding work.
    assert "index_document" in enqueue_calls


@pytest.mark.asyncio
async def test_empty_content_is_terminal_failed_no_fanout():
    """Whitespace-only content is permanent; don't enqueue follow-ups."""
    fake_parser = MagicMock()
    fake_parser.parse = AsyncMock(
        return_value=ParseResult(content="   ", title="t"),
    )

    doc = MagicMock()
    doc.status = "queued"
    update_calls: list[dict] = []
    enqueue_calls: list[str] = []

    async def _update(doc_id, **fields):
        update_calls.append(fields)

    async def _enqueue(task_type, payload, *_args, **_kw):
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
            "storage_key": "k", "source": "x.pdf",
            "filename": "x.pdf", "selected_engine": "markitdown",
        })

    final_statuses = [u.get("status") for u in update_calls if "status" in u]
    assert final_statuses[-1] == "failed"
    # No index / analyze enqueues for permanently-failed docs.
    assert "index_document" not in enqueue_calls
    assert "analyze_images" not in enqueue_calls


@pytest.mark.asyncio
async def test_image_extractor_override_replaces_parser_refs():
    """W4-b integration: when a BaseImageExtractor is registered that
    matches the filename, the worker should use its output INSTEAD of
    the parser's ``image_refs``. This is the seam future plugins (pptx
    speaker notes, native PDF images) hang off.
    """
    from parsers import image_extractor as ie
    from parsers.image_ref import ImageRef

    ie._clear_for_tests()

    parser_ref = ImageRef(name="parser.png", mime="image/png",
                          _factory=lambda: b"x")
    plugin_ref = ImageRef(name="plugin.png", mime="image/png",
                          _factory=lambda: b"y")

    fake_parser = MagicMock()
    fake_parser.parse = AsyncMock(
        return_value=ParseResult(
            content="body", title="t", image_refs=[parser_ref],
        ),
    )

    class _PptxPlugin:
        def match(self, *, mime="", ext=""):
            return ext == ".pptx"
        async def extract(self, source, *, filename=""):
            return [plugin_ref]

    ie.register_image_extractor(_PptxPlugin())

    doc = MagicMock()
    doc.status = "queued"

    captured_refs: list = []

    async def _capture_stream(refs, **kw):
        # Store the ref list the pipeline ultimately uploads
        captured_refs.extend(refs)
        return len(refs)

    async def _update(*a, **kw): pass
    async def _enqueue(*a, **kw): pass

    try:
        with (
            patch("worker.handlers.get_session") as mock_sess,
            patch("worker.handlers.registry") as mock_reg,
            patch("worker.handlers.get_storage") as mock_storage,
            patch("worker.handlers.update_doc", new=_update),
            patch("worker.queue.enqueue", new=_enqueue),
            patch("api.image_stream.stream_upload_and_store_refs",
                  new=_capture_stream),
            # enrich is pass-through — we care about identity here.
            patch("worker.handlers.extract_metadata_into_refs",
                  side_effect=lambda _md, refs: refs),
        ):
            mock_sess.return_value.__aenter__.return_value = _build_session(doc)
            mock_reg.get_by_engine.return_value = fake_parser
            mock_storage.return_value = AsyncMock(
                get=AsyncMock(return_value=b"pptx-bytes"),
            )

            from worker.handlers import handle_parse_document
            await handle_parse_document({
                "doc_id": "d1", "kb_id": "k1",
                "storage_key": "k/d/deck.pptx",
                "source": "deck.pptx", "filename": "deck.pptx",
                "selected_engine": "docling",
            })

        assert [r.name for r in captured_refs] == ["plugin.png"], (
            "plugin refs should REPLACE parser refs when extractor matches"
        )
    finally:
        ie._clear_for_tests()


@pytest.mark.asyncio
async def test_image_extractor_passthrough_when_no_plugin_matches():
    """Registering an extractor that doesn't match the current file
    must not disturb the parser's refs.
    """
    from parsers import image_extractor as ie
    from parsers.image_ref import ImageRef

    ie._clear_for_tests()

    parser_ref = ImageRef(name="from_parser.png", mime="image/png",
                          _factory=lambda: b"x")

    fake_parser = MagicMock()
    fake_parser.parse = AsyncMock(
        return_value=ParseResult(
            content="body", title="t", image_refs=[parser_ref],
        ),
    )

    class _PptxOnly:
        def match(self, *, mime="", ext=""):
            return ext == ".pptx"
        async def extract(self, source, *, filename=""):
            raise AssertionError("should not be called for .pdf")

    ie.register_image_extractor(_PptxOnly())

    doc = MagicMock()
    doc.status = "queued"
    captured_refs: list = []

    async def _capture_stream(refs, **kw):
        captured_refs.extend(refs)
        return len(refs)

    async def _update(*a, **kw): pass
    async def _enqueue(*a, **kw): pass

    try:
        with (
            patch("worker.handlers.get_session") as mock_sess,
            patch("worker.handlers.registry") as mock_reg,
            patch("worker.handlers.get_storage") as mock_storage,
            patch("worker.handlers.update_doc", new=_update),
            patch("worker.queue.enqueue", new=_enqueue),
            patch("api.image_stream.stream_upload_and_store_refs",
                  new=_capture_stream),
            patch("worker.handlers.extract_metadata_into_refs",
                  side_effect=lambda _md, refs: refs),
        ):
            mock_sess.return_value.__aenter__.return_value = _build_session(doc)
            mock_reg.get_by_engine.return_value = fake_parser
            mock_storage.return_value = AsyncMock(
                get=AsyncMock(return_value=b"pdf-bytes"),
            )

            from worker.handlers import handle_parse_document
            await handle_parse_document({
                "doc_id": "d1", "kb_id": "k1",
                "storage_key": "k/d/report.pdf",
                "source": "report.pdf", "filename": "report.pdf",
                "selected_engine": "docling",
            })

        assert [r.name for r in captured_refs] == ["from_parser.png"]
    finally:
        ie._clear_for_tests()


@pytest.mark.asyncio
async def test_skip_when_doc_already_past_parse():
    """Idempotent replay: if a second parse_document task somehow fires
    (retry after timeout) for a doc already in indexing/completed, we
    must not re-parse and double-enqueue.
    """
    doc = MagicMock()
    doc.status = "completed"  # already done; shouldn't reprocess

    update_calls: list[dict] = []
    enqueue_calls: list[str] = []
    fake_parser = MagicMock()
    fake_parser.parse = AsyncMock()

    async def _update(doc_id, **fields):
        update_calls.append(fields)

    async def _enqueue(task_type, payload, *_args, **_kw):
        enqueue_calls.append(task_type)

    with (
        patch("worker.handlers.get_session") as mock_sess,
        patch("worker.handlers.registry") as mock_reg,
        patch("worker.handlers.update_doc", new=_update),
        patch("worker.queue.enqueue", new=_enqueue),
    ):
        mock_sess.return_value.__aenter__.return_value = _build_session(doc)
        mock_reg.get_by_engine.return_value = fake_parser

        from worker.handlers import handle_parse_document
        await handle_parse_document({
            "doc_id": "d1", "kb_id": "k1",
            "storage_key": "k", "source": "x.pdf",
            "filename": "x.pdf", "selected_engine": "markitdown",
        })

    fake_parser.parse.assert_not_called()
    assert not update_calls
    assert not enqueue_calls
