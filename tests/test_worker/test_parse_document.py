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
        return_value=ParseResult(
            content="# Parsed\n\n" + "Body text with enough chars to pass the threshold. " * 2,
            title="t",
        )
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


@pytest.mark.asyncio
async def test_short_content_no_images_is_failed_empty_content():
    """Below threshold + no images → failed / empty_content, no fanout."""
    fake_parser = MagicMock()
    fake_parser.parse = AsyncMock(
        return_value=ParseResult(content="hi", title="t"),  # 2 chars < 50
    )
    doc = MagicMock(); doc.status = "queued"
    update_calls: list[dict] = []
    enqueue_calls: list[str] = []

    async def _update(doc_id, **fields):
        update_calls.append(fields)
    async def _enqueue(task_type, payload, *_a, **_kw):
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
        mock_storage.return_value = AsyncMock(get=AsyncMock(return_value=b"x"))
        from worker.handlers import handle_parse_document
        await handle_parse_document({
            "doc_id": "d", "kb_id": "k", "storage_key": "s",
            "source": "https://example.com/a", "filename": "",
            "selected_engine": "url",
        })

    final = [u for u in update_calls if u.get("status") == "failed"][-1]
    assert final["error_type"] == "empty_content"
    assert "index_document" not in enqueue_calls
    assert "download_and_store_images" not in enqueue_calls


@pytest.mark.asyncio
async def test_short_content_with_images_and_opt_in_is_image_only():
    """Below threshold + images + allow_image_only=True → image_only branch."""
    fake_parser = MagicMock()
    fake_parser.parse = AsyncMock(
        return_value=ParseResult(
            content="",
            title="note",
            image_urls=["https://cdn/a.jpg"],
            allow_image_only=True,
        ),
    )
    doc = MagicMock(); doc.status = "queued"
    update_calls: list[dict] = []
    enqueue_calls: list[tuple[str, dict]] = []

    async def _update(doc_id, **fields):
        update_calls.append(fields)
    async def _enqueue(task_type, payload, *_a, **_kw):
        enqueue_calls.append((task_type, payload))

    with (
        patch("worker.handlers.get_session") as mock_sess,
        patch("worker.handlers.registry") as mock_reg,
        patch("worker.handlers.get_storage") as mock_storage,
        patch("worker.handlers.update_doc", new=_update),
        patch("worker.queue.enqueue", new=_enqueue),
    ):
        mock_sess.return_value.__aenter__.return_value = _build_session(doc)
        mock_reg.get_by_engine.return_value = fake_parser
        mock_storage.return_value = AsyncMock(get=AsyncMock(return_value=b""))
        from worker.handlers import handle_parse_document
        await handle_parse_document({
            "doc_id": "d", "kb_id": "k", "storage_key": None,
            "source": "https://xhs/abc", "filename": "",
            "selected_engine": "url",
        })

    final = [u for u in update_calls if u.get("status") == "completed"][-1]
    assert final["error_type"] == "image_only"
    assert final["image_status"] == "pending"
    task_types = [t for t, _ in enqueue_calls]
    assert "download_and_store_images" in task_types
    assert "index_document" not in task_types  # skipped for image_only


@pytest.mark.asyncio
async def test_short_content_with_images_no_opt_in_is_failed():
    """Below threshold + images but allow_image_only=False → still failed."""
    fake_parser = MagicMock()
    fake_parser.parse = AsyncMock(
        return_value=ParseResult(
            content="",
            title="t",
            image_urls=["https://cdn/a.jpg"],
            allow_image_only=False,
        ),
    )
    doc = MagicMock(); doc.status = "queued"
    update_calls: list[dict] = []

    async def _update(doc_id, **fields):
        update_calls.append(fields)
    async def _noop(*_a, **_kw):
        pass

    with (
        patch("worker.handlers.get_session") as mock_sess,
        patch("worker.handlers.registry") as mock_reg,
        patch("worker.handlers.get_storage") as mock_storage,
        patch("worker.handlers.update_doc", new=_update),
        patch("worker.queue.enqueue", new=_noop),
    ):
        mock_sess.return_value.__aenter__.return_value = _build_session(doc)
        mock_reg.get_by_engine.return_value = fake_parser
        mock_storage.return_value = AsyncMock(get=AsyncMock(return_value=b""))
        from worker.handlers import handle_parse_document
        await handle_parse_document({
            "doc_id": "d", "kb_id": "k", "storage_key": None,
            "source": "https://mp.weixin.qq.com/s/abc", "filename": "",
            "selected_engine": "url",
        })

    final = [u for u in update_calls if u.get("status") == "failed"][-1]
    assert final["error_type"] == "empty_content"


@pytest.mark.asyncio
async def test_image_only_requires_non_empty_image_urls():
    """allow_image_only=True but no images → still failed (nothing to rescue)."""
    fake_parser = MagicMock()
    fake_parser.parse = AsyncMock(
        return_value=ParseResult(
            content="",
            title="t",
            image_urls=[],
            allow_image_only=True,
        ),
    )
    doc = MagicMock(); doc.status = "queued"
    update_calls: list[dict] = []

    async def _update(doc_id, **fields):
        update_calls.append(fields)
    async def _noop(*_a, **_kw):
        pass

    with (
        patch("worker.handlers.get_session") as mock_sess,
        patch("worker.handlers.registry") as mock_reg,
        patch("worker.handlers.get_storage") as mock_storage,
        patch("worker.handlers.update_doc", new=_update),
        patch("worker.queue.enqueue", new=_noop),
    ):
        mock_sess.return_value.__aenter__.return_value = _build_session(doc)
        mock_reg.get_by_engine.return_value = fake_parser
        mock_storage.return_value = AsyncMock(get=AsyncMock(return_value=b""))
        from worker.handlers import handle_parse_document
        await handle_parse_document({
            "doc_id": "d", "kb_id": "k", "storage_key": None,
            "source": "https://xhs/empty", "filename": "",
            "selected_engine": "url",
        })

    final = [u for u in update_calls if u.get("status") == "failed"][-1]
    assert final["error_type"] == "empty_content"


@pytest.mark.asyncio
async def test_content_exactly_at_threshold_passes():
    """len == min_content_chars passes (strict-less-than boundary)."""
    from config import get_settings
    threshold = get_settings().min_content_chars
    fake_parser = MagicMock()
    fake_parser.parse = AsyncMock(
        return_value=ParseResult(content="a" * threshold, title="t"),
    )
    doc = MagicMock(); doc.status = "queued"
    update_calls: list[dict] = []
    enqueue_calls: list[str] = []

    async def _update(doc_id, **fields):
        update_calls.append(fields)
    async def _enqueue(task_type, payload, *_a, **_kw):
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
        mock_storage.return_value = AsyncMock(get=AsyncMock(return_value=b"x"))
        from worker.handlers import handle_parse_document
        await handle_parse_document({
            "doc_id": "d", "kb_id": "k", "storage_key": "s",
            "source": "x.md", "filename": "x.md",
            "selected_engine": "markitdown",
        })

    assert "index_document" in enqueue_calls
    assert all(u.get("error_type") != "empty_content" for u in update_calls)


@pytest.mark.asyncio
async def test_rollback_min_content_chars_one_accepts_single_char():
    """MIN_CONTENT_CHARS=1 rollback: content of length 1 is accepted.

    Locks the spec's rollback guarantee — setting the env var to 1
    restores near-prior behavior where only fully-empty/whitespace
    content failed. A 1-char body should pass the threshold and
    proceed to indexing.
    """
    from config import get_settings

    # Save + restore the real setting on the lru-cached singleton.
    real_settings = get_settings()
    original_value = real_settings.min_content_chars
    real_settings.min_content_chars = 1
    try:
        fake_parser = MagicMock()
        fake_parser.parse = AsyncMock(
            return_value=ParseResult(content="x", title="t"),  # 1 char
        )
        doc = MagicMock(); doc.status = "queued"
        update_calls: list[dict] = []
        enqueue_calls: list[str] = []

        async def _update(doc_id, **fields):
            update_calls.append(fields)
        async def _enqueue(task_type, payload, *_a, **_kw):
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
            mock_storage.return_value = AsyncMock(get=AsyncMock(return_value=b"x"))
            from worker.handlers import handle_parse_document
            await handle_parse_document({
                "doc_id": "d", "kb_id": "k", "storage_key": "s",
                "source": "x.md", "filename": "x.md",
                "selected_engine": "markitdown",
            })

        # Under rollback, single-char content passes the threshold and
        # proceeds to indexing — no empty_content classification.
        assert "index_document" in enqueue_calls
        assert all(u.get("error_type") != "empty_content" for u in update_calls)
    finally:
        real_settings.min_content_chars = original_value
