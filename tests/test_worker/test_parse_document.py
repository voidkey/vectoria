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


# ---------------------------------------------------------------------------
# Per-attempt engine fallback on dep-level failures
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dep_level_failure_falls_back_to_next_engine_in_chain():
    """Regression for the cross-region mineru WriteTimeout case.

    Previously ``selected_engine`` was treated as immutable: if the
    engine bound at upload time hit a transient dependency error
    (network timeout, breaker open) the task burned all 3 queue
    retries on the same broken upstream and went dead — leaving the
    file unparseable even though pdfium would have worked locally
    in-process.

    Now: a dep-level exception triggers same-attempt fallback through
    ``registry.fallback_chain``. File-level errors keep the original
    behaviour (no fallback — see the next test).
    """
    import httpx

    mineru_parser = MagicMock()
    mineru_parser.parse = AsyncMock(
        side_effect=httpx.WriteTimeout("upload to mineru body timed out")
    )
    pdfium_parser = MagicMock()
    pdfium_parser.parse = AsyncMock(
        return_value=ParseResult(
            content="# Recovered via pdfium\n\n" + "x" * 200,
            title="t",
        )
    )

    doc = MagicMock(); doc.status = "queued"
    update_calls: list[dict] = []
    enqueue_calls: list[str] = []
    parsers_used: list[str] = []

    async def _update(doc_id, **fields):
        update_calls.append(fields)
    async def _enqueue(task_type, payload, *_a, **_kw):
        enqueue_calls.append(task_type)

    def _get_by_engine(name):
        parsers_used.append(name)
        return {"mineru": mineru_parser, "pdfium": pdfium_parser}[name]

    with (
        patch("worker.handlers.get_session") as mock_sess,
        patch("worker.handlers.registry") as mock_reg,
        patch("worker.handlers.get_storage") as mock_storage,
        patch("worker.handlers.update_doc", new=_update),
        patch("worker.queue.enqueue", new=_enqueue),
    ):
        mock_sess.return_value.__aenter__.return_value = _build_session(doc)
        mock_reg.get_by_engine.side_effect = _get_by_engine
        mock_reg.fallback_chain.return_value = ["pdfium", "markitdown"]
        mock_storage.return_value = AsyncMock(get=AsyncMock(return_value=b"%PDF"))

        from worker.handlers import handle_parse_document
        await handle_parse_document({
            "doc_id": "d-fb", "kb_id": "k1",
            "storage_key": "upload/k1/d-fb/big.pdf",
            "source": "big.pdf", "filename": "big.pdf",
            "selected_engine": "mineru",
        })

    # Both engines were tried; fallback (pdfium) was used.
    assert parsers_used == ["mineru", "pdfium"]
    # Doc reached indexing stage — not failed.
    statuses = [u.get("status") for u in update_calls if "status" in u]
    assert "indexing" in statuses
    assert "failed" not in statuses
    # parse_engine reflects the engine that actually produced content,
    # so per-engine observability (Grafana / digest / DB) doesn't lie.
    indexing_update = next(u for u in update_calls if u.get("status") == "indexing")
    assert indexing_update.get("parse_engine") == "pdfium"
    # Index task got fanned out so the user's doc actually completes.
    assert "index_document" in enqueue_calls


@pytest.mark.asyncio
async def test_parser_level_error_falls_back_through_chain():
    """Parser-level exceptions (e.g. python-pptx hitting a sharp edge
    on a specific shape, mammoth choking on an embedded equation)
    used to be terminal — handler raised immediately. Now they
    trigger fallback too: the next engine in the chain reads the
    same bytes via a different code path and may well succeed.
    Cost is one extra attempt per failure (bounded by chain length).

    Regression scenario: Office native parsers' library bugs were
    killing files that markitdown could have rescued.
    """
    boom_parser = MagicMock()
    boom_parser.parse = AsyncMock(side_effect=ValueError("library hit a sharp edge"))
    rescue_parser = MagicMock()
    rescue_parser.parse = AsyncMock(
        return_value=ParseResult(
            content="# Recovered via markitdown\n\n" + "x" * 200,
            title="t",
        )
    )

    doc = MagicMock(); doc.status = "queued"
    update_calls: list[dict] = []
    parsers_used: list[str] = []

    async def _update(doc_id, **fields):
        update_calls.append(fields)
    async def _enqueue(task_type, payload, *_a, **_kw):
        pass

    def _get_by_engine(name):
        parsers_used.append(name)
        return {"pptx-native": boom_parser, "markitdown": rescue_parser}[name]

    with (
        patch("worker.handlers.get_session") as mock_sess,
        patch("worker.handlers.registry") as mock_reg,
        patch("worker.handlers.get_storage") as mock_storage,
        patch("worker.handlers.update_doc", new=_update),
        patch("worker.queue.enqueue", new=_enqueue),
    ):
        mock_sess.return_value.__aenter__.return_value = _build_session(doc)
        mock_reg.get_by_engine.side_effect = _get_by_engine
        mock_reg.fallback_chain.return_value = ["markitdown"]
        mock_storage.return_value = AsyncMock(
            get=AsyncMock(return_value=b"PK\x03\x04"),  # zip head
        )

        from worker.handlers import handle_parse_document
        await handle_parse_document({
            "doc_id": "d-rescue", "kb_id": "k1",
            "storage_key": "upload/k1/d-rescue/deck.pptx",
            "source": "deck.pptx", "filename": "deck.pptx",
            "selected_engine": "pptx-native",
        })

    # Both engines were tried; markitdown rescued the file.
    assert parsers_used == ["pptx-native", "markitdown"]
    statuses = [u.get("status") for u in update_calls if "status" in u]
    assert "indexing" in statuses
    assert "failed" not in statuses
    indexing_update = next(u for u in update_calls if u.get("status") == "indexing")
    assert indexing_update.get("parse_engine") == "markitdown"


@pytest.mark.asyncio
async def test_fallback_chain_exhausted_marks_doc_failed_and_reraises():
    """If every engine in the chain hits a dep-level error (e.g. mineru
    timeout AND pdfium also down), the doc must be marked ``failed``
    with the last exception captured, and the exception re-raised so
    the queue can retry — not silently swallow.
    """
    import httpx

    boom_a = MagicMock()
    boom_a.parse = AsyncMock(side_effect=httpx.WriteTimeout("mineru ded"))
    boom_b = MagicMock()
    boom_b.parse = AsyncMock(side_effect=httpx.ConnectError("pdfium ded"))

    doc = MagicMock(); doc.status = "queued"
    update_calls: list[dict] = []

    async def _update(doc_id, **fields):
        update_calls.append(fields)
    async def _enqueue(task_type, payload, *_a, **_kw):
        pass

    def _get_by_engine(name):
        return {"mineru": boom_a, "pdfium": boom_b}[name]

    with (
        patch("worker.handlers.get_session") as mock_sess,
        patch("worker.handlers.registry") as mock_reg,
        patch("worker.handlers.get_storage") as mock_storage,
        patch("worker.handlers.update_doc", new=_update),
        patch("worker.queue.enqueue", new=_enqueue),
    ):
        mock_sess.return_value.__aenter__.return_value = _build_session(doc)
        mock_reg.get_by_engine.side_effect = _get_by_engine
        mock_reg.fallback_chain.return_value = ["pdfium"]
        mock_storage.return_value = AsyncMock(get=AsyncMock(return_value=b"%PDF"))

        from worker.handlers import handle_parse_document
        with pytest.raises(httpx.ConnectError):
            await handle_parse_document({
                "doc_id": "d-allfail", "kb_id": "k1",
                "storage_key": "upload/k1/d-allfail/x.pdf",
                "source": "x.pdf", "filename": "x.pdf",
                "selected_engine": "mineru",
            })

    statuses = [u.get("status") for u in update_calls if "status" in u]
    assert statuses[-1] == "failed"
    failed_update = next(u for u in update_calls if u.get("status") == "failed")
    assert failed_update.get("error_type") == "parse_error"
    # Traceback must survive — captured inside except, not from a
    # bare traceback.format_exc() out of scope. The DB row carries
    # the actual exception type for forensics rather than the empty
    # 'NoneType: None' that the previous code stored.
    trace = failed_update.get("error_trace") or ""
    assert "WriteTimeout" in trace or "ConnectError" in trace, (
        f"expected captured traceback, got: {trace!r}"
    )


@pytest.mark.asyncio
async def test_empty_content_from_first_engine_triggers_fallback():
    """Regression for the prod docx-native empty-content cases:
    Office native parsers (docx_parser / pptx_parser / xlsx_parser)
    catch internal library exceptions and return
    ``ParseResult(content="")`` rather than raising. Before this
    fix the handler would treat that as terminal empty_content and
    skip the markitdown fallback in the chain, even though
    markitdown reads the same formats via different code paths and
    often succeeds on the same files.

    Now: empty result triggers fallback the same way an exception
    does. The chain runs to completion; the file gets a real shot
    at every engine before being declared empty.
    """
    boom = MagicMock()
    boom.parse = AsyncMock(
        return_value=ParseResult(content="", title="docx")
    )
    rescue = MagicMock()
    rescue.parse = AsyncMock(
        return_value=ParseResult(
            content="# Recovered\n\n" + "x" * 200, title="t",
        )
    )

    doc = MagicMock(); doc.status = "queued"
    update_calls: list[dict] = []
    parsers_used: list[str] = []
    enqueue_calls: list[str] = []

    async def _update(doc_id, **fields):
        update_calls.append(fields)
    async def _enqueue(task_type, payload, *_a, **_kw):
        enqueue_calls.append(task_type)

    def _get_by_engine(name):
        parsers_used.append(name)
        return {"docx-native": boom, "markitdown": rescue}[name]

    with (
        patch("worker.handlers.get_session") as mock_sess,
        patch("worker.handlers.registry") as mock_reg,
        patch("worker.handlers.get_storage") as mock_storage,
        patch("worker.handlers.update_doc", new=_update),
        patch("worker.queue.enqueue", new=_enqueue),
    ):
        mock_sess.return_value.__aenter__.return_value = _build_session(doc)
        mock_reg.get_by_engine.side_effect = _get_by_engine
        mock_reg.fallback_chain.return_value = ["markitdown"]
        mock_storage.return_value = AsyncMock(get=AsyncMock(return_value=b"PK"))

        from worker.handlers import handle_parse_document
        await handle_parse_document({
            "doc_id": "d-empty-fb", "kb_id": "k1",
            "storage_key": "s/x", "source": "x.docx", "filename": "x.docx",
            "selected_engine": "docx-native",
        })

    assert parsers_used == ["docx-native", "markitdown"]
    statuses = [u.get("status") for u in update_calls if "status" in u]
    assert "indexing" in statuses
    assert "failed" not in statuses
    indexing = next(u for u in update_calls if u.get("status") == "indexing")
    assert indexing.get("parse_engine") == "markitdown"
    assert "index_document" in enqueue_calls


@pytest.mark.asyncio
async def test_all_engines_empty_terminal_empty_content_no_raise():
    """If every engine in the chain returns empty content (truly bad
    file), the terminal classification stays ``empty_content`` —
    same as the single-engine behavior before fallback existed.
    Crucially we *don't* raise from the handler: queue retries on
    the same chain would just re-produce empty content, so let the
    task complete and stop the cycle.
    """
    e1 = MagicMock()
    e1.parse = AsyncMock(return_value=ParseResult(content="", title=""))
    e2 = MagicMock()
    e2.parse = AsyncMock(return_value=ParseResult(content="  \n  ", title=""))

    doc = MagicMock(); doc.status = "queued"
    update_calls: list[dict] = []

    async def _update(doc_id, **fields):
        update_calls.append(fields)
    async def _enqueue(*_a, **_kw):
        pass

    def _get_by_engine(name):
        return {"docx-native": e1, "markitdown": e2}[name]

    with (
        patch("worker.handlers.get_session") as mock_sess,
        patch("worker.handlers.registry") as mock_reg,
        patch("worker.handlers.get_storage") as mock_storage,
        patch("worker.handlers.update_doc", new=_update),
        patch("worker.queue.enqueue", new=_enqueue),
    ):
        mock_sess.return_value.__aenter__.return_value = _build_session(doc)
        mock_reg.get_by_engine.side_effect = _get_by_engine
        mock_reg.fallback_chain.return_value = ["markitdown"]
        mock_storage.return_value = AsyncMock(get=AsyncMock(return_value=b""))

        from worker.handlers import handle_parse_document
        # Must NOT raise — queue retry won't help.
        await handle_parse_document({
            "doc_id": "d-allempty", "kb_id": "k1",
            "storage_key": "s/x", "source": "x.docx", "filename": "x.docx",
            "selected_engine": "docx-native",
        })

    statuses = [u.get("status") for u in update_calls if "status" in u]
    assert statuses[-1] == "failed"
    failed = next(u for u in update_calls if u.get("status") == "failed")
    assert failed.get("error_type") == "empty_content"


@pytest.mark.asyncio
async def test_permanent_parse_error_short_circuits_no_retry():
    """``PermanentParseError`` (e.g. URL on the unparseable blacklist)
    short-circuits the whole chain: doc gets marked ``failed``
    immediately and the handler *returns* (no raise) so the queue
    sees a successful task — no retries, no dead-letter alert.

    Distinct from regular Exception which triggers fallback chain
    and (if chain exhausts) re-raises so the queue retries.
    """
    from parsers.base import PermanentParseError

    boom = MagicMock()
    boom.parse = AsyncMock(side_effect=PermanentParseError("URL on blacklist"))
    other = MagicMock()
    other.parse = AsyncMock()  # must NOT be called — chain bypassed

    doc = MagicMock(); doc.status = "queued"
    update_calls: list[dict] = []
    parsers_used: list[str] = []

    async def _update(doc_id, **fields):
        update_calls.append(fields)
    async def _enqueue(task_type, payload, *_a, **_kw):
        pass

    def _get_by_engine(name):
        parsers_used.append(name)
        return {"url": boom, "fallback": other}[name]

    with (
        patch("worker.handlers.get_session") as mock_sess,
        patch("worker.handlers.registry") as mock_reg,
        patch("worker.handlers.get_storage") as mock_storage,
        patch("worker.handlers.update_doc", new=_update),
        patch("worker.queue.enqueue", new=_enqueue),
    ):
        mock_sess.return_value.__aenter__.return_value = _build_session(doc)
        mock_reg.get_by_engine.side_effect = _get_by_engine
        # Even though we offer a fallback, handler must NOT try it —
        # PermanentParseError is the "no engine helps" signal.
        mock_reg.fallback_chain.return_value = ["fallback"]
        mock_storage.return_value = AsyncMock(get=AsyncMock(return_value=b""))

        from worker.handlers import handle_parse_document
        # No raise — task returns successfully so queue marks it
        # completed (no retries, no dead).
        await handle_parse_document({
            "doc_id": "d-perm", "kb_id": "k1",
            "storage_key": None,
            "source": "https://blacklisted.example/video", "filename": "",
            "selected_engine": "url",
        })

    # Only the original engine was tried; fallback never invoked.
    assert parsers_used == ["url"]
    other.parse.assert_not_called()
    # Doc marked failed (terminal state), error_type=parse_error so it
    # shows up correctly in digests + dashboards.
    statuses = [u.get("status") for u in update_calls if "status" in u]
    assert "failed" in statuses
    failed = next(u for u in update_calls if u.get("status") == "failed")
    assert failed.get("error_type") == "parse_error"
    assert "blacklist" in failed.get("error_msg", "").lower() or \
           "URL on blacklist" in failed.get("error_msg", "")


@pytest.mark.asyncio
async def test_fallback_bumps_parse_fallback_total_counter():
    """Each successful fallback (used_engine != selected_engine) must
    bump ``vectoria_parse_fallback_total{from_engine, to_engine}`` so
    operators can spot upstream-link degradation patterns from
    Grafana without having to grep WARN logs.
    """
    import httpx

    boom = MagicMock()
    boom.parse = AsyncMock(side_effect=httpx.WriteTimeout("dep dead"))
    rescue = MagicMock()
    rescue.parse = AsyncMock(
        return_value=ParseResult(content="recovered " * 50, title="t")
    )

    doc = MagicMock(); doc.status = "queued"

    def _get_by_engine(name):
        return {"mineru": boom, "pdfium": rescue}[name]

    with (
        patch("worker.handlers.get_session") as mock_sess,
        patch("worker.handlers.registry") as mock_reg,
        patch("worker.handlers.get_storage") as mock_storage,
        patch("worker.handlers.update_doc", new=AsyncMock()),
        patch("worker.queue.enqueue", new=AsyncMock()),
        patch("worker.handlers.PARSE_FALLBACK_TOTAL") as mock_counter,
    ):
        mock_sess.return_value.__aenter__.return_value = _build_session(doc)
        mock_reg.get_by_engine.side_effect = _get_by_engine
        mock_reg.fallback_chain.return_value = ["pdfium"]
        mock_storage.return_value = AsyncMock(get=AsyncMock(return_value=b"%PDF"))
        mock_inc = MagicMock()
        mock_counter.labels.return_value = mock_inc

        from worker.handlers import handle_parse_document
        await handle_parse_document({
            "doc_id": "d-fbm", "kb_id": "k", "storage_key": "s",
            "source": "x.pdf", "filename": "x.pdf",
            "selected_engine": "mineru",
        })

    mock_counter.labels.assert_called_once_with(
        from_engine="mineru", to_engine="pdfium",
    )
    mock_inc.inc.assert_called_once()
