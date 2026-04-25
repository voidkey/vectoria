"""Regression tests for Fix 1: handlers must skip silently when load_doc
raises ValueError (doc was deleted while the task was queued).

Without these guards the task fails 3× and dead-letters, firing the
VectoriaDeadTaskAccumulating alert hourly forever.
"""
from unittest.mock import patch

import pytest


@pytest.mark.asyncio
async def test_index_document_skips_when_doc_missing():
    """Doc deleted between enqueue and dequeue → handler returns silently,
    no UPDATE issued, no follow-up enqueued, no exception propagated."""
    update_calls: list[dict] = []
    enqueue_calls: list[str] = []

    async def _update(doc_id, **fields):
        update_calls.append(fields)

    async def _enqueue(task_type, payload, *_a, **_kw):
        enqueue_calls.append(task_type)

    async def _load_missing(doc_id):
        raise ValueError(f"Document {doc_id} not found")

    with (
        patch("worker.handlers.load_doc", new=_load_missing),
        patch("worker.handlers.update_doc", new=_update),
        patch("worker.queue.enqueue", new=_enqueue),
    ):
        from worker.handlers import handle_index_document
        # Must not raise:
        await handle_index_document({"doc_id": "missing-doc", "kb_id": "k"})

    assert update_calls == [], f"unexpected update_doc calls: {update_calls}"
    assert enqueue_calls == [], f"unexpected enqueue calls: {enqueue_calls}"


@pytest.mark.asyncio
async def test_download_and_store_images_skips_when_doc_missing():
    """Same shape as above — handler must skip when load_doc raises."""
    update_calls: list[dict] = []
    enqueue_calls: list[str] = []

    async def _update(doc_id, **fields):
        update_calls.append(fields)

    async def _enqueue(task_type, payload, *_a, **_kw):
        enqueue_calls.append(task_type)

    async def _load_missing(doc_id):
        raise ValueError(f"Document {doc_id} not found")

    with (
        patch("worker.handlers.load_doc", new=_load_missing),
        patch("worker.handlers.update_doc", new=_update),
        patch("worker.queue.enqueue", new=_enqueue),
    ):
        from worker.handlers import handle_download_and_store_images
        # Payload must include source_url + image_urls because the handler
        # reads them from payload BEFORE calling load_doc.
        await handle_download_and_store_images({
            "doc_id": "missing-doc",
            "kb_id": "k",
            "source_url": "https://example.com/missing",
            "image_urls": ["https://cdn/a.jpg"],
        })

    assert update_calls == [], f"unexpected update_doc calls: {update_calls}"
    assert enqueue_calls == [], f"unexpected enqueue calls: {enqueue_calls}"
