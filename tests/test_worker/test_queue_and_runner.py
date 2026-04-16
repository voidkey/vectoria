"""Tests for the PG task queue and worker runner.

These tests mock the DB layer (get_session / SessionLocal) rather than hitting
a real PG instance, so they run in CI without any infra. The contract being
tested: enqueue → dequeue → dispatch → complete/fail lifecycle.
"""
import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch, AsyncMock, MagicMock

from db.models import Task


# ---------------------------------------------------------------------------
# enqueue
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_enqueue_inserts_pending_task():
    session = AsyncMock()
    session.add = MagicMock()
    session.commit = AsyncMock()

    with patch("worker.queue.get_session") as mock_gs:
        mock_gs.return_value.__aenter__.return_value = session
        from worker.queue import enqueue
        task_id = await enqueue("index_document", {"doc_id": "d1", "kb_id": "kb1"})

    assert task_id  # non-empty UUID string
    added_task = session.add.call_args[0][0]
    assert isinstance(added_task, Task)
    assert added_task.task_type == "index_document"
    assert added_task.payload == {"doc_id": "d1", "kb_id": "kb1"}
    assert added_task.status == "pending"
    session.commit.assert_called_once()


# ---------------------------------------------------------------------------
# dequeue
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dequeue_returns_none_on_empty_queue():
    session = AsyncMock()
    result = MagicMock()
    result.scalar_one_or_none.return_value = None
    session.execute = AsyncMock(return_value=result)

    from worker.queue import dequeue
    task = await dequeue(session)

    assert task is None
    session.commit.assert_not_called()


@pytest.mark.asyncio
async def test_dequeue_returns_task_and_commits():
    fake_task = Task(
        id="t1", task_type="index_document",
        payload={"doc_id": "d1"}, status="running", attempts=1,
    )
    session = AsyncMock()
    result = MagicMock()
    result.scalar_one_or_none.return_value = fake_task
    session.execute = AsyncMock(return_value=result)

    from worker.queue import dequeue
    task = await dequeue(session)

    assert task is not None
    assert task.id == "t1"
    session.commit.assert_called_once()


# ---------------------------------------------------------------------------
# complete / fail
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_complete_sets_status():
    session = AsyncMock()
    with patch("worker.queue.get_session") as mock_gs:
        mock_gs.return_value.__aenter__.return_value = session
        from worker.queue import complete
        await complete("t1")

    session.execute.assert_called_once()
    session.commit.assert_called_once()


@pytest.mark.asyncio
async def test_fail_under_max_attempts_resets_to_pending():
    """A failed task with remaining attempts goes back to pending (auto-retry)."""
    task = Task(
        id="t2", task_type="index_document", payload={},
        status="running", attempts=1, max_attempts=3,
    )
    session = AsyncMock()
    result = MagicMock()
    result.scalar_one_or_none.return_value = task
    session.execute = AsyncMock(return_value=result)

    with patch("worker.queue.get_session") as mock_gs:
        mock_gs.return_value.__aenter__.return_value = session
        from worker.queue import fail
        await fail("t2", "embedding API 500")

    assert task.status == "pending"
    assert task.error == "embedding API 500"
    session.commit.assert_called_once()


@pytest.mark.asyncio
async def test_fail_at_max_attempts_marks_dead():
    """A task that has exhausted all attempts is marked dead (DLQ equivalent)."""
    task = Task(
        id="t3", task_type="index_document", payload={},
        status="running", attempts=3, max_attempts=3,
    )
    session = AsyncMock()
    result = MagicMock()
    result.scalar_one_or_none.return_value = task
    session.execute = AsyncMock(return_value=result)

    with patch("worker.queue.get_session") as mock_gs:
        mock_gs.return_value.__aenter__.return_value = session
        from worker.queue import fail
        await fail("t3", "final failure")

    assert task.status == "dead"
    assert task.finished_at is not None
    session.commit.assert_called_once()


# ---------------------------------------------------------------------------
# handlers dispatch
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dispatch_calls_registered_handler():
    from worker.handlers import dispatch, HANDLERS
    called_with = {}

    async def fake_handler(payload):
        called_with.update(payload)

    HANDLERS["test_type"] = fake_handler
    try:
        await dispatch("test_type", {"key": "value"})
        assert called_with == {"key": "value"}
    finally:
        del HANDLERS["test_type"]


@pytest.mark.asyncio
async def test_dispatch_raises_on_unknown_type():
    from worker.handlers import dispatch
    with pytest.raises(ValueError, match="Unknown task type"):
        await dispatch("nonexistent_type", {})


# ---------------------------------------------------------------------------
# runner
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_runner_processes_tasks_and_stops():
    """Worker loop dequeues, dispatches, and completes a task."""
    task = Task(
        id="t4", task_type="index_document",
        payload={"doc_id": "d1", "kb_id": "kb1", "content": "hello"},
        status="running", attempts=1, max_attempts=3,
    )

    with (
        patch("worker.runner.SessionLocal") as mock_sl,
        patch("worker.runner.dequeue") as mock_deq,
        patch("worker.runner.dispatch", new=AsyncMock()) as mock_dispatch,
        patch("worker.runner.complete", new=AsyncMock()) as mock_complete,
    ):
        session = AsyncMock()
        mock_sl.return_value.__aenter__.return_value = session
        mock_deq.return_value = task

        from worker.runner import run_worker
        processed = await run_worker(max_iterations=1)

    assert processed == 1
    mock_dispatch.assert_called_once_with("index_document", task.payload)
    mock_complete.assert_called_once_with("t4")


@pytest.mark.asyncio
async def test_runner_calls_fail_on_handler_error():
    """When a handler throws, the runner calls fail() instead of complete()."""
    task = Task(
        id="t5", task_type="index_document",
        payload={"doc_id": "d1", "kb_id": "kb1", "content": ""},
        status="running", attempts=1, max_attempts=3,
    )

    with (
        patch("worker.runner.SessionLocal") as mock_sl,
        patch("worker.runner.dequeue") as mock_deq,
        patch("worker.runner.dispatch", new=AsyncMock(side_effect=RuntimeError("boom"))),
        patch("worker.runner.complete", new=AsyncMock()) as mock_complete,
        patch("worker.runner.fail", new=AsyncMock()) as mock_fail,
    ):
        session = AsyncMock()
        mock_sl.return_value.__aenter__.return_value = session
        mock_deq.return_value = task

        from worker.runner import run_worker
        processed = await run_worker(max_iterations=1)

    assert processed == 1
    mock_complete.assert_not_called()
    mock_fail.assert_called_once_with("t5", "boom")
