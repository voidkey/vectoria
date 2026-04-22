"""Tests for the worker runner additions in W1 Task 2:
- RSS self-kill between tasks
- WORKER_QUEUES task_type filter propagated to dequeue()
- WORKER_CONCURRENCY > 1 logs a warning (W5 placeholder)
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from db.models import Task


def _task(task_id: str = "t", task_type: str = "index_document") -> Task:
    return Task(
        id=task_id, task_type=task_type, payload={},
        status="running", attempts=1, max_attempts=3,
    )


@pytest.mark.asyncio
async def test_runner_self_kills_when_rss_exceeds_limit(monkeypatch):
    """After a task completes, if rss_bytes() reports over the configured
    limit, the runner must exit cleanly so K8s can restart the pod with
    a fresh heap.
    """
    from config import get_settings

    settings = get_settings()
    monkeypatch.setattr(settings, "worker_rss_limit_bytes", 1024)  # 1 KiB
    monkeypatch.setattr(settings, "worker_queues", "")

    with (
        patch("worker.runner.SessionLocal") as mock_sl,
        patch("worker.runner.dequeue", new=AsyncMock(return_value=_task())),
        patch("worker.runner.dispatch", new=AsyncMock()),
        patch("worker.runner.complete", new=AsyncMock()),
        patch("worker.runner.rss_bytes", return_value=99_999_999),
    ):
        mock_sl.return_value.__aenter__.return_value = AsyncMock()

        from worker.runner import run_worker
        processed = await run_worker(max_iterations=10)

    # Despite max_iterations=10, the runner must bail after the first task
    # because the post-task RSS check triggered self-kill.
    assert processed == 1


@pytest.mark.asyncio
async def test_runner_does_not_self_kill_when_rss_under_limit(monkeypatch):
    from config import get_settings

    settings = get_settings()
    monkeypatch.setattr(settings, "worker_rss_limit_bytes", 10_000_000)  # 10 MB
    monkeypatch.setattr(settings, "worker_queues", "")

    with (
        patch("worker.runner.SessionLocal") as mock_sl,
        patch("worker.runner.dequeue", new=AsyncMock(return_value=_task())),
        patch("worker.runner.dispatch", new=AsyncMock()),
        patch("worker.runner.complete", new=AsyncMock()),
        patch("worker.runner.rss_bytes", return_value=1_000_000),  # well under
    ):
        mock_sl.return_value.__aenter__.return_value = AsyncMock()

        from worker.runner import run_worker
        processed = await run_worker(max_iterations=3)

    assert processed == 3, "worker must keep running when RSS is under limit"


@pytest.mark.asyncio
async def test_runner_disabled_rss_check_when_limit_zero(monkeypatch):
    """limit_bytes=0 is the off switch — even if rss_bytes reports huge
    usage, the runner keeps processing. This is the dev default so macOS
    laptops (where rss_bytes returns 0 anyway) aren't forced into a loop.
    """
    from config import get_settings

    settings = get_settings()
    monkeypatch.setattr(settings, "worker_rss_limit_bytes", 0)
    monkeypatch.setattr(settings, "worker_queues", "")

    with (
        patch("worker.runner.SessionLocal") as mock_sl,
        patch("worker.runner.dequeue", new=AsyncMock(return_value=_task())),
        patch("worker.runner.dispatch", new=AsyncMock()),
        patch("worker.runner.complete", new=AsyncMock()),
        patch("worker.runner.rss_bytes", return_value=10**12),  # absurd
    ):
        mock_sl.return_value.__aenter__.return_value = AsyncMock()

        from worker.runner import run_worker
        processed = await run_worker(max_iterations=3)

    assert processed == 3


@pytest.mark.asyncio
async def test_runner_propagates_worker_queues_to_dequeue(monkeypatch):
    """WORKER_QUEUES="url_render,embedding" must reach dequeue() so this
    instance only consumes those task_types — enables multi-deployment
    sharding without touching code.
    """
    from config import get_settings

    settings = get_settings()
    monkeypatch.setattr(settings, "worker_queues", "url_render, embedding ")
    monkeypatch.setattr(settings, "worker_rss_limit_bytes", 0)

    captured = {}

    async def _spy_dequeue(_session, *, task_types=None):
        captured["task_types"] = task_types
        return None  # empty queue → runner exits max_iterations quickly

    with (
        patch("worker.runner.SessionLocal") as mock_sl,
        patch("worker.runner.dequeue", new=_spy_dequeue),
    ):
        mock_sl.return_value.__aenter__.return_value = AsyncMock()

        from worker.runner import run_worker
        # max_iterations only counts tasks, not empty polls → force a
        # single poll via signalling shutdown from the spy.
        # Simpler: patch _shutdown as a time-limit.
        # Instead use a controlled approach — run for a brief time.
        import worker.runner as runner_mod
        monkeypatch.setattr(runner_mod, "_POLL_INTERVAL", 0.01)
        # The loop has no natural exit when queue is empty and
        # max_iterations isn't hit. Flip _shutdown after first dequeue.
        original = _spy_dequeue

        async def _spy_and_stop(session, *, task_types=None):
            result = await original(session, task_types=task_types)
            runner_mod._shutdown = True
            return result

        with patch("worker.runner.dequeue", new=_spy_and_stop):
            try:
                await run_worker()
            finally:
                runner_mod._shutdown = False  # reset for other tests

    assert captured["task_types"] == ["url_render", "embedding"], (
        "WORKER_QUEUES must be split, stripped, and passed to dequeue"
    )


@pytest.mark.asyncio
async def test_runner_empty_worker_queues_means_accept_all(monkeypatch):
    from config import get_settings

    settings = get_settings()
    monkeypatch.setattr(settings, "worker_queues", "")
    monkeypatch.setattr(settings, "worker_rss_limit_bytes", 0)

    captured = {}

    async def _spy_dequeue(_session, *, task_types=None):
        captured["task_types"] = task_types
        return None

    import worker.runner as runner_mod

    async def _spy_and_stop(session, *, task_types=None):
        result = await _spy_dequeue(session, task_types=task_types)
        runner_mod._shutdown = True
        return result

    with (
        patch("worker.runner.SessionLocal") as mock_sl,
        patch("worker.runner.dequeue", new=_spy_and_stop),
    ):
        mock_sl.return_value.__aenter__.return_value = AsyncMock()
        monkeypatch.setattr(runner_mod, "_POLL_INTERVAL", 0.01)
        try:
            from worker.runner import run_worker
            await run_worker()
        finally:
            runner_mod._shutdown = False

    assert captured["task_types"] is None, (
        "empty WORKER_QUEUES must pass None (no filter) to dequeue"
    )


@pytest.mark.asyncio
async def test_dequeue_with_task_types_filter():
    """Verify the queue-level filter expression includes task_type.in_()
    — this is what multi-deployment worker sharding rides on.
    """
    session = AsyncMock()
    result = MagicMock()
    result.scalar_one_or_none.return_value = None
    session.execute = AsyncMock(return_value=result)

    from worker.queue import dequeue
    await dequeue(session, task_types=["url_render"])

    # Inspect the executed UPDATE statement's WHERE clause: task_type
    # filter must be present. We match the compiled SQL text since
    # constructing the exact ColumnOperators object equality is brittle.
    called_stmt = session.execute.call_args[0][0]
    sql_text = str(called_stmt.compile(
        compile_kwargs={"literal_binds": True},
    )).lower()
    assert "task_type in" in sql_text, (
        f"dequeue must include task_type filter; got:\n{sql_text}"
    )
