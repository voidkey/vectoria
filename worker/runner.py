"""Worker process: polls the PG task queue and dispatches handlers.

Run with:  uv run python -m worker
"""

import asyncio
import logging
import signal
import time
from pathlib import Path

from db.base import SessionLocal
from worker.handlers import dispatch
from worker.queue import dequeue, complete, fail, reap_dead_tasks

logger = logging.getLogger(__name__)

_POLL_INTERVAL = 1.0  # seconds between empty-queue polls
_HEARTBEAT_PATH = Path("/tmp/worker-heartbeat")
_DEAD_TASK_REAP_INTERVAL = 300  # seconds between dead-task reap cycles
_shutdown = False


def _handle_signal(sig, _frame):
    global _shutdown  # noqa: PLW0603
    logger.info("Received signal %s, shutting down after current task...", sig)
    _shutdown = True


def _touch_heartbeat() -> None:
    """Write current timestamp to heartbeat file for external liveness checks."""
    try:
        _HEARTBEAT_PATH.write_text(str(int(time.time())))
    except OSError:
        pass


async def run_worker(*, max_iterations: int | None = None) -> int:
    """Main worker loop. Returns the number of tasks processed.

    `max_iterations` is for testing — in production pass None for infinite.
    """
    processed = 0
    last_reap = time.monotonic()
    logger.info("Worker started, polling for tasks...")
    _touch_heartbeat()

    while not _shutdown:
        if max_iterations is not None and processed >= max_iterations:
            break

        # Periodically reap dead tasks (mark exhausted-retry tasks as 'dead')
        now = time.monotonic()
        if now - last_reap > _DEAD_TASK_REAP_INTERVAL:
            reaped = await reap_dead_tasks()
            if reaped:
                logger.info("Reaped %d stale tasks", reaped)
            last_reap = now

        async with SessionLocal() as session:
            task = await dequeue(session)

        if task is None:
            _touch_heartbeat()
            await asyncio.sleep(_POLL_INTERVAL)
            continue

        logger.info("Processing task %s type=%s", task.id, task.task_type)
        try:
            await dispatch(task.task_type, task.payload)
            await complete(task.id)
        except Exception as e:
            logger.exception("Task %s failed: %s", task.id, e)
            await fail(task.id, str(e))

        _touch_heartbeat()
        processed += 1

    logger.info("Worker stopped after processing %d tasks", processed)
    return processed


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)
    asyncio.run(run_worker())
