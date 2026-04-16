"""Worker process: polls the PG task queue and dispatches handlers.

Run with:  uv run python -m worker
"""

import asyncio
import logging
import signal

from db.base import SessionLocal
from worker.handlers import dispatch
from worker.queue import dequeue, complete, fail

logger = logging.getLogger(__name__)

_POLL_INTERVAL = 1.0  # seconds between empty-queue polls
_shutdown = False


def _handle_signal(sig, _frame):
    global _shutdown  # noqa: PLW0603
    logger.info("Received signal %s, shutting down after current task...", sig)
    _shutdown = True


async def run_worker(*, max_iterations: int | None = None) -> int:
    """Main worker loop. Returns the number of tasks processed.

    `max_iterations` is for testing — in production pass None for infinite.
    """
    processed = 0
    logger.info("Worker started, polling for tasks...")

    while not _shutdown:
        if max_iterations is not None and processed >= max_iterations:
            break

        async with SessionLocal() as session:
            task = await dequeue(session)

        if task is None:
            await asyncio.sleep(_POLL_INTERVAL)
            continue

        try:
            await dispatch(task.task_type, task.payload)
            await complete(task.id)
        except Exception as e:
            logger.exception("Task %s failed: %s", task.id, e)
            await fail(task.id, str(e))

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
