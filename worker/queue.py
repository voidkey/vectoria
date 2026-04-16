"""PG-backed task queue using FOR UPDATE SKIP LOCKED.

No Redis, no external broker — tasks are rows in the `tasks` table. Workers
dequeue by atomically claiming a row and setting `status = 'running'`. If a
worker dies, `locked_until` expires and the row becomes eligible again.

Design notes:
  - `enqueue()` is called from the API process (fast, one INSERT).
  - `dequeue()` is called from the worker process in a poll loop.
  - `complete() / fail()` are called by the worker after task execution.
  - `reap_stale()` reclaims rows whose `locked_until` has passed (worker
    crashed or was OOM-killed without calling fail()).
"""

import uuid
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, update, text
from sqlalchemy.ext.asyncio import AsyncSession

from db.base import get_session
from db.models import Task

logger = logging.getLogger(__name__)

# How long a running task is considered "owned" before we assume the worker
# died and allow another worker to pick it up.
_LOCK_TTL = timedelta(minutes=5)


async def enqueue(
    task_type: str,
    payload: dict,
    *,
    priority: int = 0,
    max_attempts: int = 3,
) -> str:
    """Insert a new pending task. Returns the task id."""
    task_id = str(uuid.uuid4())
    async with get_session() as session:
        task = Task(
            id=task_id,
            task_type=task_type,
            payload=payload,
            status="pending",
            priority=priority,
            max_attempts=max_attempts,
        )
        session.add(task)
        await session.commit()
    logger.info("Enqueued task %s type=%s", task_id, task_type)
    return task_id


async def dequeue(session: AsyncSession) -> Task | None:
    """Atomically claim the next pending task (or a stale running task).

    Returns the Task with status already set to 'running', or None if the
    queue is empty. The caller is responsible for calling complete() or
    fail() when done.

    Uses FOR UPDATE SKIP LOCKED so multiple workers can poll concurrently
    without blocking each other.
    """
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    # Subquery: pick the best candidate.
    subq = (
        select(Task.id)
        .where(
            # Pending tasks whose backoff (if any) has elapsed, OR
            # running tasks whose lock expired (stale worker).
            (
                (Task.status == "pending")
                & ((Task.locked_until.is_(None)) | (Task.locked_until < now))
            )
            | (
                (Task.status == "running")
                & (Task.locked_until < now)
            )
        )
        .order_by(Task.priority.desc(), Task.created_at)
        .limit(1)
        .with_for_update(skip_locked=True)
        .scalar_subquery()
    )

    # Atomic claim: update the matched row in one statement.
    stmt = (
        update(Task)
        .where(Task.id == subq)
        .values(
            status="running",
            started_at=now,
            locked_until=now + _LOCK_TTL,
            attempts=Task.attempts + 1,
        )
        .returning(Task)
    )

    result = await session.execute(stmt)
    task = result.scalar_one_or_none()
    if task:
        await session.commit()
        logger.info(
            "Dequeued task %s type=%s attempt=%d/%d",
            task.id, task.task_type, task.attempts, task.max_attempts,
        )
    return task


async def complete(task_id: str) -> None:
    """Mark a task as completed."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    async with get_session() as session:
        await session.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(status="completed", finished_at=now, error="")
        )
        await session.commit()
    logger.info("Task %s completed", task_id)


async def reap_dead_tasks() -> int:
    """Reclaim stale running tasks whose locked_until has passed and attempts
    are exhausted. Returns the number of tasks reaped.

    This handles the case where a worker was OOM-killed mid-task: the row
    stays ``status='running'`` with an expired lock. If attempts >= max_attempts,
    we mark it ``dead`` so it stops blocking the queue.
    """
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    async with get_session() as session:
        result = await session.execute(
            update(Task)
            .where(
                Task.status == "running",
                Task.locked_until < now,
                Task.attempts >= Task.max_attempts,
            )
            .values(status="dead", finished_at=now, error="Worker died (lock expired, retries exhausted)")
            .returning(Task.id)
        )
        dead_ids = result.scalars().all()
        if dead_ids:
            await session.commit()
            for tid in dead_ids:
                logger.warning("Reaped dead task %s", tid)
        return len(dead_ids)


async def fail(task_id: str, error: str) -> None:
    """Mark a task as failed. If attempts < max_attempts, reset to pending
    for automatic retry; otherwise mark as 'dead'.
    """
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    async with get_session() as session:
        result = await session.execute(select(Task).where(Task.id == task_id))
        task = result.scalar_one_or_none()
        if task is None:
            return

        if task.attempts >= task.max_attempts:
            task.status = "dead"
            task.finished_at = now
            task.error = error
            logger.warning("Task %s dead after %d attempts: %s", task_id, task.attempts, error)
        else:
            # Exponential backoff: 30s, 60s, 120s, ... capped at 10 min.
            backoff = min(30 * (2 ** (task.attempts - 1)), 600)
            task.status = "pending"
            task.locked_until = now + timedelta(seconds=backoff)
            task.error = error
            logger.info(
                "Task %s failed (attempt %d/%d), retry in %ds: %s",
                task_id, task.attempts, task.max_attempts, backoff, error,
            )
        await session.commit()
