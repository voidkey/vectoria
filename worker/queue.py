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

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from db.base import get_session
from db.models import Task
from infra.metrics import QUEUE_DEAD_TASKS, QUEUE_DEPTH, QUEUE_OLDEST_AGE_SECONDS

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
    """Insert a new pending task. Returns the task id.

    Prefer ``enqueue_in_session`` when the enqueue must be atomic with
    another insert in the caller's transaction (e.g. Document + its
    parse task) — otherwise a DB blip between the two commits can leave
    an orphan row that no worker picks up.
    """
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


def enqueue_in_session(
    session: AsyncSession,
    task_type: str,
    payload: dict,
    *,
    priority: int = 0,
    max_attempts: int = 3,
) -> str:
    """Stage a task row in the caller's session without committing.

    Intended for atomicity with a sibling insert — the caller commits
    once, and either both rows land or neither does. A DB blip between
    two separate commits (``Document.commit()`` then ``enqueue()``) would
    otherwise leave a ``queued`` Document with no worker task, and the
    per-hash dedup lookup would then match that orphan on every retry.

    Does not log (no task_id yet committed); the runner logs on dequeue.
    """
    task_id = str(uuid.uuid4())
    task = Task(
        id=task_id,
        task_type=task_type,
        payload=payload,
        status="pending",
        priority=priority,
        max_attempts=max_attempts,
    )
    session.add(task)
    return task_id


async def dequeue(
    session: AsyncSession,
    *,
    task_types: list[str] | None = None,
) -> Task | None:
    """Atomically claim the next pending task (or a stale running task).

    Returns the Task with status already set to 'running', or None if the
    queue is empty. The caller is responsible for calling complete() or
    fail() when done.

    Uses FOR UPDATE SKIP LOCKED so multiple workers can poll concurrently
    without blocking each other.

    ``task_types`` filters to only specific task_type values. Drives
    multi-deployment sharding: a ``url_render`` worker passes
    ``["url_render"]`` and never claims parse tasks even though both sit
    in the same table. ``None`` (default) accepts all task types.
    """
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    # Eligibility: pending-and-ready OR running-and-stale
    eligibility_expr = (
        (Task.status == "pending")
        & ((Task.locked_until.is_(None)) | (Task.locked_until < now))
    ) | (
        (Task.status == "running")
        & (Task.locked_until < now)
    )
    if task_types:
        eligibility_expr = eligibility_expr & Task.task_type.in_(task_types)

    # Subquery: pick the best candidate.
    subq = (
        select(Task.id)
        .where(eligibility_expr)
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


# Cap for the ``source`` label on the dead-task gauge. Keeps one pathological
# long URL / filename from bloating Prometheus label storage, while still
# leaving enough characters to identify the target at a glance.
_SOURCE_LABEL_MAX = 160


def _dead_task_source(payload: dict) -> str:
    """Pick a human-meaningful pointer from a dead task's payload.

    Priority mirrors what parse_document records in ``documents.source``:
    an external URL if we have one, else the original filename, else the
    object-store key. Empty string when none apply — the task_id label
    is always sufficient to find the row.
    """
    if not isinstance(payload, dict):
        return ""
    for key in ("source", "filename", "storage_key"):
        val = payload.get(key)
        if val:
            s = str(val)
            if len(s) > _SOURCE_LABEL_MAX:
                s = s[: _SOURCE_LABEL_MAX - 1] + "…"
            return s
    return ""


async def sample_queue_metrics() -> None:
    """Update ``QUEUE_DEPTH``, ``QUEUE_OLDEST_AGE_SECONDS``, and
    ``QUEUE_DEAD_TASKS`` gauges.

    One SELECT per category per call per worker. Cheap enough to run every
    few polling iterations in the worker loop — not on every 1 s tick.
    Operators rely on these for queue-backlog alerts; leaving them
    unobserved is the single biggest gap in the W1 observability story.
    """
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    async with get_session() as session:
        result = await session.execute(
            select(
                Task.task_type,
                func.count(Task.id),
                func.min(Task.created_at),
            )
            .where(Task.status == "pending")
            .group_by(Task.task_type)
        )
        for task_type, count, oldest in result.all():
            QUEUE_DEPTH.labels(task_type=task_type).set(count)
            age = (now - oldest).total_seconds() if oldest else 0
            QUEUE_OLDEST_AGE_SECONDS.labels(task_type=task_type).set(max(age, 0))

        # Per-task rows (not aggregated) so each dead task becomes its own
        # Prometheus series carrying task_id + source labels. That's what
        # lets the DLQ alert annotation show the actual URL / filename,
        # so operators can decide requeue-vs-delete without opening psql.
        # ``.clear()`` before re-emitting drops series for tasks that were
        # deleted since the last sample — otherwise the alert never resolves.
        dead_result = await session.execute(
            select(Task.id, Task.task_type, Task.payload)
            .where(Task.status == "dead")
        )
        QUEUE_DEAD_TASKS.clear()
        for task_id, task_type, payload in dead_result.all():
            QUEUE_DEAD_TASKS.labels(
                task_type=task_type,
                task_id=task_id,
                source=_dead_task_source(payload),
            ).set(1)


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
