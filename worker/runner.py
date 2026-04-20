"""Worker process: polls the PG task queue and dispatches handlers.

Run with:  uv run python -m worker

Runtime is configured by env (read through the pydantic Settings):
  WORKER_QUEUES           comma-separated task_type filter (empty = all)
  WORKER_CONCURRENCY      reserved for W5 (current runner is serial)
  WORKER_RSS_LIMIT_BYTES  exit when RSS exceeds this after a task (0=off)

Why the config-via-env: all worker pods share one image. Different K8s
Deployments flip which queues they subscribe to by setting WORKER_QUEUES
alone — no code path for "browser worker" vs "general worker".
"""

import asyncio
import logging
import signal
import time
from pathlib import Path

from config import get_settings
from db.base import SessionLocal
from infra.metrics import (
    TASK_DURATION_SECONDS,
    TASK_TOTAL,
    WORKER_RSS_BYTES,
    WORKER_RSS_KILLS,
    WORKER_TASKS_INFLIGHT,
)
from infra.proc import rss_bytes
from worker.handlers import dispatch
from worker.queue import (
    complete, dequeue, fail, reap_dead_tasks, sample_queue_metrics,
)

logger = logging.getLogger(__name__)

_POLL_INTERVAL = 1.0  # seconds between empty-queue polls
_HEARTBEAT_PATH = Path("/tmp/worker-heartbeat")
_DEAD_TASK_REAP_INTERVAL = 300  # seconds between dead-task reap cycles
# Queue-depth gauges updated every N seconds — cheaper than per-poll, and
# Prometheus scrape cadence is 15 s+ anyway so finer resolution is wasted.
_QUEUE_METRICS_INTERVAL = 10.0
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


def _parse_queues(raw: str) -> list[str] | None:
    """Parse WORKER_QUEUES env value. Empty/whitespace ⇒ None (accept all)."""
    items = [q.strip() for q in raw.split(",") if q.strip()]
    return items or None


def _sample_rss_and_maybe_exit(rss_limit: int) -> bool:
    """Update WORKER_RSS_BYTES and return True iff this worker should exit.

    Reads VmRSS once (O(1) syscall); on platforms without procfs (macOS
    dev) ``rss_bytes()`` returns 0 so this always returns False there.
    """
    rss = rss_bytes()
    if not rss:
        return False
    WORKER_RSS_BYTES.set(rss)
    if rss_limit > 0 and rss > rss_limit:
        logger.warning(
            "Worker RSS %d B exceeds limit %d B — exiting for restart",
            rss, rss_limit,
        )
        WORKER_RSS_KILLS.inc()
        return True
    return False


async def run_worker(*, max_iterations: int | None = None) -> int:
    """Main worker loop. Returns the number of tasks processed.

    `max_iterations` is for testing — in production pass None for infinite.
    """
    cfg = get_settings()
    rss_limit = cfg.worker_rss_limit_bytes
    task_types = _parse_queues(cfg.worker_queues)
    if cfg.worker_concurrency != 1:
        logger.warning(
            "WORKER_CONCURRENCY=%d ignored: current runner is serial; "
            "concurrent execution lands in a later milestone.",
            cfg.worker_concurrency,
        )

    processed = 0
    last_reap = time.monotonic()
    last_queue_sample = 0.0  # force immediate first sample
    logger.info(
        "Worker started queues=%s rss_limit=%d",
        task_types or "*", rss_limit,
    )
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

        # Queue-depth metrics: sampled on a coarser clock than the poll
        # loop so we don't hammer PG with one aggregate SELECT per second.
        if now - last_queue_sample > _QUEUE_METRICS_INTERVAL:
            try:
                await sample_queue_metrics()
            except Exception:  # noqa: BLE001 — telemetry must never crash the loop
                logger.exception("queue metric sampling failed")
            last_queue_sample = now

        async with SessionLocal() as session:
            task = await dequeue(session, task_types=task_types)

        if task is None:
            _touch_heartbeat()
            # Gauge update on idle loops too, so RSS graphs don't flatline
            # when the queue is empty but the pod is still drifting.
            _sample_rss_and_maybe_exit(rss_limit=0)
            await asyncio.sleep(_POLL_INTERVAL)
            continue

        logger.info("Processing task %s type=%s", task.id, task.task_type)
        t_start = time.monotonic()
        status = "completed"
        WORKER_TASKS_INFLIGHT.inc()
        try:
            await dispatch(task.task_type, task.payload)
            await complete(task.id)
        except Exception as e:
            status = "failed"
            logger.exception("Task %s failed: %s", task.id, e)
            await fail(task.id, str(e))
        finally:
            WORKER_TASKS_INFLIGHT.dec()
            elapsed = time.monotonic() - t_start
            TASK_DURATION_SECONDS.labels(
                task_type=task.task_type, status=status,
            ).observe(elapsed)
            TASK_TOTAL.labels(task_type=task.task_type, status=status).inc()

        _touch_heartbeat()
        processed += 1

        # Self-kill check AFTER task completion so we never abort mid-task.
        # If RSS breached during the task and we exit here, K8s restarts us
        # and the next task gets a fresh heap. Much cleaner than waiting
        # for the OOM-killer to end the pod unpredictably.
        if _sample_rss_and_maybe_exit(rss_limit):
            break

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
