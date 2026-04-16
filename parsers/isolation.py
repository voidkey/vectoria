"""Run parser code in a subprocess pool with per-call timeout.

Why: heavy parsers (docling, markitdown) link C extensions that can segfault
on malformed input or exhaust memory, and in-process execution takes down
the whole uvicorn worker with it. This module wraps picklable callables in
a ProcessPoolExecutor so crashes / timeouts kill only a child process.

The pool uses `spawn` (fork-safety + clean imports) and recycles workers
every N tasks to bound per-worker memory growth. On timeout we terminate
workers directly — shutdown() alone doesn't kill a stuck `convert()` call.
"""

import asyncio
import concurrent.futures
import logging
import multiprocessing
from threading import Lock
from typing import Any, Callable, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")

_MAX_WORKERS = 2
_MAX_TASKS_PER_CHILD = 10

_pool: concurrent.futures.ProcessPoolExecutor | None = None
_pool_lock = Lock()


def _get_pool() -> concurrent.futures.ProcessPoolExecutor:
    global _pool  # noqa: PLW0603
    if _pool is not None:
        return _pool
    with _pool_lock:
        if _pool is None:
            ctx = multiprocessing.get_context("spawn")
            _pool = concurrent.futures.ProcessPoolExecutor(
                max_workers=_MAX_WORKERS,
                mp_context=ctx,
                max_tasks_per_child=_MAX_TASKS_PER_CHILD,
            )
    return _pool


def _recycle_pool() -> None:
    """Terminate all workers and reset the pool.

    ProcessPoolExecutor.shutdown(wait=False) returns immediately but leaves
    existing workers running to completion — useless if a worker is stuck in
    a C-extension call. We reach into `_processes` and terminate() directly.
    """
    global _pool  # noqa: PLW0603
    with _pool_lock:
        old = _pool
        _pool = None
    if old is None:
        return
    try:
        for proc in list(old._processes.values()):  # type: ignore[attr-defined]
            try:
                proc.terminate()
            except Exception:
                pass
    except Exception:
        pass
    old.shutdown(wait=False, cancel_futures=True)


async def run_isolated(
    func: Callable[..., T], *args: Any, timeout: float = 60.0,
) -> T:
    """Run `func(*args)` in a subprocess worker, raising on timeout / crash.

    `func` must be importable (module-level, not a closure/lambda) and all
    args must be picklable. On timeout the whole pool is recycled so any
    stuck worker is actually killed; on BrokenProcessPool we do the same
    so subsequent calls don't keep hitting a dead pool.
    """
    loop = asyncio.get_running_loop()
    pool = _get_pool()
    fut = loop.run_in_executor(pool, func, *args)
    try:
        return await asyncio.wait_for(fut, timeout=timeout)
    except asyncio.TimeoutError:
        logger.warning(
            "Isolated call %s timed out after %.1fs; recycling pool",
            getattr(func, "__name__", repr(func)), timeout,
        )
        _recycle_pool()
        raise
    except concurrent.futures.process.BrokenProcessPool:
        logger.exception("Worker process died during %s; recycling pool",
                         getattr(func, "__name__", repr(func)))
        _recycle_pool()
        raise
