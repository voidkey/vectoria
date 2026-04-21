"""Run parser code in a subprocess pool with per-call timeout.

Why: heavy parsers (docling, markitdown) link C extensions that can segfault
on malformed input or exhaust memory, and in-process execution takes down
the whole uvicorn worker with it. This module wraps picklable callables in
a ProcessPoolExecutor so crashes / timeouts kill only a child process.

Pool tiers
----------
Two independent pools — ``fast`` and ``heavy`` — so a stuck docling or
mineru call (``heavy``) cannot starve the sub-second native Office parses
(``fast``). W5-2 split: before, a single 2-slot pool was shared across
every isolated parse, and one hung docling could block two native parses
for the full ``parser_timeout``. Tier is chosen by the calling parser via
``run_isolated(..., tier="fast"|"heavy")``. Defaults to ``heavy`` so
callers that don't pass the kwarg keep the old behaviour.

Stuck-worker kill
-----------------
``_recycle_pool`` sends SIGKILL (``proc.kill()``) rather than SIGTERM
(``proc.terminate()``). A parser blocked inside a C extension won't run
Python-level signal handlers, so SIGTERM is effectively ignored; SIGKILL
is delivered by the kernel and unblocks the pool for real.

The pool uses `spawn` (fork-safety + clean imports) and recycles workers
every N tasks to bound per-worker memory growth.
"""

import asyncio
import concurrent.futures
import logging
import multiprocessing
from threading import Lock
from typing import Any, Callable, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")

_MAX_WORKERS_FAST = 2
_MAX_WORKERS_HEAVY = 2
_MAX_TASKS_PER_CHILD = 10

# Tier name → slot count. Callers pass ``tier=`` into ``run_isolated``;
# unknown tiers fall through to ``heavy`` so a typo doesn't spawn an
# unbounded set of pools.
_TIER_CONFIG: dict[str, int] = {
    "fast":  _MAX_WORKERS_FAST,
    "heavy": _MAX_WORKERS_HEAVY,
}

_pools: dict[str, concurrent.futures.ProcessPoolExecutor] = {}
_pool_lock = Lock()


def _resolve_tier(tier: str) -> str:
    return tier if tier in _TIER_CONFIG else "heavy"


def _get_pool(tier: str = "heavy") -> concurrent.futures.ProcessPoolExecutor:
    tier = _resolve_tier(tier)
    existing = _pools.get(tier)
    if existing is not None:
        return existing
    with _pool_lock:
        existing = _pools.get(tier)
        if existing is None:
            ctx = multiprocessing.get_context("spawn")
            existing = concurrent.futures.ProcessPoolExecutor(
                max_workers=_TIER_CONFIG[tier],
                mp_context=ctx,
                max_tasks_per_child=_MAX_TASKS_PER_CHILD,
            )
            _pools[tier] = existing
    return existing


def _kill_pool_children(pool: concurrent.futures.ProcessPoolExecutor) -> None:
    """SIGKILL all children of a pool, best-effort.

    Reaches into ``_processes`` — a CPython implementation detail but
    the only way to actually interrupt a worker blocked in a C extension.
    Wrapped defensively so a future CPython refactor at worst degrades
    to "timeout without hard-kill" rather than crashing the caller.
    """
    try:
        procs = list(pool._processes.values())  # type: ignore[attr-defined]
    except Exception:
        logger.warning(
            "ProcessPoolExecutor internal changed — cannot SIGKILL stuck "
            "workers. Upgrade isolation.py for this Python version."
        )
        return
    for proc in procs:
        try:
            proc.kill()  # SIGKILL — SIGTERM is ignored by C-blocked threads
        except Exception:
            logger.debug("failed to kill pid=%s", getattr(proc, "pid", "?"),
                         exc_info=True)


def _recycle_pool(tier: str | None = None) -> None:
    """Terminate workers in the given tier (or all tiers) and reset.

    ProcessPoolExecutor.shutdown(wait=False) returns immediately but leaves
    existing workers running to completion — useless if a worker is stuck
    in a C-extension call. We reach into ``_processes`` and SIGKILL
    directly so the pool actually unblocks.

    ``tier=None`` recycles every pool (used by tests + shutdown paths).
    """
    with _pool_lock:
        if tier is None:
            targets = _pools
            _pools_snapshot = dict(targets)
            _pools.clear()
        else:
            resolved = _resolve_tier(tier)
            old = _pools.pop(resolved, None)
            _pools_snapshot = {resolved: old} if old is not None else {}

    for name, old in _pools_snapshot.items():
        if old is None:
            continue
        _kill_pool_children(old)
        try:
            old.shutdown(wait=False, cancel_futures=True)
        except Exception:
            logger.exception("error shutting down pool tier=%s", name)


async def run_isolated(
    func: Callable[..., T], *args: Any, timeout: float = 60.0,
    tier: str = "heavy",
) -> T:
    """Run `func(*args)` in a subprocess worker, raising on timeout / crash.

    `func` must be importable (module-level, not a closure/lambda) and all
    args must be picklable. On timeout the whole pool is recycled so any
    stuck worker is actually killed; on BrokenProcessPool we do the same
    so subsequent calls don't keep hitting a dead pool.

    ``tier`` picks which pool serves this call. ``"fast"`` for native
    Office parsers (docx/pptx/xlsx) — they never hang in C extensions and
    deserve isolation from slow callers. ``"heavy"`` for docling / mineru
    / markitdown where a single call can saturate RSS and benefit from
    a 2-slot cap.
    """
    loop = asyncio.get_running_loop()
    pool = _get_pool(tier)
    fut = loop.run_in_executor(pool, func, *args)
    try:
        return await asyncio.wait_for(fut, timeout=timeout)
    except asyncio.TimeoutError:
        logger.warning(
            "Isolated call %s timed out after %.1fs; recycling tier=%s",
            getattr(func, "__name__", repr(func)), timeout, tier,
        )
        _recycle_pool(tier)
        raise
    except concurrent.futures.process.BrokenProcessPool:
        logger.exception(
            "Worker process died during %s; recycling tier=%s",
            getattr(func, "__name__", repr(func)), tier,
        )
        _recycle_pool(tier)
        raise
