"""Tests for parsers.isolation — subprocess isolation with timeout + crash recovery.

These are slow (spawn pool bootstrap ~1s) but correctness-critical: the whole
point is surviving worker death, and there's no way to test that without
actually killing workers.
"""
import asyncio
import concurrent.futures
import pytest

from parsers import isolation
from tests.test_parsers._isolation_fixtures import (
    fast_add, slow_hang, crash_worker, raise_value_error,
)


@pytest.fixture(autouse=True)
def _reset_pool():
    """Each test starts with a fresh pool and tears down at the end."""
    isolation._recycle_pool()
    yield
    isolation._recycle_pool()


@pytest.mark.asyncio
async def test_runs_in_subprocess_and_returns_result():
    result = await isolation.run_isolated(fast_add, 2, 3, timeout=30.0)
    assert result == 5


@pytest.mark.asyncio
async def test_timeout_raises_and_pool_recovers():
    """A stuck worker must raise TimeoutError, and subsequent calls must succeed."""
    with pytest.raises(asyncio.TimeoutError):
        await isolation.run_isolated(slow_hang, 30.0, timeout=0.5)

    # Pool was recycled; next call goes through a fresh worker.
    result = await isolation.run_isolated(fast_add, 1, 1, timeout=30.0)
    assert result == 2


@pytest.mark.asyncio
async def test_worker_crash_does_not_kill_parent_and_pool_recovers():
    """Hard worker exit (e.g. C extension segfault) must surface as an error,
    not bring down the parent; the next call succeeds against a recycled pool.
    """
    with pytest.raises((concurrent.futures.process.BrokenProcessPool, Exception)):
        await isolation.run_isolated(crash_worker, timeout=10.0)

    result = await isolation.run_isolated(fast_add, 4, 5, timeout=30.0)
    assert result == 9


@pytest.mark.asyncio
async def test_exception_propagates_without_killing_pool():
    """Ordinary Python exceptions must propagate as-is; pool stays healthy."""
    with pytest.raises(ValueError, match="boom"):
        await isolation.run_isolated(raise_value_error, timeout=10.0)

    # Pool not recycled — this call reuses same worker.
    result = await isolation.run_isolated(fast_add, 7, 8, timeout=30.0)
    assert result == 15
