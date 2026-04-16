"""Top-level helpers for test_isolation. Must be importable (not local closures)
so ProcessPoolExecutor workers can pickle them.
"""
import os
import time


def fast_add(a: int, b: int) -> int:
    return a + b


def slow_hang(seconds: float) -> str:
    time.sleep(seconds)
    return "done"


def crash_worker() -> None:
    # Simulate a hard crash (segfault-equivalent): worker process exits with
    # non-zero status before returning a result.
    os._exit(137)


def raise_value_error() -> None:
    raise ValueError("boom")
