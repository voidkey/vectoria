"""Internal diagnostics — gated by the standard auth deps.

Intent is Stage-2 observability for the OOM investigation: show whether RSS
growth is recoverable fragmentation (pseudo-leak cleared by ``malloc_trim``)
or a real leak (retained live objects that survive ``gc.collect()``).
"""
import ctypes
import gc
import os

from fastapi import APIRouter

router = APIRouter(prefix="/debug")


def _read_proc_status() -> dict[str, str]:
    fields = ("VmRSS", "VmSize", "VmPeak", "VmHWM", "RssAnon", "RssFile", "RssShmem")
    out: dict[str, str] = {}
    try:
        with open("/proc/self/status") as f:
            for line in f:
                key, _, rest = line.partition(":")
                if key in fields:
                    out[key] = rest.strip()
    except OSError as e:
        out["error"] = str(e)
    return out


def _malloc_trim() -> int | str:
    try:
        libc = ctypes.CDLL("libc.so.6")
        return int(libc.malloc_trim(0))
    except OSError as e:
        return f"libc.so.6 unavailable: {e}"


@router.get("/mem")
async def mem():
    """Return a memory snapshot, then run ``gc.collect()`` + ``malloc_trim``
    and return a second snapshot. Diffing the two tells you how much of the
    current RSS is recoverable vs pinned by live objects.
    """
    before = _read_proc_status()
    gc_count_before = gc.get_count()

    collected = gc.collect()
    trimmed = _malloc_trim()

    after = _read_proc_status()
    return {
        "pid": os.getpid(),
        "before": before,
        "after_gc_and_trim": after,
        "gc": {
            "count_before_collect": list(gc_count_before),
            "count_after_collect": list(gc.get_count()),
            "collected_objects": collected,
            "total_tracked_objects": len(gc.get_objects()),
        },
        "malloc_trim_returned": trimmed,
    }
