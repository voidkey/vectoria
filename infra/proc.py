"""Process inspection helpers.

Only Linux is supported with real numbers — workers run in Linux
containers in production. On other platforms (dev machines) the helpers
return 0 so callers naturally skip RSS-based gating.
"""
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

_STATUS_PATH = Path("/proc/self/status")


def rss_bytes() -> int:
    """Current resident set size in bytes, or 0 if unavailable.

    Reads ``VmRSS`` from ``/proc/self/status`` on Linux. This returns the
    *current* RSS (unlike ``resource.getrusage().ru_maxrss`` which is the
    peak and never decreases).

    Returns 0 on:
      - non-Linux platforms (no procfs)
      - read errors (e.g. /proc unmounted in a test container)
      - parse failures (kernel changed format — unlikely)

    Callers that gate on this (``worker_rss_limit_bytes``) must treat 0
    as "don't act". Returning a non-zero sentinel would misleadingly
    trigger the limit check on dev machines.
    """
    try:
        text = _STATUS_PATH.read_text()
    except OSError:
        return 0
    for line in text.splitlines():
        if line.startswith("VmRSS:"):
            try:
                # Format:  "VmRSS:   123456 kB"
                return int(line.split()[1]) * 1024
            except (IndexError, ValueError):
                logger.warning("Unexpected VmRSS line format: %r", line)
                return 0
    return 0
