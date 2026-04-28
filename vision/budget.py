"""Vision API spend tracker + daily budget guardrail.

Two responsibilities, one module:

1. **Observability**: every call increments
   ``vectoria_vision_cost_usd_total{purpose}`` by an estimated cost
   so Grafana can chart spend trends and alerts can fire on
   sustained burn-rate. The estimate is flat per call (controlled
   by ``settings.vision_cost_per_call_usd``); real $ depends on
   tokens, but this counter is monotonic and good enough for trend.

2. **Guardrail**: ``CostTracker.over_budget()`` returns True when
   today's accumulated spend is past
   ``settings.vision_daily_budget_usd``. ``VisionNativeParser``
   checks this in ``is_available()`` so registry falls back to
   ocr-native (rapidocr, free + local) once the cap is hit.

State is in-memory and per-process — a multi-worker host gets N×
the configured ceiling in aggregate. That's a conscious trade:
exact accounting would need Redis (added complexity, network round
trip on hot path), and this is a *soft* guardrail anyway. Tune the
config value with the worker count in mind, or rely on the metric
+ alertmanager for hard ceilings.

Day boundary is UTC; counter naturally rolls when the date flips.
"""
from __future__ import annotations

import threading
from datetime import datetime, timezone

from config import get_settings
from infra.metrics import VISION_COST_USD_TOTAL


class CostTracker:
    """In-memory tracker for vision API spend per UTC day."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._date = datetime.now(timezone.utc).date()
        self._spent_today_usd = 0.0

    def record(self, *, purpose: str) -> None:
        """Record a vision call. ``purpose`` is the metric label —
        bounded to {describe, parse} to keep cardinality safe.
        Cost is taken from settings (rough flat estimate).
        """
        cost = float(get_settings().vision_cost_per_call_usd or 0.0)
        VISION_COST_USD_TOTAL.labels(purpose=purpose).inc(cost)
        with self._lock:
            today = datetime.now(timezone.utc).date()
            if today != self._date:
                # Day rolled: reset window. The Prometheus counter
                # keeps climbing — that's fine, it's monotonic for
                # rate() math; we just reset the in-memory window.
                self._date = today
                self._spent_today_usd = 0.0
            self._spent_today_usd += cost

    def spent_today_usd(self) -> float:
        """Today's accumulated spend (self-resetting on UTC day flip)."""
        with self._lock:
            today = datetime.now(timezone.utc).date()
            if today != self._date:
                self._date = today
                self._spent_today_usd = 0.0
            return self._spent_today_usd

    def over_budget(self) -> bool:
        """True if today's spend has crossed
        ``settings.vision_daily_budget_usd``. 0 = no cap (always
        False).
        """
        cap = float(get_settings().vision_daily_budget_usd or 0.0)
        if cap <= 0.0:
            return False
        return self.spent_today_usd() >= cap


_tracker: CostTracker | None = None
_tracker_lock = threading.Lock()


def get_cost_tracker() -> CostTracker:
    """Module-level singleton so all call sites share a window."""
    global _tracker  # noqa: PLW0603
    if _tracker is not None:
        return _tracker
    with _tracker_lock:
        if _tracker is None:
            _tracker = CostTracker()
    return _tracker


def _reset_for_tests() -> None:
    """Test helper — wipes the singleton so tests get a clean
    slate. Don't call from production code.
    """
    global _tracker  # noqa: PLW0603
    with _tracker_lock:
        _tracker = None
