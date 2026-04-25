"""Tests for sample_queue_metrics — specifically the dead-task gauge shape.

The gauge is what drives the DLQ alert. We want each dead task to produce
its own series with enough label context (task_id, source) that operators
can tell what's dead without opening psql.
"""
import pytest
from unittest.mock import patch, AsyncMock, MagicMock


def _dead_series() -> dict:
    """Snapshot QUEUE_DEAD_TASKS samples as {labels_dict_tuple: value}."""
    from infra.metrics import QUEUE_DEAD_TASKS
    out = {}
    for metric in QUEUE_DEAD_TASKS.collect():
        for sample in metric.samples:
            out[tuple(sorted(sample.labels.items()))] = sample.value
    return out


def _reset_dead_gauge():
    from infra.metrics import QUEUE_DEAD_TASKS
    QUEUE_DEAD_TASKS.clear()


def _mock_session(dead_rows):
    """Build an AsyncMock session whose execute() returns
    (pending_result, dead_result) in order.
    """
    session = AsyncMock()
    pending_result = MagicMock()
    pending_result.all.return_value = []
    dead_result = MagicMock()
    dead_result.all.return_value = dead_rows
    session.execute = AsyncMock(side_effect=[pending_result, dead_result])
    return session


@pytest.mark.asyncio
async def test_emits_one_series_per_dead_task():
    """Two dead tasks → two series with distinct task_id labels."""
    _reset_dead_gauge()
    session = _mock_session([
        ("task-a", "parse_document", {"source": "https://example.com/a"}),
        ("task-b", "index_document", {"doc_id": "d1"}),
    ])

    with patch("worker.queue.get_session") as mock_gs:
        mock_gs.return_value.__aenter__.return_value = session
        from worker.queue import sample_queue_metrics
        await sample_queue_metrics()

    series = _dead_series()
    label_sets = [dict(k) for k in series.keys()]
    assert len(label_sets) == 2
    assert {"task_type": "parse_document", "task_id": "task-a",
            "source": "https://example.com/a"} in label_sets
    assert {"task_type": "index_document", "task_id": "task-b",
            "source": ""} in label_sets
    # Each series carries value 1 — the "count" is how many series exist.
    assert all(v == 1 for v in series.values())


@pytest.mark.asyncio
async def test_source_prefers_url_then_filename_then_storage_key():
    """source label derivation: payload.source > payload.filename > payload.storage_key > ''."""
    _reset_dead_gauge()
    session = _mock_session([
        ("t1", "parse_document", {"source": "https://a", "filename": "fn", "storage_key": "sk"}),
        ("t2", "parse_document", {"filename": "fn.pdf", "storage_key": "sk"}),
        ("t3", "parse_document", {"storage_key": "uploads/abc.pdf"}),
        ("t4", "parse_document", {}),
    ])

    with patch("worker.queue.get_session") as mock_gs:
        mock_gs.return_value.__aenter__.return_value = session
        from worker.queue import sample_queue_metrics
        await sample_queue_metrics()

    by_task = {dict(k)["task_id"]: dict(k)["source"] for k in _dead_series().keys()}
    assert by_task["t1"] == "https://a"
    assert by_task["t2"] == "fn.pdf"
    assert by_task["t3"] == "uploads/abc.pdf"
    assert by_task["t4"] == ""


@pytest.mark.asyncio
async def test_source_truncated_when_very_long():
    """Pathological long URLs must not blow up Prometheus label storage."""
    _reset_dead_gauge()
    long_url = "https://example.com/" + ("x" * 500)
    session = _mock_session([
        ("t1", "parse_document", {"source": long_url}),
    ])

    with patch("worker.queue.get_session") as mock_gs:
        mock_gs.return_value.__aenter__.return_value = session
        from worker.queue import sample_queue_metrics
        await sample_queue_metrics()

    source = next(iter(_dead_series().keys()))
    source_val = dict(source)["source"]
    # Bounded — exact cap is an internal choice, but operators agreed
    # "short enough to fit in a wecom hint line" which is well under 200.
    assert len(source_val) <= 200
    assert source_val.startswith("https://example.com/")
    # Truncation marker present so operators know there's more they can't see.
    assert source_val.endswith("…")


@pytest.mark.asyncio
async def test_stale_series_cleared_when_dead_task_gone():
    """After operator deletes the dead row, its series must disappear
    on the next sample — otherwise the alert never resolves.
    """
    _reset_dead_gauge()

    # First sample: one dead task present.
    session1 = _mock_session([
        ("gone", "parse_document", {"source": "https://x"}),
    ])
    with patch("worker.queue.get_session") as mock_gs:
        mock_gs.return_value.__aenter__.return_value = session1
        from worker.queue import sample_queue_metrics
        await sample_queue_metrics()
    assert any(dict(k)["task_id"] == "gone" for k in _dead_series().keys())

    # Operator deletes that task. Second sample: table empty.
    session2 = _mock_session([])
    with patch("worker.queue.get_session") as mock_gs:
        mock_gs.return_value.__aenter__.return_value = session2
        await sample_queue_metrics()

    assert _dead_series() == {}, "stale series still present after dead task removed"


@pytest.mark.asyncio
async def test_sample_clears_stale_pending_gauges_for_vanished_task_types():
    """Regression: when a task_type previously had pending tasks but
    now has zero, the GROUP BY query doesn't return a row for it,
    the for-loop skips it, and the old value lingers forever — driving
    the VectoriaQueueTaskAging alert into a permanent false fire.

    The fix calls .clear() on QUEUE_DEPTH and QUEUE_OLDEST_AGE_SECONDS
    before the for loop, so vanished types are dropped from the
    registry and Prometheus stops emitting their series.
    """
    from infra.metrics import QUEUE_DEPTH, QUEUE_OLDEST_AGE_SECONDS

    # Pre-set both gauges as if a previous sampler call saw 5 pending
    # tasks of type "ghost" with oldest age 999 seconds.
    QUEUE_DEPTH.labels(task_type="ghost").set(5)
    QUEUE_OLDEST_AGE_SECONDS.labels(task_type="ghost").set(999.0)

    # Sanity check: the ghost values are present before sampling.
    def _depth_labels() -> set[str]:
        return {
            sample.labels.get("task_type")
            for metric in QUEUE_DEPTH.collect()
            for sample in metric.samples
        }

    def _age_labels() -> set[str]:
        return {
            sample.labels.get("task_type")
            for metric in QUEUE_OLDEST_AGE_SECONDS.collect()
            for sample in metric.samples
        }

    assert "ghost" in _depth_labels()
    assert "ghost" in _age_labels()

    # Reset the dead gauge so this test doesn't interact with prior
    # tests' leftover dead series.
    _reset_dead_gauge()

    # _mock_session() returns empty pending_result + empty dead_result.
    # The GROUP BY query yields zero rows; without .clear(), "ghost"
    # series persists. With .clear(), it's dropped.
    session = _mock_session([])

    with patch("worker.queue.get_session") as mock_gs:
        mock_gs.return_value.__aenter__.return_value = session
        from worker.queue import sample_queue_metrics
        await sample_queue_metrics()

    assert "ghost" not in _depth_labels(), (
        "QUEUE_DEPTH still has stale ghost series after sample"
    )
    assert "ghost" not in _age_labels(), (
        "QUEUE_OLDEST_AGE_SECONDS still has stale ghost series after sample"
    )
