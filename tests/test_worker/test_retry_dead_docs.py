"""Auto-retry of parse-failed docs.

Guards the eligibility filter — the criteria are intentionally
narrow because each retry costs a worker slot, and re-enqueueing
the wrong things (empty_content, currently-running tasks) creates
duplicates / churn. Tests verify each gate in isolation plus the
end-to-end re-enqueue + status reset on a single eligible doc.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _doc(doc_id="d1", *, status="failed", error_type="parse_error",
         created_at=None, kb_id="k1", source="x.pdf",
         storage_key="upload/k1/d1/x.pdf", title="x.pdf",
         parse_engine="mineru"):
    """Build a Document-like mock matching only the fields the
    retry script reads. Using MagicMock(spec=Document) would also
    work but trips on SQLAlchemy attribute machinery for unset
    fields; plain MagicMock keeps the test focused on the contract.
    """
    d = MagicMock()
    d.id = doc_id; d.status = status; d.error_type = error_type
    d.created_at = created_at or datetime.now(timezone.utc).replace(tzinfo=None)
    d.kb_id = kb_id; d.source = source; d.storage_key = storage_key
    d.title = title; d.parse_engine = parse_engine
    return d


@pytest.mark.asyncio
async def test_dry_run_does_not_enqueue_or_update():
    """--dry-run is the safe-by-default mode for ops to preview
    impact. Must not touch the queue or the docs table — only log."""
    enqueue_calls: list[tuple] = []
    update_calls: list[tuple] = []

    async def _enqueue(task_type, payload, *_a, **_kw):
        enqueue_calls.append((task_type, payload))
    async def _update(doc_id, **fields):
        update_calls.append((doc_id, fields))

    eligible = [_doc("d1"), _doc("d2"), _doc("d3")]

    with (
        patch("worker.retry_dead_docs.find_eligible_docs", new=AsyncMock(return_value=eligible)),
        patch("worker.queue.enqueue", new=_enqueue),
        patch("worker.retry_dead_docs.update_doc", new=_update),
        patch("worker.retry_dead_docs.get_session") as mock_sess,
    ):
        mock_sess.return_value.__aenter__.return_value = MagicMock()
        from worker.retry_dead_docs import retry_dead_docs
        re, sk = await retry_dead_docs(dry_run=True)

    assert re == 3 and sk == 0
    assert enqueue_calls == []
    assert update_calls == []


@pytest.mark.asyncio
async def test_apply_re_enqueues_and_resets_status():
    """Live mode: each eligible doc gets a fresh parse_document task
    enqueued and its doc row goes back to status='queued' with error
    fields cleared. Selected engine is recomputed via registry so
    docs with stale ``parse_engine`` (e.g. mineru when its breaker
    is now open) automatically pick up the current preference chain.
    """
    enqueue_calls: list[tuple] = []
    update_calls: list[tuple] = []

    async def _enqueue(task_type, payload, *_a, **_kw):
        enqueue_calls.append((task_type, payload))
    async def _update(doc_id, **fields):
        update_calls.append((doc_id, fields))

    eligible = [_doc("d1", parse_engine="mineru", title="x.pdf")]

    with (
        patch("worker.retry_dead_docs.find_eligible_docs", new=AsyncMock(return_value=eligible)),
        patch("worker.queue.enqueue", new=_enqueue),
        patch("worker.retry_dead_docs.update_doc", new=_update),
        patch("worker.retry_dead_docs.get_session") as mock_sess,
    ):
        mock_sess.return_value.__aenter__.return_value = MagicMock()
        # Pin auto_select so the test isn't sensitive to registry-state
        # (mineru breaker open vs closed during test run).
        with patch("parsers.registry.registry.auto_select", return_value="pdfium"):
            from worker.retry_dead_docs import retry_dead_docs
            re, sk = await retry_dead_docs(dry_run=False)

    assert re == 1 and sk == 0
    # Task enqueued with the freshly-selected engine, not the stale one.
    assert len(enqueue_calls) == 1
    task_type, payload = enqueue_calls[0]
    assert task_type == "parse_document"
    assert payload["doc_id"] == "d1"
    assert payload["selected_engine"] == "pdfium"  # not the stale "mineru"
    # Doc reset to queued with error fields cleared so polling clients
    # see the new state cleanly.
    assert update_calls == [(
        "d1", {"status": "queued", "error_type": None, "error_msg": "", "error_trace": None},
    )]


@pytest.mark.asyncio
async def test_enqueue_failure_marks_skipped_does_not_reset_doc():
    """If enqueueing the new task fails (Redis down, etc.), don't
    update the doc — leave it in 'failed' so the next cron run can
    try again. Skip count goes up so the operator notices."""
    eligible = [_doc("d1")]
    update_calls: list[tuple] = []

    async def _failing_enqueue(*_a, **_kw):
        raise RuntimeError("redis down")
    async def _update(doc_id, **fields):
        update_calls.append((doc_id, fields))

    with (
        patch("worker.retry_dead_docs.find_eligible_docs", new=AsyncMock(return_value=eligible)),
        patch("worker.queue.enqueue", new=_failing_enqueue),
        patch("worker.retry_dead_docs.update_doc", new=_update),
        patch("worker.retry_dead_docs.get_session") as mock_sess,
    ):
        mock_sess.return_value.__aenter__.return_value = MagicMock()
        with patch("parsers.registry.registry.auto_select", return_value="pdfium"):
            from worker.retry_dead_docs import retry_dead_docs
            re, sk = await retry_dead_docs(dry_run=False)

    assert re == 0 and sk == 1
    assert update_calls == []  # doc untouched on enqueue failure


def test_build_payload_picks_engine_via_registry_for_files():
    """Files: registry.auto_select(filename=…) is the source of
    truth for which engine to retry with. The doc's stored
    parse_engine is informational only — caller doesn't lock to it.
    """
    from worker.retry_dead_docs import _build_payload
    doc = _doc("d1", title="deck.pptx", storage_key="s/x", source="deck.pptx")
    with patch("parsers.registry.registry.auto_select", return_value="pptx-native") as mock_sel:
        payload = _build_payload(doc)
    mock_sel.assert_called_once_with(filename="deck.pptx")
    assert payload["selected_engine"] == "pptx-native"
    assert payload["doc_id"] == "d1"
    assert payload["storage_key"] == "s/x"


def test_build_payload_picks_engine_via_registry_for_urls():
    """URL docs have storage_key=None and source=URL string."""
    from worker.retry_dead_docs import _build_payload
    doc = _doc(
        "d2", title="article", storage_key=None,
        source="https://example.com/post",
    )
    with patch("parsers.registry.registry.auto_select", return_value="url") as mock_sel:
        payload = _build_payload(doc)
    mock_sel.assert_called_once_with(url="https://example.com/post")
    assert payload["selected_engine"] == "url"
    assert payload["filename"] == ""    # URL ingest has no filename
    assert payload["storage_key"] is None


@pytest.mark.asyncio
async def test_eligibility_caps_repeated_dead_tasks():
    """Regression: an old failed doc whose URL is genuinely
    unparseable (anti-bot, 404, dead site) used to get reborn-and-
    re-killed every hour by the cron, polluting the dead-task
    alert. The ``max_dead_tasks`` filter (default 2) caps that —
    a failed doc carries the original dead task (count=1), one
    auto-retry brings count to 2, then no more retries.

    Query-shape test: confirms the dead-task count subquery is in
    the WHERE clause with the right cap. Full integration is covered
    by live cron runs after deploy.
    """
    from worker.retry_dead_docs import find_eligible_docs

    captured = {}

    class _FakeResult:
        def scalars(self): return self
        def all(self): return []

    class _FakeSession:
        async def execute(self, stmt, params=None):
            captured["sql"] = str(stmt.compile(compile_kwargs={"literal_binds": True}))
            return _FakeResult()

    await find_eligible_docs(
        _FakeSession(),
        max_age_hours=24, retry_lockout_minutes=60,
        max_dead_tasks=2, limit=50,
    )

    sql = captured["sql"]
    assert "count" in sql.lower() and "dead" in sql.lower(), (
        f"expected dead-task count subquery, got:\n{sql}"
    )
    assert "< 2" in sql or "<2" in sql, (
        f"expected cap <2 (max_dead_tasks=2) in SQL, got:\n{sql}"
    )


@pytest.mark.asyncio
async def test_permanent_error_type_excluded_parse_error_included():
    """Regression guard: docs with error_type='permanent' must NEVER be
    re-queued by retry_dead_docs; docs with error_type='parse_error'
    within the same age/task window MUST be re-queued.

    This is the contract that makes the permanent-failure fix safe:
    find_eligible_docs filters on error_type == 'parse_error' exactly,
    so inserting a 'permanent' doc (as handlers.py now does for
    PermanentParseError) is automatically excluded without any change
    to retry_dead_docs logic.

    Uses the SQL-inspection pattern from the query-shape test above so
    we don't need a live DB while still asserting the WHERE-clause
    contract directly.
    """
    from worker.retry_dead_docs import find_eligible_docs

    captured = {}

    class _FakeResult:
        def scalars(self): return self
        def all(self): return []

    class _FakeSession:
        async def execute(self, stmt, params=None):
            captured["sql"] = str(stmt.compile(compile_kwargs={"literal_binds": True}))
            return _FakeResult()

    await find_eligible_docs(
        _FakeSession(),
        max_age_hours=24, retry_lockout_minutes=60,
        max_dead_tasks=2, limit=50,
    )

    sql = captured["sql"]
    # The WHERE clause must pin to 'parse_error' exactly.
    # A permanent doc (error_type='permanent') won't match this predicate,
    # so it is silently excluded — no change to retry_dead_docs required.
    assert "parse_error" in sql, (
        f"expected 'parse_error' filter in eligibility SQL, got:\n{sql}"
    )
    assert "permanent" not in sql, (
        "retry_dead_docs must NOT include 'permanent' in its eligibility "
        f"query — that would accidentally re-queue permanent failures:\n{sql}"
    )
