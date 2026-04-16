"""Tests for reap_dead_tasks — the periodic dead-task cleanup."""

import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch, AsyncMock, MagicMock


@pytest.mark.asyncio
async def test_reap_dead_tasks_marks_exhausted():
    """Tasks with expired lock AND exhausted retries should be marked dead."""
    session = AsyncMock()
    result = MagicMock()
    result.scalars.return_value.all.return_value = ["task-1", "task-2"]
    session.execute = AsyncMock(return_value=result)

    with patch("worker.queue.get_session") as mock_gs:
        mock_gs.return_value.__aenter__.return_value = session
        from worker.queue import reap_dead_tasks
        count = await reap_dead_tasks()

    assert count == 2
    session.commit.assert_called_once()


@pytest.mark.asyncio
async def test_reap_dead_tasks_none_to_reap():
    """When no dead tasks exist, no commit should happen."""
    session = AsyncMock()
    result = MagicMock()
    result.scalars.return_value.all.return_value = []
    session.execute = AsyncMock(return_value=result)

    with patch("worker.queue.get_session") as mock_gs:
        mock_gs.return_value.__aenter__.return_value = session
        from worker.queue import reap_dead_tasks
        count = await reap_dead_tasks()

    assert count == 0
    session.commit.assert_not_called()
