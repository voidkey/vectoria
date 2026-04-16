"""Concurrent upload throttling.

The API must bound how many ingestions run simultaneously so N parallel
uploads don't each hold ~50MB in memory at once.
"""
import asyncio
import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from datetime import datetime

from parsers.base import ParseResult


@pytest.mark.asyncio
async def test_concurrent_uploads_beyond_limit_get_429(client):
    """When max_concurrent_ingestions slots are occupied, extra uploads
    get 429 immediately instead of queuing and piling up memory.
    """
    from config import get_settings
    cfg = get_settings()
    # Force limit to 1 for test — any second concurrent request must be rejected.
    original = cfg.max_concurrent_ingestions
    cfg.max_concurrent_ingestions = 1

    # A parser that blocks until we release it, simulating a slow parse.
    gate = asyncio.Event()
    fake_parser = MagicMock()

    async def _slow_parse(*args, **kwargs):
        await gate.wait()
        return ParseResult(content="ok", images={}, title="t")

    fake_parser.parse = _slow_parse

    try:
        with (
            patch("api.routes.documents._validate_kb", new=AsyncMock()),
            patch("api.routes.documents.get_storage") as mock_storage,
            patch("api.routes.documents.registry") as mock_reg,
            patch("api.routes.documents.get_session") as mock_sess,
            patch("worker.queue.enqueue", new=AsyncMock()),
            patch("api.routes.documents.extract_outline", return_value=[]),
        ):
            mock_storage.return_value = AsyncMock()
            mock_reg.auto_select.return_value = "markitdown"
            mock_reg.get_by_engine.return_value = fake_parser

            session = AsyncMock()
            miss = MagicMock()
            miss.scalar_one_or_none.return_value = None
            session.execute = AsyncMock(return_value=miss)
            session.add = MagicMock()
            session.commit = AsyncMock()
            session.refresh = AsyncMock(side_effect=lambda o: (
                setattr(o, "id", "d1"),
                setattr(o, "created_at", datetime(2026, 1, 1)),
            ))
            mock_sess.return_value.__aenter__.return_value = session

            # Launch two concurrent uploads.
            req1 = asyncio.create_task(client.post(
                "/v1/knowledgebases/kb-x/documents/file",
                files={"file": ("a.txt", b"aaa", "text/plain")},
            ))
            # Small yield so req1 enters the semaphore first.
            await asyncio.sleep(0.01)
            req2 = asyncio.create_task(client.post(
                "/v1/knowledgebases/kb-x/documents/file",
                files={"file": ("b.txt", b"bbb", "text/plain")},
            ))

            # req2 should return immediately with 429.
            resp2 = await asyncio.wait_for(req2, timeout=2.0)
            assert resp2.status_code == 429, resp2.text
            assert resp2.json()["code"] == 1206  # INGEST_BUSY

            # Release the gate so req1 completes.
            gate.set()
            resp1 = await asyncio.wait_for(req1, timeout=5.0)
            assert resp1.status_code == 201, resp1.text
    finally:
        cfg.max_concurrent_ingestions = original
