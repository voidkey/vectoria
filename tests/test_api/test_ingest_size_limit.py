import pytest
from unittest.mock import patch, AsyncMock, MagicMock

from parsers.base import ParseResult


@pytest.mark.asyncio
async def test_ingest_file_rejects_oversized_parsed_content(client):
    """Parsed content larger than max_content_chars must return 413 before
    hitting the DB or index pipeline — a 45MB text otherwise explodes memory
    in the splitter/embedder and OOM-kills the server.
    """
    from config import get_settings
    limit = get_settings().max_content_chars
    oversized = "a" * (limit + 1)

    fake_parser = MagicMock()
    fake_parser.parse = AsyncMock(
        return_value=ParseResult(content=oversized, images={}, title="big")
    )

    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_storage") as mock_storage,
        patch("api.routes.documents.registry") as mock_registry,
        patch("api.routes.documents.get_session") as mock_sess,
        patch("worker.queue.enqueue", new=AsyncMock()) as mock_enqueue,
    ):
        mock_storage.return_value = AsyncMock()
        mock_registry.auto_select.return_value = "markitdown"
        mock_registry.get_by_engine.return_value = fake_parser
        session = AsyncMock()
        session.add = MagicMock()
        # Dedup lookup must miss (return None) so we reach the parse step.
        miss = MagicMock()
        miss.scalar_one_or_none.return_value = None
        session.execute = AsyncMock(return_value=miss)
        mock_sess.return_value.__aenter__.return_value = session

        resp = await client.post(
            "/v1/knowledgebases/kb-x/documents/file",
            files={"file": ("big.txt", b"ignored", "text/plain")},
        )

    assert resp.status_code == 413, resp.text
    body = resp.json()
    assert body["code"] == 1203  # CONTENT_TOO_LARGE
    session.add.assert_not_called()
    mock_enqueue.assert_not_called()
