import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from datetime import datetime


@pytest.mark.asyncio
async def test_create_knowledgebase(client):
    mock_kb = MagicMock()
    mock_kb.id = "kb-123"
    mock_kb.name = "Test KB"
    mock_kb.description = "desc"
    mock_kb.created_at = datetime(2026, 1, 1)

    with patch("api.routes.knowledgebase.get_session") as mock_sess:
        session = AsyncMock()
        session.add = MagicMock()
        session.commit = AsyncMock()
        session.refresh = AsyncMock(side_effect=lambda obj: setattr(obj, "id", "kb-123") or setattr(obj, "created_at", datetime(2026,1,1)))
        mock_sess.return_value.__aenter__.return_value = session

        with patch("api.routes.knowledgebase.KnowledgeBase", return_value=mock_kb):
            resp = await client.post("/v1/knowledgebases", json={"name": "Test KB", "description": "desc"})

    assert resp.status_code == 201


@pytest.mark.asyncio
async def test_list_knowledgebases(client):
    with patch("api.routes.knowledgebase.get_session") as mock_sess:
        session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        session.scalar = AsyncMock(return_value=0)
        session.execute = AsyncMock(return_value=mock_result)
        mock_sess.return_value.__aenter__.return_value = session

        resp = await client.get("/v1/knowledgebases")

    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 0
    assert body["offset"] == 0
    assert body["limit"] == 50
    assert body["items"] == []
