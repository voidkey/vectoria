import pytest

from config import get_settings


@pytest.mark.asyncio
async def test_query_returns_503_when_indexing_disabled(client, monkeypatch):
    monkeypatch.setattr(get_settings(), "enable_indexing", False)
    resp = await client.post(
        "/v1/knowledgebases/some-kb/query",
        json={"query": "hello"},
    )
    assert resp.status_code == 503
    body = resp.json()
    assert body["code"] == 1402  # INDEXING_DISABLED
