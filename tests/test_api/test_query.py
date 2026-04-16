import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from rag.steps.base import PipelineContext
from vectorstore.base import SearchResult


def _make_ctx(answer: str, sources: list) -> PipelineContext:
    ctx = PipelineContext(query="test", kb_id="kb1")
    ctx.answer = answer
    ctx.sources = sources
    return ctx


@pytest.mark.asyncio
async def test_query_returns_answer(client):
    ctx = _make_ctx(
        answer="The answer is 42.",
        sources=[{"chunk_id": "c1", "content": "42 is the answer", "score": 0.9, "doc_id": "d1"}],
    )

    with patch("api.routes.query.build_default_pipeline") as mock_build, \
         patch("api.routes.query.PgVectorStore") as mock_store_cls, \
         patch("api.routes.query.get_embedder") as mock_get_emb:

        mock_pipeline = AsyncMock()
        mock_pipeline.run = AsyncMock(return_value=ctx)
        mock_build.return_value = mock_pipeline

        mock_store = AsyncMock()
        mock_store_cls.create = AsyncMock(return_value=mock_store)
        mock_get_emb.return_value = MagicMock()

        resp = await client.post("/v1/knowledgebases/kb1/query", json={"query": "what is 42?"})

    assert resp.status_code == 200
    body = resp.json()
    assert body["answer"] == "The answer is 42."
    assert len(body["sources"]) == 1


@pytest.mark.asyncio
async def test_query_empty_query(client):
    resp = await client.post("/v1/knowledgebases/kb1/query", json={"query": ""})
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_query_whitespace_only(client):
    resp = await client.post("/v1/knowledgebases/kb1/query", json={"query": "   "})
    assert resp.status_code == 422
