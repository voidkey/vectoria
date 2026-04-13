import httpx
from fastapi import APIRouter
from openai import AsyncOpenAI

from api.errors import AppError, ErrorCode
from api.schemas import QueryRequest, QueryResponse
from config import get_settings
from rag.embedder import get_embedder
from rag.pipeline import build_default_pipeline
from rag.steps.query_rewrite import QueryRewriteStep
from rag.steps.rerank import RerankStep
from vectorstore.pgvector import PgVectorStore

router = APIRouter(prefix="/knowledgebases")

_llm_client: AsyncOpenAI | None = None
_rerank_client: httpx.AsyncClient | None = None


def _get_llm_client() -> AsyncOpenAI:
    global _llm_client  # noqa: PLW0603
    if _llm_client is None:
        cfg = get_settings()
        _llm_client = AsyncOpenAI(
            base_url=cfg.openai_base_url,
            api_key=cfg.openai_api_key.get_secret_value(),
        )
    return _llm_client


def _get_rerank_client() -> httpx.AsyncClient:
    global _rerank_client  # noqa: PLW0603
    if _rerank_client is None:
        _rerank_client = httpx.AsyncClient(timeout=30.0)
    return _rerank_client


@router.post("/{kb_id}/query", response_model=QueryResponse)
async def query_kb(kb_id: str, body: QueryRequest):
    if not body.query.strip():
        raise AppError(422, ErrorCode.QUERY_ERROR, "query must not be empty")

    store = await PgVectorStore.create()
    embedder = get_embedder()

    pipeline = build_default_pipeline(
        store=store, embedder=embedder,
        llm_client=_get_llm_client(), rerank_client=_get_rerank_client(),
    )

    # Apply per-request overrides
    if not body.query_rewrite:
        for step in pipeline.steps:
            if isinstance(step, QueryRewriteStep):
                step.enabled = False
    if body.rerank:
        for step in pipeline.steps:
            if isinstance(step, RerankStep):
                step.enabled = True

    try:
        ctx = await pipeline.run(body.query, kb_id=kb_id, top_k=body.top_k)
    finally:
        await store.close()

    return QueryResponse(answer=ctx.answer, sources=ctx.sources)
