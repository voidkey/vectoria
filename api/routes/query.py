from fastapi import APIRouter, HTTPException
from openai import AsyncOpenAI

from api.schemas import QueryRequest, QueryResponse
from config import get_settings
from rag.embedder import Embedder
from rag.pipeline import build_default_pipeline
from store.pgvector import PgVectorStore

router = APIRouter(prefix="/knowledgebases")


@router.post("/{kb_id}/query", response_model=QueryResponse)
async def query_kb(kb_id: str, body: QueryRequest):
    if not body.query.strip():
        raise HTTPException(status_code=422, detail="query must not be empty")

    cfg = get_settings()
    llm = AsyncOpenAI(
        base_url=cfg.openai_base_url,
        api_key=cfg.openai_api_key.get_secret_value(),
    )
    store = await PgVectorStore.create()
    embedder = Embedder()

    pipeline = build_default_pipeline(store=store, embedder=embedder, llm_client=llm)

    # Apply per-request overrides
    if not body.query_rewrite:
        pipeline.steps[0].enabled = False
    if body.rerank:
        pipeline.steps[3].enabled = True

    try:
        ctx = await pipeline.run(body.query, kb_id=kb_id, top_k=body.top_k)
    finally:
        await store.close()

    return QueryResponse(answer=ctx.answer, sources=ctx.sources)
