import logging

import httpx
from rag.steps.base import PipelineStep, PipelineContext
from config import get_settings

logger = logging.getLogger(__name__)


class RerankStep(PipelineStep):
    """Calls an OpenAI-compatible reranker API."""

    def __init__(self, client: httpx.AsyncClient, enabled: bool = False):
        super().__init__(enabled=enabled)
        self._client = client

    async def run(self, ctx: PipelineContext) -> PipelineContext:
        cfg = get_settings()
        if not cfg.reranker_base_url:
            ctx.final_results = ctx.fused_results
            return ctx

        query = ctx.rewritten_query or ctx.query
        try:
            resp = await self._client.post(
                f"{cfg.reranker_base_url}/rerank",
                json={
                    "query": query,
                    "documents": [r.content for r in ctx.fused_results],
                    "top_n": ctx.top_k,
                },
                headers={"Authorization": f"Bearer {cfg.openai_api_key.get_secret_value()}"},
            )
            resp.raise_for_status()
            n_fused = len(ctx.fused_results)
            results = []
            for item in resp.json()["results"]:
                idx = item["index"]
                if 0 <= idx < n_fused:
                    results.append(ctx.fused_results[idx])
                else:
                    logger.warning("Reranker returned out-of-range index %d (n=%d)", idx, n_fused)
            ctx.final_results = results
        except Exception:
            logger.exception("Rerank failed, falling back to fused results")
            ctx.final_results = ctx.fused_results[: ctx.top_k]
        return ctx
