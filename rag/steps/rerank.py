import logging

import httpx
from rag.steps.base import PipelineStep, PipelineContext
from config import get_settings

logger = logging.getLogger(__name__)

# Cap on how many fused results we forward to the reranker. The fused
# set can be large (top_k * 2 per search × 2 searches = 4*top_k, plus
# overlap), but most rerankers charge per document and slow down
# linearly. 20 is the elbow: enough to give the cross-encoder room to
# find the true top-K, small enough that cost doesn't explode for
# large KBs. Tune via env if operators need a different balance.
_RERANK_INPUT_CAP = 20


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

        # Cap input to the reranker: sending 40+ documents burns
        # reranker quota for marginal gain beyond top ~20.
        candidates = ctx.fused_results[:_RERANK_INPUT_CAP]
        query = ctx.rewritten_query or ctx.query
        try:
            resp = await self._client.post(
                f"{cfg.reranker_base_url}/rerank",
                json={
                    "query": query,
                    "documents": [r.content for r in candidates],
                    "top_n": ctx.top_k,
                },
                headers={"Authorization": f"Bearer {cfg.openai_api_key.get_secret_value()}"},
            )
            resp.raise_for_status()
            n_candidates = len(candidates)
            results = []
            for item in resp.json()["results"]:
                idx = item["index"]
                if 0 <= idx < n_candidates:
                    results.append(candidates[idx])
                else:
                    logger.warning(
                        "Reranker returned out-of-range index %d (n=%d)",
                        idx, n_candidates,
                    )
            ctx.final_results = results
            ctx.rerank_applied = True
        except Exception:
            logger.exception("Rerank failed, falling back to fused results")
            ctx.final_results = ctx.fused_results[: ctx.top_k]
        return ctx
