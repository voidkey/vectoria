import httpx
from rag.steps.base import PipelineStep, PipelineContext
from config import get_settings


class RerankStep(PipelineStep):
    """Calls an OpenAI-compatible reranker API."""

    def __init__(self, enabled: bool = False):
        self.enabled = enabled

    async def run(self, ctx: PipelineContext) -> PipelineContext:
        # Reranker not configured — pass through
        cfg = get_settings()
        if not cfg.reranker_base_url:
            ctx.final_results = ctx.fused_results
            return ctx

        query = ctx.rewritten_query or ctx.query
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(
                    f"{cfg.reranker_base_url}/rerank",
                    json={
                        "query": query,
                        "documents": [r.content for r in ctx.fused_results],
                        "top_n": ctx.top_k,
                    },
                    headers={"Authorization": f"Bearer {cfg.openai_api_key.get_secret_value()}"},
                )
                resp.raise_for_status()
                ranked_indices = [item["index"] for item in resp.json()["results"]]
                ctx.final_results = [ctx.fused_results[i] for i in ranked_indices]
        except Exception:
            ctx.final_results = ctx.fused_results[: ctx.top_k]
        return ctx
