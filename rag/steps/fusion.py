from rag.steps.base import PipelineStep, PipelineContext
from store.pgvector import rrf_fuse


class FusionStep(PipelineStep):
    def __init__(self, k: int = 60):
        self._k = k

    async def run(self, ctx: PipelineContext) -> PipelineContext:
        ctx.fused_results = rrf_fuse(ctx.vector_results, ctx.keyword_results, k=self._k)
        ctx.final_results = ctx.fused_results
        return ctx
