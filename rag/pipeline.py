from rag.steps.base import PipelineContext, PipelineStep
from config import get_settings


class Pipeline:
    def __init__(self, steps: list[PipelineStep]):
        self.steps = steps

    async def run(self, query: str, kb_id: str, top_k: int = 5) -> PipelineContext:
        ctx = PipelineContext(query=query, kb_id=kb_id, top_k=top_k)
        for step in self.steps:
            if step.enabled:
                ctx = await step.run(ctx)
        return ctx


def build_default_pipeline(store, embedder, llm_client, rerank_client) -> Pipeline:
    """Build the standard pipeline from configured steps."""
    from rag.steps.retrieve import RetrieveStep
    from rag.steps.fusion import FusionStep
    from rag.steps.query_rewrite import QueryRewriteStep
    from rag.steps.rerank import RerankStep
    from rag.steps.expand import ExpandStep
    from rag.steps.generate import GenerateStep

    cfg = get_settings()
    return Pipeline(steps=[
        QueryRewriteStep(llm_client=llm_client, enabled=cfg.enable_query_rewrite),
        RetrieveStep(store=store, embedder=embedder),
        FusionStep(),
        RerankStep(client=rerank_client, enabled=cfg.enable_reranker),
        ExpandStep(store=store),
        GenerateStep(llm_client=llm_client),
    ])
