import asyncio
from rag.steps.base import PipelineStep, PipelineContext
from store.base import VectorStore
from rag.embedder import Embedder


class RetrieveStep(PipelineStep):
    def __init__(self, store: VectorStore, embedder: Embedder):
        self._store = store
        self._embedder = embedder

    async def run(self, ctx: PipelineContext) -> PipelineContext:
        query = ctx.rewritten_query or ctx.query
        embedding = await self._embedder.embed(query)
        vector_task = self._store.vector_search(embedding, ctx.kb_id, ctx.top_k * 2)
        keyword_task = self._store.keyword_search(query, ctx.kb_id, ctx.top_k * 2)
        ctx.vector_results, ctx.keyword_results = await asyncio.gather(vector_task, keyword_task)
        return ctx
