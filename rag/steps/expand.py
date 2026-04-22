from rag.steps.base import PipelineStep, PipelineContext
from vectorstore.base import VectorStore, SearchResult


class ExpandStep(PipelineStep):
    """Replace child chunks with their parent chunks for richer context.

    When a reranker picks top-K children, it scored them based on
    child-level content. Silently swapping those children for their
    parent chunks means the LLM receives content the reranker never
    evaluated — ordering becomes incoherent, and the parent may even
    include off-topic sibling chunks. When ``ctx.rerank_applied`` is
    True we preserve the reranker's decisions; otherwise (pure hybrid
    retrieval without cross-encoder reordering) we expand as before.
    """

    def __init__(self, store: VectorStore, enabled: bool = True):
        self.enabled = enabled
        self._store = store

    async def run(self, ctx: PipelineContext) -> PipelineContext:
        # Respect reranker decisions — child-level precision wins over
        # parent-level context when the reranker already ran.
        if ctx.rerank_applied:
            return ctx

        parent_ids = [r.parent_id for r in ctx.final_results if r.parent_id]
        if not parent_ids:
            return ctx

        parents = await self._store.get_by_ids(parent_ids)
        parent_map = {p.chunk_id: p for p in parents}

        expanded: list[SearchResult] = []
        seen: set[str] = set()
        for r in ctx.final_results:
            key = r.parent_id or r.chunk_id
            if key not in seen:
                seen.add(key)
                expanded.append(parent_map.get(r.parent_id, r) if r.parent_id else r)

        ctx.final_results = expanded
        return ctx
