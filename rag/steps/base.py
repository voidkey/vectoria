from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from vectorstore.base import SearchResult


@dataclass
class PipelineContext:
    query: str
    kb_id: str
    top_k: int = 5
    rewritten_query: str = ""
    vector_results: list[SearchResult] = field(default_factory=list)
    keyword_results: list[SearchResult] = field(default_factory=list)
    fused_results: list[SearchResult] = field(default_factory=list)
    final_results: list[SearchResult] = field(default_factory=list)
    # Set by RerankStep when the cross-encoder actually ordered results
    # (i.e. reranker base URL configured and the call succeeded). Used
    # by ExpandStep to skip the child→parent swap so the reranker's
    # child-level decisions aren't discarded.
    rerank_applied: bool = False
    answer: str = ""
    sources: list[dict] = field(default_factory=list)


class PipelineStep(ABC):
    def __init__(self, enabled: bool = True):
        self.enabled = enabled

    @abstractmethod
    async def run(self, ctx: PipelineContext) -> PipelineContext: ...
