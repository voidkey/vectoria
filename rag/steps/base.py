from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from store.base import SearchResult


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
    answer: str = ""
    sources: list[dict] = field(default_factory=list)


class PipelineStep(ABC):
    enabled: bool = True

    @abstractmethod
    async def run(self, ctx: PipelineContext) -> PipelineContext: ...
