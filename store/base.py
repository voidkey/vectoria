from abc import ABC, abstractmethod
from dataclasses import dataclass, field


@dataclass
class ChunkData:
    id: str
    doc_id: str
    kb_id: str
    content: str
    embedding: list[float]
    chunk_index: int
    parent_id: str | None = None


@dataclass
class SearchResult:
    chunk_id: str
    content: str
    score: float
    doc_id: str
    parent_id: str | None = None


class VectorStore(ABC):
    @abstractmethod
    async def upsert(self, chunks: list[ChunkData]) -> None: ...

    @abstractmethod
    async def vector_search(
        self, embedding: list[float], kb_id: str, top_k: int
    ) -> list[SearchResult]: ...

    @abstractmethod
    async def keyword_search(
        self, query: str, kb_id: str, top_k: int
    ) -> list[SearchResult]: ...

    @abstractmethod
    async def delete_by_doc(self, doc_id: str) -> None: ...

    @abstractmethod
    async def delete_by_kb(self, kb_id: str) -> None: ...

    @abstractmethod
    async def get_by_ids(self, chunk_ids: list[str]) -> list[SearchResult]: ...
