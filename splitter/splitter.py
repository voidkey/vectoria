import uuid
from dataclasses import dataclass


@dataclass
class Chunk:
    id: str
    content: str
    index: int
    parent_id: str | None = None  # set for child chunks in parent-child mode


class Splitter:
    """
    Fixed-size token-approximate splitter with optional parent-child mode.

    parent_chunk_size > 0 enables parent-child:
      - Parent chunks (large) are stored for context expansion
      - Child chunks (small) are indexed for retrieval
      - Child chunks have parent_id pointing to their parent chunk
    """

    def __init__(
        self,
        chunk_size: int = 512,
        chunk_overlap: int = 64,
        parent_chunk_size: int = 0,
    ):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.parent_chunk_size = parent_chunk_size

    def split(self, text: str) -> list[Chunk]:
        if not text.strip():
            return []

        if self.parent_chunk_size > 0:
            return self._parent_child_split(text)
        return self._fixed_split(text, chunk_size=self.chunk_size, parent_id=None)

    def _fixed_split(
        self, text: str, chunk_size: int, parent_id: str | None, index_offset: int = 0
    ) -> list[Chunk]:
        # Split by words to approximate token count (1 word ≈ 1.3 tokens)
        words = text.split()
        approx_words = int(chunk_size / 1.3)
        overlap_words = int(self.chunk_overlap / 1.3)

        chunks: list[Chunk] = []
        start = 0
        idx = index_offset

        while start < len(words):
            end = min(start + approx_words, len(words))
            content = " ".join(words[start:end])
            chunks.append(Chunk(id=str(uuid.uuid4()), content=content, index=idx, parent_id=parent_id))
            idx += 1
            if end == len(words):
                break
            start = end - overlap_words

        return chunks

    def _parent_child_split(self, text: str) -> list[Chunk]:
        parents = self._fixed_split(text, chunk_size=self.parent_chunk_size, parent_id=None)
        all_chunks: list[Chunk] = []
        child_idx = 0

        for parent in parents:
            all_chunks.append(parent)
            children = self._fixed_split(
                parent.content,
                chunk_size=self.chunk_size,
                parent_id=parent.id,
                index_offset=child_idx,
            )
            all_chunks.extend(children)
            child_idx += len(children)

        return all_chunks
