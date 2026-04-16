import re
import uuid
from dataclasses import dataclass

# Separator priority: paragraph > line > Chinese sentence > English sentence > clause > word > char
_DEFAULT_SEPARATORS = ["\n\n", "\n", "。", ".", "？", "！", "?", "!", "；", ";", " ", ""]


@dataclass
class Chunk:
    id: str
    content: str
    index: int
    parent_id: str | None = None  # set for child chunks in parent-child mode


class Splitter:
    """
    Recursive character text splitter with optional parent-child mode.

    Uses a separator priority list (paragraph → line → sentence → word → char)
    to recursively split text while preserving document structure.

    parent_chunk_size > 0 enables parent-child:
      - Parent chunks (large) are stored for context expansion
      - Child chunks (small) are indexed for retrieval
      - Child chunks have parent_id pointing to their parent chunk
    """

    def __init__(
        self,
        chunk_size: int = 512,
        chunk_overlap: int = 50,
        parent_chunk_size: int = 0,
        separators: list[str] | None = None,
    ):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.parent_chunk_size = parent_chunk_size
        self.separators = separators or _DEFAULT_SEPARATORS

    def split(self, text: str) -> list[Chunk]:
        if not text.strip():
            return []

        if self.parent_chunk_size > 0:
            return self._parent_child_split(text)
        return self._build_chunks(text, chunk_size=self.chunk_size, parent_id=None)

    def _build_chunks(
        self, text: str, chunk_size: int, parent_id: str | None, index_offset: int = 0,
    ) -> list[Chunk]:
        splits = self._recursive_split(text, self.separators, chunk_size)
        return self._merge_splits(splits, chunk_size, parent_id, index_offset)

    def _recursive_split(
        self, text: str, separators: list[str], chunk_size: int,
    ) -> list[str]:
        """Recursively split text using separators in priority order."""
        if len(text) <= chunk_size:
            return [text] if text.strip() else []

        # Find the best separator that actually splits this text
        sep = ""
        remaining_seps = separators
        for i, candidate in enumerate(separators):
            if candidate == "":
                sep = candidate
                remaining_seps = []
                break
            if candidate in text:
                sep = candidate
                remaining_seps = separators[i + 1 :]
                break

        # Split by the chosen separator. When no separator works, slice by
        # chunk_size rather than materializing one str per character — a
        # multi-MB text with no separators otherwise allocates tens of millions
        # of single-char str objects (~50 bytes each) and blows up memory.
        if sep == "":
            pieces = [text[i : i + chunk_size] for i in range(0, len(text), chunk_size)]
        else:
            pieces = _split_keeping_separator(text, sep)

        # Recursively split pieces that are still too large
        result: list[str] = []
        for piece in pieces:
            if not piece.strip():
                continue
            if len(piece) <= chunk_size:
                result.append(piece)
            elif remaining_seps:
                result.extend(self._recursive_split(piece, remaining_seps, chunk_size))
            else:
                result.append(piece)
        return result

    def _merge_splits(
        self,
        splits: list[str],
        chunk_size: int,
        parent_id: str | None,
        index_offset: int,
    ) -> list[Chunk]:
        """Merge small splits into chunks respecting chunk_size and overlap."""
        chunks: list[Chunk] = []
        current: list[str] = []
        current_len = 0
        idx = index_offset

        for piece in splits:
            piece_len = len(piece)

            if current_len + piece_len > chunk_size and current:
                # Flush current chunk
                content = "".join(current).strip()
                if content:
                    chunks.append(Chunk(
                        id=str(uuid.uuid4()), content=content,
                        index=idx, parent_id=parent_id,
                    ))
                    idx += 1

                # Keep overlap from the tail of current chunk
                overlap: list[str] = []
                overlap_len = 0
                for s in reversed(current):
                    if overlap_len + len(s) > self.chunk_overlap:
                        break
                    overlap.append(s)
                    overlap_len += len(s)
                overlap.reverse()
                current = overlap
                current_len = overlap_len

            current.append(piece)
            current_len += piece_len

        # Flush remaining
        content = "".join(current).strip()
        if content:
            chunks.append(Chunk(
                id=str(uuid.uuid4()), content=content,
                index=idx, parent_id=parent_id,
            ))

        return chunks

    def _parent_child_split(self, text: str) -> list[Chunk]:
        parents = self._build_chunks(text, chunk_size=self.parent_chunk_size, parent_id=None)
        all_chunks: list[Chunk] = []
        child_idx = 0

        for parent in parents:
            all_chunks.append(parent)
            children = self._build_chunks(
                parent.content,
                chunk_size=self.chunk_size,
                parent_id=parent.id,
                index_offset=child_idx,
            )
            all_chunks.extend(children)
            child_idx += len(children)

        return all_chunks


def _split_keeping_separator(text: str, sep: str) -> list[str]:
    """Split text by separator, keeping the separator attached to the preceding piece."""
    escaped = re.escape(sep)
    parts = re.split(f"({escaped})", text)
    # Re-attach separators: [content, sep, content, sep, ...] → [content+sep, content+sep, ...]
    result: list[str] = []
    i = 0
    while i < len(parts):
        piece = parts[i]
        if i + 1 < len(parts) and parts[i + 1] == sep:
            piece += parts[i + 1]
            i += 2
        else:
            i += 1
        if piece:
            result.append(piece)
    return result
