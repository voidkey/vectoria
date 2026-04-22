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


class Splitter:
    """Recursive character text splitter.

    Separator priority (paragraph → line → sentence → word → char)
    preserves document structure while respecting ``chunk_size``.
    Adjacent chunks overlap by ``chunk_overlap`` characters so
    sentence-straddling context isn't lost at chunk boundaries.

    Parent-child mode used to be in here; it was removed in W6-6
    because the worker's ``handle_index_document`` only ever indexed
    the parent-sized chunks — children were built and discarded. The
    "small-chunk retrieval + parent expansion at query time" pattern
    is valuable when implemented fully, but the half-version made
    ``parent_chunk_size`` silently mean ``chunk_size`` while carrying
    confusing dead code in Splitter, worker, and the RAG pipeline.
    Re-add with proper end-to-end plumbing and an eval harness when
    we actually want it.
    """

    def __init__(
        self,
        chunk_size: int = 512,
        chunk_overlap: int = 50,
        separators: list[str] | None = None,
    ):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.separators = separators or _DEFAULT_SEPARATORS

    def split(self, text: str) -> list[Chunk]:
        if not text.strip():
            return []
        splits = self._recursive_split(text, self.separators, self.chunk_size)
        return self._merge_splits(splits)

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

    def _merge_splits(self, splits: list[str]) -> list[Chunk]:
        """Merge small splits into chunks respecting chunk_size and overlap."""
        chunks: list[Chunk] = []
        current: list[str] = []
        current_len = 0
        idx = 0

        for piece in splits:
            piece_len = len(piece)

            if current_len + piece_len > self.chunk_size and current:
                content = "".join(current).strip()
                if content:
                    chunks.append(Chunk(
                        id=str(uuid.uuid4()), content=content, index=idx,
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

        content = "".join(current).strip()
        if content:
            chunks.append(Chunk(
                id=str(uuid.uuid4()), content=content, index=idx,
            ))

        return chunks


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
