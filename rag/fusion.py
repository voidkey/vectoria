"""Reciprocal Rank Fusion (RRF) — combines two ranked result lists."""

from vectorstore.base import SearchResult


def rrf_fuse(
    vector_results: list[SearchResult],
    keyword_results: list[SearchResult],
    k: int = 60,
) -> list[SearchResult]:
    """Reciprocal Rank Fusion — combines two ranked lists.

    For each result, the fused score is:
        score = Σ 1 / (k + rank + 1)
    summed across all lists in which the result appears.
    """
    scores: dict[str, float] = {}
    best: dict[str, SearchResult] = {}

    for rank, r in enumerate(vector_results):
        scores[r.chunk_id] = scores.get(r.chunk_id, 0) + 1 / (k + rank + 1)
        best[r.chunk_id] = r

    for rank, r in enumerate(keyword_results):
        scores[r.chunk_id] = scores.get(r.chunk_id, 0) + 1 / (k + rank + 1)
        if r.chunk_id not in best:
            best[r.chunk_id] = r

    return sorted(
        [SearchResult(chunk_id=cid, content=best[cid].content, score=s, doc_id=best[cid].doc_id, parent_id=best[cid].parent_id)
         for cid, s in scores.items()],
        key=lambda r: r.score,
        reverse=True,
    )
