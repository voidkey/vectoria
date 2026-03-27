from store.base import SearchResult
from store.pgvector import rrf_fuse


def _r(chunk_id: str, score: float) -> SearchResult:
    return SearchResult(chunk_id=chunk_id, content="test", score=score, doc_id="d1")


def test_rrf_combines_results():
    vector_results = [_r("a", 0.9), _r("b", 0.8), _r("c", 0.7)]
    keyword_results = [_r("b", 1.0), _r("a", 0.5), _r("d", 0.3)]

    fused = rrf_fuse(vector_results, keyword_results, k=60)

    ids = [r.chunk_id for r in fused]
    # "b" appears in both lists, should rank high
    assert ids.index("b") < ids.index("c")
    assert ids.index("b") < ids.index("d")


def test_rrf_deduplicates():
    vector_results = [_r("a", 0.9), _r("a", 0.8)]  # duplicate
    keyword_results = [_r("a", 1.0)]
    fused = rrf_fuse(vector_results, keyword_results, k=60)
    assert len([r for r in fused if r.chunk_id == "a"]) == 1


def test_rrf_vector_only():
    vector_results = [_r("a", 0.9), _r("b", 0.8)]
    fused = rrf_fuse(vector_results, [], k=60)
    assert [r.chunk_id for r in fused] == ["a", "b"]
