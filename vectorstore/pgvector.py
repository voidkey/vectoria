from __future__ import annotations
import json
import asyncpg
from vectorstore.base import VectorStore, ChunkData, SearchResult
from config import get_settings


def rrf_fuse(
    vector_results: list[SearchResult],
    keyword_results: list[SearchResult],
    k: int = 60,
) -> list[SearchResult]:
    """Reciprocal Rank Fusion — combines two ranked lists."""
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


_CREATE_EXTENSION = "CREATE EXTENSION IF NOT EXISTS vector"
_CREATE_CHUNKS_TABLE = """
CREATE TABLE IF NOT EXISTS chunks (
    id          TEXT PRIMARY KEY,
    doc_id      TEXT NOT NULL,
    kb_id       TEXT NOT NULL,
    content     TEXT NOT NULL,
    embedding   vector({dim}),
    chunk_index INTEGER NOT NULL DEFAULT 0,
    parent_id   TEXT,
    content_tsv tsvector GENERATED ALWAYS AS (to_tsvector('simple', content)) STORED
)
"""
_CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS chunks_kb_id_idx ON chunks (kb_id)",
    "CREATE INDEX IF NOT EXISTS chunks_doc_id_idx ON chunks (doc_id)",
    "CREATE INDEX IF NOT EXISTS chunks_tsv_idx ON chunks USING GIN (content_tsv)",
]


class PgVectorStore(VectorStore):
    def __init__(self, conn: asyncpg.Connection, dimensions: int = 1536):
        self._conn = conn
        self._dims = dimensions

    @classmethod
    async def create(cls) -> "PgVectorStore":
        cfg = get_settings()
        dsn = cfg.database_url.get_secret_value().replace("+asyncpg", "")
        conn = await asyncpg.connect(dsn)
        store = cls(conn, dimensions=cfg.embedding_dimensions)
        await store._ensure_schema()
        return store

    async def _ensure_schema(self) -> None:
        await self._conn.execute(_CREATE_EXTENSION)
        await self._conn.execute(_CREATE_CHUNKS_TABLE.format(dim=self._dims))
        for idx_sql in _CREATE_INDEXES:
            await self._conn.execute(idx_sql)

    async def upsert(self, chunks: list[ChunkData]) -> None:
        records = [
            (c.id, c.doc_id, c.kb_id, c.content, json.dumps(c.embedding), c.chunk_index, c.parent_id)
            for c in chunks
        ]
        await self._conn.executemany(
            """
            INSERT INTO chunks (id, doc_id, kb_id, content, embedding, chunk_index, parent_id)
            VALUES ($1, $2, $3, $4, $5::vector, $6, $7)
            ON CONFLICT (id) DO UPDATE SET content=EXCLUDED.content, embedding=EXCLUDED.embedding, chunk_index=EXCLUDED.chunk_index, parent_id=EXCLUDED.parent_id
            """,
            records,
        )

    async def vector_search(
        self, embedding: list[float], kb_id: str, top_k: int
    ) -> list[SearchResult]:
        rows = await self._conn.fetch(
            """
            SELECT id, content, doc_id, parent_id,
                   1 - (embedding <=> $1::vector) AS score
            FROM chunks
            WHERE kb_id = $2
            ORDER BY embedding <=> $1::vector
            LIMIT $3
            """,
            json.dumps(embedding), kb_id, top_k,
        )
        return [SearchResult(chunk_id=r["id"], content=r["content"], score=r["score"], doc_id=r["doc_id"], parent_id=r["parent_id"]) for r in rows]

    async def keyword_search(
        self, query: str, kb_id: str, top_k: int
    ) -> list[SearchResult]:
        rows = await self._conn.fetch(
            """
            SELECT id, content, doc_id, parent_id,
                   ts_rank(content_tsv, plainto_tsquery('simple', $1)) AS score
            FROM chunks
            WHERE kb_id = $2 AND content_tsv @@ plainto_tsquery('simple', $1)
            ORDER BY score DESC
            LIMIT $3
            """,
            query, kb_id, top_k,
        )
        return [SearchResult(chunk_id=r["id"], content=r["content"], score=r["score"], doc_id=r["doc_id"], parent_id=r["parent_id"]) for r in rows]

    async def delete_by_doc(self, doc_id: str) -> None:
        await self._conn.execute("DELETE FROM chunks WHERE doc_id = $1", doc_id)

    async def delete_by_kb(self, kb_id: str) -> None:
        await self._conn.execute("DELETE FROM chunks WHERE kb_id = $1", kb_id)

    async def get_by_ids(self, chunk_ids: list[str]) -> list[SearchResult]:
        rows = await self._conn.fetch(
            "SELECT id, content, doc_id, parent_id FROM chunks WHERE id = ANY($1)",
            chunk_ids,
        )
        return [SearchResult(chunk_id=r["id"], content=r["content"], score=1.0, doc_id=r["doc_id"], parent_id=r["parent_id"]) for r in rows]

    async def close(self) -> None:
        await self._conn.close()

    async def __aenter__(self) -> "PgVectorStore":
        return self

    async def __aexit__(self, *args) -> None:
        await self.close()
