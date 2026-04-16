from __future__ import annotations
import json
import asyncpg
from vectorstore.base import VectorStore, ChunkData, SearchResult
from config import get_settings


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

# ---------------------------------------------------------------------------
# Module-level connection pool (lazily initialised, shared across requests)
# ---------------------------------------------------------------------------
_pool: asyncpg.Pool | None = None
_pool_initialised: bool = False


async def _get_pool() -> asyncpg.Pool:
    """Return (and lazily create) the shared asyncpg connection pool."""
    global _pool, _pool_initialised  # noqa: PLW0603
    if _pool is not None:
        return _pool
    cfg = get_settings()
    dsn = cfg.database_url.get_secret_value().replace("+asyncpg", "")
    _pool = await asyncpg.create_pool(dsn, min_size=2, max_size=10)
    if not _pool_initialised:
        # Safety net: ensure table exists even if Alembic migration hasn't run.
        # The canonical schema is managed by alembic migration c3d4e5f6a7b8.
        async with _pool.acquire() as conn:
            await conn.execute(_CREATE_EXTENSION)
            await conn.execute(_CREATE_CHUNKS_TABLE.format(dim=cfg.embedding_dimensions))
            for idx_sql in _CREATE_INDEXES:
                await conn.execute(idx_sql)
        _pool_initialised = True
    return _pool


async def close_pool() -> None:
    """Shut down the pool. Called from app lifespan."""
    global _pool, _pool_initialised  # noqa: PLW0603
    if _pool is not None:
        await _pool.close()
        _pool = None
        _pool_initialised = False


class PgVectorStore(VectorStore):
    def __init__(self, pool: asyncpg.Pool, dimensions: int = 1536):
        self._pool = pool
        self._dims = dimensions

    @classmethod
    async def create(cls) -> "PgVectorStore":
        cfg = get_settings()
        pool = await _get_pool()
        return cls(pool, dimensions=cfg.embedding_dimensions)

    async def upsert(self, chunks: list[ChunkData]) -> None:
        records = [
            (c.id, c.doc_id, c.kb_id, c.content, json.dumps(c.embedding), c.chunk_index, c.parent_id)
            for c in chunks
        ]
        async with self._pool.acquire() as conn:
            await conn.executemany(
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
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
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
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
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
        async with self._pool.acquire() as conn:
            await conn.execute("DELETE FROM chunks WHERE doc_id = $1", doc_id)

    async def delete_by_kb(self, kb_id: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute("DELETE FROM chunks WHERE kb_id = $1", kb_id)

    async def get_by_ids(self, chunk_ids: list[str]) -> list[SearchResult]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT id, content, doc_id, parent_id FROM chunks WHERE id = ANY($1)",
                chunk_ids,
            )
        return [SearchResult(chunk_id=r["id"], content=r["content"], score=1.0, doc_id=r["doc_id"], parent_id=r["parent_id"]) for r in rows]

    async def close(self) -> None:
        # No-op: pool lifetime is managed at module level, not per-store.
        pass

    async def __aenter__(self) -> "PgVectorStore":
        return self

    async def __aexit__(self, *args) -> None:
        pass
