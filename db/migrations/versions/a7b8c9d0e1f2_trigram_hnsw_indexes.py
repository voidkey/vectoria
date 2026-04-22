"""trigram + hnsw indexes on chunks

Revision ID: a7b8c9d0e1f2
Revises: f6a7b8c9d0e1
Create Date: 2026-04-22 12:30:00.000000

W6-1a retrieval-quality fix:

Keyword search
--------------
The existing ``content_tsv`` column uses ``to_tsvector('simple', ...)``
which tokenizes by whitespace / punctuation. CJK text has no word
separators, so a Chinese paragraph becomes exactly one token and
``plainto_tsquery('simple', 'some Chinese query')`` matches effectively
nothing unless the entire content is the exact query. In our traffic
~70% of ingested content is Chinese (WeChat / Xiaohongshu / X / …),
so BM25-style keyword recall was silently near-zero for those KBs —
hybrid search was effectively vector-only.

Swap to ``pg_trgm`` (stdlib extension since 9.1): a GIN trigram index
on ``content`` supports character-level similarity scoring that works
identically for CJK, mixed, and Latin text. No extra Docker deps, no
new tokenizer to install, one migration.

The legacy ``content_tsv`` column + ``chunks_tsv_idx`` are kept in place
for rollback safety; a follow-up migration removes them once we've
lived with trigram for a release cycle.

Vector search
-------------
There was no ANN index on ``chunks.embedding`` — pgvector falls back
to an exact scan + sort. At ~23K chunks per KB the latency is single-
digit ms; past ~100K the linear scan becomes the dominant cost of
``/query``. Add an HNSW index with ``vector_cosine_ops`` so retrieval
latency stays flat as the KB grows.

HNSW interacts with the ``WHERE kb_id = ...`` filter via post-filter;
for very selective filters (tiny KBs) the planner will still choose
the btree kb_id index + exact sort. Setting ``hnsw.ef_search`` at
query time (done in vectorstore/pgvector.py) gives the retrieval step
a recall knob without changing the index.
"""
from typing import Sequence, Union

from alembic import op


revision: str = 'a7b8c9d0e1f2'
down_revision: Union[str, Sequence[str], None] = 'f6a7b8c9d0e1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    op.execute(
        "CREATE INDEX IF NOT EXISTS chunks_content_trgm_idx "
        "ON chunks USING GIN (content gin_trgm_ops)"
    )
    # HNSW index on embeddings. Build params balance index build time
    # against recall; m=16, ef_construction=64 are pgvector's
    # recommended defaults. Build is O(N log N) — on a typical 23K-row
    # table it finishes in seconds; a large-KB deploy (1M+) should
    # CREATE INDEX CONCURRENTLY instead, but alembic's standard op
    # can't do that transactionally.
    op.execute(
        "CREATE INDEX IF NOT EXISTS chunks_embedding_hnsw_idx "
        "ON chunks USING hnsw (embedding vector_cosine_ops) "
        "WITH (m = 16, ef_construction = 64)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS chunks_embedding_hnsw_idx")
    op.execute("DROP INDEX IF EXISTS chunks_content_trgm_idx")
    # Keep pg_trgm extension — other features may depend on it.
