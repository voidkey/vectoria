"""create chunks table under alembic management

Revision ID: c3d4e5f6a7b8
Revises: b2c3d4e5f6a7
Create Date: 2026-04-16 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c3d4e5f6a7b8'
down_revision: Union[str, Sequence[str], None] = 'b2c3d4e5f6a7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # pgvector extension (idempotent)
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")

    # Create chunks table only if it doesn't already exist (may have been
    # created by the old PgVectorStore._ensure_schema code path).
    op.execute("""
    CREATE TABLE IF NOT EXISTS chunks (
        id          TEXT PRIMARY KEY,
        doc_id      TEXT NOT NULL,
        kb_id       TEXT NOT NULL,
        content     TEXT NOT NULL,
        embedding   vector(1536),
        chunk_index INTEGER NOT NULL DEFAULT 0,
        parent_id   TEXT,
        content_tsv tsvector GENERATED ALWAYS AS (to_tsvector('simple', content)) STORED
    )
    """)

    # Indexes (idempotent)
    op.execute("CREATE INDEX IF NOT EXISTS chunks_kb_id_idx ON chunks (kb_id)")
    op.execute("CREATE INDEX IF NOT EXISTS chunks_doc_id_idx ON chunks (doc_id)")
    op.execute("CREATE INDEX IF NOT EXISTS chunks_tsv_idx ON chunks USING GIN (content_tsv)")


def downgrade() -> None:
    op.drop_table("chunks")
