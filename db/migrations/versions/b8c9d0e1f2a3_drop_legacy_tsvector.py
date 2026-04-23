"""drop legacy content_tsv column and index on chunks

Revision ID: b8c9d0e1f2a3
Revises: a7b8c9d0e1f2
Create Date: 2026-04-22 14:30:00.000000

W6-6 cleanup. The ``content_tsv`` generated column + ``chunks_tsv_idx``
GIN index were kept through W6-1a as a rollback safety net — in case
the new trigram-based ``keyword_search`` turned out worse in
production. After several days of live traffic with the trigram
path proving out on real CJK queries, the legacy tsvector
surface is pure storage waste (every chunk carries a second tokenized
representation nothing reads).

Dropping reduces per-row write cost (one fewer generated column to
populate on INSERT/UPDATE) and frees the ~10% of chunks-table size
the GIN index consumed.

Downgrade recreates both, but the column is GENERATED so the historic
values materialise automatically from ``content``.
"""
from typing import Sequence, Union

from alembic import op


revision: str = 'b8c9d0e1f2a3'
down_revision: Union[str, Sequence[str], None] = 'a7b8c9d0e1f2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("DROP INDEX IF EXISTS chunks_tsv_idx")
    op.execute("ALTER TABLE chunks DROP COLUMN IF EXISTS content_tsv")


def downgrade() -> None:
    # Re-add as generated column; row values auto-populate from content.
    op.execute(
        "ALTER TABLE chunks ADD COLUMN content_tsv tsvector "
        "GENERATED ALWAYS AS (to_tsvector('simple', content)) STORED"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS chunks_tsv_idx "
        "ON chunks USING GIN (content_tsv)"
    )
