"""add page to document_images

Revision ID: d0e1f2a3b4c5
Revises: c9d0e1f2a3b4
Create Date: 2026-04-30 10:00:00.000000

1-based page number for paginated source docs (PDF), populated from
MinerU's content_list. Nullable because non-paginated formats and
pre-existing rows have no meaningful page; backfill is not required —
old rows simply read as ``page IS NULL`` in queries.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'd0e1f2a3b4c5'
down_revision: Union[str, None] = 'c9d0e1f2a3b4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'document_images',
        sa.Column('page', sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column('document_images', 'page')
