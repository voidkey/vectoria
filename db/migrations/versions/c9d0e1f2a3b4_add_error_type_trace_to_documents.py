"""add error_type + error_trace to documents (bad-case collection)

Revision ID: c9d0e1f2a3b4
Revises: b8c9d0e1f2a3
Create Date: 2026-04-23 09:00:00.000000

W6: bad-case collection. ``error_msg`` (existing) is the short human
summary shown in the UI. ``error_type`` carries the structured outcome
(parse_error / empty_content / too_large / indexing_error / ...) so
digest queries can GROUP BY without string-parsing. ``error_trace``
holds the full Python traceback for repro — no cap, because the goal
is debugging, not UX.

The composite (status, created_at DESC) index makes the digest query
fast even with millions of rows: "failed documents in the last 24h"
becomes a range scan instead of a full table scan.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'c9d0e1f2a3b4'
down_revision: Union[str, Sequence[str], None] = 'b8c9d0e1f2a3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'documents',
        sa.Column('error_type', sa.String(length=32), nullable=True),
    )
    op.add_column(
        'documents',
        sa.Column('error_trace', sa.Text(), nullable=True),
    )
    op.create_index(
        'ix_documents_status_created_at',
        'documents',
        ['status', sa.text('created_at DESC')],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index('ix_documents_status_created_at', table_name='documents')
    op.drop_column('documents', 'error_trace')
    op.drop_column('documents', 'error_type')
