"""add index_status to documents

Revision ID: e53807e0235a
Revises: e1f2a3b4c5d6
Create Date: 2026-06-03 17:39:20.885847

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = 'e53807e0235a'
down_revision: Union[str, None] = 'e1f2a3b4c5d6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "documents",
        sa.Column(
            "index_status", sa.String(length=20),
            nullable=False, server_default="pending",
        ),
    )
    # Backfill from existing state, without mutating `status`. Each WHERE is
    # mutually exclusive.
    op.execute(
        "UPDATE documents SET index_status='completed' "
        "WHERE status='completed' AND chunk_count > 0"
    )
    # completed + 0 chunks == nothing indexable was embedded: image_only docs
    # (error_type='image_only') and any empty-body completion. 'skipped' is the
    # correct terminal index state for them.
    op.execute(
        "UPDATE documents SET index_status='skipped' "
        "WHERE status='completed' AND chunk_count = 0"
    )
    op.execute(
        "UPDATE documents SET index_status='failed' "
        "WHERE status='failed' AND error_type='indexing_error'"
    )
    op.execute(
        "UPDATE documents SET index_status='skipped' "
        "WHERE status='failed' AND error_type IS DISTINCT FROM 'indexing_error'"
    )
    # Rows not matched above (status in queued/parsing/indexing — i.e. genuinely
    # in-flight) keep the 'pending' server_default, which is the correct
    # in-flight state; the worker will set the terminal value when it runs.


def downgrade() -> None:
    op.drop_column("documents", "index_status")
