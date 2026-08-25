"""add user-edited content pointer to documents

Revision ID: a3f5c1d90b47
Revises: f1a2b3c4d5e6
Create Date: 2026-08-25 10:00:00.000000

Downstream consumers post-process our parse output (LLM cleanup,
restructuring) and need somewhere to put the improved version. These
three columns are a *pointer*, deliberately orthogonal to ``content``:

  edited_storage_key — object key of the current edited artifact, or
                       NULL when the document has never been edited (or
                       the edit was withdrawn via DELETE .../edited).
  edited_revision    — monotonic counter, bumped atomically on every
                       write. Never reset by DELETE, so a withdrawn
                       revision's key is never reused by a later write.
  edited_at          — timestamp of the current edit; NULL alongside
                       a NULL storage key.

``content`` keeps its existing meaning (our parse output) and stays the
only input to chunking/embedding — edits do not enter retrieval. See
docs/API.md §4.5.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'a3f5c1d90b47'
down_revision: Union[str, None] = 'f1a2b3c4d5e6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'documents',
        sa.Column('edited_storage_key', sa.Text(), nullable=True),
    )
    # server_default so the backfill of existing rows is done by PG in
    # one pass instead of a Python-side UPDATE over the whole table.
    op.add_column(
        'documents',
        sa.Column(
            'edited_revision', sa.Integer(), nullable=False, server_default='0',
        ),
    )
    op.add_column(
        'documents',
        sa.Column('edited_at', sa.DateTime(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column('documents', 'edited_at')
    op.drop_column('documents', 'edited_revision')
    op.drop_column('documents', 'edited_storage_key')
