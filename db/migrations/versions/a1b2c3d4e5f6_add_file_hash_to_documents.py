"""add file_hash to documents

Revision ID: a1b2c3d4e5f6
Revises: d7cc8faf7297
Create Date: 2026-04-15 22:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, Sequence[str], None] = 'd7cc8faf7297'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        'documents',
        sa.Column('file_hash', sa.String(length=32), nullable=True),
    )
    op.create_index(
        'ix_documents_file_hash', 'documents', ['file_hash'], unique=False,
    )
    # Composite index for per-KB dedup lookup.
    op.create_index(
        'ix_documents_kb_id_file_hash', 'documents', ['kb_id', 'file_hash'],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index('ix_documents_kb_id_file_hash', table_name='documents')
    op.drop_index('ix_documents_file_hash', table_name='documents')
    op.drop_column('documents', 'file_hash')
