"""add file_hash_sha256 to documents

Revision ID: f6a7b8c9d0e1
Revises: e5f6a7b8c9d0
Create Date: 2026-04-21 15:20:00.000000

W5-4: MD5 is kept as a legacy read-only fallback during the migration
period — existing rows carry only ``file_hash`` (MD5), and writers
dual-write until all live dedup keys migrate over. Callers read
``file_hash_sha256`` first; if NULL, fall back to MD5 (old data).

Once the retention window has rolled over (all live docs have a
sha256 populated), a follow-up migration can drop the MD5 column.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'f6a7b8c9d0e1'
down_revision: Union[str, Sequence[str], None] = 'e5f6a7b8c9d0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        'documents',
        sa.Column('file_hash_sha256', sa.String(length=64), nullable=True),
    )
    op.create_index(
        'ix_documents_file_hash_sha256',
        'documents',
        ['file_hash_sha256'],
        unique=False,
    )
    op.create_index(
        'ix_documents_kb_id_file_hash_sha256',
        'documents',
        ['kb_id', 'file_hash_sha256'],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(
        'ix_documents_kb_id_file_hash_sha256', table_name='documents',
    )
    op.drop_index(
        'ix_documents_file_hash_sha256', table_name='documents',
    )
    op.drop_column('documents', 'file_hash_sha256')
