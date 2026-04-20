"""add phash to document_images

Revision ID: e5f6a7b8c9d0
Revises: d4e5f6a7b8c9
Create Date: 2026-04-20 21:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = 'e5f6a7b8c9d0'
down_revision: Union[str, None] = 'd4e5f6a7b8c9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 16-hex-char = 64-bit phash. Nullable because existing rows have
    # no computed value and we don't want to block the migration on a
    # backfill — backfill happens lazily as images are re-indexed.
    op.add_column(
        'document_images',
        sa.Column('phash', sa.String(16), nullable=True),
    )
    op.create_index(
        'ix_document_images_phash',
        'document_images',
        ['phash'],
    )


def downgrade() -> None:
    op.drop_index('ix_document_images_phash', table_name='document_images')
    op.drop_column('document_images', 'phash')
