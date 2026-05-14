"""add page_count to documents

Revision ID: e1f2a3b4c5d6
Revises: d0e1f2a3b4c5
Create Date: 2026-05-14 10:00:00.000000

Source-document page/slide count (PDF pages, PPTX slides). Nullable
because legacy/binary formats and pre-existing rows have no value;
docx/doc never populate this column (Word's notion of "page" depends
on render-time layout — there's no honest static answer).
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'e1f2a3b4c5d6'
down_revision: Union[str, None] = 'd0e1f2a3b4c5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'documents',
        sa.Column('page_count', sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column('documents', 'page_count')
