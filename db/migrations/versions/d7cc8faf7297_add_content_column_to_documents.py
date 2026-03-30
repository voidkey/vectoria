"""add content column to documents

Revision ID: d7cc8faf7297
Revises: 005bc745cf1b
Create Date: 2026-03-30 14:56:49.203878

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd7cc8faf7297'
down_revision: Union[str, Sequence[str], None] = '005bc745cf1b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('documents', sa.Column('content', sa.Text(), server_default='', nullable=False))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('documents', 'content')
