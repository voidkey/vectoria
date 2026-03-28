"""update documents status default to indexing

Revision ID: 005bc745cf1b
Revises: 0034d96e823e
Create Date: 2026-03-28 21:06:39.226877

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '005bc745cf1b'
down_revision: Union[str, Sequence[str], None] = '0034d96e823e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        "documents", "status",
        server_default="indexing",
    )


def downgrade() -> None:
    op.alter_column(
        "documents", "status",
        server_default="processing",
    )
