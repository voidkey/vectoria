"""add error_code to documents

Revision ID: f1a2b3c4d5e6
Revises: bfec50b35889
Create Date: 2026-07-16 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f1a2b3c4d5e6'
down_revision: Union[str, Sequence[str], None] = 'bfec50b35889'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "documents",
        sa.Column("error_code", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("documents", "error_code")
