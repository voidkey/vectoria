"""document kind + profile for site captures

Revision ID: bfec50b35889
Revises: e53807e0235a
Create Date: 2026-07-14 11:21:12.535790

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'bfec50b35889'
down_revision: Union[str, Sequence[str], None] = 'e53807e0235a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("documents", sa.Column(
        "kind", sa.String(length=20), nullable=False, server_default="document",
    ))
    op.add_column("documents", sa.Column("profile", sa.JSON(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("documents", "profile")
    op.drop_column("documents", "kind")
