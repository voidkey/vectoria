"""add owner to knowledge_bases

Revision ID: b7d2e4f6a8c1
Revises: a3f5c1d90b47
Create Date: 2026-10-19 15:00:00.000000

  owner_uid     — JWT ``uid`` of the end user who created the KB. JWT
                  callers (and services acting on behalf of a user) may
                  only reach KBs they own. NULL for service-created KBs
                  and for every KB created before this revision.
  owner_team_id — team scope (``meta.stid``) the creator was acting in;
                  recorded for audit, not enforced.

Both nullable with no default, so the ADD COLUMN is metadata-only on
PostgreSQL. Existing rows stay NULL until backfilled; see
``kb_legacy_unowned_access`` in config.py.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'b7d2e4f6a8c1'
down_revision: Union[str, None] = 'a3f5c1d90b47'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('knowledge_bases', sa.Column('owner_uid', sa.String(64), nullable=True))
    op.add_column('knowledge_bases', sa.Column('owner_team_id', sa.String(64), nullable=True))
    op.create_index('ix_knowledge_bases_owner_uid', 'knowledge_bases', ['owner_uid'])


def downgrade() -> None:
    op.drop_index('ix_knowledge_bases_owner_uid', table_name='knowledge_bases')
    op.drop_column('knowledge_bases', 'owner_team_id')
    op.drop_column('knowledge_bases', 'owner_uid')
