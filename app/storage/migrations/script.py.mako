"""Generic single-database configuration."""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = 'initial'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    pass


def downgrade():
    pass
