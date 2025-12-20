from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = '0002_universe_shortlist_flags'
down_revision = '0001_initial'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('instruments', sa.Column('amortization_flag', sa.Boolean(), nullable=True))
    op.add_column('instruments', sa.Column('has_call_offer', sa.Boolean(), nullable=True))
    op.add_column('instruments', sa.Column('eligible', sa.Boolean(), nullable=False, server_default=sa.false()))
    op.add_column('instruments', sa.Column('eligible_reason', sa.String(), nullable=True))
    op.add_column('instruments', sa.Column('eligibility_checked_at', sa.DateTime(), nullable=True))
    op.add_column('instruments', sa.Column('is_shortlisted', sa.Boolean(), nullable=False, server_default=sa.false()))

    # clear server defaults
    op.alter_column('instruments', 'eligible', server_default=None)
    op.alter_column('instruments', 'is_shortlisted', server_default=None)


def downgrade():
    op.drop_column('instruments', 'is_shortlisted')
    op.drop_column('instruments', 'eligibility_checked_at')
    op.drop_column('instruments', 'eligible_reason')
    op.drop_column('instruments', 'eligible')
    op.drop_column('instruments', 'has_call_offer')
    op.drop_column('instruments', 'amortization_flag')
