from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = '0001_initial'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'instruments',
        sa.Column('isin', sa.String(), primary_key=True),
        sa.Column('figi', sa.String()),
        sa.Column('name', sa.String()),
        sa.Column('issuer', sa.String()),
        sa.Column('nominal', sa.Float()),
        sa.Column('maturity_date', sa.Date()),
        sa.Column('segment', sa.String()),
        sa.Column('updated_at', sa.DateTime()),
    )
    op.create_table(
        'orderbook_snapshots',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('isin', sa.String()),
        sa.Column('ts', sa.DateTime()),
        sa.Column('bids_json', sa.JSON()),
        sa.Column('asks_json', sa.JSON()),
        sa.Column('best_bid', sa.Float()),
        sa.Column('best_ask', sa.Float()),
    )
    op.create_table(
        'events',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('isin', sa.String()),
        sa.Column('ts', sa.DateTime()),
        sa.Column('ytm_mid', sa.Float()),
        sa.Column('ytm_event', sa.Float()),
        sa.Column('delta_ytm_bps', sa.Float()),
        sa.Column('ask_lots_window', sa.Float()),
        sa.Column('ask_notional_window', sa.Float()),
        sa.Column('spread_ytm_bps', sa.Float()),
        sa.Column('score', sa.Float()),
        sa.Column('stress_flag', sa.Boolean()),
        sa.Column('near_maturity_flag', sa.Boolean()),
        sa.Column('payload_json', sa.JSON()),
    )


def downgrade():
    op.drop_table('events')
    op.drop_table('orderbook_snapshots')
    op.drop_table('instruments')
