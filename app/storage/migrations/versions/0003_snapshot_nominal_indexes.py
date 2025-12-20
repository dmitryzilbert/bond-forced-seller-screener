from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0003_snapshot_nominal_indexes"
down_revision = "0002_universe_shortlist_flags"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "orderbook_snapshots",
        sa.Column("nominal", sa.Float(), nullable=False, server_default="1000"),
    )
    op.alter_column("orderbook_snapshots", "nominal", server_default=None)

    op.create_index(
        "ix_orderbook_snapshots_isin_ts",
        "orderbook_snapshots",
        ["isin", "ts"],
        unique=False,
    )
    op.create_index(
        "ix_events_isin_ts",
        "events",
        ["isin", "ts"],
        unique=False,
    )


def downgrade():
    op.drop_index("ix_events_isin_ts", table_name="events")
    op.drop_index("ix_orderbook_snapshots_isin_ts", table_name="orderbook_snapshots")
    op.drop_column("orderbook_snapshots", "nominal")
