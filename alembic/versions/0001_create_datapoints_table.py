"""Create datapoints table.

Revision ID: 0001
Revises:
Create Date: 2026-04-10
"""

from alembic import op
import sqlalchemy as sa

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "datapoints",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("metric_id", sa.Text(), nullable=False),
        sa.Column("metric_name", sa.Text(), nullable=True),
        sa.Column("period", sa.Text(), nullable=False),
        sa.Column("period_type", sa.Text(), nullable=True),
        sa.Column("period_start", sa.Date(), nullable=False),
        sa.Column("period_end", sa.Date(), nullable=False),
        sa.Column("value", sa.Numeric(precision=20, scale=4), nullable=False),
        sa.Column("scale_power", sa.Integer(), nullable=False),
        sa.Column("entity_scope", sa.Text(), nullable=False),
        sa.Column("source", sa.Text(), nullable=True),
        sa.Column("filing", sa.Text(), nullable=True),
        sa.Column("file", sa.Text(), nullable=False),
        sa.Column("table_id", sa.Integer(), nullable=True),
        sa.Column("raw_metric", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "metric_id", "period", "entity_scope", "file",
            name="uq_datapoint_source",
        ),
    )

    # Composite index for the primary query pattern: metric + date range
    op.create_index("ix_dp_metric_period", "datapoints", ["metric_id", "period_start", "period_end"])
    # Scope filter index
    op.create_index("ix_dp_scope", "datapoints", ["entity_scope"])
    # Filing filter / audit index
    op.create_index("ix_dp_filing", "datapoints", ["filing"])


def downgrade() -> None:
    op.drop_index("ix_dp_filing", table_name="datapoints")
    op.drop_index("ix_dp_scope", table_name="datapoints")
    op.drop_index("ix_dp_metric_period", table_name="datapoints")
    op.drop_table("datapoints")
