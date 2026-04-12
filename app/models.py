"""
SQLAlchemy ORM models for the financial time-series database.

Rationale — single-table design:
    The source data is already flat (one row = one data point). Splitting
    into a metrics lookup table would add joins for minimal storage savings
    given the small cardinality of metric_ids. A single denormalised table
    gives simpler queries and faster reads for Excel/Power Query consumers.

Rationale — unique constraint on (metric_id, period, entity_scope, file):
    - 'file' identifies the exact source document (e.g. '102263_v1.json').
    - Two filings can legitimately report the same (metric, period, scope)
      with different values (restated comparatives), so 'filing' alone is
      insufficient.
    - Using 'file' ensures that re-uploading the same source file is
      idempotent (upsert), while still allowing the same period to appear
      from a different source file.

Rationale — period_start / period_end DATE columns:
    Storing parsed dates alongside the raw period tag enables chronological
    ORDER BY and date-range WHERE clauses, which are not possible with the
    raw string ('12M19', '1T20', ...). Excel Power Query can use these for
    proper time-axis charts.
"""

from datetime import date, datetime

from sqlalchemy import (
    BigInteger,
    Date,
    DateTime,
    Index,
    Integer,
    Numeric,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects import sqlite as _sqlite_dialect
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class Datapoint(Base):
    """A single financial time-series observation extracted from a filing.

    Attributes:
        id: Auto-incrementing surrogate primary key.
        metric_id: Normalised metric identifier (e.g. 'baixa_imobilizado').
        metric_name: Human-readable Portuguese label for the metric.
        period: Raw period tag from the source filing (e.g. '12M19', '1T20').
        period_type: Classification of the period window
            ('full_year' | 'quarter' | 'year_to_date').
        period_start: First day of the reporting period (computed on ingest).
        period_end: Last day of the reporting period (computed on ingest).
        value: Raw numeric value as reported in the filing.
        scale_power: Power-of-10 multiplier (3 = thousands, 6 = millions).
        entity_scope: Whether the data is for the consolidated group
            ('consolidado') or the parent company only ('controladora').
        source: Provenance label (typically 'reported').
        filing: Human-readable filing reference (e.g. 'DFP 2020 Ann').
        file: Source filename used as the deduplication key.
        table_id: Table identifier within the source filing document.
        raw_metric: Original metric label text as it appeared in the filing;
            may differ across filings for the same metric_id due to rewording.
        created_at: Timestamp of first insertion.
        updated_at: Timestamp of last upsert update.
    """

    __tablename__ = "datapoints"

    # BigInteger on PostgreSQL; Integer on SQLite so ROWID aliasing kicks in
    # and autoincrement works without supplying an explicit id value.
    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer(), "sqlite"),
        primary_key=True,
        autoincrement=True,
    )

    # ── Core identifiers ──────────────────────────────────────────────────────
    metric_id: Mapped[str] = mapped_column(Text, nullable=False)
    metric_name: Mapped[str | None] = mapped_column(Text, nullable=True)

    # ── Period ────────────────────────────────────────────────────────────────
    period: Mapped[str] = mapped_column(Text, nullable=False)
    period_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    period_start: Mapped[date] = mapped_column(Date, nullable=False)
    period_end: Mapped[date] = mapped_column(Date, nullable=False)

    # ── Value ─────────────────────────────────────────────────────────────────
    # Numeric(20, 4): supports values up to 10^16 with 4 decimal places.
    # Avoids float rounding errors that would corrupt financial figures.
    value: Mapped[float] = mapped_column(Numeric(20, 4), nullable=False)
    scale_power: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # ── Scope & provenance ────────────────────────────────────────────────────
    entity_scope: Mapped[str] = mapped_column(Text, nullable=False)
    source: Mapped[str | None] = mapped_column(Text, nullable=True)
    filing: Mapped[str | None] = mapped_column(Text, nullable=True)
    file: Mapped[str] = mapped_column(Text, nullable=False)
    table_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    raw_metric: Mapped[str | None] = mapped_column(Text, nullable=True)

    # ── Audit timestamps ──────────────────────────────────────────────────────
    created_at: Mapped[datetime] = mapped_column(
        DateTime(), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(), nullable=True
    )

    __table_args__ = (
        # Deduplication constraint — see module docstring for rationale.
        UniqueConstraint(
            "metric_id",
            "period",
            "entity_scope",
            "file",
            name="uq_datapoint_source",
        ),
        # Rationale — composite index on (metric_id, period_start, period_end):
        #   The most common query pattern is "give me all periods for metric X
        #   between date A and date B". This index satisfies both the equality
        #   filter on metric_id and the range scan on the date columns.
        Index("ix_dp_metric_period", "metric_id", "period_start", "period_end"),
        # Rationale — separate index on entity_scope:
        #   Queries frequently filter by scope alone (e.g. Power Query pulling
        #   all consolidated metrics). A dedicated index avoids full-table scans.
        Index("ix_dp_scope", "entity_scope"),
        # Rationale — index on filing:
        #   Useful for the upload endpoint to check which filings are already
        #   stored and for audit queries.
        Index("ix_dp_filing", "filing"),
    )
