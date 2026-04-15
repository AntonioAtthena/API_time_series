"""
Data ingestion service: parses period strings and upserts rows into the database.

Period format reference (Brazilian CVM filings):
    {N}M{YY}  — N-month cumulative window within year 20YY.
                 '12M19' = full year 2019 (Jan 1 → Dec 31)
                 '9M24'  = first 9 months of 2024 (Jan 1 → Sep 30)
                 '6M22'  = first 6 months of 2022 (Jan 1 → Jun 30)

    {N}T{YY}  — Nth trimestre (quarter) of year 20YY.
                 '1T20'  = Q1 2020 (Jan 1  → Mar 31)
                 '2T20'  = Q2 2020 (Apr 1  → Jun 30)
                 '3T20'  = Q3 2020 (Jul 1  → Sep 30)
                 '4T20'  = Q4 2020 (Oct 1  → Dec 31)

Upsert strategy — INSERT … ON CONFLICT DO UPDATE:
    Re-running the same file must not create duplicate rows. SQLite 3.24+
    supports this natively. The four-column key (metric_id, period,
    entity_scope, file) maps to the UniqueConstraint defined on the model.
    created_at is intentionally excluded from the update set so the original
    insertion timestamp is preserved across re-uploads.
"""

import re
from calendar import monthrange
from datetime import date
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models import Datapoint
from app.schemas import RawDatapoint, UploadResponse

# Pick the dialect-specific insert that matches the configured database.
# Both SQLite (3.24+) and PostgreSQL (9.5+) support ON CONFLICT DO UPDATE,
# but the Insert subclass must come from the correct dialect so SQLAlchemy
# compiles the statement correctly against the active connection.
if settings.database_url.startswith("sqlite"):
    from sqlalchemy.dialects.sqlite import insert as _dialect_insert
else:
    from sqlalchemy.dialects.postgresql import insert as _dialect_insert


# ── Period parsing ────────────────────────────────────────────────────────────

_NM_PATTERN = re.compile(r"^(\d{1,2})M(\d{2})$", re.IGNORECASE)
_NT_PATTERN = re.compile(r"^(\d)T(\d{2})$", re.IGNORECASE)

# Quarter boundaries: (start_month, start_day, end_month, end_day)
_QUARTER_BOUNDS: dict[int, tuple[int, int, int, int]] = {
    1: (1, 1, 3, 31),
    2: (4, 1, 6, 30),
    3: (7, 1, 9, 30),
    4: (10, 1, 12, 31),
}


def parse_period(period: str) -> tuple[date, date]:
    """Convert a CVM period tag into (start_date, end_date).

    Args:
        period: Raw period string from the filing JSON (e.g. '12M19', '1T20').

    Returns:
        A tuple of (period_start, period_end) as Python date objects.

    Raises:
        ValueError: If the period string does not match any known format.

    Examples:
        >>> parse_period('12M19')
        (date(2019, 1, 1), date(2019, 12, 31))
        >>> parse_period('9M24')
        (date(2024, 1, 1), date(2024, 9, 30))
        >>> parse_period('1T20')
        (date(2020, 1, 1), date(2020, 3, 31))
        >>> parse_period('3T23')
        (date(2023, 7, 1), date(2023, 9, 30))
    """
    m = _NM_PATTERN.match(period)
    if m:
        months = int(m.group(1))
        year = 2000 + int(m.group(2))
        if not (1 <= months <= 12):
            raise ValueError(f"Month count out of range in period '{period}': {months}")
        start = date(year, 1, 1)
        last_day = monthrange(year, months)[1]
        end = date(year, months, last_day)
        return start, end

    m = _NT_PATTERN.match(period)
    if m:
        quarter = int(m.group(1))
        year = 2000 + int(m.group(2))
        if quarter not in _QUARTER_BOUNDS:
            raise ValueError(f"Quarter out of range in period '{period}': {quarter}")
        sm, sd, em, ed = _QUARTER_BOUNDS[quarter]
        return date(year, sm, sd), date(year, em, ed)

    raise ValueError(
        f"Unrecognised period format: '{period}'. "
        "Expected '{N}M{YY}' (e.g. '12M19') or '{N}T{YY}' (e.g. '1T20')."
    )


# ── Ingestion logic ───────────────────────────────────────────────────────────


async def ingest_datapoints(
    rows: list[RawDatapoint],
    db: AsyncSession,
    source_filename: str,
) -> UploadResponse:
    """Parse, validate, and upsert a list of data points into the database.

    Args:
        rows: Validated RawDatapoint objects parsed from the uploaded JSON.
        db: An active async SQLAlchemy session (injected by FastAPI).
        source_filename: The original filename from the HTTP upload.

    Returns:
        UploadResponse summarising how many rows were inserted vs. updated.
    """
    errors: list[str] = []
    records: list[dict[str, Any]] = []

    for idx, row in enumerate(rows):
        try:
            period_start, period_end = parse_period(row.period)
        except ValueError as exc:
            errors.append(f"Row {idx} ({row.metric_id} / {row.period}): {exc}")
            continue

        records.append(
            {
                "metric_id": row.metric_id,
                "metric_name": row.metric_name,
                "period": row.period,
                "period_type": row.period_type,
                "period_start": period_start,
                "period_end": period_end,
                "value": row.value,
                "scale_power": row.scale_power,
                "entity_scope": row.entity_scope,
                "source": row.source,
                "filing": row.filing,
                "file": row.file,
                "table_id": row.table_id,
                "raw_metric": row.raw_metric,
            }
        )

    if not records:
        return UploadResponse(
            filename=source_filename,
            total_rows=len(rows),
            inserted=0,
            updated=0,
            errors=errors,
        )

    # Count pre-existing rows to distinguish inserts vs. updates in the summary.
    result = await db.execute(
        select(Datapoint.metric_id, Datapoint.period, Datapoint.entity_scope, Datapoint.file).where(
            Datapoint.file.in_({r["file"] for r in records})
        )
    )
    pre_existing = {(row.metric_id, row.period, row.entity_scope, row.file) for row in result}
    pre_existing_count = sum(
        1 for r in records
        if (r["metric_id"], r["period"], r["entity_scope"], r["file"]) in pre_existing
    )

    # Upsert: INSERT … ON CONFLICT(unique columns) DO UPDATE SET …
    # index_elements must match the four columns in the UniqueConstraint.
    # created_at is excluded so the original insertion timestamp is preserved.
    stmt = _dialect_insert(Datapoint).values(records)
    upsert_stmt = stmt.on_conflict_do_update(
        index_elements=["metric_id", "period", "entity_scope", "file"],
        set_={
            "metric_name": stmt.excluded.metric_name,
            "period_type": stmt.excluded.period_type,
            "period_start": stmt.excluded.period_start,
            "period_end": stmt.excluded.period_end,
            "value": stmt.excluded.value,
            "scale_power": stmt.excluded.scale_power,
            "source": stmt.excluded.source,
            "filing": stmt.excluded.filing,
            "table_id": stmt.excluded.table_id,
            "raw_metric": stmt.excluded.raw_metric,
            "updated_at": func.now(),
        },
    )

    await db.execute(upsert_stmt)
    await db.commit()

    inserted = len(records) - pre_existing_count
    updated = pre_existing_count

    return UploadResponse(
        filename=source_filename,
        total_rows=len(rows),
        inserted=inserted,
        updated=updated,
        errors=errors,
    )
