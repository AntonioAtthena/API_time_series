"""
Upload router — accepts a JSON file and ingests it into the database.

Endpoint: POST /api/v1/upload
"""

import json

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import require_api_key
from app.database import get_db
from app.schemas import RawDatapoint, UploadResponse
from app.services.ingest import ingest_datapoints

router = APIRouter(
    prefix="/api/v1",
    tags=["Upload"],
    dependencies=[Depends(require_api_key)],
)

# Max allowed file size: 50 MB.  Prevents accidental uploads of giant files.
_MAX_FILE_BYTES = 50 * 1024 * 1024


@router.post(
    "/upload",
    response_model=UploadResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Ingest a time-series JSON file",
    description=(
        "Accepts a JSON array of data point objects (matching the time_series.json format). "
        "Each row is validated and upserted into the database. "
        "Re-uploading the same file is safe — existing rows are updated, not duplicated."
    ),
)
async def upload_filing(
    file: UploadFile = File(..., description="JSON array file (time_series.json format)"),
    db: AsyncSession = Depends(get_db),
) -> UploadResponse:
    """Parse and ingest an uploaded JSON filing file.

    Args:
        file: The uploaded JSON file (multipart/form-data).
        db: Injected async database session.

    Returns:
        UploadResponse with insert/update counts and any row-level errors.

    Raises:
        HTTPException 400: If the file is not valid JSON or not a JSON array.
        HTTPException 413: If the file exceeds 50 MB.
    """
    # ── Size guard ────────────────────────────────────────────────────────────
    contents = await file.read()
    if len(contents) > _MAX_FILE_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"File exceeds the 50 MB limit ({len(contents):,} bytes received).",
        )

    # ── JSON decode ───────────────────────────────────────────────────────────
    # Rationale — explicit UTF-8 decode before json.loads:
    #   The source files may originate from Windows tools that embed a UTF-8
    #   BOM. Using 'utf-8-sig' strips the BOM silently if present.
    try:
        text_content = contents.decode("utf-8-sig")
        raw_data = json.loads(text_content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid JSON: {exc}",
        )

    if not isinstance(raw_data, list):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Expected a JSON array at the root level.",
        )

    if len(raw_data) == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The JSON array is empty.",
        )

    # ── Row-level Pydantic validation ─────────────────────────────────────────
    # Rationale — validate all rows and collect errors rather than failing fast:
    #   A partial upload is worse than a complete one. We report all bad rows
    #   so the user can fix the source file in one pass.
    validated_rows: list[RawDatapoint] = []
    validation_errors: list[str] = []

    for idx, item in enumerate(raw_data):
        try:
            validated_rows.append(RawDatapoint.model_validate(item))
        except ValidationError as exc:
            validation_errors.append(f"Row {idx}: {exc.error_count()} error(s) — {exc.errors()[0]['msg']}")

    # ── Ingest valid rows ─────────────────────────────────────────────────────
    response = await ingest_datapoints(validated_rows, db, file.filename or "unknown")
    response.errors = validation_errors + response.errors
    return response
