#!/bin/bash
# Container entrypoint: apply pending Alembic migrations then start the ASGI server.
# Running migrations here (rather than inside the Python app) keeps schema management
# out of the hot path and ensures the DB is fully migrated before traffic arrives.
set -e

echo "==> Running database migrations..."
python -m alembic upgrade head

echo "==> Starting uvicorn..."
exec uvicorn app.main:app --host 0.0.0.0 --port 8000
