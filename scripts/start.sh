#!/bin/bash
# Container entrypoint: start the ASGI server.
# Tables are created automatically by SQLAlchemy on first startup (see main.py lifespan).
set -e

echo "==> Starting uvicorn..."
exec uvicorn app.main:app --host 0.0.0.0 --port 8000
