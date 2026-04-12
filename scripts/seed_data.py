#!/usr/bin/env python3
"""
Seed script: uploads time_series.json to the running API.

Usage:
    # From the repo root, after 'docker-compose up':
    pip install requests          # one-time, if not already installed
    python scripts/seed_data.py

Environment overrides (optional):
    API_URL     Base URL of the running API  (default: http://localhost:8000)
    API_KEY     X-API-Key header value       (default: dev-api-key-change-me)
    DATA_FILE   Path to the JSON file        (default: ./time_series.json)

The script is fully idempotent — running it multiple times produces the same
database state because the API uses INSERT … ON CONFLICT DO UPDATE.
"""

import json
import os
import pathlib
import sys

try:
    import requests
except ImportError:
    sys.exit(
        "ERROR: 'requests' is not installed.\n"
        "Run:  pip install requests\n"
        "Then retry this script."
    )

# ── Configuration ─────────────────────────────────────────────────────────────

API_URL   = os.environ.get("API_URL",   "http://localhost:8001")
API_KEY   = os.environ.get("API_KEY",   "dev-api-key-change-me")
DATA_FILE = pathlib.Path(os.environ.get("DATA_FILE", "time_series.json"))

UPLOAD_ENDPOINT = f"{API_URL}/api/v1/upload"
HEALTH_ENDPOINT = f"{API_URL}/health"

# ── Health check ──────────────────────────────────────────────────────────────

print(f"Checking API health at {HEALTH_ENDPOINT} ...")
try:
    resp = requests.get(HEALTH_ENDPOINT, timeout=10)
    resp.raise_for_status()
    print(f"  API is up: {resp.json()}")
except requests.exceptions.ConnectionError:
    sys.exit(
        f"ERROR: Cannot reach {API_URL}.\n"
        "Make sure the containers are running:\n"
        "  docker-compose up -d\n"
        "Then wait a few seconds and retry."
    )
except requests.exceptions.HTTPError as exc:
    sys.exit(f"ERROR: Health check failed: {exc}")

# ── Load data file ────────────────────────────────────────────────────────────

if not DATA_FILE.exists():
    sys.exit(
        f"ERROR: Data file not found: {DATA_FILE}\n"
        "Run this script from the repository root, or set the DATA_FILE env var."
    )

print(f"\nLoading {DATA_FILE} ...")
with DATA_FILE.open("rb") as fh:
    raw_bytes = fh.read()

row_count = len(json.loads(raw_bytes))
print(f"  {row_count} rows found in file.")

# ── Upload ────────────────────────────────────────────────────────────────────

print(f"\nUploading to {UPLOAD_ENDPOINT} ...")
resp = requests.post(
    UPLOAD_ENDPOINT,
    headers={"X-API-Key": API_KEY},
    files={"file": (DATA_FILE.name, raw_bytes, "application/json")},
    timeout=60,
)

if resp.status_code == 401:
    sys.exit(
        "ERROR: 401 Unauthorized.\n"
        f"Check that API_KEY matches the API_KEYS value in docker-compose.yml.\n"
        f"Current API_KEY: {API_KEY!r}"
    )

resp.raise_for_status()
result = resp.json()

# ── Report ────────────────────────────────────────────────────────────────────

print("\n=== Ingest result ===")
print(f"  File       : {result['filename']}")
print(f"  Total rows : {result['total_rows']}")
print(f"  Inserted   : {result['inserted']}")
print(f"  Updated    : {result['updated']}")

if result["errors"]:
    print(f"\n  Warnings ({len(result['errors'])}):")
    for err in result["errors"]:
        print(f"    - {err}")
else:
    print("  Errors     : none")

print("\nDone. The database is ready.")
