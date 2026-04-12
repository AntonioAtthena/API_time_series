# Financial Time-Series API

A lightweight FastAPI service that ingests `time_series.json` into a local SQLite database and exposes a flat JSON endpoint optimised for Excel Power Query.

**No Docker. No PostgreSQL. No configuration.**
Just Python, `pip install`, and one command.

---

## Quick Start

### 1. Install dependencies

```bash
cd financial_api
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS / Linux
source .venv/bin/activate

pip install -r requirements.txt
```

### 2. Start the API

```bash
uvicorn app.main:app --reload
```

On first launch SQLAlchemy creates `financial.db` automatically — no migration commands needed.

```
INFO:     Started server process
INFO:     Waiting for application startup.
INFO:     Application startup complete.
INFO:     Uvicorn running on http://127.0.0.1:8000
```

### 3. Seed the database

Open a second terminal (with the venv active), then from the repo root:

```bash
python scripts/seed_data.py
```

Expected output:

```
Checking API health at http://localhost:8000/health ...
  API is up: {'status': 'ok'}

Loading time_series.json ...
  263 rows found in file.

Uploading to http://localhost:8000/api/v1/upload ...

=== Ingest result ===
  File       : time_series.json
  Total rows : 263
  Inserted   : 263
  Updated    : 0
  Errors     : none

Done. The database is ready.
```

Re-running the seed script is safe — it upserts, so you'll see `Inserted: 0, Updated: 263` on subsequent runs.

### 4. Browse the interactive docs

Open **http://localhost:8000/docs** in your browser to explore and test every endpoint with the built-in Swagger UI.

---

## Verify the data was ingested

```bash
# Count rows — look for "total": 263
curl http://localhost:8000/api/v1/datapoints?page_size=1 -H "X-API-Key: dev-api-key-change-me"

# List all 13 metrics
curl http://localhost:8000/api/v1/metrics -H "X-API-Key: dev-api-key-change-me"

# List all filings
curl http://localhost:8000/api/v1/filings -H "X-API-Key: dev-api-key-change-me"
```

---

## Pull into Excel Power Query (one click)

Use the `/flat` endpoint — it returns a bare JSON array that Power Query renders as a Table immediately, without any field expansion.

### Step-by-step

1. In Excel: **Data → Get Data → From Web → Advanced**
2. Enter the URL:
   ```
   http://localhost:8000/api/v1/datapoints/flat
   ```
3. Add HTTP request header:

   | Header name | Value |
   |-------------|-------|
   | `X-API-Key` | `dev-api-key-change-me` |

4. Click **OK** → Power Query opens a Table with all 263 rows sorted by `period_start`.
5. Use `period_start` and `period_end` (ISO date strings) for timeline charts and date-range slicers.

### Optional filters (append as query parameters)

| Parameter | Example | Effect |
|-----------|---------|--------|
| `metric_id` | `?metric_id=baixa_imobilizado` | Single metric |
| `entity_scope` | `?entity_scope=consolidado` | Consolidated only |
| `period_type` | `?period_type=full_year` | Full-year periods only |
| `period_from` | `?period_from=2019-01-01` | On or after date |
| `period_to` | `?period_to=2024-12-31` | On or before date |

### Power Query M snippet (for parameterised refresh)

```m
let
    ApiKey  = "dev-api-key-change-me",
    BaseUrl = "http://localhost:8000/api/v1/datapoints/flat",
    Source  = Json.Document(
                  Web.Contents(BaseUrl, [Headers=[#"X-API-Key"=ApiKey]])
              ),
    Table   = Table.FromList(Source, Splitter.SplitByNothing()),
    Expanded = Table.ExpandRecordColumn(Table, "Column1",
                  {"metric_id","metric_name","period","period_type",
                   "period_start","period_end","value","scale_power",
                   "entity_scope","filing","file"})
in
    Expanded
```

---

## Response shape

### GET /api/v1/datapoints/flat — plain array (Excel-first)

```json
[
  {
    "id": 1,
    "metric_id": "baixa_imobilizado",
    "metric_name": "Baixa de imobilizado/direito de uso",
    "period": "12M19",
    "period_type": "full_year",
    "period_start": "2019-01-01",
    "period_end": "2019-12-31",
    "value": 14677.0,
    "scale_power": 3,
    "entity_scope": "consolidado",
    "filing": "DFP 2020 Ann",
    "file": "102263_v1.json",
    "created_at": "2026-04-10T12:00:00"
  }
]
```

### GET /api/v1/datapoints — paginated (scripts / large datasets)

```json
{
  "data": [ { "...same fields..." } ],
  "page": 1,
  "page_size": 100,
  "total": 263,
  "total_pages": 3
}
```

---

## Period format reference

| Format  | Meaning                | Dates                    |
|---------|------------------------|--------------------------|
| `12M19` | Full year 2019         | 2019-01-01 → 2019-12-31  |
| `9M24`  | First 9 months of 2024 | 2024-01-01 → 2024-09-30  |
| `6M22`  | First 6 months of 2022 | 2022-01-01 → 2022-06-30  |
| `1T20`  | Q1 2020                | 2020-01-01 → 2020-03-31  |
| `3T23`  | Q3 2023                | 2023-07-01 → 2023-09-30  |

---

## Configuration

All settings have safe defaults — no `.env` file required to run locally.

| Variable       | Default                              | Description                            |
|----------------|--------------------------------------|----------------------------------------|
| `DATABASE_URL` | `sqlite+aiosqlite:///./financial.db` | Database file path                     |
| `API_KEYS`     | `dev-api-key-change-me`              | Comma-separated list of valid API keys |
| `ENV`          | `development`                        | Set to `production` to hide /docs      |

To override, copy `.env.example` to `.env` and edit it.

---

## Useful commands

```bash
# Reset the database (delete and re-seed)
rm financial.db
python scripts/seed_data.py   # starts uvicorn first if not running

# Upload with a custom API key
API_KEY=my-key python scripts/seed_data.py

# Upload a different file
DATA_FILE=path/to/other.json python scripts/seed_data.py
```
