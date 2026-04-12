# Setup Guide

## Prerequisites
- Python 3.12+
- PostgreSQL 14+ (local or cloud)
- pip

---

## 1 — Install dependencies

```bash
cd financial_api
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS / Linux
source .venv/bin/activate

pip install -r requirements.txt
```

---

## 2 — Configure environment

```bash
cp .env.example .env
```

Edit `.env`:

```env
DATABASE_URL=postgresql+asyncpg://postgres:yourpassword@localhost:5432/financial_db
API_KEYS=your-secret-key-here
ENV=development
```

Generate a secure API key:
```bash
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

---

## 3 — Create the database

```bash
# In psql or pgAdmin:
CREATE DATABASE financial_db;
```

---

## 4 — Run migrations

```bash
alembic upgrade head
```

This creates the `datapoints` table with all indexes and the unique constraint.

To generate a new migration after changing `models.py`:
```bash
alembic revision --autogenerate -m "describe your change"
alembic upgrade head
```

---

## 5 — Start the server

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

- Swagger UI: http://localhost:8000/docs
- Health check: http://localhost:8000/health

---

## 6 — Upload time_series.json

```bash
curl -X POST http://localhost:8000/api/v1/upload \
  -H "X-API-Key: your-secret-key-here" \
  -F "file=@../time_series.json"
```

Response:
```json
{
  "filename": "time_series.json",
  "total_rows": 280,
  "inserted": 280,
  "updated": 0,
  "errors": []
}
```

Re-upload the same file — no duplicates created:
```json
{
  "inserted": 0,
  "updated": 280,
  ...
}
```

---

## 7 — Query data (test)

```bash
# All consolidated full-year data for baixa_imobilizado
curl "http://localhost:8000/api/v1/datapoints?metric_id=baixa_imobilizado&entity_scope=consolidado&period_type=full_year" \
  -H "X-API-Key: your-secret-key-here"

# List all metrics (for Excel dropdowns)
curl "http://localhost:8000/api/v1/metrics" \
  -H "X-API-Key: your-secret-key-here"
```

---

## 8 — Excel Power Query connection

In Excel → **Data → Get Data → From Web → Advanced**:

- URL: `http://your-api-host/api/v1/datapoints?metric_id=baixa_imobilizado&entity_scope=consolidado&page_size=1000`
- Add HTTP header: `X-API-Key` = `your-secret-key-here`

Or use the parameterised M function — see the architecture document.

---

## Production deployment (Railway — fastest path)

```bash
# Install Railway CLI
npm i -g @railway/cli

railway login
railway init
railway add postgresql        # provisions a free Postgres instance
railway up                    # deploys the FastAPI container

# Set env vars in Railway dashboard:
#   DATABASE_URL  (Railway auto-sets this as $DATABASE_URL)
#   API_KEYS
#   ENV=production
```

The `Dockerfile` approach (for AWS ECS / Azure App Service):

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```
