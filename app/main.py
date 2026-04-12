"""
FastAPI application entry point.

Brazilian financial market API conventions applied:
  - BCB SGS style: /api/v1/serie/{metric_id} → {data, valor} time series
  - CVM convention: Portuguese field names, escala_monetaria, moeda BRL
  - BRAPI convention: envelope responses with requestedAt timestamp
  - Public /info endpoint following open-data portal standards (CVM, BCB)

On startup, SQLAlchemy creates all tables automatically — no migration tool
or database server needed. A local financial.db file is created on first run.
"""

import json
import pathlib
from contextlib import asynccontextmanager
from collections.abc import AsyncGenerator
from datetime import datetime

from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import ValidationError
from sqlalchemy import distinct, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import AsyncSessionLocal, Base, engine, get_db
from app.models import Datapoint
from app.routers import datapoints, upload
from app.schemas import DatapointResponse, InfoResponse, RawDatapoint
from app.services.ingest import ingest_datapoints


async def _auto_seed() -> None:
    """Seed the database from time_series.json on first startup (when DB is empty)."""
    async with AsyncSessionLocal() as db:
        count = (await db.execute(select(func.count(Datapoint.id)))).scalar_one()
        if count > 0:
            return  # Already seeded — skip

        seed_file = pathlib.Path("time_series.json")
        if not seed_file.exists():
            return  # No seed file found — skip silently

        raw = json.loads(seed_file.read_bytes().decode("utf-8-sig"))
        if not isinstance(raw, list) or not raw:
            return

        validated: list[RawDatapoint] = []
        for item in raw:
            try:
                validated.append(RawDatapoint.model_validate(item))
            except ValidationError:
                pass

        if validated:
            result = await ingest_datapoints(validated, db, seed_file.name)
            print(f"==> Auto-seeded {result.inserted} rows from {seed_file.name}")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Create tables on startup; seed DB if empty; dispose connection pool on shutdown."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await _auto_seed()
    yield
    await engine.dispose()


app = FastAPI(
    title="API de Séries Temporais Financeiras",
    description=(
        "Serve dados normalizados de demonstrações financeiras brasileiras "
        "(DFP e ITR da CVM), seguindo as convenções das principais APIs do "
        "mercado financeiro brasileiro: BCB SGS, CVM Portal Dados Abertos e BRAPI. "
        "\n\n"
        "**Endpoints principais:**\n"
        "- `GET /` — todos os dados em array plano (Excel / Power Query)\n"
        "- `GET /api/v1/serie/{metric_id}` — série temporal no estilo BCB SGS\n"
        "- `GET /api/v1/dados` — consulta paginada com parâmetros em português\n"
        "- `GET /info` — metadados públicos do dataset\n"
    ),
    version="1.0.0",
    docs_url="/docs" if settings.env == "development" else None,
    redoc_url="/redoc" if settings.env == "development" else None,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if settings.env == "development" else [],
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["X-API-Key", "Content-Type"],
)

app.include_router(datapoints.router)
app.include_router(upload.router)


# ── GET / ─────────────────────────────────────────────────────────────────────
# Flat array, no auth — paste directly into Excel Power Query.

@app.get(
    "/",
    response_model=list[DatapointResponse],
    tags=["Dados Financeiros"],
    summary="Todos os dados — array plano (sem autenticação)",
    description=(
        "Retorna todos os registros como array JSON simples, ordenado por "
        "**period_start**. Sem necessidade de API key. \n\n"
        "Cole esta URL diretamente no Excel: "
        "**Dados → Obter Dados → De Web** → `http://localhost:8000/`"
    ),
)
async def get_all(db: AsyncSession = Depends(get_db)) -> list[DatapointResponse]:
    result = await db.execute(
        select(Datapoint).order_by(Datapoint.period_start, Datapoint.metric_id)
    )
    return [DatapointResponse.model_validate(row) for row in result.scalars().all()]


# ── GET /info ─────────────────────────────────────────────────────────────────
# Public metadata endpoint — follows CVM / BCB open-data portal conventions.
# No authentication required.

@app.get(
    "/info",
    response_model=InfoResponse,
    tags=["Metadata"],
    summary="Informações sobre o dataset",
    description=(
        "Retorna metadados públicos do dataset: total de registros, métricas "
        "disponíveis, intervalo de datas e informações de escala. "
        "Segue o padrão dos portais de dados abertos brasileiros (CVM, BCB). "
        "**Não requer autenticação.**"
    ),
)
async def info(db: AsyncSession = Depends(get_db)) -> InfoResponse:
    # Single aggregate query for counts and date bounds
    agg = (await db.execute(
        select(
            func.count(Datapoint.id).label("total"),
            func.count(distinct(Datapoint.metric_id)).label("metricas"),
            func.count(distinct(Datapoint.filing)).label("demonstracoes"),
            func.min(Datapoint.period_start).label("inicio"),
            func.max(Datapoint.period_end).label("fim"),
        )
    )).one()

    scopes: list[str] = [
        row for (row,) in (
            await db.execute(
                select(distinct(Datapoint.entity_scope))
                .where(Datapoint.entity_scope.isnot(None))
                .order_by(Datapoint.entity_scope)
            )
        ).all()
    ]

    return InfoResponse(
        api="API de Séries Temporais Financeiras",
        versao=app.version,
        descricao=(
            "Dados de demonstrações financeiras brasileiras (DFP e ITR) "
            "extraídos de arquivos CVM. Valores em BRL."
        ),
        total_registros=agg.total,
        metricas_disponiveis=agg.metricas,
        demonstracoes_disponiveis=agg.demonstracoes,
        periodo_inicial=agg.inicio,
        periodo_final=agg.fim,
        escopos_disponiveis=scopes,
        moeda="BRL",
        requestedAt=datetime.now(),
    )
