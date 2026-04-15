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
from datetime import date, datetime, timezone

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import ValidationError
from sqlalchemy import distinct, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import require_api_key
from app.config import settings
from app.database import AsyncSessionLocal, Base, engine, get_db
from app.logging_config import get_logger, setup_logging
from app.middleware import RateLimitMiddleware, RequestLoggingMiddleware
from app.models import Datapoint
from app.routers import datapoints, upload
from app.schemas import DatapointResponse, InfoResponse, PrivacidadeResponse, RawDatapoint
from app.services.ingest import ingest_datapoints

# Initialise logging before anything else so that startup messages are captured.
setup_logging(env=settings.env)
logger = get_logger(__name__)


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
            logger.info(
                "auto_seed_complete",
                extra={"inserted": result.inserted, "file": seed_file.name},
            )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Initialise the database, seed if empty, then dispose the pool on shutdown.

    Schema strategy:
      SQLite (local dev)  — create_all runs automatically so no migration tool
                            is needed to start the app locally.
      PostgreSQL (Docker / production) — schema is owned exclusively by Alembic.
                            start.sh runs `alembic upgrade head` before this
                            lifespan executes, so create_all must NOT run or it
                            will silently bypass the migration history and can
                            leave the schema in an inconsistent state.
    """
    logger.info("startup", extra={"env": settings.env, "database_url": settings.database_url.split("///")[0]})

    if settings.env != "development" and "dev-api-key-change-me" in settings.api_keys:
        logger.error(
            "insecure_default_api_key",
            extra={
                "env": settings.env,
                "detail": (
                    "A chave de API padrão 'dev-api-key-change-me' não pode ser usada "
                    "fora do ambiente de desenvolvimento. "
                    "Defina a variável de ambiente API_KEYS com uma chave segura antes de iniciar. "
                    "Gere uma: python -c \"import secrets; print(secrets.token_urlsafe(32))\""
                ),
            },
        )
        raise RuntimeError("Default API key must not be used in production")

    if settings.database_url.startswith("sqlite"):
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    await _auto_seed()
    logger.info("ready")
    yield
    await engine.dispose()
    logger.info("shutdown")


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

# Middleware is applied in reverse registration order (last added = outermost).
# Desired order: RateLimit → RequestLogging → CORSMiddleware → route handler.
# So register them in reverse: CORS first, then RequestLogging, then RateLimit.

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if settings.env == "development" else [],
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["X-API-Key", "Content-Type"],
)

app.add_middleware(RequestLoggingMiddleware)

if settings.rate_limit_per_minute > 0:
    app.add_middleware(
        RateLimitMiddleware,
        calls_per_minute=settings.rate_limit_per_minute,
        upload_calls_per_minute=settings.rate_limit_upload_per_minute,
    )

app.include_router(datapoints.router)
app.include_router(upload.router)


# ── GET / ─────────────────────────────────────────────────────────────────────
# Flat array — Excel / Power Query shortcut.  Requires API key.

@app.get(
    "/",
    response_model=list[DatapointResponse],
    tags=["Dados Financeiros"],
    summary="Todos os dados — array plano (requer autenticação)",
    description=(
        "Retorna até 10 000 registros como array JSON simples, ordenado por "
        "**period_start**. Requer header `X-API-Key` ou parâmetro `?api_key=`.\n\n"
        "Cole esta URL diretamente no Excel: "
        "**Dados → Obter Dados → De Web → Avançado** → `http://localhost:8000/?api_key=<SUA_CHAVE>`"
    ),
    dependencies=[Depends(require_api_key)],
)
async def get_all(db: AsyncSession = Depends(get_db)) -> list[DatapointResponse]:
    result = await db.execute(
        select(Datapoint)
        .order_by(Datapoint.period_start, Datapoint.metric_id)
        .limit(10_000)
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
async def info(db: AsyncSession = Depends(get_db)) -> InfoResponse:  # noqa: F811
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
        requestedAt=datetime.now(timezone.utc),
    )


# ── GET /privacidade ──────────────────────────────────────────────────────────
# LGPD Art. 9º: data subjects must be able to access clear information about
# how their personal data is processed.  This endpoint provides that disclosure
# in a machine-readable form so compliance tools and API clients can inspect it
# programmatically.  No authentication required — this is a public legal notice.

@app.get(
    "/privacidade",
    response_model=PrivacidadeResponse,
    tags=["Compliance"],
    summary="Aviso de privacidade (LGPD)",
    description=(
        "Retorna o aviso de privacidade estruturado conforme a **Lei Geral de "
        "Proteção de Dados — Lei nº 13.709/2018**. "
        "Descreve quais dados pessoais são coletados, a finalidade, a base legal, "
        "o prazo de retenção e os direitos do titular. "
        "**Não requer autenticação.**"
    ),
)
async def privacidade() -> PrivacidadeResponse:
    from app.schemas import DadoPessoalColetado
    retencao_logs = f"{settings.lgpd_retencao_logs_dias} dias"
    contato = settings.lgpd_encarregado_email or "Não configurado — defina LGPD_ENCARREGADO_EMAIL"
    return PrivacidadeResponse(
        controlador=settings.lgpd_responsavel,
        encarregado_email=contato,
        dados_pessoais_coletados=[
            DadoPessoalColetado(
                dado="Endereço IP do cliente",
                finalidade=(
                    "Segurança da informação, controle de acesso, "
                    "limitação de taxa de requisições (rate limiting) e "
                    "prevenção de abusos."
                ),
                base_legal="Art. 7º, IX LGPD — legítimo interesse do controlador",
                retencao=f"Logs de acesso retidos por {retencao_logs} na camada de infraestrutura, "
                         "após o que são excluídos automaticamente.",
            ),
        ],
        direitos_do_titular=[
            "Confirmação da existência de tratamento (Art. 18, I)",
            "Acesso aos dados (Art. 18, II)",
            "Correção de dados incompletos ou inexatos (Art. 18, III)",
            "Anonimização, bloqueio ou eliminação de dados desnecessários (Art. 18, IV)",
            "Portabilidade dos dados (Art. 18, V)",
            "Eliminação dos dados tratados com consentimento (Art. 18, VI)",
            "Informação sobre compartilhamento com terceiros (Art. 18, VII)",
            "Revogação do consentimento (Art. 18, IX)",
        ],
        contato_para_solicitacoes=contato,
        aviso=(
            "Os endereços IP são registrados em logs de acesso gerenciados pela "
            "infraestrutura de hospedagem. O prazo de retenção é configurável via "
            "a variável de ambiente LGPD_RETENCAO_LOGS_DIAS (padrão: "
            f"{settings.lgpd_retencao_logs_dias} dias). "
            "Esta API não coleta, armazena nem compartilha quaisquer outros dados "
            "pessoais dos usuários."
        ),
        atualizado_em=date(2026, 4, 15),
    )


# ── GET /health ───────────────────────────────────────────────────────────────
# Liveness probe: always returns 200 while the process is running.
# Used by Docker/Kubernetes to decide whether to restart the container.

@app.get(
    "/health",
    tags=["Sistema"],
    summary="Liveness probe",
    description="Retorna 200 enquanto o processo estiver rodando. Usado por orquestradores (Docker, Kubernetes) para checar se o contêiner deve ser reiniciado.",
)
async def health() -> dict:
    return {"status": "ok"}


# ── GET /readiness ────────────────────────────────────────────────────────────
# Readiness probe: confirms the process is ready to serve traffic by
# executing a cheap SELECT against the database.

@app.get(
    "/readiness",
    tags=["Sistema"],
    summary="Readiness probe",
    description="Verifica se o banco de dados está acessível. Retorna 200 se pronto para receber tráfego, 503 se o banco estiver indisponível.",
)
async def readiness(db: AsyncSession = Depends(get_db)) -> dict:
    from sqlalchemy import text
    try:
        await db.execute(text("SELECT 1"))
    except Exception as exc:
        logger.error("readiness_check_failed", extra={"error": str(exc)})
        raise HTTPException(status_code=503, detail="Banco de dados indisponível.")
    return {"status": "ready"}
