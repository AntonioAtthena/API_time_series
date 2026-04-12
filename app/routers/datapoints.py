"""
Datapoints router — endpoints following Brazilian financial market API conventions.

Endpoint summary
────────────────
Authenticated (X-API-Key header required):

  GET /api/v1/datapoints          Paginated list (English params) — BRAPI envelope style
  GET /api/v1/datapoints/flat     Flat array (Excel / Power Query one-click)
  GET /api/v1/metrics             Distinct metric_ids with counts
  GET /api/v1/filings             Distinct filing reference strings

  GET /api/v1/dados               Portuguese alias for /datapoints
  GET /api/v1/metricas            Portuguese alias for /metrics
  GET /api/v1/demonstracoes       Portuguese alias for /filings

  GET /api/v1/serie/{metric_id}   BCB SGS-style time series for a single metric
                                  → {"metric_id":…, "dados":[{"data":"dd/MM/yyyy","valor":…}]}
"""

from datetime import date, datetime
from math import ceil

from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from sqlalchemy import distinct, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import require_api_key
from app.database import get_db
from app.models import Datapoint
from app.schemas import (
    DatapointResponse,
    MetricSummary,
    PaginatedResponse,
    SerieItem,
    SerieResponse,
    ValorPontoResponse,
)
from app.schemas import _ESCALA_MAP, _TIPO_PERIODO_PT

router = APIRouter(
    prefix="/api/v1",
    tags=["Dados Financeiros"],
    dependencies=[Depends(require_api_key)],
)


# ── Shared helpers ────────────────────────────────────────────────────────────

def _apply_filters(
    query,
    *,
    metric_id: str | None,
    entity_scope: str | None,
    period_type: str | None,
    filing: str | None,
    period_from: date | None,
    period_to: date | None,
    periodo: str | None = None,
):
    """Apply optional WHERE clauses to a SQLAlchemy select statement."""
    if metric_id is not None:
        query = query.where(Datapoint.metric_id == metric_id)
    if entity_scope is not None:
        query = query.where(Datapoint.entity_scope == entity_scope)
    if period_type is not None:
        query = query.where(Datapoint.period_type == period_type)
    if filing is not None:
        query = query.where(Datapoint.filing == filing)
    if period_from is not None:
        query = query.where(Datapoint.period_start >= period_from)
    if period_to is not None:
        query = query.where(Datapoint.period_end <= period_to)
    if periodo is not None:
        query = query.where(Datapoint.period == periodo)
    return query


def _parse_br_date(value: str | None) -> date | None:
    """Accept ISO (YYYY-MM-DD) or Brazilian (DD/MM/YYYY) date strings.

    Returns None if value is None. Raises HTTPException 422 on bad format.
    """
    if value is None:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    raise HTTPException(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        detail=(
            f"Data inválida: '{value}'. "
            "Use o formato ISO (AAAA-MM-DD) ou brasileiro (DD/MM/AAAA)."
        ),
    )


# ── GET /datapoints ───────────────────────────────────────────────────────────

@router.get(
    "/datapoints",
    response_model=PaginatedResponse[DatapointResponse],
    summary="Consultar dados financeiros (paginado)",
    description=(
        "Retorna uma lista paginada de pontos de dados da série temporal. "
        "Todos os filtros são opcionais e combináveis. "
        "Resposta no formato envelope BRAPI com campo **requestedAt**."
    ),
)
async def get_datapoints(
    metric_id: str | None = Query(None, description="Identificador da métrica, ex: 'baixa_imobilizado'."),
    entity_scope: str | None = Query(None, description="Escopo da entidade: 'consolidado' ou 'controladora'."),
    period_type: str | None = Query(None, description="Tipo de período: 'full_year', 'quarter' ou 'year_to_date'."),
    filing: str | None = Query(None, description="Referência do demonstrativo, ex: 'DFP 2020 Ann'."),
    period_from: date | None = Query(None, description="period_start >= esta data (formato ISO YYYY-MM-DD)."),
    period_to: date | None = Query(None, description="period_end <= esta data (formato ISO YYYY-MM-DD)."),
    page: int = Query(1, ge=1, description="Número da página (base 1)."),
    page_size: int = Query(100, ge=1, le=1000, description="Linhas por página (máx 1 000)."),
    db: AsyncSession = Depends(get_db),
) -> PaginatedResponse[DatapointResponse]:
    base_q = _apply_filters(
        select(Datapoint),
        metric_id=metric_id, entity_scope=entity_scope,
        period_type=period_type, filing=filing,
        period_from=period_from, period_to=period_to,
    )
    total: int = (await db.execute(select(func.count()).select_from(base_q.subquery()))).scalar_one()
    rows = (await db.execute(
        base_q.order_by(Datapoint.period_start, Datapoint.metric_id)
              .offset((page - 1) * page_size).limit(page_size)
    )).scalars().all()
    return PaginatedResponse(
        data=[DatapointResponse.model_validate(r) for r in rows],
        page=page, page_size=page_size, total=total,
        total_pages=ceil(total / page_size) if total > 0 else 0,
    )


# ── GET /datapoints/flat ──────────────────────────────────────────────────────

@router.get(
    "/datapoints/flat",
    response_model=list[DatapointResponse],
    summary="Todos os dados como array plano (Excel / Power Query)",
    description=(
        "Retorna os dados como array JSON simples, sem envelope de paginação. "
        "O Power Query abre diretamente como Tabela em um clique. "
        "Limitado a 10 000 linhas."
    ),
)
async def get_datapoints_flat(
    metric_id: str | None = Query(None),
    entity_scope: str | None = Query(None),
    period_type: str | None = Query(None),
    filing: str | None = Query(None),
    period_from: date | None = Query(None),
    period_to: date | None = Query(None),
    db: AsyncSession = Depends(get_db),
) -> list[DatapointResponse]:
    q = _apply_filters(
        select(Datapoint),
        metric_id=metric_id, entity_scope=entity_scope,
        period_type=period_type, filing=filing,
        period_from=period_from, period_to=period_to,
    ).order_by(Datapoint.period_start, Datapoint.metric_id).limit(10_000)
    rows = (await db.execute(q)).scalars().all()
    return [DatapointResponse.model_validate(r) for r in rows]


# ── GET /metrics ──────────────────────────────────────────────────────────────

@router.get(
    "/metrics",
    response_model=list[MetricSummary],
    summary="Listar métricas disponíveis",
    description="Retorna os identificadores de métricas com seus nomes e contagens.",
)
async def list_metrics(db: AsyncSession = Depends(get_db)) -> list[MetricSummary]:
    stmt = (
        select(
            Datapoint.metric_id,
            func.max(Datapoint.metric_name).label("metric_name"),
            func.count(Datapoint.id).label("data_point_count"),
        )
        .group_by(Datapoint.metric_id)
        .order_by(Datapoint.metric_id)
    )
    rows = (await db.execute(stmt)).all()
    return [MetricSummary(metric_id=r.metric_id, metric_name=r.metric_name, data_point_count=r.data_point_count) for r in rows]


# ── GET /filings ──────────────────────────────────────────────────────────────

@router.get(
    "/filings",
    response_model=list[str],
    summary="Listar demonstrativos disponíveis",
    description="Retorna os identificadores de demonstrativos (DFP/ITR) no banco.",
)
async def list_filings(db: AsyncSession = Depends(get_db)) -> list[str]:
    stmt = select(distinct(Datapoint.filing)).where(Datapoint.filing.isnot(None)).order_by(Datapoint.filing)
    return [row for (row,) in (await db.execute(stmt)).all()]


# ── GET /serie/{metric_id}  —  BCB SGS style ──────────────────────────────────
# Mirrors the Banco Central do Brasil SGS pattern:
#   api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados?dataInicial=…&dataFinal=…
#
# Differences / additions vs. BCB:
#   • Path uses metric_id (string) instead of a numeric series code
#   • Adds tipo_periodo (anual/trimestral/acumulado) for DFP/ITR context
#   • Adds entidade (consolidado/controladora) — not present in macroeconomic series
#   • Supports both ISO and Brazilian date formats (BCB only accepts DD/MM/YYYY)

@router.get(
    "/serie/{metric_id}",
    response_model=SerieResponse,
    summary="Série temporal de uma métrica (estilo BCB SGS)",
    description=(
        "Retorna a série temporal de uma métrica no formato BCB SGS: "
        "`{data: 'DD/MM/YYYY', valor: float}`. "
        "Filtros opcionais: **escopo** (consolidado|controladora), "
        "**periodo** (tag bruta, ex: '12M23'), "
        "**data_inicio** e **data_fim** nos formatos ISO ou DD/MM/AAAA."
    ),
)
async def get_serie(
    metric_id: str = Path(description="Identificador da métrica, ex: 'baixa_imobilizado'."),
    escopo: str | None = Query(
        None,
        description="Escopo da entidade: 'consolidado' ou 'controladora'.",
    ),
    periodo: str | None = Query(
        None,
        description="Tag de período exata, ex: '12M23', '1T24'. Case-insensitive não aplicado — use exatamente como cadastrado.",
    ),
    data_inicio: str | None = Query(
        None,
        description="Data inicial — ISO (AAAA-MM-DD) ou brasileiro (DD/MM/AAAA).",
        examples="01/01/2019",
    ),
    data_fim: str | None = Query(
        None,
        description="Data final — ISO (AAAA-MM-DD) ou brasileiro (DD/MM/AAAA).",
        examples="31/12/2024",
    ),
    db: AsyncSession = Depends(get_db),
) -> SerieResponse:
    period_from = _parse_br_date(data_inicio)
    period_to = _parse_br_date(data_fim)

    q = _apply_filters(
        select(Datapoint),
        metric_id=metric_id, entity_scope=escopo,
        period_type=None, filing=None,
        period_from=period_from, period_to=period_to,
        periodo=periodo,
    ).order_by(Datapoint.period_end, Datapoint.entity_scope)

    rows = (await db.execute(q)).scalars().all()

    if not rows:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Nenhum dado encontrado para a métrica '{metric_id}'.",
        )

    # Derive escala_monetaria from the first row (all rows share the same scale)
    escala = _ESCALA_MAP.get(rows[0].scale_power, f"10^{rows[0].scale_power}")
    metric_name = rows[0].metric_name

    dados = [
        SerieItem(
            data=row.period_end.strftime("%d/%m/%Y"),   # BCB/CVM: DD/MM/YYYY
            data_iso=row.period_end,
            valor=float(row.value),
            periodo=row.period,
            tipo_periodo_en=row.period_type,
            tipo_periodo=_TIPO_PERIODO_PT.get(row.period_type or "", None),
            entidade=row.entity_scope,
        )
        for row in rows
    ]

    return SerieResponse(
        metric_id=metric_id,
        metric_name=metric_name,
        moeda="BRL",
        escala_monetaria=escala,
        total=len(dados),
        dados=dados,
    )


# ── GET /serie/{metric_id}/ponto  —  valor único ──────────────────────────────
# Consulta pontual: métrica + período específico + escopo obrigatório.
# Retorna exatamente um valor ou 404.

@router.get(
    "/serie/{metric_id}/ponto",
    response_model=ValorPontoResponse,
    summary="Valor único de uma métrica em um período e escopo específicos",
    description=(
        "Retorna **um único valor** para a combinação métrica + período + escopo. "
        "Útil para consultas pontuais sem percorrer a série inteira. \n\n"
        "**Parâmetros obrigatórios:** `escopo` e (`periodo` ou `data_referencia`). \n\n"
        "- `escopo`: `'consolidado'` ou `'controladora'` \n"
        "- `periodo`: tag bruta exata, ex: `'12M23'`, `'1T24'` \n"
        "- `data_referencia`: data de encerramento do período — ISO (AAAA-MM-DD) "
        "ou brasileiro (DD/MM/AAAA). Usado quando a tag de período não é conhecida."
    ),
)
async def get_ponto(
    metric_id: str = Path(description="Identificador da métrica, ex: 'receita_liquida'."),
    escopo: str = Query(
        description="Escopo obrigatório: 'consolidado' ou 'controladora'.",
    ),
    periodo: str | None = Query(
        None,
        description="Tag de período exata, ex: '12M23', '3T22'.",
    ),
    data_referencia: str | None = Query(
        None,
        description=(
            "Data de encerramento do período — ISO (AAAA-MM-DD) ou DD/MM/AAAA. "
            "Alternativa ao parâmetro `periodo`."
        ),
    ),
    db: AsyncSession = Depends(get_db),
) -> ValorPontoResponse:
    if periodo is None and data_referencia is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Informe ao menos um dos parâmetros: 'periodo' ou 'data_referencia'.",
        )

    data_ref = _parse_br_date(data_referencia)

    q = select(Datapoint).where(
        Datapoint.metric_id == metric_id,
        Datapoint.entity_scope == escopo,
    )
    if periodo is not None:
        q = q.where(Datapoint.period == periodo)
    if data_ref is not None:
        q = q.where(Datapoint.period_end == data_ref)

    # Se ambos foram fornecidos, o WHERE já combina os dois filtros.
    # Ordena por period_end desc para retornar o mais recente quando houver
    # múltiplos arquivos com o mesmo (métrica, período, escopo).
    q = q.order_by(Datapoint.period_end.desc(), Datapoint.updated_at.desc()).limit(1)

    row = (await db.execute(q)).scalars().first()

    if row is None:
        detail = (
            f"Nenhum dado encontrado para métrica='{metric_id}', "
            f"escopo='{escopo}'"
        )
        if periodo:
            detail += f", periodo='{periodo}'"
        if data_referencia:
            detail += f", data_referencia='{data_referencia}'"
        detail += "."
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=detail)

    return ValorPontoResponse(
        metric_id=row.metric_id,
        metric_name=row.metric_name,
        escopo=row.entity_scope,
        periodo=row.period,
        tipo_periodo_en=row.period_type,
        tipo_periodo=_TIPO_PERIODO_PT.get(row.period_type or "", None),
        data=row.period_end.strftime("%d/%m/%Y"),
        data_iso=row.period_end,
        valor=float(row.value),
        escala_monetaria=_ESCALA_MAP.get(row.scale_power, f"10^{row.scale_power}"),
    )


# ── Portuguese aliases ────────────────────────────────────────────────────────
# Mirrors CVM Portal Dados Abertos naming conventions.
# Accepts Brazilian date format (DD/MM/AAAA) in addition to ISO.

@router.get(
    "/dados",
    response_model=PaginatedResponse[DatapointResponse],
    summary="Consultar dados (parâmetros em português)",
    description=(
        "Alias português para **/datapoints**. "
        "Aceita datas em formato brasileiro (DD/MM/AAAA) além do ISO. "
        "Use **escopo** em vez de *entity_scope*, **data_inicio**/**data_fim** "
        "em vez de *period_from*/*period_to*."
    ),
)
async def get_dados(
    metric_id: str | None = Query(None, description="Identificador da métrica."),
    escopo: str | None = Query(None, description="'consolidado' ou 'controladora'."),
    periodo: str | None = Query(None, description="Tag de período exata, ex: '12M23', '1T24'."),
    tipo_periodo: str | None = Query(None, description="'full_year', 'quarter' ou 'year_to_date'."),
    demonstracao: str | None = Query(None, description="Ex: 'DFP 2020 Ann'."),
    data_inicio: str | None = Query(None, description="Data inicial (AAAA-MM-DD ou DD/MM/AAAA)."),
    data_fim: str | None = Query(None, description="Data final (AAAA-MM-DD ou DD/MM/AAAA)."),
    pagina: int = Query(1, ge=1, alias="pagina", description="Número da página (base 1)."),
    tamanho_pagina: int = Query(100, ge=1, le=1000, description="Linhas por página (máx 1 000)."),
    db: AsyncSession = Depends(get_db),
) -> PaginatedResponse[DatapointResponse]:
    period_from = _parse_br_date(data_inicio)
    period_to = _parse_br_date(data_fim)

    base_q = _apply_filters(
        select(Datapoint),
        metric_id=metric_id, entity_scope=escopo,
        period_type=tipo_periodo, filing=demonstracao,
        period_from=period_from, period_to=period_to,
        periodo=periodo,
    )
    total: int = (await db.execute(select(func.count()).select_from(base_q.subquery()))).scalar_one()
    rows = (await db.execute(
        base_q.order_by(Datapoint.period_start, Datapoint.metric_id)
              .offset((pagina - 1) * tamanho_pagina).limit(tamanho_pagina)
    )).scalars().all()

    return PaginatedResponse(
        data=[DatapointResponse.model_validate(r) for r in rows],
        page=pagina, page_size=tamanho_pagina, total=total,
        total_pages=ceil(total / tamanho_pagina) if total > 0 else 0,
    )


@router.get(
    "/metricas",
    response_model=list[MetricSummary],
    summary="Listar métricas (alias português)",
    description="Alias português para **/metrics**.",
)
async def list_metricas(db: AsyncSession = Depends(get_db)) -> list[MetricSummary]:
    return await list_metrics(db)


@router.get(
    "/demonstracoes",
    response_model=list[str],
    summary="Listar demonstrativos (alias português)",
    description=(
        "Alias português para **/filings**. "
        "Retorna os demonstrativos disponíveis (DFP/ITR) em ordem alfabética."
    ),
)
async def list_demonstracoes(db: AsyncSession = Depends(get_db)) -> list[str]:
    return await list_filings(db)
