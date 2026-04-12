"""
Datapoints router — endpoints following Brazilian financial market API conventions.

Endpoint summary
────────────────
Authenticated (X-API-Key header required):

  GET /api/v1/datapoints          Paginated list (English params) — BRAPI envelope style
  GET /api/v1/datapoints/flat     Flat array (Excel / Power Query one-click)
                                  Supports ?format=csv for spreadsheet download
  GET /api/v1/metrics             Distinct metric_ids with counts
  GET /api/v1/filings             Distinct filing reference strings

  GET /api/v1/dados               Portuguese alias for /datapoints
  GET /api/v1/metricas            Portuguese alias for /metrics
  GET /api/v1/demonstracoes       Portuguese alias for /filings

  GET /api/v1/serie/{metric_id}   BCB SGS-style time series for a single metric
                                  → {"metric_id":…, "dados":[{"data":"dd/MM/yyyy","valor":…}]}
                                  Supports ?format=csv for download

  GET /api/v1/series              Batch: multiple metrics in a single request
                                  ?metric_id=a,b,c  → list[SerieResponse]
                                  Supports ?format=csv (merged spreadsheet)

All endpoints that accept metric_id support comma-separated batch values:
  ?metric_id=baixa_ativos,contingencias,total
"""

import csv
import io
from collections import defaultdict
from math import ceil

from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from fastapi.responses import StreamingResponse
from sqlalchemy import distinct, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import require_api_key
from app.database import get_db
from app.excel import (
    build_datapoints_xlsx,
    build_serie_xlsx,
    build_series_xlsx,
    make_zip_response,
)
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

def _parse_metric_ids(value: str | None) -> list[str] | None:
    """Parse a comma-separated metric_id string into a list.

    Returns None if value is None, otherwise splits by comma and strips whitespace.
    Example: 'baixa_ativos,contingencias, total' → ['baixa_ativos', 'contingencias', 'total']
    """
    if value is None:
        return None
    return [m.strip() for m in value.split(",") if m.strip()]


def _apply_filters(
    query,
    *,
    metric_ids: list[str] | None,
    entity_scope: str | None,
    period_type: str | None,
    period: str | None = None,
):
    """Apply optional WHERE clauses to a SQLAlchemy select statement.

    metric_ids: list of metric_id values — uses IN() for multiple, = for one.
    """
    if metric_ids is not None:
        if len(metric_ids) == 1:
            query = query.where(Datapoint.metric_id == metric_ids[0])
        else:
            query = query.where(Datapoint.metric_id.in_(metric_ids))
    if entity_scope is not None:
        query = query.where(Datapoint.entity_scope == entity_scope)
    if period_type is not None:
        query = query.where(Datapoint.period_type == period_type)
    if period is not None:
        query = query.where(Datapoint.period == period)
    return query



def _to_csv_response(rows: list[dict], filename: str) -> StreamingResponse:
    """Serialize a list of dicts as a UTF-8 CSV download response."""
    buf = io.StringIO()
    if rows:
        writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ── GET /datapoints ───────────────────────────────────────────────────────────

@router.get(
    "/datapoints",
    response_model=PaginatedResponse[DatapointResponse],
    summary="Consultar dados financeiros (paginado)",
    description=(
        "Retorna uma lista paginada de pontos de dados da série temporal. "
        "Todos os filtros são opcionais e combináveis. "
        "**metric_id** aceita múltiplos valores separados por vírgula: `?metric_id=a,b,c`. "
        "Resposta no formato envelope BRAPI com campo **requestedAt**."
    ),
)
async def get_datapoints(
    metric_id: str | None = Query(None, description="Identificador(es) da métrica — separe por vírgula para múltiplas: 'baixa_imobilizado,contingencias'."),
    entity_scope: str | None = Query(None, description="Escopo da entidade: 'consolidado' ou 'controladora'."),
    period_type: str | None = Query(None, description="Tipo de período: 'full_year', 'quarter' ou 'year_to_date'."),
    period: str | None = Query(None, description="Tag de período exata, ex: '12M23', '1T24'."),
    page: int = Query(1, ge=1, description="Número da página (base 1)."),
    page_size: int = Query(100, ge=1, le=1000, description="Linhas por página (máx 1 000)."),
    db: AsyncSession = Depends(get_db),
) -> PaginatedResponse[DatapointResponse]:
    base_q = _apply_filters(
        select(Datapoint),
        metric_ids=_parse_metric_ids(metric_id), entity_scope=entity_scope,
        period_type=period_type, period=period,
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
        "**metric_id** aceita múltiplos valores separados por vírgula: `?metric_id=a,b,c`. "
        "Use **?format=csv** para download direto como CSV. "
        "Use **?format=xlsx** para download de um ZIP contendo CSV + planilha Excel com gráficos. "
        "Limitado a 10 000 linhas."
    ),
    responses={200: {"content": {
        "text/csv": {"schema": {"type": "string"}},
        "application/zip": {"schema": {"type": "string", "format": "binary"}},
    }}},
)
async def get_datapoints_flat(
    metric_id: str | None = Query(None, description="Identificador(es) da métrica — separe por vírgula para múltiplas."),
    entity_scope: str | None = Query(None, description="Escopo da entidade: 'consolidado' ou 'controladora'."),
    period_type: str | None = Query(None, description="Tipo de período: 'full_year', 'quarter' ou 'year_to_date'."),
    period: str | None = Query(None, description="Tag de período exata, ex: '12M23', '1T24'."),
    format: str | None = Query(None, description="Formato de saída: 'csv' → CSV puro; 'xlsx' → ZIP com CSV + Excel (gráficos)."),
    db: AsyncSession = Depends(get_db),
):
    q = _apply_filters(
        select(Datapoint),
        metric_ids=_parse_metric_ids(metric_id), entity_scope=entity_scope,
        period_type=period_type, period=period,
    ).order_by(Datapoint.period_start, Datapoint.metric_id).limit(10_000)
    rows = (await db.execute(q)).scalars().all()
    data = [DatapointResponse.model_validate(r) for r in rows]

    if format == "csv":
        return _to_csv_response(
            [d.model_dump() for d in data],
            filename="datapoints.csv",
        )

    if format == "xlsx":
        dicts = [d.model_dump() for d in data]
        xlsx_bytes = build_datapoints_xlsx(dicts)
        return make_zip_response(dicts, xlsx_bytes, base_filename="datapoints")

    return data


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
#   • Supports ?format=csv for spreadsheet download

@router.get(
    "/serie/{metric_id}",
    response_model=SerieResponse,
    summary="Série temporal de uma métrica (estilo BCB SGS)",
    description=(
        "Retorna a série temporal de uma métrica no formato BCB SGS: "
        "`{data: 'DD/MM/YYYY', valor: float}`. "
        "Filtros opcionais: **entity_scope** (consolidado|controladora), "
        "**period_type** (full_year|quarter|year_to_date), "
        "**period** (tag bruta, ex: '12M23', '1T24'). "
        "Use **?format=csv** para download como CSV. "
        "Use **?format=xlsx** para download de um ZIP com CSV + planilha Excel com gráfico."
    ),
    responses={200: {"content": {
        "text/csv": {"schema": {"type": "string"}},
        "application/zip": {"schema": {"type": "string", "format": "binary"}},
    }}},
)
async def get_serie(
    metric_id: str = Path(description="Identificador da métrica, ex: 'baixa_imobilizado'."),
    entity_scope: str | None = Query(None, description="Escopo da entidade: 'consolidado' ou 'controladora'."),
    period_type: str | None = Query(None, description="Tipo de período: 'full_year', 'quarter' ou 'year_to_date'."),
    period: str | None = Query(None, description="Tag de período exata, ex: '12M23', '1T24'."),
    format: str | None = Query(None, description="Formato de saída: 'csv' → CSV puro; 'xlsx' → ZIP com CSV + Excel (gráfico)."),
    db: AsyncSession = Depends(get_db),
):
    q = _apply_filters(
        select(Datapoint),
        metric_ids=[metric_id], entity_scope=entity_scope,
        period_type=period_type, period=period,
    ).order_by(Datapoint.period_end, Datapoint.entity_scope)

    rows = (await db.execute(q)).scalars().all()

    if not rows:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Nenhum dado encontrado para a métrica '{metric_id}'.",
        )

    escala = _ESCALA_MAP.get(rows[0].scale_power, f"10^{rows[0].scale_power}")
    metric_name = rows[0].metric_name

    dados = [
        SerieItem(
            data=row.period_end.strftime("%d/%m/%Y"),
            data_iso=row.period_end,
            valor=float(row.value),
            periodo=row.period,
            tipo_periodo_en=row.period_type,
            tipo_periodo=_TIPO_PERIODO_PT.get(row.period_type or "", None),
            entidade=row.entity_scope,
        )
        for row in rows
    ]

    if format in ("csv", "xlsx"):
        csv_rows = [
            {
                "metric_id": metric_id,
                "metric_name": metric_name,
                "data_iso": item.data_iso,
                "data": item.data,
                "valor": item.valor,
                "periodo": item.periodo,
                "tipo_periodo": item.tipo_periodo,
                "entidade": item.entidade,
                "moeda": "BRL",
                "escala_monetaria": escala,
            }
            for item in dados
        ]
        if format == "csv":
            return _to_csv_response(csv_rows, filename=f"serie_{metric_id}.csv")

        xlsx_bytes = build_serie_xlsx(csv_rows, metric_id, metric_name, escala)
        return make_zip_response(csv_rows, xlsx_bytes, base_filename=f"serie_{metric_id}")

    return SerieResponse(
        metric_id=metric_id,
        metric_name=metric_name,
        moeda="BRL",
        escala_monetaria=escala,
        total=len(dados),
        dados=dados,
    )


# ── GET /series  —  batch de múltiplas métricas ───────────────────────────────
# BRAPI-inspired pattern: múltiplos identificadores em uma única chamada.
# Retorna lista de SerieResponse, uma por métrica.
# Suporta ?format=csv para planilha consolidada com todas as séries.

@router.get(
    "/series",
    response_model=list[SerieResponse],
    summary="Séries temporais de múltiplas métricas (batch)",
    description=(
        "Retorna séries temporais para uma ou mais métricas em uma única chamada. "
        "Passe os identificadores separados por vírgula em **metric_id**: "
        "`?metric_id=baixa_ativos,contingencias,total`. "
        "Aceita os mesmos filtros de `/serie/{metric_id}`. "
        "Use **?format=csv** para download de todas as séries em um CSV único. "
        "Use **?format=xlsx** para download de um ZIP com CSV + planilha Excel com uma aba e gráfico por métrica."
    ),
    responses={200: {"content": {
        "text/csv": {"schema": {"type": "string"}},
        "application/zip": {"schema": {"type": "string", "format": "binary"}},
    }}},
)
async def get_series_batch(
    metric_id: str = Query(
        description=(
            "Identificadores das métricas separados por vírgula. "
            "Ex: `baixa_ativos,contingencias,total`."
        )
    ),
    entity_scope: str | None = Query(None, description="Escopo da entidade: 'consolidado' ou 'controladora'."),
    period_type: str | None = Query(None, description="Tipo de período: 'full_year', 'quarter' ou 'year_to_date'."),
    period: str | None = Query(None, description="Tag de período exata, ex: '12M23', '1T24'."),
    format: str | None = Query(None, description="Formato de saída: 'csv' → CSV único; 'xlsx' → ZIP com CSV + Excel (aba por métrica)."),
    db: AsyncSession = Depends(get_db),
):
    metric_ids = _parse_metric_ids(metric_id)
    if not metric_ids:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Informe ao menos um metric_id.",
        )

    q = _apply_filters(
        select(Datapoint),
        metric_ids=metric_ids, entity_scope=entity_scope,
        period_type=period_type, period=period,
    ).order_by(Datapoint.metric_id, Datapoint.period_end, Datapoint.entity_scope)

    rows = (await db.execute(q)).scalars().all()

    if not rows:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Nenhum dado encontrado para as métricas: {', '.join(metric_ids)}.",
        )

    grouped: dict[str, list] = defaultdict(list)
    for row in rows:
        grouped[row.metric_id].append(row)

    result: list[SerieResponse] = []
    csv_rows: list[dict] = []
    series_data: list[dict] = []   # para build_series_xlsx

    for mid in metric_ids:
        metric_rows = grouped.get(mid)
        if not metric_rows:
            continue

        escala = _ESCALA_MAP.get(metric_rows[0].scale_power, f"10^{metric_rows[0].scale_power}")
        metric_name = metric_rows[0].metric_name

        dados = [
            SerieItem(
                data=row.period_end.strftime("%d/%m/%Y"),
                data_iso=row.period_end,
                valor=float(row.value),
                periodo=row.period,
                tipo_periodo_en=row.period_type,
                tipo_periodo=_TIPO_PERIODO_PT.get(row.period_type or "", None),
                entidade=row.entity_scope,
            )
            for row in metric_rows
        ]

        result.append(SerieResponse(
            metric_id=mid,
            metric_name=metric_name,
            moeda="BRL",
            escala_monetaria=escala,
            total=len(dados),
            dados=dados,
        ))

        if format in ("csv", "xlsx"):
            metric_csv: list[dict] = [
                {
                    "metric_id": mid,
                    "metric_name": metric_name,
                    "data_iso": item.data_iso,
                    "data": item.data,
                    "valor": item.valor,
                    "periodo": item.periodo,
                    "tipo_periodo": item.tipo_periodo,
                    "entidade": item.entidade,
                    "moeda": "BRL",
                    "escala_monetaria": escala,
                }
                for item in dados
            ]
            csv_rows.extend(metric_csv)
            if format == "xlsx":
                series_data.append({
                    "metric_id": mid,
                    "metric_name": metric_name,
                    "escala": escala,
                    "rows": metric_csv,
                })

    slug = "_".join(metric_ids[:3])
    if len(metric_ids) > 3:
        slug += f"_e_mais_{len(metric_ids) - 3}"

    if format == "csv":
        return _to_csv_response(csv_rows, filename=f"series_{slug}.csv")

    if format == "xlsx":
        xlsx_bytes = build_series_xlsx(series_data)
        return make_zip_response(csv_rows, xlsx_bytes, base_filename=f"series_{slug}")

    return result


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
        "**Parâmetros obrigatórios:** `entity_scope` e `period`. \n\n"
        "- `entity_scope`: `'consolidado'` ou `'controladora'` \n"
        "- `period`: tag bruta exata, ex: `'12M23'`, `'1T24'`"
    ),
)
async def get_ponto(
    metric_id: str = Path(description="Identificador da métrica, ex: 'receita_liquida'."),
    entity_scope: str = Query(description="Escopo obrigatório: 'consolidado' ou 'controladora'."),
    period: str | None = Query(None, description="Tag de período exata, ex: '12M23', '3T22'."),
    db: AsyncSession = Depends(get_db),
) -> ValorPontoResponse:
    if period is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Informe o parâmetro 'period'.",
        )

    q = select(Datapoint).where(
        Datapoint.metric_id == metric_id,
        Datapoint.entity_scope == entity_scope,
        Datapoint.period == period,
    )

    # Ordena por period_end desc para retornar o mais recente quando houver
    # múltiplos arquivos com o mesmo (métrica, período, escopo).
    q = q.order_by(Datapoint.period_end.desc(), Datapoint.updated_at.desc()).limit(1)

    row = (await db.execute(q)).scalars().first()

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"Nenhum dado encontrado para métrica='{metric_id}', "
                f"entity_scope='{entity_scope}', period='{period}'."
            ),
        )

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

@router.get(
    "/dados",
    response_model=PaginatedResponse[DatapointResponse],
    summary="Consultar dados (parâmetros em português)",
    description=(
        "Alias português para **/datapoints**. "
        "**metric_id** aceita múltiplos valores separados por vírgula: `?metric_id=a,b,c`. "
        "Use **escopo** em vez de *entity_scope*, **tipo_periodo** em vez de *period_type*, "
        "**periodo** em vez de *period*."
    ),
)
async def get_dados(
    metric_id: str | None = Query(None, description="Identificador(es) da métrica — separe por vírgula para múltiplas."),
    escopo: str | None = Query(None, description="'consolidado' ou 'controladora'."),
    periodo: str | None = Query(None, description="Tag de período exata, ex: '12M23', '1T24'."),
    tipo_periodo: str | None = Query(None, description="'full_year', 'quarter' ou 'year_to_date'."),
    pagina: int = Query(1, ge=1, alias="pagina", description="Número da página (base 1)."),
    tamanho_pagina: int = Query(100, ge=1, le=1000, description="Linhas por página (máx 1 000)."),
    db: AsyncSession = Depends(get_db),
) -> PaginatedResponse[DatapointResponse]:
    base_q = _apply_filters(
        select(Datapoint),
        metric_ids=_parse_metric_ids(metric_id), entity_scope=escopo,
        period_type=tipo_periodo, period=periodo,
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
