"""
Pydantic v2 schemas for request validation and response serialisation.

Conventions follow the main Brazilian financial market APIs:
  - BCB SGS (api.bcb.gov.br): flat {data, valor} time-series arrays
  - CVM Portal Dados Abertos: Portuguese field names, escala_monetaria, moeda
  - BRAPI (brapi.dev): results envelope with requestedAt timestamp
"""

from datetime import date, datetime
from typing import Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator

# ── Escala mapping (CVM convention) ──────────────────────────────────────────
# CVM's ESCALA_MOEDA field describes the unit of monetary values.
_ESCALA_MAP: dict[int, str] = {
    0: "unidades",
    3: "milhares",   # R$ mil  — most common in DFP/ITR filings
    6: "milhões",    # R$ mi
    9: "bilhões",    # R$ bi
}

# ── Tipo de período (CVM Portuguese labels) ───────────────────────────────────
_TIPO_PERIODO_PT: dict[str, str] = {
    "full_year":    "anual",        # DFP — exercício completo
    "quarter":      "trimestral",   # ITR quarter (3M, 6M not full year)
    "year_to_date": "acumulado",    # YTD accumulation
}


# ── Input schema (mirrors time_series.json exactly) ──────────────────────────

class RawDatapoint(BaseModel):
    """One row from the source JSON file. Used only during ingest."""

    metric_id: str
    metric_name: str | None = None
    period: str
    period_type: str | None = None
    value: float
    scale_power: int = 0
    entity_scope: str
    source: str | None = None
    filing: str | None = None
    file: str
    table_id: int | None = None
    raw_metric: str | None = None

    @field_validator("period")
    @classmethod
    def period_must_be_parseable(cls, v: str) -> str:
        import re
        if re.match(r"^\d{1,2}M\d{2}$", v, re.IGNORECASE):
            return v
        if re.match(r"^\dT\d{2}$", v, re.IGNORECASE):
            return v
        raise ValueError(
            f"Formato de período não reconhecido: '{v}'. "
            "Formatos aceitos: '12M19' (N meses) ou '1T20' (trimestre)."
        )


# ── Output schema (API response) ─────────────────────────────────────────────

class DatapointResponse(BaseModel):
    """API response for a single data point.

    Enriched with CVM-standard fields:
      - moeda: currency code (always BRL for this dataset)
      - escala_monetaria: human-readable scale label (milhares, milhões, etc.)

    Dates are serialised as ISO-8601 strings for Excel Power Query compatibility.
    """

    model_config = ConfigDict(from_attributes=True)

    id: int
    metric_id: str
    metric_name: str | None
    period: str
    period_type: str | None
    period_start: date
    period_end: date
    value: float
    scale_power: int
    entity_scope: str
    filing: str | None
    file: str
    created_at: datetime

    # ── CVM-convention computed fields ────────────────────────────────────────
    @computed_field
    @property
    def moeda(self) -> str:
        """Moeda dos valores (padrão CVM: BRL)."""
        return "BRL"

    @computed_field
    @property
    def escala_monetaria(self) -> str:
        """Unidade monetária dos valores (padrão CVM: ESCALA_MOEDA).

        Exemplos: 'milhares' para scale_power=3, 'milhões' para scale_power=6.
        """
        return _ESCALA_MAP.get(self.scale_power, f"10^{self.scale_power}")


# ── BCB SGS-style time-series schemas ─────────────────────────────────────────
# Mirrors the Banco Central do Brasil SGS API shape:
#   api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados
#   → [{"data": "dd/MM/yyyy", "valor": float}]

class SerieItem(BaseModel):
    """One observation in a BCB SGS-style time series.

    Attributes:
        data: End-of-period date in Brazilian format (dd/MM/yyyy) — matches
              CVM and BCB reporting convention.
        data_iso: Same date in ISO-8601 format (YYYY-MM-DD) for programmatic use.
        valor: Reported value.
        periodo: CVM period tag (e.g. '12M19', '1T20').
        tipo_periodo_en: Period type in English (full_year, quarter, year_to_date).
        tipo_periodo: Period type in Portuguese (anual, trimestral, acumulado).
        entidade: Entity scope (consolidado | controladora).
    """

    data: str          # dd/MM/yyyy — BCB/CVM convention
    data_iso: date     # ISO date — for programmatic sorting
    valor: float
    periodo: str
    tipo_periodo_en: str | None
    tipo_periodo: str | None
    entidade: str


class SerieResponse(BaseModel):
    """BCB SGS-style response envelope for a single metric's time series.

    Attributes:
        metric_id: Normalised metric identifier.
        metric_name: Human-readable Portuguese label.
        moeda: Currency code (BRL).
        escala_monetaria: Value unit (milhares, milhões, etc.).
        total: Number of observations returned.
        dados: Ordered list of observations (chronological, period_end asc).
    """

    metric_id: str
    metric_name: str | None
    moeda: str = "BRL"
    escala_monetaria: str
    total: int
    dados: list[SerieItem]


# ── Dataset metadata schema ───────────────────────────────────────────────────

class InfoResponse(BaseModel):
    """Public dataset metadata — similar to open-data portals (CVM, BCB).

    Attributes:
        api: API name.
        versao: API version.
        descricao: Dataset description.
        total_registros: Total rows in the database.
        metricas_disponiveis: Number of distinct metrics.
        demonstracoes_disponiveis: Number of distinct filings.
        periodo_inicial: Earliest period start date in the database.
        periodo_final: Latest period end date in the database.
        escopos_disponiveis: List of distinct entity scopes.
        moeda: Currency of all monetary values.
        requestedAt: ISO-8601 timestamp of this response.
    """

    api: str
    versao: str
    descricao: str
    total_registros: int
    metricas_disponiveis: int
    demonstracoes_disponiveis: int
    periodo_inicial: date | None
    periodo_final: date | None
    escopos_disponiveis: list[str]
    moeda: str = "BRL"
    requestedAt: datetime


# ── Pagination wrapper (BRAPI-style envelope) ─────────────────────────────────

T = TypeVar("T")


class PaginatedResponse(BaseModel, Generic[T]):
    """Paginated response envelope.

    Follows BRAPI convention of including a requestedAt timestamp so clients
    can cache responses and detect staleness.

    Attributes:
        data: Page of result items.
        page: Current page number (1-indexed).
        page_size: Items per page.
        total: Total matching rows across all pages.
        total_pages: Total number of pages.
        requestedAt: ISO-8601 timestamp when this response was generated.
    """

    data: list[T]
    page: int
    page_size: int
    total: int
    total_pages: int
    requestedAt: datetime = Field(default_factory=datetime.now)


# ── Metric summary ────────────────────────────────────────────────────────────

class MetricSummary(BaseModel):
    """Lightweight metric descriptor returned by GET /api/v1/metricas.

    Attributes:
        metric_id: Normalised identifier.
        metric_name: Human-readable Portuguese label.
        data_point_count: Number of data points for this metric.
    """

    model_config = ConfigDict(from_attributes=True)

    metric_id: str
    metric_name: str | None
    data_point_count: int


# ── Upload response ────────────────────────────────────────────────────────────

class UploadResponse(BaseModel):
    """Summary returned after a JSON file ingestion.

    Attributes:
        filename: Uploaded file name.
        total_rows: Rows parsed from the file.
        inserted: New rows inserted.
        updated: Existing rows updated (upserted).
        errors: Row-level validation error messages, if any.
    """

    filename: str
    total_rows: int
    inserted: int
    updated: int
    errors: list[str] = Field(default_factory=list)
