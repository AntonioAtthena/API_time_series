"""
API key authentication via a custom FastAPI dependency.

Rationale — API keys over OAuth2:
    Excel Power Query and custom functions both support custom HTTP headers
    trivially (Headers=[#"X-API-Key"="..."]).  OAuth2 requires a token
    refresh flow that is significantly harder to implement in M Language.
    For a single-tenant / small-team scenario, API keys transmitted over
    HTTPS provide adequate security without friction.

Security properties:
    - Keys are stored in memory (loaded from env), never logged.
    - Comparison is done with secrets.compare_digest to prevent timing attacks.
    - A 401 response deliberately omits detail about *why* authentication
      failed to avoid leaking information to an attacker.
"""

import secrets

from fastapi import HTTPException, Query, Request, Security, status
from fastapi.security import APIKeyHeader

from app.config import settings
from app.logging_config import get_logger

logger = get_logger(__name__)

# The header name clients must send their key in.
_API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)


def _mask_key(key: str) -> str:
    """Return the first 4 characters followed by '***' for safe log output."""
    return key[:4] + "***" if len(key) >= 4 else "***"


async def require_api_key(
    request: Request,
    header_key: str | None = Security(_API_KEY_HEADER),
    api_key: str | None = Query(None, alias="api_key", include_in_schema=False),
) -> str:
    """Valida a chave de API recebida via header X-API-Key ou query param ?api_key=.

    O header tem prioridade. O query param permite colar a URL direto no navegador.
    """
    client_ip = (
        request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        or (request.client.host if request.client else "unknown")
    )
    candidate = header_key or api_key

    if candidate is None:
        logger.warning(
            "auth_failed",
            extra={
                "reason": "key_missing",
                "client_ip": client_ip,
                "path": request.url.path,
                "method": request.method,
            },
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Chave de API ausente. Envie via header 'X-API-Key' ou parâmetro '?api_key='.",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    for valid_key in settings.api_keys:
        if secrets.compare_digest(candidate, valid_key):
            logger.debug(
                "auth_ok",
                extra={
                    "masked_key": _mask_key(candidate),
                    "client_ip": client_ip,
                    "path": request.url.path,
                    "method": request.method,
                },
            )
            return candidate

    logger.warning(
        "auth_failed",
        extra={
            "reason": "key_invalid",
            "masked_key": _mask_key(candidate),
            "client_ip": client_ip,
            "path": request.url.path,
            "method": request.method,
        },
    )
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Chave de API inválida.",
        headers={"WWW-Authenticate": "ApiKey"},
    )
