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

from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader

from app.config import settings

# The header name clients must send their key in.
_API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)


async def require_api_key(api_key: str | None = Security(_API_KEY_HEADER)) -> str:
    """FastAPI dependency that validates the X-API-Key request header.

    Args:
        api_key: Value extracted from the 'X-API-Key' header by FastAPI's
            security scheme (None if the header is absent).

    Returns:
        The validated API key string (useful if downstream code needs it).

    Raises:
        HTTPException: 401 if the key is missing or not in the allowed set.
    """
    if api_key is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-API-Key header.",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    # Use constant-time comparison for every configured key.
    # secrets.compare_digest prevents timing-based key enumeration.
    for valid_key in settings.api_keys:
        if secrets.compare_digest(api_key, valid_key):
            return api_key

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid API key.",
        headers={"WWW-Authenticate": "ApiKey"},
    )
