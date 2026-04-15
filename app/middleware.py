"""
ASGI middleware for the Financial API.

RequestLoggingMiddleware
    Logs every HTTP request: method, path, status code, and wall-clock duration.
    Masks the ?api_key= query param before writing to the log so keys never
    appear in log aggregators.

RateLimitMiddleware
    Sliding-window in-memory rate limiter keyed by client IP.
    Two independent windows are tracked per IP:
      - "general"  — all non-upload requests   (default: 60 req/min)
      - "upload"   — POST /upload requests      (default: 10 req/min)
    Health probe paths (/health, /readiness) are always exempt.

    Note: counters live in-process memory and reset on restart.  For
    multi-worker or multi-instance deployments, replace with a Redis-backed
    limiter (e.g. slowapi + Redis).
"""

import re
import time
from collections import defaultdict, deque
from typing import Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from app.logging_config import get_logger

logger = get_logger(__name__)

# Masks the value of ?api_key= (or &api_key=) in URLs before logging.
_API_KEY_RE = re.compile(r"((?:\?|&)api_key=)[^&]*", re.IGNORECASE)

# These paths must never be rate-limited — health probes must always succeed.
_RATE_LIMIT_EXEMPT = frozenset({"/health", "/readiness"})


def _client_ip(request: Request) -> str:
    """Return the real client IP, respecting X-Forwarded-For when present."""
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _sanitize_url(url: str) -> str:
    """Replace the api_key value in a URL string with '***'."""
    return _API_KEY_RE.sub(r"\1***", url)


# ── Request Logging ───────────────────────────────────────────────────────────

class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Log every HTTP request with method, path, status code, and duration."""

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        start = time.perf_counter()
        response = await call_next(request)
        duration_ms = round((time.perf_counter() - start) * 1000, 2)

        logger.info(
            "http_request",
            extra={
                "method": request.method,
                "path": request.url.path,
                "url": _sanitize_url(str(request.url)),
                "status_code": response.status_code,
                "duration_ms": duration_ms,
                "client_ip": _client_ip(request),
            },
        )
        return response


# ── Rate Limiting ─────────────────────────────────────────────────────────────

class RateLimitMiddleware(BaseHTTPMiddleware):
    """Sliding-window in-memory rate limiter per client IP.

    Args:
        calls_per_minute:        Limit for all general endpoints (default 60).
        upload_calls_per_minute: Stricter limit for POST /upload (default 10).
    """

    def __init__(
        self,
        app,
        *,
        calls_per_minute: int = 60,
        upload_calls_per_minute: int = 10,
    ) -> None:
        super().__init__(app)
        self._limit_general = calls_per_minute
        self._limit_upload = upload_calls_per_minute
        # ip:window_type → deque of request timestamps (float, unix seconds)
        self._windows: dict[str, deque[float]] = defaultdict(deque)

    def _is_upload(self, request: Request) -> bool:
        return request.method == "POST" and request.url.path.endswith("/upload")

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        if request.url.path in _RATE_LIMIT_EXEMPT:
            return await call_next(request)

        ip = _client_ip(request)
        is_upload = self._is_upload(request)
        limit = self._limit_upload if is_upload else self._limit_general
        window_key = f"{ip}:{'upload' if is_upload else 'general'}"

        now = time.time()
        cutoff = now - 60.0
        timestamps = self._windows[window_key]

        # Evict timestamps older than the 60-second window (O(1) with deque)
        while timestamps and timestamps[0] <= cutoff:
            timestamps.popleft()

        if len(timestamps) >= limit:
            retry_after = max(1, int(61 - (now - timestamps[0])))
            logger.warning(
                "rate_limit_exceeded",
                extra={
                    "client_ip": ip,
                    "path": request.url.path,
                    "method": request.method,
                    "window": "upload" if is_upload else "general",
                    "limit_per_minute": limit,
                    "retry_after_seconds": retry_after,
                },
            )
            return JSONResponse(
                status_code=429,
                content={
                    "detail": (
                        f"Limite de requisições excedido. "
                        f"Tente novamente em {retry_after} segundo(s)."
                    )
                },
                headers={"Retry-After": str(retry_after)},
            )

        timestamps.append(now)
        return await call_next(request)
