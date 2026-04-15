"""
Logging configuration for the Financial API.

Development  → human-readable output:  "2024-01-15 10:23:01 [INFO] app.main: startup"
Production   → JSON structured output:  {"timestamp":"…","level":"INFO","logger":"app.main","message":"startup",…}

Usage:
    from app.logging_config import setup_logging, get_logger

    setup_logging(env="production")   # call once at application startup
    logger = get_logger(__name__)
    logger.info("startup", extra={"env": "production", "db": "postgresql"})
"""

import json
import logging
import logging.config
from datetime import datetime, timezone
from typing import Any


class _JsonFormatter(logging.Formatter):
    """Emit each log record as a single-line JSON object.

    All fields passed via ``extra=`` in the logging call are merged into the
    top-level JSON object, making them trivially queryable in CloudWatch,
    Datadog, Loki, etc.
    """

    # Standard LogRecord attributes — these are not re-emitted as extra fields.
    _SKIP = frozenset({
        "name", "msg", "args", "levelname", "levelno", "pathname",
        "filename", "module", "exc_info", "exc_text", "stack_info",
        "lineno", "funcName", "created", "msecs", "relativeCreated",
        "thread", "threadName", "processName", "process", "message",
        "taskName",
    })

    def format(self, record: logging.LogRecord) -> str:
        record.message = record.getMessage()
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.message,
        }
        # Merge caller-supplied extra= fields
        for key, value in record.__dict__.items():
            if key not in self._SKIP and not key.startswith("_"):
                payload[key] = value

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str, ensure_ascii=False)


def setup_logging(env: str = "development") -> None:
    """Configure the root logger based on the runtime environment.

    Call this exactly once, before the ASGI app starts accepting requests.

    Args:
        env: ``"production"`` enables JSON output and INFO-level threshold;
             any other value uses a human-readable format at DEBUG level.
    """
    is_prod = env == "production"

    if is_prod:
        formatter: dict[str, Any] = {"()": _JsonFormatter}
    else:
        formatter = {
            "format": "%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        }

    logging.config.dictConfig({
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {"default": formatter},
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
                "formatter": "default",
            },
        },
        "root": {
            "level": "INFO" if is_prod else "DEBUG",
            "handlers": ["console"],
        },
        # Silence noisy third-party loggers in production
        "loggers": {
            "uvicorn.access": {"level": "WARNING", "propagate": True},
            "sqlalchemy.engine": {"level": "WARNING", "propagate": True},
        },
    })


def get_logger(name: str) -> logging.Logger:
    """Return a named logger.  Thin wrapper kept for import consistency."""
    return logging.getLogger(name)
