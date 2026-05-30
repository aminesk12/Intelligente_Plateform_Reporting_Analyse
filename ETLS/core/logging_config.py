"""Centralised logging configuration for the ETL platform.

Call :func:`setup_logging` once at the entry point (CLI, Prefect flow, notebook).
Library modules should only ever call ``logging.getLogger(__name__)`` and never
configure handlers themselves.
"""

from __future__ import annotations

import logging
import logging.config
import os
from typing import Optional

_DEFAULT_FORMAT = (
    "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
)

_configured = False


def setup_logging(
    level: Optional[str] = None,
    *,
    log_file: Optional[str] = None,
    fmt: str = _DEFAULT_FORMAT,
    force: bool = False,
) -> None:
    """Configure root logging for the platform.

    Parameters
    ----------
    level:
        Log level name (``"DEBUG"``, ``"INFO"`` ...). Falls back to the
        ``ETL_LOG_LEVEL`` environment variable, then ``"INFO"``.
    log_file:
        Optional path; when given, logs are also written there.
    fmt:
        Log line format.
    force:
        Reconfigure even if logging was already set up in this process.
    """
    global _configured
    if _configured and not force:
        return

    resolved_level = (level or os.getenv("ETL_LOG_LEVEL") or "INFO").upper()

    handlers: dict[str, dict] = {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "standard",
            "level": resolved_level,
        }
    }
    if log_file:
        handlers["file"] = {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "standard",
            "level": resolved_level,
            "filename": log_file,
            "maxBytes": 10 * 1024 * 1024,
            "backupCount": 5,
            "encoding": "utf-8",
        }

    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {"standard": {"format": fmt}},
            "handlers": handlers,
            "loggers": {
                # Quiet noisy third-party libraries by default.
                "urllib3": {"level": "WARNING"},
                "sqlalchemy.engine": {"level": "WARNING"},
            },
            "root": {
                "level": resolved_level,
                "handlers": list(handlers.keys()),
            },
        }
    )
    _configured = True
