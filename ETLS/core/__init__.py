"""Shared ETL foundation: base classes, config, logging, retries, errors."""

from ETLS.core.base_extractor import BaseExtractor, ExtractionResult
from ETLS.core.config import load_yaml_config
from ETLS.core.exceptions import (
    ConfigError,
    ETLError,
    ExtractionError,
    SourceConnectionError,
    ValidationError,
)
from ETLS.core.logging_config import setup_logging
from ETLS.core.retry import with_retry

__all__ = [
    "BaseExtractor",
    "ExtractionResult",
    "load_yaml_config",
    "setup_logging",
    "with_retry",
    "ETLError",
    "ConfigError",
    "ExtractionError",
    "SourceConnectionError",
    "ValidationError",
]
