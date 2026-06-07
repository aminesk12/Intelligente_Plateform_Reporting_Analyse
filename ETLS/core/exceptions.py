"""Typed exception hierarchy for the ETL platform.

A single root (``ETLError``) lets orchestration code catch *any* expected
platform failure with one ``except`` while still allowing fine-grained handling.
"""

from __future__ import annotations


class ETLError(Exception):
    """Base class for every error raised by the ETL platform."""


class ConfigError(ETLError):
    """Raised when configuration is missing, malformed, or fails validation."""


class ValidationError(ETLError):
    """Raised when a source or its inputs fail pre-extraction validation."""


class SourceConnectionError(ETLError):
    """Raised when a data source cannot be reached or authenticated."""


class ExtractionError(ETLError):
    """Raised when reading data from a source fails."""


class TransformationError(ETLError):
    """Raised when a transformation step fails to process its input."""
