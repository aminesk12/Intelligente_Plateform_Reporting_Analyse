"""The common contract every extractor in the platform implements.

Design
------
* Subclasses implement :meth:`validate` (cheap pre-flight checks) and
  :meth:`_extract` (the actual read returning a ``pandas.DataFrame``).
* Callers always use the public :meth:`extract`, which wraps the subclass
  logic with validation, timing, logging, and uniform error translation, and
  returns an :class:`ExtractionResult` (data + metadata).

This uniform return type is what lets the downstream transform/load layers and
the Prefect orchestration treat every source identically.
"""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from ETLS.core.exceptions import ETLError, ExtractionError


@dataclass(slots=True)
class ExtractionResult:
    """The output of any extraction: the data plus provenance metadata."""

    data: pd.DataFrame
    source_type: str
    source_name: str
    extracted_at: datetime
    duration_seconds: float
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def row_count(self) -> int:
        return len(self.data)

    @property
    def column_count(self) -> int:
        return self.data.shape[1]

    @property
    def is_empty(self) -> bool:
        return self.data.empty

    def summary(self) -> dict[str, Any]:
        """A JSON-serialisable summary suitable for logging or run metadata."""
        return {
            "source_type": self.source_type,
            "source_name": self.source_name,
            "extracted_at": self.extracted_at.isoformat(),
            "duration_seconds": round(self.duration_seconds, 3),
            "row_count": self.row_count,
            "column_count": self.column_count,
            "columns": list(self.data.columns),
            **self.metadata,
        }

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        return (
            f"ExtractionResult(source={self.source_type}:{self.source_name!r}, "
            f"rows={self.row_count}, cols={self.column_count}, "
            f"took={self.duration_seconds:.2f}s)"
        )


class BaseExtractor(ABC):
    """Abstract base for all data-source extractors.

    Subclasses must set the class attribute :attr:`source_type` and implement
    :meth:`validate` and :meth:`_extract`.
    """

    #: Short machine-readable source kind, e.g. "excel", "database", "api".
    source_type: str = "base"

    def __init__(self, name: str | None = None) -> None:
        self.name = name or self.__class__.__name__
        self.logger = logging.getLogger(f"etl.extract.{self.source_type}")

    # -- contract to implement -------------------------------------------------

    @abstractmethod
    def validate(self) -> None:
        """Cheap pre-flight checks (file exists, config complete, reachable...).

        Raise :class:`~ETLS.core.exceptions.ValidationError` or
        :class:`~ETLS.core.exceptions.SourceConnectionError` on failure.
        """

    @abstractmethod
    def _extract(self, **kwargs: Any) -> pd.DataFrame:
        """Perform the actual read and return a DataFrame."""

    # -- public API ------------------------------------------------------------

    def extract(self, **kwargs: Any) -> ExtractionResult:
        """Validate, extract, and wrap the result with provenance metadata."""
        self.logger.info("Starting extraction: %s", self.name)
        self.validate()

        start = time.perf_counter()
        try:
            df = self._extract(**kwargs)
        except ETLError:
            # Already a typed platform error — let it propagate unchanged.
            self.logger.exception("Extraction failed: %s", self.name)
            raise
        except Exception as exc:  # noqa: BLE001 - translate unexpected errors
            self.logger.exception("Extraction failed: %s", self.name)
            raise ExtractionError(
                f"{self.source_type} extraction '{self.name}' failed: {exc}"
            ) from exc

        duration = time.perf_counter() - start

        if not isinstance(df, pd.DataFrame):
            raise ExtractionError(
                f"{self.name}._extract must return a pandas DataFrame, "
                f"got {type(df).__name__}"
            )

        result = ExtractionResult(
            data=df,
            source_type=self.source_type,
            source_name=self.name,
            extracted_at=datetime.now(timezone.utc),
            duration_seconds=duration,
            metadata=self._build_metadata(df, **kwargs),
        )
        self.logger.info(
            "Extracted %d rows x %d cols from %s in %.2fs",
            result.row_count,
            result.column_count,
            self.name,
            duration,
        )
        return result

    # -- hooks -----------------------------------------------------------------

    def _build_metadata(self, df: pd.DataFrame, **kwargs: Any) -> dict[str, Any]:
        """Override to attach source-specific metadata to the result."""
        return {}
