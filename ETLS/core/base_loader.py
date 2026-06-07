"""The common contract every loader in the platform implements.

Design
------
* Subclasses implement :meth:`validate` (cheap pre-flight checks),
  :meth:`_load` (the actual write returning a metadata dict), and
  :meth:`_destination_label` (a human-readable destination identifier).
* Callers always use the public :meth:`load`, which wraps the subclass
  logic with validation, timing, logging, and uniform error translation, and
  returns a :class:`LoadResult` (destination info + metadata).

Input can be an :class:`~ETLS.core.base_extractor.ExtractionResult`, a
:class:`~ETLS.core.base_transformer.TransformationResult`, or a raw
``pd.DataFrame``.  This uniform input contract lets loaders plug into
any point in the ETL pipeline and lets the Prefect orchestration treat
every load step identically.
"""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Union

import pandas as pd

from ETLS.core.base_extractor import ExtractionResult
from ETLS.core.base_transformer import TransformationResult
from ETLS.core.exceptions import ETLError, LoadError


# Any of these can be passed to BaseLoader.load().
LoadInput = Union[ExtractionResult, TransformationResult, pd.DataFrame]


@dataclass(slots=True)
class LoadResult:
    """The output of any load operation: destination info plus provenance metadata."""

    loader_type: str
    loader_name: str
    destination: str
    loaded_at: datetime
    duration_seconds: float
    rows_loaded: int
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def is_empty(self) -> bool:
        return self.rows_loaded == 0

    def summary(self) -> dict[str, Any]:
        """A JSON-serialisable summary suitable for logging or run metadata."""
        return {
            "loader_type": self.loader_type,
            "loader_name": self.loader_name,
            "destination": self.destination,
            "loaded_at": self.loaded_at.isoformat(),
            "duration_seconds": round(self.duration_seconds, 3),
            "rows_loaded": self.rows_loaded,
            **self.metadata,
        }

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        return (
            f"LoadResult("
            f"loader={self.loader_type}:{self.loader_name!r}, "
            f"destination={self.destination!r}, "
            f"rows={self.rows_loaded}, "
            f"took={self.duration_seconds:.2f}s)"
        )


def _unwrap(source: LoadInput) -> tuple[pd.DataFrame, int]:
    """Extract the DataFrame and row count from any accepted input type."""
    df = source if isinstance(source, pd.DataFrame) else source.data
    return df, len(df)


class BaseLoader(ABC):
    """Abstract base for all data loaders.

    Subclasses must set the class attribute :attr:`loader_type` and
    implement :meth:`validate`, :meth:`_load`, and :meth:`_destination_label`.
    """

    #: Short machine-readable loader kind, e.g. "database", "file", "api".
    loader_type: str = "base"

    def __init__(self, name: str | None = None) -> None:
        self.name = name or self.__class__.__name__
        self.logger = logging.getLogger(f"etl.load.{self.loader_type}")

    # -- contract to implement -------------------------------------------------

    @abstractmethod
    def validate(self) -> None:
        """Cheap pre-flight checks (connection reachable, path writable, ...).

        Raise :class:`~ETLS.core.exceptions.ValidationError` or
        :class:`~ETLS.core.exceptions.DestinationConnectionError` on failure.
        """

    @abstractmethod
    def _load(self, df: pd.DataFrame, **kwargs: Any) -> dict[str, Any]:
        """Write *df* to the destination.

        Returns a dict of loader-specific metadata to embed in :class:`LoadResult`.
        """

    @abstractmethod
    def _destination_label(self) -> str:
        """Return a short human-readable identifier for the destination."""

    # -- public API ------------------------------------------------------------

    def load(self, source: LoadInput, **kwargs: Any) -> LoadResult:
        """Validate, load, and wrap the result with provenance metadata."""
        self.logger.info("Starting load: %s", self.name)
        self.validate()

        df, rows = _unwrap(source)

        start = time.perf_counter()
        try:
            extra_meta = self._load(df, **kwargs)
        except ETLError:
            self.logger.exception("Load failed: %s", self.name)
            raise
        except Exception as exc:  # noqa: BLE001 - translate unexpected errors
            self.logger.exception("Load failed: %s", self.name)
            raise LoadError(
                f"{self.loader_type} load '{self.name}' failed: {exc}"
            ) from exc

        duration = time.perf_counter() - start

        result = LoadResult(
            loader_type=self.loader_type,
            loader_name=self.name,
            destination=self._destination_label(),
            loaded_at=datetime.now(timezone.utc),
            duration_seconds=duration,
            rows_loaded=rows,
            metadata=extra_meta or {},
        )
        self.logger.info(
            "Loaded %d rows to %s via %s in %.2fs",
            result.rows_loaded,
            result.destination,
            self.name,
            duration,
        )
        return result
