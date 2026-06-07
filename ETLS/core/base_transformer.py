"""The common contract every transformer in the platform implements.

Design
------
* Subclasses implement :meth:`validate` (cheap pre-flight checks) and
  :meth:`_transform` (the actual work returning a ``pandas.DataFrame``).
* Callers always use the public :meth:`transform`, which wraps the subclass
  logic with validation, timing, logging, and uniform error translation, and
  returns a :class:`TransformationResult` (data + metadata).

Input can be an :class:`~ETLS.core.base_extractor.ExtractionResult`, a
:class:`TransformationResult` from a prior step, or a raw ``pd.DataFrame``.
This uniform input/output contract lets transformers compose freely and lets
the Prefect orchestration treat every step identically.
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
from ETLS.core.exceptions import ETLError, TransformationError


# Any of these can be passed to BaseTransformer.transform().
DataInput = Union[ExtractionResult, "TransformationResult", pd.DataFrame]


@dataclass(slots=True)
class TransformationResult:
    """The output of any transformation: the data plus provenance metadata."""

    data: pd.DataFrame
    transformer_type: str
    transformer_name: str
    transformed_at: datetime
    duration_seconds: float
    input_rows: int
    output_rows: int
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def row_delta(self) -> int:
        """Rows added (positive) or removed (negative) by this transformation."""
        return self.output_rows - self.input_rows

    @property
    def column_count(self) -> int:
        return self.data.shape[1]

    @property
    def is_empty(self) -> bool:
        return self.data.empty

    def summary(self) -> dict[str, Any]:
        """A JSON-serialisable summary suitable for logging or run metadata."""
        return {
            "transformer_type": self.transformer_type,
            "transformer_name": self.transformer_name,
            "transformed_at": self.transformed_at.isoformat(),
            "duration_seconds": round(self.duration_seconds, 3),
            "input_rows": self.input_rows,
            "output_rows": self.output_rows,
            "row_delta": self.row_delta,
            "columns": list(self.data.columns),
            **self.metadata,
        }

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        return (
            f"TransformationResult("
            f"transformer={self.transformer_type}:{self.transformer_name!r}, "
            f"rows={self.input_rows}→{self.output_rows}, "
            f"took={self.duration_seconds:.2f}s)"
        )


def _unwrap(source: DataInput) -> tuple[pd.DataFrame, int]:
    """Extract the DataFrame and row count from any accepted input type."""
    df = source if isinstance(source, pd.DataFrame) else source.data
    return df, len(df)


class BaseTransformer(ABC):
    """Abstract base for all data transformers.

    Subclasses must set the class attribute :attr:`transformer_type` and
    implement :meth:`validate` and :meth:`_transform`.
    """

    #: Short machine-readable transformer kind, e.g. "cleaning", "mapping".
    transformer_type: str = "base"

    def __init__(self, name: str | None = None) -> None:
        self.name = name or self.__class__.__name__
        self.logger = logging.getLogger(f"etl.transform.{self.transformer_type}")

    # -- contract to implement -------------------------------------------------

    @abstractmethod
    def validate(self) -> None:
        """Check that the transformer is correctly configured.

        Raise :class:`~ETLS.core.exceptions.ValidationError` on failure.
        """

    @abstractmethod
    def _transform(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """Apply the transformation and return the resulting DataFrame."""

    # -- public API ------------------------------------------------------------

    def transform(self, source: DataInput, **kwargs: Any) -> TransformationResult:
        """Validate, transform, and wrap the result with provenance metadata."""
        self.logger.info("Starting transformation: %s", self.name)
        self.validate()

        df_in, input_rows = _unwrap(source)

        start = time.perf_counter()
        try:
            df_out = self._transform(df_in.copy(), **kwargs)
        except ETLError:
            self.logger.exception("Transformation failed: %s", self.name)
            raise
        except Exception as exc:  # noqa: BLE001
            self.logger.exception("Transformation failed: %s", self.name)
            raise TransformationError(
                f"{self.transformer_type} transformation '{self.name}' failed: {exc}"
            ) from exc

        duration = time.perf_counter() - start

        if not isinstance(df_out, pd.DataFrame):
            raise TransformationError(
                f"{self.name}._transform must return a pandas DataFrame, "
                f"got {type(df_out).__name__}"
            )

        result = TransformationResult(
            data=df_out,
            transformer_type=self.transformer_type,
            transformer_name=self.name,
            transformed_at=datetime.now(timezone.utc),
            duration_seconds=duration,
            input_rows=input_rows,
            output_rows=len(df_out),
            metadata=self._build_metadata(df_in, df_out, **kwargs),
        )
        self.logger.info(
            "Transformed %d→%d rows via %s in %.2fs",
            result.input_rows,
            result.output_rows,
            self.name,
            duration,
        )
        return result

    # -- hooks -----------------------------------------------------------------

    def _build_metadata(
        self, df_in: pd.DataFrame, df_out: pd.DataFrame, **kwargs: Any
    ) -> dict[str, Any]:
        """Override to attach transformer-specific metadata to the result."""
        return {}
