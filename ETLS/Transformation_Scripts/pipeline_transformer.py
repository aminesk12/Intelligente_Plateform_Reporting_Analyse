"""Pipeline transformer: chain multiple transformers in a single pass.

Each step receives the output DataFrame of the previous step. The final
:class:`~ETLS.core.base_transformer.TransformationResult` carries a
``steps`` metadata key with the per-step summary so the full lineage is
traceable in logs and Prefect run metadata.

Example
-------
>>> from ETLS.Transformation_Scripts import (
...     CleaningTransformer, MappingTransformer, PipelineTransformer
... )
>>> pipeline = PipelineTransformer(
...     steps=[
...         CleaningTransformer(normalize_columns=True, drop_duplicates=True),
...         MappingTransformer(rename={"cust_id": "customer_id"}),
...     ],
...     name="orders_pipeline",
... )
>>> result = pipeline.transform(extraction_result)
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from ETLS.core.base_transformer import BaseTransformer
from ETLS.core.exceptions import ValidationError


class PipelineTransformer(BaseTransformer):
    """Run a sequence of transformers in order.

    Parameters
    ----------
    steps:
        Ordered list of :class:`BaseTransformer` instances to apply.
    stop_on_empty:
        When ``True`` (default) the pipeline stops early and returns whatever
        DataFrame is current if a step produces an empty result. This prevents
        useless downstream work and makes the empty-result traceable in the
        per-step metadata. Set to ``False`` to always run every step.
    """

    transformer_type = "pipeline"

    def __init__(
        self,
        steps: list[BaseTransformer],
        *,
        stop_on_empty: bool = True,
        name: str | None = None,
    ) -> None:
        super().__init__(name=name)
        self.steps = steps
        self.stop_on_empty = stop_on_empty

    def validate(self) -> None:
        if not self.steps:
            raise ValidationError("PipelineTransformer: 'steps' must not be empty.")
        for i, step in enumerate(self.steps):
            if not isinstance(step, BaseTransformer):
                raise ValidationError(
                    f"PipelineTransformer: step[{i}] ({step!r}) is not a "
                    "BaseTransformer instance."
                )
            # Fail fast: validate every step before running any.
            step.validate()

    def _transform(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        self._step_summaries: list[dict[str, Any]] = []

        for step in self.steps:
            result = step.transform(df, **kwargs)
            self._step_summaries.append(result.summary())
            df = result.data

            if self.stop_on_empty and df.empty:
                self.logger.warning(
                    "Pipeline '%s' stopped early: step '%s' returned an empty DataFrame.",
                    self.name,
                    step.name,
                )
                break

        return df

    def _build_metadata(
        self, df_in: pd.DataFrame, df_out: pd.DataFrame, **_: Any
    ) -> dict[str, Any]:
        return {
            "step_count": len(self.steps),
            "steps_run": len(getattr(self, "_step_summaries", [])),
            "steps": getattr(self, "_step_summaries", []),
        }
