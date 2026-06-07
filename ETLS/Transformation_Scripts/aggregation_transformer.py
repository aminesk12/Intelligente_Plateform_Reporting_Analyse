"""Aggregation transformer: groupby / pivot-table / sort operations.

Two mutually exclusive modes:

* **groupby mode** — ``group_by`` + ``agg`` spec, mirrors ``DataFrame.groupby().agg()``.
* **pivot mode**   — ``pivot_index`` + ``pivot_columns`` + ``pivot_values``,
  mirrors ``pd.pivot_table()``.

An optional ``sort_by`` applies to the output of either mode.
"""

from __future__ import annotations

from typing import Any, Callable, Union

import pandas as pd

from ETLS.core.base_transformer import BaseTransformer
from ETLS.core.exceptions import ValidationError

AggFunc = Union[str, Callable, list, dict]


class AggregationTransformer(BaseTransformer):
    """Aggregate a DataFrame using groupby+agg or a pivot table.

    **Groupby mode** (set ``group_by`` and ``agg``):

    >>> t = AggregationTransformer(
    ...     group_by=["region", "product"],
    ...     agg={"revenue": "sum", "quantity": ["sum", "mean"]},
    ... )

    **Pivot mode** (set ``pivot_index``, ``pivot_columns``, ``pivot_values``):

    >>> t = AggregationTransformer(
    ...     pivot_index="region",
    ...     pivot_columns="quarter",
    ...     pivot_values="revenue",
    ...     pivot_aggfunc="sum",
    ... )

    Parameters
    ----------
    group_by:
        Column(s) to group on (groupby mode).
    agg:
        Aggregation spec — same syntax as ``DataFrame.agg()``:
        ``{"col": "sum"}`` or ``{"col": ["sum", "mean"]}``.
    pivot_index:
        Row-grouping column for the pivot table (pivot mode).
    pivot_columns:
        Column whose unique values become new column headers.
    pivot_values:
        Column(s) to aggregate inside the pivot.
    pivot_aggfunc:
        Aggregation function for the pivot (default ``"sum"``).
    sort_by:
        Column(s) to sort the result by (applied after aggregation).
    ascending:
        Sort direction — ``True`` (default) or a list aligned with ``sort_by``.
    """

    transformer_type = "aggregation"

    def __init__(
        self,
        *,
        group_by: list[str] | None = None,
        agg: dict[str, Any] | None = None,
        pivot_index: str | None = None,
        pivot_columns: str | None = None,
        pivot_values: str | list[str] | None = None,
        pivot_aggfunc: AggFunc = "sum",
        sort_by: list[str] | None = None,
        ascending: bool | list[bool] = True,
        name: str | None = None,
    ) -> None:
        super().__init__(name=name)
        self.group_by = group_by
        self.agg = agg
        self.pivot_index = pivot_index
        self.pivot_columns = pivot_columns
        self.pivot_values = pivot_values
        self.pivot_aggfunc = pivot_aggfunc
        self.sort_by = sort_by
        self.ascending = ascending

    def validate(self) -> None:
        using_groupby = self.group_by is not None
        using_pivot = self.pivot_index is not None

        if using_groupby and using_pivot:
            raise ValidationError(
                "AggregationTransformer: provide either group_by/agg or pivot_*, not both."
            )
        if not using_groupby and not using_pivot:
            raise ValidationError(
                "AggregationTransformer: set either 'group_by'+'agg' or 'pivot_index'+"
                "'pivot_columns'+'pivot_values'."
            )
        if using_groupby and not self.agg:
            raise ValidationError(
                "AggregationTransformer: 'agg' is required when 'group_by' is set."
            )
        if using_pivot and not (self.pivot_columns and self.pivot_values):
            raise ValidationError(
                "AggregationTransformer: 'pivot_columns' and 'pivot_values' are "
                "required when 'pivot_index' is set."
            )

    def _transform(self, df: pd.DataFrame, **_: Any) -> pd.DataFrame:
        if self.pivot_index is not None:
            df = pd.pivot_table(
                df,
                index=self.pivot_index,
                columns=self.pivot_columns,
                values=self.pivot_values,
                aggfunc=self.pivot_aggfunc,
            ).reset_index()
            # Flatten MultiIndex columns that pivot_table may produce.
            df.columns = [
                "_".join(str(part) for part in col).strip("_") if isinstance(col, tuple) else str(col)
                for col in df.columns
            ]
        else:
            df = df.groupby(self.group_by, as_index=False).agg(self.agg)
            # Flatten MultiIndex columns produced by multi-function agg.
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [
                    "_".join(str(p) for p in col).strip("_") for col in df.columns
                ]

        if self.sort_by:
            existing = [c for c in self.sort_by if c in df.columns]
            if existing:
                df = df.sort_values(existing, ascending=self.ascending).reset_index(drop=True)

        return df

    def _build_metadata(
        self, df_in: pd.DataFrame, df_out: pd.DataFrame, **_: Any
    ) -> dict[str, Any]:
        return {
            "mode": "pivot" if self.pivot_index is not None else "groupby",
            "group_by": self.group_by,
            "agg": str(self.agg) if self.agg else None,
            "pivot_index": self.pivot_index,
            "pivot_columns": self.pivot_columns,
            "sorted_by": self.sort_by,
        }
