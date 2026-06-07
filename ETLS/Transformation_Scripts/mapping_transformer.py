"""Mapping transformer: structural and relabelling operations on a DataFrame.

Operations (all optional, applied in the order listed below):

1. ``rename``        — rename columns via a ``{old: new}`` dict.
2. ``value_maps``    — replace cell values per column via lookup tables.
3. ``add_columns``   — append constant or computed columns.
4. ``keep_columns``  — select a subset of columns (all others dropped).
   *OR*
   ``drop_columns``  — remove specific columns.
   (``keep_columns`` and ``drop_columns`` are mutually exclusive.)
"""

from __future__ import annotations

from typing import Any, Callable

import pandas as pd

from ETLS.core.base_transformer import BaseTransformer
from ETLS.core.exceptions import ValidationError


class MappingTransformer(BaseTransformer):
    """Rename, filter, and remap columns/values.

    Parameters
    ----------
    rename:
        ``{old_column_name: new_column_name}`` — rename columns.
    value_maps:
        ``{column: {old_value: new_value}}`` — replace cell values using a
        lookup table. Values not found in the map are left unchanged.
    add_columns:
        ``{new_column: value_or_callable}`` — add new columns. A scalar is
        broadcast to every row; a callable receives the full DataFrame and must
        return a Series aligned to its index.
    keep_columns:
        List of columns to retain (all others are dropped). Applied after
        renaming, so use the *new* names here. Mutually exclusive with
        ``drop_columns``.
    drop_columns:
        List of columns to remove. Applied after renaming. Mutually exclusive
        with ``keep_columns``.
    """

    transformer_type = "mapping"

    def __init__(
        self,
        *,
        rename: dict[str, str] | None = None,
        value_maps: dict[str, dict[Any, Any]] | None = None,
        add_columns: dict[str, Any | Callable[[pd.DataFrame], pd.Series]] | None = None,
        keep_columns: list[str] | None = None,
        drop_columns: list[str] | None = None,
        name: str | None = None,
    ) -> None:
        super().__init__(name=name)
        self.rename = rename or {}
        self.value_maps = value_maps or {}
        self.add_columns = add_columns or {}
        self.keep_columns = keep_columns
        self.drop_columns = drop_columns or []

    def validate(self) -> None:
        if self.keep_columns is not None and self.drop_columns:
            raise ValidationError(
                "MappingTransformer: 'keep_columns' and 'drop_columns' are mutually exclusive."
            )

    def _transform(self, df: pd.DataFrame, **_: Any) -> pd.DataFrame:
        if self.rename:
            df = df.rename(columns=self.rename)

        for col, vmap in self.value_maps.items():
            if col in df.columns:
                # Use .map() but fall back to the original value for unmatched keys.
                df[col] = df[col].map(lambda v, m=vmap: m.get(v, v))

        for col, value in self.add_columns.items():
            df[col] = value(df) if callable(value) else value

        if self.keep_columns is not None:
            existing = [c for c in self.keep_columns if c in df.columns]
            missing = [c for c in self.keep_columns if c not in df.columns]
            if missing:
                self.logger.warning(
                    "keep_columns: columns not found and skipped: %s", missing
                )
            df = df[existing]
        elif self.drop_columns:
            to_drop = [c for c in self.drop_columns if c in df.columns]
            df = df.drop(columns=to_drop)

        return df

    def _build_metadata(
        self, df_in: pd.DataFrame, df_out: pd.DataFrame, **_: Any
    ) -> dict[str, Any]:
        return {
            "renamed": self.rename,
            "value_maps_applied": list(self.value_maps.keys()),
            "columns_added": list(self.add_columns.keys()),
            "columns_kept": self.keep_columns,
            "columns_dropped": [c for c in self.drop_columns if c in df_in.columns],
        }
