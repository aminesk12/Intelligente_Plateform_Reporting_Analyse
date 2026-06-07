"""Cleaning transformer: data-quality operations on a DataFrame.

Operations (all optional, controlled by constructor arguments):

* ``normalize_columns`` — convert column names to lowercase ``snake_case``.
* ``strip_strings``     — strip leading/trailing whitespace from string columns.
* ``fill_na``           — fill nulls per column with a specified value.
* ``drop_na``           — drop rows that contain nulls (all columns or a subset).
* ``drop_duplicates``   — remove exact duplicate rows (all columns or a subset key).
* ``cast``              — coerce column dtypes (supports any pandas dtype string and
                          ``"datetime64[ns]"`` which routes through ``pd.to_datetime``).
"""

from __future__ import annotations

import re
from typing import Any

import pandas as pd

from ETLS.core.base_transformer import BaseTransformer
from ETLS.core.exceptions import ValidationError


class CleaningTransformer(BaseTransformer):
    """Apply common data-cleaning operations to a DataFrame.

    Parameters
    ----------
    normalize_columns:
        Convert column names to lowercase snake_case (e.g. ``"Total Sales"``
        → ``"total_sales"``). Applied first so downstream options can use the
        normalised names.
    strip_strings:
        Strip leading/trailing whitespace from all ``object``-dtype columns.
    fill_na:
        ``{column: fill_value}`` — fill nulls in specific columns only.
    drop_na:
        ``True`` → drop rows where *any* column is null.
        ``list[str]`` → drop rows where any column in the list is null.
        ``False`` (default) → do nothing.
    drop_duplicates:
        ``True`` → drop exact duplicate rows (keep first).
        ``list[str]`` → treat the listed columns as the duplicate key.
        ``False`` (default) → do nothing.
    cast:
        ``{column: dtype}`` — cast columns to a pandas dtype string, e.g.
        ``{"amount": "float64", "event_date": "datetime64[ns]"}``.
    """

    transformer_type = "cleaning"

    def __init__(
        self,
        *,
        normalize_columns: bool = False,
        strip_strings: bool = True,
        fill_na: dict[str, Any] | None = None,
        drop_na: bool | list[str] = False,
        drop_duplicates: bool | list[str] = False,
        cast: dict[str, str] | None = None,
        name: str | None = None,
    ) -> None:
        super().__init__(name=name)
        self.normalize_columns = normalize_columns
        self.strip_strings = strip_strings
        self.fill_na = fill_na or {}
        self.drop_na = drop_na
        self.drop_duplicates = drop_duplicates
        self.cast = cast or {}

    def validate(self) -> None:
        if not isinstance(self.fill_na, dict):
            raise ValidationError("CleaningTransformer: 'fill_na' must be a dict.")
        if not isinstance(self.cast, dict):
            raise ValidationError("CleaningTransformer: 'cast' must be a dict.")

    def _transform(self, df: pd.DataFrame, **_: Any) -> pd.DataFrame:
        if self.normalize_columns:
            df = self._normalize_column_names(df)

        if self.strip_strings:
            str_cols = df.select_dtypes(include="object").columns
            df[str_cols] = df[str_cols].apply(lambda col: col.str.strip())

        if self.fill_na:
            valid = {c: v for c, v in self.fill_na.items() if c in df.columns}
            df = df.fillna(valid)

        if self.drop_na is True:
            df = df.dropna()
        elif isinstance(self.drop_na, list) and self.drop_na:
            cols = [c for c in self.drop_na if c in df.columns]
            df = df.dropna(subset=cols)

        if self.drop_duplicates is True:
            df = df.drop_duplicates().reset_index(drop=True)
        elif isinstance(self.drop_duplicates, list) and self.drop_duplicates:
            cols = [c for c in self.drop_duplicates if c in df.columns]
            df = df.drop_duplicates(subset=cols).reset_index(drop=True)

        for col, dtype in self.cast.items():
            if col not in df.columns:
                self.logger.warning("Cast skipped: column '%s' not found.", col)
                continue
            try:
                if "datetime" in str(dtype):
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                else:
                    df[col] = df[col].astype(dtype)
            except (ValueError, TypeError) as exc:
                self.logger.warning("Cast failed for column '%s' → '%s': %s", col, dtype, exc)

        return df

    @staticmethod
    def _normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
        def to_snake(name: str) -> str:
            name = re.sub(r"[\s\-\.]+", "_", name.strip())
            name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
            name = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", name)
            return name.lower()

        df.columns = [to_snake(str(c)) for c in df.columns]
        return df

    def _build_metadata(
        self, df_in: pd.DataFrame, df_out: pd.DataFrame, **_: Any
    ) -> dict[str, Any]:
        return {
            "columns_normalized": self.normalize_columns,
            "nulls_dropped": self.drop_na is not False,
            "duplicates_dropped": self.drop_duplicates is not False,
            "columns_cast": list(self.cast.keys()),
            "nulls_filled": list(self.fill_na.keys()),
        }
