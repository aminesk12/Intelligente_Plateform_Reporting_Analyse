"""Tests for the BaseExtractor contract and ExtractionResult wrapper."""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytest

from ETLS.core.base_extractor import BaseExtractor, ExtractionResult
from ETLS.core.exceptions import ExtractionError, ValidationError


class _GoodExtractor(BaseExtractor):
    """Minimal concrete extractor returning a fixed frame."""

    source_type = "dummy"

    def __init__(self, df: pd.DataFrame, **kw):
        super().__init__(**kw)
        self._df = df

    def validate(self) -> None:
        pass

    def _extract(self, **kwargs) -> pd.DataFrame:
        return self._df

    def _build_metadata(self, df, **kwargs):
        return {"extra": "meta"}


class _BadReturnExtractor(_GoodExtractor):
    def _extract(self, **kwargs):
        return ["not", "a", "frame"]


class _FailingValidate(_GoodExtractor):
    def validate(self) -> None:
        raise ValidationError("nope")


class _RaisesPlain(_GoodExtractor):
    def _extract(self, **kwargs):
        raise RuntimeError("boom")


# -- ExtractionResult --------------------------------------------------------


def test_extraction_result_properties(sample_frame):
    res = ExtractionResult(
        data=sample_frame,
        source_type="dummy",
        source_name="t",
        extracted_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        duration_seconds=1.2345,
    )
    assert res.row_count == 3
    assert res.column_count == 3
    assert res.is_empty is False

    summary = res.summary()
    assert summary["row_count"] == 3
    assert summary["column_count"] == 3
    assert summary["columns"] == ["id", "name", "amount"]
    assert summary["duration_seconds"] == 1.234  # rounded to 3 dp


def test_extraction_result_empty():
    res = ExtractionResult(
        data=pd.DataFrame(),
        source_type="dummy",
        source_name="t",
        extracted_at=datetime.now(timezone.utc),
        duration_seconds=0.0,
    )
    assert res.is_empty is True
    assert res.row_count == 0


def test_summary_merges_metadata(sample_frame):
    res = ExtractionResult(
        data=sample_frame,
        source_type="dummy",
        source_name="t",
        extracted_at=datetime.now(timezone.utc),
        duration_seconds=0.0,
        metadata={"file": "x.csv"},
    )
    assert res.summary()["file"] == "x.csv"


# -- BaseExtractor.extract ---------------------------------------------------


def test_extract_returns_result_with_provenance(sample_frame):
    ext = _GoodExtractor(sample_frame, name="my-source")
    result = ext.extract()

    assert isinstance(result, ExtractionResult)
    assert result.source_type == "dummy"
    assert result.source_name == "my-source"
    assert result.row_count == 3
    assert result.duration_seconds >= 0
    assert result.metadata == {"extra": "meta"}
    assert result.extracted_at.tzinfo is timezone.utc


def test_default_name_is_class_name(sample_frame):
    assert _GoodExtractor(sample_frame).name == "_GoodExtractor"


def test_extract_rejects_non_dataframe(sample_frame):
    with pytest.raises(ExtractionError, match="must return a pandas DataFrame"):
        _BadReturnExtractor(sample_frame).extract()


def test_validation_error_propagates(sample_frame):
    with pytest.raises(ValidationError, match="nope"):
        _FailingValidate(sample_frame).extract()


def test_plain_exception_is_translated(sample_frame):
    """Unexpected (non-ETL) errors are wrapped in ExtractionError."""
    with pytest.raises(ExtractionError, match="boom"):
        _RaisesPlain(sample_frame).extract()


def test_cannot_instantiate_abstract_base():
    with pytest.raises(TypeError):
        BaseExtractor()  # type: ignore[abstract]
