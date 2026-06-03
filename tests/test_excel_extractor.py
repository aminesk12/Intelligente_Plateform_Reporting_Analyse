"""Tests for ExcelExtractor."""

from __future__ import annotations

import pandas as pd
import pytest

from ETLS.Extraction_Scripts.Excel_Extractor import ExcelExtractor
from ETLS.core.exceptions import ValidationError


@pytest.fixture
def workbook(tmp_path, sample_frame):
    """Write `sample_frame` to a two-sheet workbook and return its path."""
    path = tmp_path / "book.xlsx"
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        sample_frame.to_excel(writer, sheet_name="first", index=False)
        sample_frame.head(1).to_excel(writer, sheet_name="second", index=False)
    return path


def test_extract_default_sheet(workbook, sample_frame):
    ext = ExcelExtractor(workbook)
    result = ext.extract()
    assert result.source_type == "excel"
    assert result.row_count == 3
    pd.testing.assert_frame_equal(result.data, sample_frame)


def test_name_defaults_to_filename(workbook):
    assert ExcelExtractor(workbook).name == "book.xlsx"


def test_extract_named_sheet(workbook):
    ext = ExcelExtractor(workbook, sheet_name="second")
    assert ext.extract().row_count == 1


def test_sheet_override_at_extract_time(workbook):
    ext = ExcelExtractor(workbook, sheet_name="first")
    # extract-time kwarg wins over the constructor default
    assert ext.extract(sheet_name="second").row_count == 1


def test_read_options_forwarded(workbook):
    ext = ExcelExtractor(workbook, read_options={"usecols": ["id"]})
    result = ext.extract()
    assert list(result.data.columns) == ["id"]


def test_metadata(workbook):
    ext = ExcelExtractor(workbook, sheet_name="first")
    meta = ext.extract().metadata
    assert meta["file_path"] == str(workbook)
    assert meta["sheet_name"] == "first"
    assert meta["file_size_bytes"] > 0


def test_validate_missing_file(tmp_path):
    ext = ExcelExtractor(tmp_path / "nope.xlsx")
    with pytest.raises(ValidationError, match="File not found"):
        ext.extract()


def test_validate_directory_not_file(tmp_path):
    # A directory with an .xlsx-looking name still isn't a file.
    d = tmp_path / "weird.xlsx"
    d.mkdir()
    ext = ExcelExtractor(d)
    with pytest.raises(ValidationError, match="Not a file"):
        ext.extract()


def test_validate_unsupported_extension(tmp_path):
    bad = tmp_path / "data.csv"
    bad.write_text("a,b\n1,2\n", encoding="utf-8")
    ext = ExcelExtractor(bad)
    with pytest.raises(ValidationError, match="Unsupported Excel format"):
        ext.extract()


def test_engine_selection(workbook):
    ext = ExcelExtractor(workbook)
    assert ext._engine_for(".xls") == "xlrd"
    assert ext._engine_for(".xlsx") == "openpyxl"
    assert ext._engine_for(".xlsm") == "openpyxl"


def test_from_config(tmp_path, workbook):
    cfg = tmp_path / "cfg.yaml"
    cfg.write_text(
        "excel:\n"
        f"  file_path: {workbook}\n"
        "  sheet_name: second\n"
        "  name: configured\n",
        encoding="utf-8",
    )
    ext = ExcelExtractor.from_config(cfg)
    assert ext.name == "configured"
    assert ext.sheet_name == "second"
    assert ext.extract().row_count == 1
