"""End-to-end ETL test: ExcelExtractor -> DataSphereLoader (SAP HANA mocked).

The HANA driver (hdbcli) is mocked so this test runs without a real DataSphere
instance. It verifies that:
  - ExcelExtractor reads the Excel fixture correctly
  - CleaningTransformer normalises and deduplicates the data
  - DataSphereLoader calls the driver with the correct table/schema/DDL
  - LoadResult carries the right row count and metadata

Run from the project root:
    python test_sap_datasphere_pipeline.py
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

import ETLS.Loading_Scripts.datasphere_loader as _ds_module  # noqa: E402

from ETLS.core.logging_config import setup_logging  # noqa: E402
from ETLS.Extraction_Scripts import ExcelExtractor  # noqa: E402
from ETLS.Loading_Scripts.datasphere_loader import DataSphereLoader  # noqa: E402
from ETLS.Transformation_Scripts import CleaningTransformer  # noqa: E402

EXCEL_CFG    = "ETLS/config/excel_config.yaml"
OUTPUT_DIR   = Path("DATA/Output")
COFFEE_CSV   = "DATA/Excel/Coffe Sales.csv"
COFFEE_XLSX  = "DATA/Output/coffee_sales.xlsx"


def _banner(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Setup — create Excel fixture from CSV if missing
# ---------------------------------------------------------------------------

def _create_excel_fixture() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    pd.read_csv(COFFEE_CSV).to_excel(COFFEE_XLSX, index=False)
    print(f"  Fixture : {COFFEE_XLSX}")


# ---------------------------------------------------------------------------
# Mock helpers
# ---------------------------------------------------------------------------

def _build_mock_hdbapi(table_exists: bool = False) -> MagicMock:
    """Return a mock hdbcli.dbapi whose connect() returns a usable mock connection."""
    mock_cursor = MagicMock()
    # _table_exists() SELECT COUNT(*) → 1 if table exists, else 0
    mock_cursor.fetchone.return_value = (1 if table_exists else 0,)

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    mock_hdbapi = MagicMock()
    mock_hdbapi.connect.return_value = mock_conn

    return mock_hdbapi, mock_conn, mock_cursor


# ---------------------------------------------------------------------------
# Pipeline 1 — coffee sales: new table (if_exists='replace')
# ---------------------------------------------------------------------------

def run_pipeline_replace() -> None:
    _banner("PIPELINE 1: ExcelExtractor -> DataSphereLoader  (if_exists='replace')")

    # -- Extract ---------------------------------------------------------------
    print("\n[1] Extracting from", COFFEE_XLSX, "...")
    extractor = ExcelExtractor.from_config(EXCEL_CFG, section="coffee")
    result = extractor.extract()

    print(f"  Extracted  : {result.row_count} rows, {result.column_count} cols")
    print(f"  Duration   : {result.duration_seconds:.2f}s")
    print(f"  Columns    : {list(result.data.columns)}")

    # -- Transform -------------------------------------------------------------
    print("\n[2] Cleaning ...")
    cleaner = CleaningTransformer(
        normalize_columns=True,
        strip_strings=True,
        drop_na=True,
        drop_duplicates=True,
        cast={"date": "datetime64[ns]", "datetime": "datetime64[ns]"},
        name="coffee_clean",
    )
    cleaned = cleaner.transform(result)
    print(f"  After clean: {cleaned.output_rows} rows (delta {cleaned.row_delta:+d})")
    print(f"  Columns    : {list(cleaned.data.columns)}")

    # -- Load (mocked DataSphere) -----------------------------------------------
    print("\n[3] Loading to SAP DataSphere (mocked hdbcli, if_exists='replace') ...")

    mock_hdbapi, mock_conn, mock_cursor = _build_mock_hdbapi(table_exists=True)

    with (
        patch.object(_ds_module, "_HAS_HDBCLI", True),
        patch.object(_ds_module, "_HAS_SQLALCHEMY_HANA", False),
        patch.object(_ds_module, "_hdbapi", mock_hdbapi, create=True),
    ):
        loader = DataSphereLoader(
            host="test-space.hana.ondemand.com",
            port=443,
            user="DS_TEST_USER",
            password="test_password",
            table="COFFEE_SALES",
            space_schema="C_SPACE_TEST123",
            if_exists="replace",
            chunksize=500,
            name="coffee_datasphere",
        )
        load_result = loader.load(cleaned)

    _print_load_result(load_result)

    # Verify mock interactions
    assert load_result.rows_loaded == cleaned.output_rows, (
        f"Row count mismatch: {load_result.rows_loaded} != {cleaned.output_rows}"
    )
    assert load_result.metadata["table"] == "COFFEE_SALES"
    assert load_result.metadata["schema"] == "C_SPACE_TEST123"
    assert load_result.metadata["driver"] == "hdbcli"
    assert load_result.metadata["if_exists"] == "replace"

    # DROP TABLE should have been called because table existed and if_exists='replace'
    drop_calls = [
        str(call) for call in mock_cursor.execute.call_args_list
        if "DROP" in str(call).upper()
    ]
    assert drop_calls, "Expected a DROP TABLE call for if_exists='replace'"

    print("\n  [verify] rows_loaded matches cleaned row count  OK")
    print("  [verify] table / schema / driver metadata correct  OK")
    print("  [verify] DROP TABLE called for replace mode  OK")


# ---------------------------------------------------------------------------
# Pipeline 2 — coffee sales: append mode (no DDL, just INSERT)
# ---------------------------------------------------------------------------

def run_pipeline_append() -> None:
    _banner("PIPELINE 2: ExcelExtractor -> DataSphereLoader  (if_exists='append')")

    # -- Extract ---------------------------------------------------------------
    print("\n[1] Extracting from", COFFEE_XLSX, "...")
    extractor = ExcelExtractor.from_config(EXCEL_CFG, section="coffee")
    result = extractor.extract()
    print(f"  Extracted  : {result.row_count} rows, {result.column_count} cols")

    # -- Load (mocked DataSphere, table already exists) ------------------------
    print("\n[2] Loading to SAP DataSphere (mocked hdbcli, if_exists='append') ...")

    mock_hdbapi, mock_conn, mock_cursor = _build_mock_hdbapi(table_exists=True)

    with (
        patch.object(_ds_module, "_HAS_HDBCLI", True),
        patch.object(_ds_module, "_HAS_SQLALCHEMY_HANA", False),
        patch.object(_ds_module, "_hdbapi", mock_hdbapi, create=True),
    ):
        loader = DataSphereLoader(
            host="test-space.hana.ondemand.com",
            port=443,
            user="DS_TEST_USER",
            password="test_password",
            table="COFFEE_SALES",
            space_schema="C_SPACE_TEST123",
            if_exists="append",
            chunksize=200,
            name="coffee_datasphere_append",
        )
        load_result = loader.load(result)

    _print_load_result(load_result)

    # No DROP / CREATE should have been called in append mode
    ddl_calls = [
        str(call) for call in mock_cursor.execute.call_args_list
        if any(kw in str(call).upper() for kw in ("DROP", "CREATE TABLE"))
    ]
    assert not ddl_calls, f"Unexpected DDL in append mode: {ddl_calls}"

    # executemany should have been called at least once (the INSERT batches)
    assert mock_cursor.executemany.called, "Expected executemany() for bulk INSERT"

    print("\n  [verify] no DDL in append mode  OK")
    print("  [verify] executemany() called for bulk INSERT  OK")
    print(f"  [verify] executemany call count: {mock_cursor.executemany.call_count}  OK")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _print_load_result(load_result) -> None:
    print(f"  Destination: {load_result.destination}")
    print(f"  Rows loaded: {load_result.rows_loaded}")
    print(f"  Duration   : {load_result.duration_seconds:.2f}s")
    print(f"  Driver     : {load_result.metadata.get('driver', 'N/A')}")
    print(f"  Table      : {load_result.metadata.get('table', 'N/A')}")
    print(f"  Schema     : {load_result.metadata.get('schema', 'N/A')}")
    print(f"  if_exists  : {load_result.metadata.get('if_exists', 'N/A')}")


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    setup_logging(level="WARNING")

    _banner("SETUP: Creating Excel fixture from CSV")
    _create_excel_fixture()

    run_pipeline_replace()
    run_pipeline_append()

    _banner("ALL PIPELINES COMPLETE")
    print("  SAP DataSphere loader verified with mocked HANA connection.")
    print("\nDone.\n")
