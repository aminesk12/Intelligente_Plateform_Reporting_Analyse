"""End-to-end ETL test: ExcelExtractor -> CleaningTransformer -> DatabaseLoader (SQLite).

Run from the project root:
    python test_db_etl_pipeline.py

Setup
-----
Converts the two CSVs in DATA/Excel/ into .xlsx fixtures so ExcelExtractor
can read them. The fixtures land in DATA/Output/ and are overwritten each run.

Pipelines
---------
1. Coffee Sales    : ExcelExtractor -> CleaningTransformer -> SQLite (table: coffee_sales)
2. Chocolate Sales : ExcelExtractor -> MappingTransformer -> CleaningTransformer -> SQLite (table: chocolate_sales)

Verify
------
After each load, the table is read back with pandas and the row count is
confirmed to match what was loaded.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import sqlalchemy

sys.path.insert(0, str(Path(__file__).parent))

from ETLS.core.logging_config import setup_logging
from ETLS.Extraction_Scripts import ExcelExtractor
from ETLS.Loading_Scripts import DatabaseLoader
from ETLS.Transformation_Scripts import CleaningTransformer, MappingTransformer, PipelineTransformer

EXCEL_CFG   = "ETLS/config/excel_config.yaml"
DB_CFG      = "ETLS/config/database_config.yaml"
DB_URL      = "sqlite:///DATA/Output/etl_test.db"
OUTPUT_DIR  = Path("DATA/Output")

COFFEE_CSV    = "DATA/Excel/Coffe Sales.csv"
CHOCOLATE_CSV = "DATA/Excel/Chocolate Sales.csv"
COFFEE_XLSX   = "DATA/Output/coffee_sales.xlsx"
CHOCOLATE_XLSX = "DATA/Output/chocolate_sales.xlsx"


def _banner(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Setup — create Excel fixtures from the CSVs
# ---------------------------------------------------------------------------

def _create_excel_fixtures() -> None:
    """Convert the source CSVs to .xlsx so ExcelExtractor can read them."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    pd.read_csv(COFFEE_CSV).to_excel(COFFEE_XLSX, index=False)
    pd.read_csv(CHOCOLATE_CSV).to_excel(CHOCOLATE_XLSX, index=False)
    print(f"  Fixtures : {COFFEE_XLSX}")
    print(f"             {CHOCOLATE_XLSX}")


# ---------------------------------------------------------------------------
# Verification helper
# ---------------------------------------------------------------------------

def _verify_table(db_url: str, table: str, expected_rows: int) -> None:
    """Read the table back from the DB and confirm the row count."""
    engine = sqlalchemy.create_engine(db_url)
    with engine.connect() as conn:
        df_back = pd.read_sql_table(table, conn)
    status = "OK" if len(df_back) == expected_rows else "MISMATCH"
    print(f"  [verify] {table}: {len(df_back)} rows in DB (expected {expected_rows}) -> {status}")
    engine.dispose()


# ---------------------------------------------------------------------------
# Pipeline 1 — Coffee Sales
# ---------------------------------------------------------------------------

def run_coffee_pipeline() -> None:
    _banner("PIPELINE 1: Coffee Sales  (ExcelExtractor -> DatabaseLoader)")

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

    # -- Load ------------------------------------------------------------------
    print("\n[3] Loading to SQLite table 'coffee_sales' ...")
    with DatabaseLoader(
        DB_URL,
        table="coffee_sales",
        if_exists="replace",
        name="coffee_db_out",
    ) as loader:
        load_result = loader.load(cleaned)

    print(f"  Destination: {load_result.destination}")
    print(f"  Rows loaded: {load_result.rows_loaded}")
    print(f"  Duration   : {load_result.duration_seconds:.2f}s")

    # -- Verify ----------------------------------------------------------------
    _verify_table(DB_URL, "coffee_sales", cleaned.output_rows)


# ---------------------------------------------------------------------------
# Pipeline 2 — Chocolate Sales
# ---------------------------------------------------------------------------

def run_chocolate_pipeline() -> None:
    _banner("PIPELINE 2: Chocolate Sales  (ExcelExtractor -> DatabaseLoader)")

    # -- Extract ---------------------------------------------------------------
    print("\n[1] Extracting from", CHOCOLATE_XLSX, "...")
    extractor = ExcelExtractor.from_config(EXCEL_CFG, section="chocolate")
    result = extractor.extract()

    print(f"  Extracted  : {result.row_count} rows, {result.column_count} cols")
    print(f"  Duration   : {result.duration_seconds:.2f}s")
    print(f"  Columns    : {list(result.data.columns)}")

    # -- Transform -------------------------------------------------------------
    print("\n[2] Mapping + Cleaning ...")
    pipeline = PipelineTransformer(
        steps=[
            MappingTransformer(
                add_columns={
                    "amount_usd": lambda d: (
                        d["Amount"].str.replace(r"[$,]", "", regex=True).astype(float)
                    ),
                },
                drop_columns=["Amount"],
                name="choc_map",
            ),
            CleaningTransformer(
                normalize_columns=True,
                strip_strings=True,
                drop_na=True,
                drop_duplicates=True,
                cast={"date": "datetime64[ns]"},
                name="choc_clean",
            ),
        ],
        name="chocolate_pipeline",
    )
    cleaned = pipeline.transform(result)
    print(f"  After clean: {cleaned.output_rows} rows (delta {cleaned.row_delta:+d})")
    print(f"  Columns    : {list(cleaned.data.columns)}")

    # -- Load ------------------------------------------------------------------
    print("\n[3] Loading to SQLite table 'chocolate_sales' ...")
    with DatabaseLoader(
        DB_URL,
        table="chocolate_sales",
        if_exists="replace",
        name="chocolate_db_out",
    ) as loader:
        load_result = loader.load(cleaned)

    print(f"  Destination: {load_result.destination}")
    print(f"  Rows loaded: {load_result.rows_loaded}")
    print(f"  Duration   : {load_result.duration_seconds:.2f}s")

    # -- Verify ----------------------------------------------------------------
    _verify_table(DB_URL, "chocolate_sales", cleaned.output_rows)


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    setup_logging(level="WARNING")

    _banner("SETUP: Creating Excel fixtures from CSVs")
    _create_excel_fixtures()

    run_coffee_pipeline()
    run_chocolate_pipeline()

    _banner("ALL PIPELINES COMPLETE")
    print(f"  Database   : {DB_URL.replace('sqlite:///', '')}")
    print("  Tables     : coffee_sales, chocolate_sales")
    print("\nDone.\n")
