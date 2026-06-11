"""End-to-end ETL test: DatabaseExtractor -> FileLoader.

Uses the SQLite database created by test_db_etl_pipeline.py as the source.
If the DB is missing the required tables, a setup step recreates them from
the source CSVs.

Pipelines
---------
1. Full-table extract  : DatabaseExtractor(coffee_sales)   -> FileLoader (CSV)
2. Full-table extract  : DatabaseExtractor(chocolate_sales) -> FileLoader (Excel)
3. Custom SQL query    : DatabaseExtractor(filtered query)  -> FileLoader (JSON)
4. Chunked extract     : DatabaseExtractor(chunksize=500)   -> FileLoader (Parquet)

Verify
------
Each output file is read back and its row count is compared against what
DatabaseExtractor reported.

Run from the project root:
    python test_db_file_pipeline.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import sqlalchemy

sys.path.insert(0, str(Path(__file__).parent))

from ETLS.core.logging_config import setup_logging
from ETLS.Extraction_Scripts.DataBase_Extractor import DatabaseExtractor
from ETLS.Loading_Scripts.file_loader import FileLoader

DB_URL     = "sqlite:///DATA/Output/etl_test.db"
OUTPUT_DIR = Path("DATA/Output")

COFFEE_CSV    = "DATA/Excel/Coffe Sales.csv"
CHOCOLATE_CSV = "DATA/Excel/Chocolate Sales.csv"


def _banner(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Setup — ensure the SQLite DB has the source tables
# ---------------------------------------------------------------------------

def _ensure_db_tables() -> None:
    """Create coffee_sales / chocolate_sales in SQLite if they are missing."""
    engine = sqlalchemy.create_engine(DB_URL)
    inspector = sqlalchemy.inspect(engine)
    existing = inspector.get_table_names()

    if "coffee_sales" not in existing:
        print("  coffee_sales missing -> creating from CSV ...")
        pd.read_csv(COFFEE_CSV).to_sql(
            "coffee_sales", engine, index=False, if_exists="replace"
        )
    else:
        print("  coffee_sales already present")

    if "chocolate_sales" not in existing:
        print("  chocolate_sales missing -> creating from CSV ...")
        df = pd.read_csv(CHOCOLATE_CSV)
        df["amount_usd"] = (
            df["Amount"].str.replace(r"[$,]", "", regex=True).astype(float)
        )
        df.drop(columns=["Amount"], inplace=True)
        df.to_sql("chocolate_sales", engine, index=False, if_exists="replace")
    else:
        print("  chocolate_sales already present")

    engine.dispose()


# ---------------------------------------------------------------------------
# Verification helper
# ---------------------------------------------------------------------------

def _verify_file(path: str, expected_rows: int) -> None:
    p = Path(path)
    suffix = p.suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(p)
    elif suffix in {".xlsx", ".xls"}:
        df = pd.read_excel(p)
    elif suffix == ".json":
        df = pd.read_json(p)
    elif suffix == ".parquet":
        df = pd.read_parquet(p)
    else:
        print(f"  [verify] unknown extension {suffix} — skipping read-back")
        return

    status = "OK" if len(df) == expected_rows else "MISMATCH"
    print(
        f"  [verify] {p.name}: {len(df)} rows in file "
        f"(expected {expected_rows}) -> {status}"
    )


# ---------------------------------------------------------------------------
# Pipeline 1 — full table -> CSV
# ---------------------------------------------------------------------------

def run_pipeline_csv() -> None:
    _banner("PIPELINE 1: coffee_sales table -> CSV")

    print("\n[1] Extracting full table 'coffee_sales' from SQLite ...")
    with DatabaseExtractor(DB_URL, name="sqlite_coffee") as extractor:
        result = extractor.extract(table="coffee_sales")

    print(f"  Extracted  : {result.row_count} rows, {result.column_count} cols")
    print(f"  Duration   : {result.duration_seconds:.2f}s")
    print(f"  Columns    : {list(result.data.columns)}")

    out_path = "DATA/Output/coffee_sales_export.csv"
    print(f"\n[2] Loading to {out_path} ...")
    loader = FileLoader(out_path, name="coffee_csv_out")
    load_result = loader.load(result)

    _print_load_result(load_result)
    _verify_file(load_result.metadata["path"], result.row_count)


# ---------------------------------------------------------------------------
# Pipeline 2 — full table -> Excel
# ---------------------------------------------------------------------------

def run_pipeline_excel() -> None:
    _banner("PIPELINE 2: chocolate_sales table -> Excel")

    print("\n[1] Extracting full table 'chocolate_sales' from SQLite ...")
    with DatabaseExtractor(DB_URL, name="sqlite_chocolate") as extractor:
        result = extractor.extract(table="chocolate_sales")

    print(f"  Extracted  : {result.row_count} rows, {result.column_count} cols")
    print(f"  Duration   : {result.duration_seconds:.2f}s")
    print(f"  Columns    : {list(result.data.columns)}")

    out_path = "DATA/Output/chocolate_sales_export.xlsx"
    print(f"\n[2] Loading to {out_path} ...")
    loader = FileLoader(
        out_path,
        write_options={"sheet_name": "ChocolateSales", "freeze_panes": (1, 0)},
        name="chocolate_excel_out",
    )
    load_result = loader.load(result)

    _print_load_result(load_result)
    _verify_file(load_result.metadata["path"], result.row_count)


# ---------------------------------------------------------------------------
# Pipeline 3 — custom SQL query -> JSON
# ---------------------------------------------------------------------------

def run_pipeline_json() -> None:
    _banner("PIPELINE 3: filtered SQL query -> JSON")

    query = "SELECT * FROM coffee_sales WHERE coffee_name = 'Latte' LIMIT 200"
    print(f"\n[1] Running query: {query}")
    with DatabaseExtractor(DB_URL, name="sqlite_cash_coffee") as extractor:
        result = extractor.extract(query=query)

    print(f"  Extracted  : {result.row_count} rows, {result.column_count} cols")
    print(f"  Duration   : {result.duration_seconds:.2f}s")

    out_path = "DATA/Output/coffee_cash_sales.json"
    print(f"\n[2] Loading to {out_path} ...")
    loader = FileLoader(
        out_path,
        write_options={"orient": "records", "indent": 2},
        name="coffee_json_out",
    )
    load_result = loader.load(result)

    _print_load_result(load_result)
    _verify_file(load_result.metadata["path"], result.row_count)


# ---------------------------------------------------------------------------
# Pipeline 4 — chunked extract -> Parquet
# ---------------------------------------------------------------------------

def run_pipeline_parquet() -> None:
    _banner("PIPELINE 4: chunked extract -> Parquet")

    print("\n[1] Extracting 'coffee_sales' in 500-row chunks ...")
    with DatabaseExtractor(DB_URL, name="sqlite_coffee_chunked") as extractor:
        result = extractor.extract(table="coffee_sales", chunksize=500)

    print(f"  Extracted  : {result.row_count} rows, {result.column_count} cols")
    print(f"  Duration   : {result.duration_seconds:.2f}s")

    out_path = "DATA/Output/coffee_sales_export.parquet"
    print(f"\n[2] Loading to {out_path} ...")
    loader = FileLoader(out_path, name="coffee_parquet_out")
    try:
        load_result = loader.load(result)
        _print_load_result(load_result)
        _verify_file(load_result.metadata["path"], result.row_count)
    except ImportError as exc:
        print(f"  [skip] Parquet requires pyarrow: {exc}")
        print("         Install with: pip install pyarrow")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _print_load_result(load_result) -> None:
    print(f"  Destination: {load_result.destination}")
    print(f"  Rows loaded: {load_result.rows_loaded}")
    print(f"  Duration   : {load_result.duration_seconds:.2f}s")
    print(f"  Format     : {load_result.metadata.get('format', 'N/A')}")
    print(f"  File       : {load_result.metadata.get('path', 'N/A')}")
    print(f"  Size       : {load_result.metadata.get('file_size_bytes', 'N/A')} bytes")


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    setup_logging(level="WARNING")

    _banner("SETUP: Ensuring SQLite source tables exist")
    _ensure_db_tables()

    run_pipeline_csv()
    run_pipeline_excel()
    run_pipeline_json()
    run_pipeline_parquet()

    _banner("ALL PIPELINES COMPLETE")
    print(f"  Source DB  : {DB_URL.replace('sqlite:///', '')}")
    print("  Output dir : DATA/Output/")
    print("  Files      : coffee_sales_export.csv")
    print("               chocolate_sales_export.xlsx")
    print("               coffee_cash_sales.json")
    print("               coffee_sales_export.parquet  (if pyarrow installed)")
    print("\nDone.\n")
