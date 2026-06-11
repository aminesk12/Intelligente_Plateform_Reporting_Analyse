"""End-to-end ETL test using the two CSV files in DATA/Excel/.

Run from the project root:
    python test_etl_pipeline.py

Pipelines
---------
1. Coffee Sales   : clean → aggregate by coffee type  → DATA/Output/coffee_*.csv
2. Chocolate Sales: parse currency → clean → aggregate by country → DATA/Output/chocolate_*.csv
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

from ETLS.core.logging_config import setup_logging
from ETLS.Loading_Scripts import FileLoader
from ETLS.Transformation_Scripts import (
    AggregationTransformer,
    CleaningTransformer,
    MappingTransformer,
    PipelineTransformer,
)

COFFEE_CSV = "DATA/Excel/Coffe Sales.csv"
CHOCOLATE_CSV = "DATA/Excel/Chocolate Sales.csv"


def _banner(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Pipeline 1 — Coffee Sales
# ---------------------------------------------------------------------------

def run_coffee_pipeline() -> None:
    _banner("PIPELINE 1: Coffee Sales")

    # --- Extract ---
    df = pd.read_csv(COFFEE_CSV)
    print(f"Extracted : {len(df)} rows | columns: {list(df.columns)}")

    # --- Transform ---
    pipeline = PipelineTransformer(
        steps=[
            CleaningTransformer(
                normalize_columns=True,   # "coffee_name" stays clean
                strip_strings=True,
                drop_na=True,
                drop_duplicates=True,
                cast={
                    "date":     "datetime64[ns]",
                    "datetime": "datetime64[ns]",
                },
                name="coffee_clean",
            ),
        ],
        name="coffee_pipeline",
    )
    cleaned = pipeline.transform(df)
    print(f"After clean: {cleaned.output_rows} rows | row delta: {cleaned.row_delta}")
    print(f"Columns    : {list(cleaned.data.columns)}")

    # Aggregate: total + mean revenue and transaction count per coffee type
    agg = AggregationTransformer(
        group_by=["coffee_name"],
        agg={"money": ["sum", "mean", "count"]},
        sort_by=["money_sum"],
        ascending=False,
        name="coffee_agg",
    )
    aggregated = agg.transform(cleaned)
    print(f"\nAggregated by coffee type ({aggregated.output_rows} rows):")
    print(aggregated.data.to_string(index=False))

    # --- Load ---
    with FileLoader("DATA/Output/coffee_sales_clean_{timestamp}.csv",
                    name="coffee_clean_out") as loader:
        r = loader.load(cleaned)
        print(f"\n[FileLoader] cleaned  -> {r.metadata.get('path')} ({r.rows_loaded} rows)")

    with FileLoader("DATA/Output/coffee_sales_by_type_{timestamp}.csv",
                    name="coffee_agg_out") as loader:
        r = loader.load(aggregated)
        print(f"[FileLoader] agg      -> {r.metadata.get('path')} ({r.rows_loaded} rows)")


# ---------------------------------------------------------------------------
# Pipeline 2 — Chocolate Sales
# ---------------------------------------------------------------------------

def run_chocolate_pipeline() -> None:
    _banner("PIPELINE 2: Chocolate Sales")

    # --- Extract ---
    df = pd.read_csv(CHOCOLATE_CSV)
    print(f"Extracted : {len(df)} rows | columns: {list(df.columns)}")
    print(f"Sample Amount values: {df['Amount'].head(3).tolist()}")

    # --- Transform ---
    pipeline = PipelineTransformer(
        steps=[
            # Step 1: parse "$5,320.00" → float, drop original Amount column
            MappingTransformer(
                add_columns={
                    "amount_usd": lambda d: (
                        d["Amount"].str.replace(r"[$,]", "", regex=True).astype(float)
                    ),
                },
                drop_columns=["Amount"],
                name="choc_map",
            ),
            # Step 2: normalise names, clean, cast date
            CleaningTransformer(
                normalize_columns=True,   # "Sales Person" → "sales_person", etc.
                strip_strings=True,
                drop_na=True,
                drop_duplicates=True,
                cast={"date": "datetime64[ns]"},
                name="choc_clean",
            ),
        ],
        name="chocolate_pipeline",
    )
    cleaned = pipeline.transform(df)
    print(f"After clean: {cleaned.output_rows} rows | row delta: {cleaned.row_delta}")
    print(f"Columns    : {list(cleaned.data.columns)}")

    # Aggregate: revenue and boxes per country, sorted largest first
    agg = AggregationTransformer(
        group_by=["country"],
        agg={"amount_usd": "sum", "boxes_shipped": "sum"},
        sort_by=["amount_usd"],
        ascending=False,
        name="choc_agg",
    )
    aggregated = agg.transform(cleaned)
    print(f"\nAggregated by country ({aggregated.output_rows} rows):")
    print(aggregated.data.to_string(index=False))

    # --- Load ---
    with FileLoader("DATA/Output/chocolate_sales_clean_{timestamp}.csv",
                    name="choc_clean_out") as loader:
        r = loader.load(cleaned)
        print(f"\n[FileLoader] cleaned  -> {r.metadata.get('path')} ({r.rows_loaded} rows)")

    with FileLoader("DATA/Output/chocolate_sales_by_country_{timestamp}.csv",
                    name="choc_agg_out") as loader:
        r = loader.load(aggregated)
        print(f"[FileLoader] agg      -> {r.metadata.get('path')} ({r.rows_loaded} rows)")


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    setup_logging(level="WARNING")   # suppress INFO noise; set "DEBUG" to see all steps

    run_coffee_pipeline()
    run_chocolate_pipeline()

    print("\n\nAll pipelines complete. Check DATA/Output/ for the result files.")
