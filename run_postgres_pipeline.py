"""End-to-end ETL run: CSV data → transform → load into the PostgreSQL "PFE_DATABASE" database.

Run from the project root:
    python run_postgres_pipeline.py

Prerequisites
-------------
1. `pip install -r requirements.txt` (installs the `psycopg2-binary` driver).
2. `cp .env.example .env` and fill in POSTGRES_HOST / POSTGRES_PORT /
   POSTGRES_DB / POSTGRES_USER / POSTGRES_PASSWORD for your PFE_DATABASE database.

Pipelines
---------
1. Coffee Sales   : clean            → etls_coffee_sales
                     → aggregate by coffee type → etls_coffee_sales_by_type
2. Chocolate Sales: parse currency, clean       → etls_chocolate_sales
                     → aggregate by country      → etls_chocolate_sales_by_country

Each table is loaded with `if_exists="replace"`, so reruns are safe.
"""

from __future__ import annotations

import pandas as pd

from ETLS.core.config import load_yaml_config
from ETLS.core.logging_config import setup_logging
from ETLS.Loading_Scripts import DatabaseLoader
from ETLS.Transformation_Scripts import (
    AggregationTransformer,
    CleaningTransformer,
    MappingTransformer,
    PipelineTransformer,
)

COFFEE_CSV = "DATA/Excel/Coffe Sales.csv"
CHOCOLATE_CSV = "DATA/Excel/Chocolate Sales.csv"
DATABASE_CONFIG = "ETLS/config/database_config.yaml"


def _banner(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print("=" * 60)


def _postgres_loader(connection_url: str, schema: str | None, table: str) -> DatabaseLoader:
    """Build a DatabaseLoader for one table in the PostgreSQL PFE_DATABASE database."""
    return DatabaseLoader(
        connection_url=connection_url,
        table=table,
        schema=schema,
        if_exists="replace",
        name=f"postgres_pfe.{table}",
    )


# ---------------------------------------------------------------------------
# Pipeline 1 — Coffee Sales
# ---------------------------------------------------------------------------

def run_coffee_pipeline(connection_url: str, schema: str | None) -> None:
    _banner("PIPELINE 1: Coffee Sales -> PostgreSQL PFE_DATABASE")

    # --- Extract ---
    df = pd.read_csv(COFFEE_CSV)
    print(f"Extracted : {len(df)} rows | columns: {list(df.columns)}")

    # --- Transform ---
    pipeline = PipelineTransformer(
        steps=[
            CleaningTransformer(
                normalize_columns=True,
                strip_strings=True,
                drop_na=True,
                drop_duplicates=True,
                cast={
                    "date": "datetime64[ns]",
                    "datetime": "datetime64[ns]",
                },
                name="coffee_clean",
            ),
        ],
        name="coffee_pipeline",
    )
    cleaned = pipeline.transform(df)
    print(f"After clean: {cleaned.output_rows} rows | row delta: {cleaned.row_delta}")

    agg = AggregationTransformer(
        group_by=["coffee_name"],
        agg={"money": ["sum", "mean", "count"]},
        sort_by=["money_sum"],
        ascending=False,
        name="coffee_agg",
    )
    aggregated = agg.transform(cleaned)
    print(f"Aggregated by coffee type: {aggregated.output_rows} rows")

    # --- Load ---
    with _postgres_loader(connection_url, schema, "etls_coffee_sales") as loader:
        r = loader.load(cleaned)
        print(f"[Postgres] cleaned -> {r.destination} ({r.rows_loaded} rows)")

    with _postgres_loader(connection_url, schema, "etls_coffee_sales_by_type") as loader:
        r = loader.load(aggregated)
        print(f"[Postgres] agg     -> {r.destination} ({r.rows_loaded} rows)")


# ---------------------------------------------------------------------------
# Pipeline 2 — Chocolate Sales
# ---------------------------------------------------------------------------

def run_chocolate_pipeline(connection_url: str, schema: str | None) -> None:
    _banner("PIPELINE 2: Chocolate Sales -> PostgreSQL PFE_DATABASE")

    # --- Extract ---
    df = pd.read_csv(CHOCOLATE_CSV)
    print(f"Extracted : {len(df)} rows | columns: {list(df.columns)}")

    # --- Transform ---
    pipeline = PipelineTransformer(
        steps=[
            # Step 1: parse "$5,320.00" -> float, drop original Amount column
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
    cleaned = pipeline.transform(df)
    print(f"After clean: {cleaned.output_rows} rows | row delta: {cleaned.row_delta}")

    agg = AggregationTransformer(
        group_by=["country"],
        agg={"amount_usd": "sum", "boxes_shipped": "sum"},
        sort_by=["amount_usd"],
        ascending=False,
        name="choc_agg",
    )
    aggregated = agg.transform(cleaned)
    print(f"Aggregated by country: {aggregated.output_rows} rows")

    # --- Load ---
    with _postgres_loader(connection_url, schema, "etls_chocolate_sales") as loader:
        r = loader.load(cleaned)
        print(f"[Postgres] cleaned -> {r.destination} ({r.rows_loaded} rows)")

    with _postgres_loader(connection_url, schema, "etls_chocolate_sales_by_country") as loader:
        r = loader.load(aggregated)
        print(f"[Postgres] agg     -> {r.destination} ({r.rows_loaded} rows)")


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    setup_logging(level="WARNING")  # suppress INFO noise; set "DEBUG" to see all steps

    postgres_cfg = load_yaml_config(DATABASE_CONFIG, section="postgres")
    connection_url = postgres_cfg.get("connection_url") or DatabaseLoader._build_url(postgres_cfg)
    schema = postgres_cfg.get("schema")

    run_coffee_pipeline(connection_url, schema)
    run_chocolate_pipeline(connection_url, schema)

    print("\n\nAll pipelines complete. Tables loaded into the PostgreSQL PFE_DATABASE database.")
