"""End-to-end ETL run: CSV data → transform → load into the PostgreSQL "PFE_DATABASE" database.

Run from the project root:
    python run_postgres_pipeline.py

Prerequisites
-------------
1. `pip install -r requirements.txt` (installs the `psycopg2-binary` driver).
2. `cp .env.example .env` and fill in POSTGRES_HOST / POSTGRES_PORT /
   POSTGRES_DB / POSTGRES_USER / POSTGRES_PASSWORD for your PFE_DATABASE database.

Sources are declared in ETLS/config/sources.yaml, not in this file — adding a
new CSV only needs a new entry there (file/clean/aggregation/targets). A
source only needs a change *here* if its transform requires logic beyond
CleaningTransformer/AggregationTransformer (e.g. the chocolate source's
currency-string parsing) — add that one step to CUSTOM_STEP_FACTORIES below
and reference it by name via the source's `custom_steps` list in the YAML.

Each table is loaded with `if_exists="replace"`, so reruns are safe.
"""

from __future__ import annotations

import sys
from typing import Any, Callable

import pandas as pd

from ETLS.core.base_transformer import BaseTransformer
from ETLS.core.config import load_yaml_config
from ETLS.core.logging_config import setup_logging
from ETLS.Loading_Scripts import DatabaseLoader
from ETLS.Transformation_Scripts import (
    AggregationTransformer,
    CleaningTransformer,
    MappingTransformer,
    PipelineTransformer,
)

DATABASE_CONFIG = "ETLS/config/database_config.yaml"
SOURCES_CONFIG = "ETLS/config/sources.yaml"


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
# Custom steps: transformations too bespoke for plain YAML (e.g. an arbitrary
# parsing callable). Referenced by name from a source's `custom_steps` list
# in ETLS/config/sources.yaml.
# ---------------------------------------------------------------------------

def _parse_chocolate_amount() -> MappingTransformer:
    """Parse "$5,320.00" -> float, dropping the original Amount column."""
    return MappingTransformer(
        add_columns={
            "amount_usd": lambda d: (
                d["Amount"].str.replace(r"[$,]", "", regex=True).astype(float)
            ),
        },
        drop_columns=["Amount"],
        name="choc_map",
    )


CUSTOM_STEP_FACTORIES: dict[str, Callable[[], BaseTransformer]] = {
    "parse_chocolate_amount": _parse_chocolate_amount,
}


# ---------------------------------------------------------------------------

def run_source(cfg: dict[str, Any], connection_url: str, schema: str | None, index: int) -> None:
    """Extract, clean, aggregate, and load one source — entirely driven by
    its ``sources.yaml`` entry."""
    name = cfg["name"]
    label = cfg.get("label", name)
    _banner(f"PIPELINE {index}: {label} -> PostgreSQL PFE_DATABASE")

    # --- Extract ---
    df = pd.read_csv(cfg["file"])
    print(f"Extracted : {len(df)} rows | columns: {list(df.columns)}")

    # --- Transform ---
    steps: list[BaseTransformer] = [
        CUSTOM_STEP_FACTORIES[step_name]() for step_name in cfg.get("custom_steps", [])
    ]
    steps.append(CleaningTransformer(**cfg["clean"], name=f"{name}_clean"))
    pipeline = PipelineTransformer(steps=steps, name=f"{name}_pipeline")
    cleaned = pipeline.transform(df)
    print(f"After clean: {cleaned.output_rows} rows | row delta: {cleaned.row_delta}")

    agg_cfg = dict(cfg["aggregation"])
    log_label = agg_cfg.pop("log_label")
    agg = AggregationTransformer(**agg_cfg, name=f"{name}_agg")
    aggregated = agg.transform(cleaned)
    print(f"Aggregated {log_label}: {aggregated.output_rows} rows")

    # --- Load ---
    targets = cfg["targets"]
    with _postgres_loader(connection_url, schema, targets["cleaned"]) as loader:
        r = loader.load(cleaned)
        print(f"[Postgres] cleaned -> {r.destination} ({r.rows_loaded} rows)")

    with _postgres_loader(connection_url, schema, targets["aggregated"]) as loader:
        r = loader.load(aggregated)
        print(f"[Postgres] agg     -> {r.destination} ({r.rows_loaded} rows)")


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    setup_logging(level="WARNING")  # suppress INFO noise; set "DEBUG" to see all steps

    postgres_cfg = load_yaml_config(DATABASE_CONFIG, section="postgres")
    connection_url = postgres_cfg.get("connection_url") or DatabaseLoader._build_url(postgres_cfg)
    schema = postgres_cfg.get("schema")

    sources = load_yaml_config(SOURCES_CONFIG, section="sources")

    failure_count = 0
    for i, source_cfg in enumerate(sources, start=1):
        try:
            run_source(source_cfg, connection_url, schema, i)
        except Exception as exc:  # noqa: BLE001 - isolate one bad source from the rest
            print(f"\n[FAILED] source '{source_cfg.get('name', i)}': {exc}", file=sys.stderr)
            failure_count += 1

    if failure_count:
        sys.exit(1)

    print("\n\nAll pipelines complete. Tables loaded into the PostgreSQL PFE_DATABASE database.")
