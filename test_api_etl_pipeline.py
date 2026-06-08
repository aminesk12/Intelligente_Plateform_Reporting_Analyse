"""End-to-end ETL test: API Extractor → FileLoader + APILoader.

Run from the project root:
    python test_api_etl_pipeline.py

Pipeline
--------
1. Extract : APIExtractor  ← Alpha Vantage /query?function=MARKET_STATUS
2. Load A  : FileLoader    → DATA/Output/market_status_{timestamp}.csv
3. Load B  : FileLoader    → DATA/Output/market_status_{timestamp}.json
4. Load C  : APILoader     → https://httpbin.org/post  (public echo endpoint)
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from ETLS.core.logging_config import setup_logging
from ETLS.Extraction_Scripts import APIExtractor
from ETLS.Loading_Scripts import APILoader, FileLoader

CONFIG = "ETLS/config/api_config.yaml"


def _banner(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print("=" * 60)


def run_api_pipeline() -> None:
    _banner("API ETL PIPELINE: Alpha Vantage Market Status")

    # ── 1. Extract ────────────────────────────────────────────────────────────
    print("\n[1] Extracting from Alpha Vantage Market Status API ...")
    extractor = APIExtractor.from_config(CONFIG)
    result = extractor.extract()

    print(f"  Extracted  : {result.row_count} rows, {result.column_count} cols")
    print(f"  Duration   : {result.duration_seconds:.2f}s")
    print(f"  Source     : {result.metadata.get('base_url')}")
    print(f"  Columns    : {list(result.data.columns)}")
    print(f"\n  Preview:\n{result.data.to_string(index=False)}")

    if result.is_empty:
        print("\n  WARNING: empty result — skipping loaders.")
        return

    # ── 2a. FileLoader → CSV ──────────────────────────────────────────────────
    print("\n[2a] FileLoader -> CSV ...")
    with FileLoader(
        "DATA/Output/market_status_{timestamp}.csv",
        name="market_csv_out",
    ) as loader:
        csv_result = loader.load(result)

    print(f"  Path       : {csv_result.metadata.get('path')}")
    print(f"  Rows       : {csv_result.rows_loaded}")
    print(f"  File size  : {csv_result.metadata.get('file_size_bytes')} bytes")

    # ── 2b. FileLoader → JSON ─────────────────────────────────────────────────
    print("\n[2b] FileLoader -> JSON ...")
    with FileLoader(
        "DATA/Output/market_status_{timestamp}.json",
        name="market_json_out",
    ) as loader:
        json_result = loader.load(result)

    print(f"  Path       : {json_result.metadata.get('path')}")
    print(f"  Rows       : {json_result.rows_loaded}")
    print(f"  File size  : {json_result.metadata.get('file_size_bytes')} bytes")

    # ── 3. APILoader → httpbin echo ───────────────────────────────────────────
    # httpbin.org/post echoes back whatever JSON body it receives — perfect for
    # verifying the loader sends the correct payload without needing a real API.
    print("\n[3] APILoader -> https://httpbin.org/post (echo endpoint) ...")
    with APILoader(
        "https://httpbin.org/post",
        method="POST",
        batch_size=5,       # exercise batching logic with small slices
        name="market_api_out",
    ) as loader:
        api_result = loader.load(result)

    print(f"  Batches sent  : {api_result.metadata.get('batches_sent')}")
    print(f"  Status codes  : {api_result.metadata.get('status_codes')}")
    print(f"  Rows loaded   : {api_result.rows_loaded}")

    # ── Summary ───────────────────────────────────────────────────────────────
    _banner("PIPELINE COMPLETE")
    print(f"  Rows extracted   : {result.row_count}")
    print(f"  CSV output       : {csv_result.metadata.get('path')}")
    print(f"  JSON output      : {json_result.metadata.get('path')}")
    print(f"  API batches sent : {api_result.metadata.get('batches_sent')}")
    print(f"  Total duration   : "
          f"{result.duration_seconds + csv_result.duration_seconds + json_result.duration_seconds + api_result.duration_seconds:.2f}s")


if __name__ == "__main__":
    setup_logging(level="WARNING")   # set "DEBUG" to see full request logs
    run_api_pipeline()
    print("\nDone. Check DATA/Output/ for the result files.\n")
