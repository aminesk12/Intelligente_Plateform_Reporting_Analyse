# ETL Layer

The extraction layer of the **Intelligent Reporting & Analysis Platform**.
Every data source is read through a uniform contract so that downstream
transform/load steps and the Prefect orchestration treat all sources the same.

## Architecture

```
ETLS/
├── core/                       # shared foundation
│   ├── base_extractor.py       # BaseExtractor (ABC) + ExtractionResult
│   ├── config.py               # YAML loader with ${ENV} expansion
│   ├── logging_config.py       # setup_logging()
│   ├── retry.py                # with_retry() exponential-backoff decorator
│   └── exceptions.py           # ETLError hierarchy
├── Extraction_Scripts/
│   ├── Excel_Extractor.py      # ExcelExtractor
│   ├── DataBase_Extractor.py   # DatabaseExtractor (SQLAlchemy, any DB)
│   ├── API_Extractor.py        # APIExtractor (REST, auth, pagination)
│   └── ERP_SAP_Extractor.py    # SAPODataExtractor (SAP Gateway OData)
└── config/                     # one YAML per source (secrets via ${ENV})
```

### The contract

Every extractor subclasses `BaseExtractor` and implements `validate()` +
`_extract()`. Callers use the public `extract()`, which validates, times the
read, handles errors uniformly, and returns an **`ExtractionResult`**:

```python
result.data              # the pandas DataFrame
result.row_count         # rows / columns
result.source_type       # "excel" | "database" | "api" | "sap_odata"
result.extracted_at      # UTC timestamp
result.duration_seconds
result.metadata          # source-specific provenance
result.summary()         # JSON-serialisable run summary
```

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env        # then fill in secrets
```

## Usage

Each extractor can be built directly or from its YAML config.

```python
from ETLS.core import setup_logging
from ETLS.Extraction_Scripts import (
    ExcelExtractor, DatabaseExtractor, APIExtractor, SAPODataExtractor,
)

setup_logging("INFO")

# Excel
res = ExcelExtractor("data/raw/sales.xlsx", sheet_name="Orders").extract()

# Database — by query (with bound params) or whole table
with DatabaseExtractor("postgresql+psycopg2://user:pw@host/db") as db:
    res = db.extract(query="SELECT * FROM sales WHERE year = :y", params={"y": 2025})
    res = db.extract(table="customers", schema="public", chunksize=50_000)

# REST API — auth + pagination handled internally
with APIExtractor.from_config("ETLS/config/api_config.yaml") as api:
    res = api.extract()

# SAP via OData
with SAPODataExtractor.from_config("ETLS/config/sap_config.yaml") as sap:
    res = sap.extract()

print(res.summary())
```

## Configuration

Config files live in `ETLS/config/`. Secrets are referenced as `${VAR}` or
`${VAR:default}` and resolved from the environment (or a `.env` file). See each
`*_config.yaml` for documented examples.

## SAP options

`SAPODataExtractor` covers the portable **OData** path (no extra SDK needed).
Two alternatives are available if your landscape needs them:

| Path        | When                              | Dependency                  |
|-------------|-----------------------------------|-----------------------------|
| OData       | S/4HANA / Gateway REST services   | `requests` (built in)       |
| RFC / BAPI  | classic ABAP function modules     | `pyrfc` + SAP NW RFC SDK    |
| SAP HANA    | direct SQL on HANA                | use `DatabaseExtractor` with a `hana+hdbcli://` URL |

## Next steps

This is the **Extract** layer. Planned, following the same contract:

- `Transformation_Scripts/` — cleaning, typing, business rules
- `Loading_Scripts/` — load to the warehouse (destination TBD)
- `flows/` — Prefect flows that orchestrate extract → transform → load
```
