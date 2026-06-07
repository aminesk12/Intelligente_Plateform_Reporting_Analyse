"""SAP DataSphere loader: writes a DataFrame to an Open SQL Schema table.

SAP DataSphere exposes an **Open SQL Schema** per space that external tools can
connect to via the SAP HANA Cloud database layer.  This loader writes a
DataFrame into a local table within that schema using one of two drivers,
selected automatically at runtime:

* **sqlalchemy-hana** (preferred) — enables ``pandas.DataFrame.to_sql()`` and
  consistent behaviour with the rest of the platform.  Install with::

      pip install sqlalchemy-hana

* **hdbcli** (fallback) — the official SAP HANA Python client; used when
  sqlalchemy-hana is not installed.  Performs a bulk INSERT via
  ``cursor.executemany()``.  Install with::

      pip install hdbcli

At least one of these must be installed; the loader raises ``ImportError`` at
validate-time if neither is available.

Prerequisites in DataSphere
---------------------------
1. Enable **Open SQL Schema** for your space (Space Settings → Open SQL Schema).
2. Note the **technical schema name** (e.g. ``C_SPACE_12345ABCDE``).
3. Create a database user with INSERT/CREATE privileges on that schema.
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import pandas as pd

# -- optional driver detection -------------------------------------------------

try:
    import sqlalchemy_hana  # noqa: F401 - registers the hana+hdbcli dialect
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import Engine
    from sqlalchemy.exc import SQLAlchemyError
    _HAS_SQLALCHEMY_HANA = True
except ImportError:
    _HAS_SQLALCHEMY_HANA = False

try:
    import hdbcli.dbapi as _hdbapi
    _HAS_HDBCLI = True
except ImportError:
    _HAS_HDBCLI = False

# ------------------------------------------------------------------------------

from ETLS.core.base_loader import BaseLoader
from ETLS.core.config import load_yaml_config
from ETLS.core.exceptions import DestinationConnectionError, ValidationError
from ETLS.core.retry import with_retry


# Pandas dtype name → SAP HANA SQL column type
_DTYPE_TO_HANA: dict[str, str] = {
    "int8":          "TINYINT",
    "int16":         "SMALLINT",
    "int32":         "INTEGER",
    "int64":         "BIGINT",
    "uint8":         "SMALLINT",
    "uint16":        "INTEGER",
    "uint32":        "BIGINT",
    "uint64":        "DECIMAL(20,0)",
    "float32":       "REAL",
    "float64":       "DOUBLE",
    "bool":          "BOOLEAN",
    "object":        "NVARCHAR(5000)",
    "string":        "NVARCHAR(5000)",
    "category":      "NVARCHAR(5000)",
    "date":          "DATE",
}


def _hana_type(dtype: Any) -> str:
    """Map a pandas dtype to the corresponding SAP HANA SQL type name."""
    name = str(dtype)
    if name.startswith("datetime64"):
        return "TIMESTAMP"
    return _DTYPE_TO_HANA.get(name, "NVARCHAR(5000)")


class DataSphereLoader(BaseLoader):
    """Write a DataFrame to SAP DataSphere via its Open SQL Schema.

    Parameters
    ----------
    host:
        SAP HANA Cloud hostname, e.g. ``"xxxxx.hana.ondemand.com"``.
    port:
        TCP port — always ``443`` for HANA Cloud / DataSphere (default).
    user:
        Database user with ``INSERT`` (and ``CREATE TABLE`` for
        ``if_exists="replace"``) privileges on the space schema.
    password:
        Database user's password.
    table:
        Target table name within the DataSphere space.
    space_schema:
        Technical schema name of the DataSphere space (found in
        Space Settings → Open SQL Schema, e.g. ``"C_SPACE_12345ABCDE"``).
    if_exists:
        What to do when the table already exists:

        * ``"append"`` (default) — insert rows without touching the schema.
        * ``"replace"`` — drop and recreate the table, then insert.
        * ``"fail"`` — raise an error if the table already exists.
    chunksize:
        Number of rows per INSERT batch.  ``None`` sends all rows in a single
        call (fine for small DataFrames; use a value like ``5000`` for large
        loads to avoid memory pressure).
    encrypt:
        Require TLS for the connection (always ``True`` on HANA Cloud).
    validate_certificate:
        Verify the server's SSL certificate (default ``True``; set to
        ``False`` only in development/test environments).

    Examples
    --------
    >>> loader = DataSphereLoader(
    ...     host="xxxxx.hana.ondemand.com",
    ...     port=443,
    ...     user="DS_USER",
    ...     password="secret",
    ...     table="SALES_FACT",
    ...     space_schema="C_SPACE_12345ABCDE",
    ...     if_exists="append",
    ...     chunksize=5000,
    ... )
    >>> result = loader.load(transformation_result)

    >>> # From a YAML config file
    >>> loader = DataSphereLoader.from_config("ETLS/config/datasphere_config.yaml")
    >>> result = loader.load(df)

    >>> # Context manager — connection closed on exit
    >>> with DataSphereLoader(...) as loader:
    ...     result = loader.load(extraction_result)
    """

    loader_type = "datasphere"

    def __init__(
        self,
        host: str,
        port: int = 443,
        *,
        user: str,
        password: str,
        table: str,
        space_schema: str,
        if_exists: str = "append",
        chunksize: int | None = None,
        encrypt: bool = True,
        validate_certificate: bool = True,
        name: str | None = None,
    ) -> None:
        super().__init__(name=name or f"datasphere:{host}/{space_schema}.{table}")
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.table = table
        self.space_schema = space_schema
        self.if_exists = if_exists
        self.chunksize = chunksize
        self.encrypt = encrypt
        self.validate_certificate = validate_certificate
        self._engine: "Engine | None" = None

    @classmethod
    def from_config(
        cls, path: str | Path, *, section: str = "datasphere"
    ) -> "DataSphereLoader":
        """Instantiate from a YAML config file.

        Expected YAML structure::

            datasphere:
              host: "${DATASPHERE_HOST}"
              port: 443
              user: "${DATASPHERE_USER}"
              password: "${DATASPHERE_PASSWORD}"
              table: "SALES_FACT"
              space_schema: "${DATASPHERE_SPACE_SCHEMA}"
              if_exists: "append"
              chunksize: 5000
        """
        cfg = load_yaml_config(path, section=section)
        return cls(
            host=cfg["host"],
            port=cfg.get("port", 443),
            user=cfg["user"],
            password=cfg["password"],
            table=cfg["table"],
            space_schema=cfg["space_schema"],
            if_exists=cfg.get("if_exists", "append"),
            chunksize=cfg.get("chunksize"),
            encrypt=cfg.get("encrypt", True),
            validate_certificate=cfg.get("validate_certificate", True),
            name=cfg.get("name"),
        )

    # -- engine / connection lifecycle -----------------------------------------

    @property
    def engine(self) -> "Engine":
        """Lazy SQLAlchemy engine (only created on first access)."""
        if not _HAS_SQLALCHEMY_HANA:
            raise ImportError(
                "sqlalchemy-hana is required for the engine property. "
                "Install with `pip install sqlalchemy-hana`."
            )
        if self._engine is None:
            url = (
                f"hana+hdbcli://{self.user}:{self.password}"
                f"@{self.host}:{self.port}/"
            )
            self._engine = create_engine(
                url,
                connect_args={
                    "encrypt": self.encrypt,
                    "sslValidateCertificate": self.validate_certificate,
                },
            )
        return self._engine

    def _hdbcli_connect(self) -> Any:
        """Open a raw hdbcli connection."""
        return _hdbapi.connect(
            address=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            encrypt=self.encrypt,
            sslValidateCertificate=self.validate_certificate,
        )

    def close(self) -> None:
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None

    def __enter__(self) -> "DataSphereLoader":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- contract --------------------------------------------------------------

    @with_retry(attempts=3, base_delay=2.0)
    def validate(self) -> None:
        """Check config and verify the DataSphere endpoint is reachable."""
        if not _HAS_SQLALCHEMY_HANA and not _HAS_HDBCLI:
            raise ImportError(
                "DataSphereLoader requires 'sqlalchemy-hana' or 'hdbcli'. "
                "Install with: pip install hdbcli sqlalchemy-hana"
            )
        if not self.table:
            raise ValidationError("DataSphereLoader: 'table' must not be empty.")
        if not self.space_schema:
            raise ValidationError(
                "DataSphereLoader: 'space_schema' (the technical schema name of "
                "the DataSphere space) must not be empty."
            )
        if self.if_exists not in {"fail", "replace", "append"}:
            raise ValidationError(
                f"DataSphereLoader: 'if_exists' must be 'fail', 'replace', or "
                f"'append'; got {self.if_exists!r}."
            )
        self._ping()

    def _ping(self) -> None:
        """Open a connection and run a trivial query to confirm reachability."""
        try:
            if _HAS_SQLALCHEMY_HANA:
                with self.engine.connect() as conn:
                    conn.execute(text("SELECT 1 FROM DUMMY"))
            else:
                conn = self._hdbcli_connect()
                cursor = conn.cursor()
                cursor.execute("SELECT 1 FROM DUMMY")
                conn.close()
        except Exception as exc:  # noqa: BLE001
            raise DestinationConnectionError(
                f"Cannot connect to DataSphere '{self.host}': {exc}"
            ) from exc

    def _load(self, df: pd.DataFrame, **kwargs: Any) -> dict[str, Any]:
        if df.empty:
            self.logger.warning("Load skipped: input DataFrame is empty.")
            return {"skipped": True, "reason": "empty_dataframe"}

        if _HAS_SQLALCHEMY_HANA:
            return self._load_via_sqlalchemy(df)
        return self._load_via_hdbcli(df)

    def _destination_label(self) -> str:
        return f"{self.host}/{self.space_schema}.{self.table}"

    # -- private load paths ----------------------------------------------------

    def _load_via_sqlalchemy(self, df: pd.DataFrame) -> dict[str, Any]:
        """Write using pandas.to_sql() with the sqlalchemy-hana dialect."""
        self.logger.debug(
            "Using sqlalchemy-hana path → %s.%s", self.space_schema, self.table
        )
        with self.engine.begin() as conn:
            df.to_sql(
                self.table,
                conn,
                schema=self.space_schema,
                if_exists=self.if_exists,
                index=False,
                chunksize=self.chunksize,
                method="multi",
            )
        return {
            "driver": "sqlalchemy-hana",
            "table": self.table,
            "schema": self.space_schema,
            "if_exists": self.if_exists,
        }

    def _load_via_hdbcli(self, df: pd.DataFrame) -> dict[str, Any]:
        """Write using hdbcli directly with executemany() bulk INSERT."""
        self.logger.debug(
            "Using hdbcli path → %s.%s", self.space_schema, self.table
        )
        conn = self._hdbcli_connect()
        try:
            cursor = conn.cursor()
            self._handle_if_exists(cursor, df)
            self._bulk_insert(cursor, df)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        return {
            "driver": "hdbcli",
            "table": self.table,
            "schema": self.space_schema,
            "if_exists": self.if_exists,
        }

    # -- hdbcli helpers --------------------------------------------------------

    def _table_exists(self, cursor: Any) -> bool:
        cursor.execute(
            "SELECT COUNT(*) FROM SYS.TABLES "
            "WHERE SCHEMA_NAME = ? AND TABLE_NAME = ?",
            (self.space_schema, self.table),
        )
        return cursor.fetchone()[0] > 0

    def _handle_if_exists(self, cursor: Any, df: pd.DataFrame) -> None:
        exists = self._table_exists(cursor)
        if exists:
            if self.if_exists == "fail":
                raise ValidationError(
                    f"Table '{self.space_schema}.{self.table}' already exists "
                    "and if_exists='fail'."
                )
            if self.if_exists == "replace":
                self.logger.debug(
                    "Dropping table %s.%s", self.space_schema, self.table
                )
                cursor.execute(
                    f'DROP TABLE "{self.space_schema}"."{self.table}" CASCADE'
                )
                self._create_table(cursor, df)
        else:
            self._create_table(cursor, df)

    def _create_table(self, cursor: Any, df: pd.DataFrame) -> None:
        col_defs = ", ".join(
            f'"{col}" {_hana_type(df[col].dtype)}' for col in df.columns
        )
        ddl = f'CREATE TABLE "{self.space_schema}"."{self.table}" ({col_defs})'
        self.logger.debug("Creating table: %s", ddl)
        cursor.execute(ddl)

    def _bulk_insert(self, cursor: Any, df: pd.DataFrame) -> None:
        col_list = ", ".join(f'"{c}"' for c in df.columns)
        placeholders = ", ".join("?" * len(df.columns))
        sql = (
            f'INSERT INTO "{self.space_schema}"."{self.table}" '
            f"({col_list}) VALUES ({placeholders})"
        )

        # Replace NaN/NaT with None so hdbcli sends SQL NULL.
        df_clean = df.where(df.notna(), other=None)
        rows = [tuple(row) for row in df_clean.itertuples(index=False, name=None)]

        batch = self.chunksize or len(rows)
        for i in range(0, len(rows), batch):
            cursor.executemany(sql, rows[i : i + batch])
            self.logger.debug(
                "Inserted rows %d–%d into %s.%s",
                i + 1,
                min(i + batch, len(rows)),
                self.space_schema,
                self.table,
            )
