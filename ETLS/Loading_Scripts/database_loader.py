"""Relational database loader built on SQLAlchemy.

Works with any database SQLAlchemy can reach (PostgreSQL, MySQL/MariaDB,
SQL Server, SQLite, Oracle, ...) given the right driver installed.  Writes
a DataFrame to a single target table via :func:`pandas.DataFrame.to_sql`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import Engine
    from sqlalchemy.exc import SQLAlchemyError
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "DatabaseLoader requires SQLAlchemy.  Install with `pip install sqlalchemy`."
    ) from exc

from ETLS.core.base_loader import BaseLoader
from ETLS.core.config import load_yaml_config
from ETLS.core.exceptions import DestinationConnectionError, ValidationError
from ETLS.core.retry import with_retry


class DatabaseLoader(BaseLoader):
    """Write a DataFrame to a relational database table via SQLAlchemy.

    Parameters
    ----------
    connection_url:
        Full SQLAlchemy connection URL, e.g.
        ``"postgresql+psycopg2://user:pw@host:5432/mydb"``.
    table:
        Target table name.
    schema:
        Database schema that owns the table (optional; defaults to the
        engine's default schema).
    if_exists:
        What to do when the table already exists:

        * ``"append"`` (default) — insert rows into the existing table.
        * ``"replace"`` — drop and recreate the table before inserting.
        * ``"fail"`` — raise an error if the table already exists.
    chunksize:
        Number of rows written per INSERT batch.  ``None`` writes all rows in
        a single statement.
    method:
        Passed to ``pandas.DataFrame.to_sql``.  ``"multi"`` (default) batches
        column values into a single INSERT, which is faster on most databases.
        Pass ``None`` for single-row inserts or a callable for custom logic.
    index:
        Whether to write the DataFrame index as a column (default ``False``).
    connect_args:
        Extra keyword arguments forwarded to the underlying DBAPI ``connect()``.
    pool_pre_ping:
        Issue a ``SELECT 1`` on each connection checkout to detect stale
        connections (recommended for long-running pipelines).

    Examples
    --------
    >>> loader = DatabaseLoader(
    ...     "postgresql+psycopg2://user:pw@host:5432/mydb",
    ...     table="sales_staging",
    ...     if_exists="replace",
    ... )
    >>> result = loader.load(transformation_result)

    >>> # Stream from a config file
    >>> loader = DatabaseLoader.from_config("ETLS/config/database_load.yaml")
    >>> result = loader.load(df)

    >>> # Use as a context manager to guarantee engine disposal
    >>> with DatabaseLoader("sqlite:///local.db", table="events") as loader:
    ...     result = loader.load(extraction_result)
    """

    loader_type = "database"

    def __init__(
        self,
        connection_url: str,
        table: str,
        *,
        schema: str | None = None,
        if_exists: str = "append",
        chunksize: int | None = None,
        method: str | None = "multi",
        index: bool = False,
        connect_args: dict[str, Any] | None = None,
        pool_pre_ping: bool = True,
        name: str | None = None,
    ) -> None:
        super().__init__(name=name or self._safe_name(connection_url, table))
        self.connection_url = connection_url
        self.table = table
        self.schema = schema
        self.if_exists = if_exists
        self.chunksize = chunksize
        self.method = method
        self.index = index
        self.connect_args = connect_args or {}
        self.pool_pre_ping = pool_pre_ping
        self._engine: Engine | None = None

    @staticmethod
    def _safe_name(url: str, table: str) -> str:
        """Build a log-safe name from a connection URL (no credentials)."""
        try:
            from sqlalchemy.engine import make_url
            u = make_url(url)
            host = u.host or "local"
            db = u.database or ""
            return f"{u.drivername.split('+')[0]}:{host}/{db}.{table}"
        except Exception:  # noqa: BLE001
            return f"database.{table}"

    @classmethod
    def from_config(
        cls, path: str | Path, *, section: str = "database"
    ) -> "DatabaseLoader":
        """Instantiate from a YAML config file.

        The config section must contain ``table`` and either ``connection_url``
        or the individual fields ``drivername``, ``host``, ``database``, etc.
        """
        cfg = load_yaml_config(path, section=section)
        url = cfg.get("connection_url") or cls._build_url(cfg)
        return cls(
            connection_url=url,
            table=cfg["table"],
            schema=cfg.get("schema"),
            if_exists=cfg.get("if_exists", "append"),
            chunksize=cfg.get("chunksize"),
            method=cfg.get("method", "multi"),
            index=cfg.get("index", False),
            connect_args=cfg.get("connect_args"),
            pool_pre_ping=cfg.get("pool_pre_ping", True),
            name=cfg.get("name"),
        )

    @staticmethod
    def _build_url(cfg: dict[str, Any]) -> str:
        """Assemble a SQLAlchemy URL from individual config fields."""
        try:
            from sqlalchemy.engine import URL
            return URL.create(
                drivername=cfg["drivername"],
                username=cfg.get("username"),
                password=cfg.get("password"),
                host=cfg.get("host"),
                port=cfg.get("port"),
                database=cfg.get("database"),
                query=cfg.get("query_params", {}),
            ).render_as_string(hide_password=False)
        except KeyError as exc:
            raise ValidationError(
                "Database config needs either 'connection_url' or "
                f"'drivername' (+ host/database). Missing: {exc}"
            ) from exc

    # -- engine lifecycle ------------------------------------------------------

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            self._engine = create_engine(
                self.connection_url,
                pool_pre_ping=self.pool_pre_ping,
                connect_args=self.connect_args,
            )
        return self._engine

    def close(self) -> None:
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None

    def __enter__(self) -> "DatabaseLoader":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- contract --------------------------------------------------------------

    @with_retry(attempts=3, base_delay=1.0, exceptions=(SQLAlchemyError,))
    def validate(self) -> None:
        """Open a connection and run a trivial query to confirm reachability."""
        if not self.table:
            raise ValidationError("DatabaseLoader: 'table' must not be empty.")
        if self.if_exists not in {"fail", "replace", "append"}:
            raise ValidationError(
                f"DatabaseLoader: 'if_exists' must be 'fail', 'replace', or 'append'; "
                f"got {self.if_exists!r}."
            )
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        except SQLAlchemyError as exc:
            raise DestinationConnectionError(
                f"Cannot connect to database '{self.name}': {exc}"
            ) from exc

    def _load(self, df: pd.DataFrame, **kwargs: Any) -> dict[str, Any]:
        if df.empty:
            self.logger.warning("Load skipped: input DataFrame is empty.")
            return {"skipped": True, "reason": "empty_dataframe"}

        with self.engine.begin() as conn:
            df.to_sql(
                self.table,
                conn,
                schema=self.schema,
                if_exists=self.if_exists,
                index=self.index,
                chunksize=self.chunksize,
                method=self.method,
            )

        return {
            "table": self.table,
            "schema": self.schema,
            "if_exists": self.if_exists,
        }

    def _destination_label(self) -> str:
        schema_prefix = f"{self.schema}." if self.schema else ""
        return f"{self.name}/{schema_prefix}{self.table}"
