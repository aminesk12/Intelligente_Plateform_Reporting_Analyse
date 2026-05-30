"""Relational database extractor built on SQLAlchemy.

Works with any database SQLAlchemy can reach (PostgreSQL, MySQL/MariaDB,
SQL Server, SQLite, Oracle, ...) given the right driver installed. Supports
extraction by full table or by arbitrary SQL query, parameter binding,
optional chunked reads for large result sets, and connection retries.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterator

import pandas as pd

try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import Engine
    from sqlalchemy.exc import SQLAlchemyError
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "DatabaseExtractor requires SQLAlchemy. Install with `pip install sqlalchemy`."
    ) from exc

from ETLS.core.base_extractor import BaseExtractor
from ETLS.core.config import load_yaml_config
from ETLS.core.exceptions import SourceConnectionError, ValidationError
from ETLS.core.retry import with_retry


class DatabaseExtractor(BaseExtractor):
    """Extract data from a relational database via SQLAlchemy.

    Provide a full SQLAlchemy ``connection_url`` (recommended, keeps secrets in
    one place), or supply one via config. Then extract either a whole table
    (``table=...``) or a custom query (``query=...``).

    Examples
    --------
    >>> ext = DatabaseExtractor("postgresql+psycopg2://user:pw@host:5432/db")
    >>> result = ext.extract(query="SELECT * FROM sales WHERE year = :y", params={"y": 2025})
    >>> result = ext.extract(table="customers", schema="public")
    """

    source_type = "database"

    def __init__(
        self,
        connection_url: str,
        *,
        connect_args: dict[str, Any] | None = None,
        pool_pre_ping: bool = True,
        name: str | None = None,
    ) -> None:
        super().__init__(name=name or self._safe_name(connection_url))
        self.connection_url = connection_url
        self.connect_args = connect_args or {}
        self.pool_pre_ping = pool_pre_ping
        self._engine: Engine | None = None

    @staticmethod
    def _safe_name(url: str) -> str:
        """Build a log-safe name from a connection URL (no credentials)."""
        try:
            from sqlalchemy.engine import make_url

            u = make_url(url)
            return f"{u.drivername.split('+')[0]}:{u.host or 'local'}/{u.database or ''}"
        except Exception:  # noqa: BLE001
            return "database"

    @classmethod
    def from_config(
        cls, path: str | Path, *, section: str = "database"
    ) -> "DatabaseExtractor":
        cfg = load_yaml_config(path, section=section)
        url = cfg.get("connection_url") or cls._build_url(cfg)
        return cls(
            connection_url=url,
            connect_args=cfg.get("connect_args"),
            pool_pre_ping=cfg.get("pool_pre_ping", True),
            name=cfg.get("name"),
        )

    @staticmethod
    def _build_url(cfg: dict[str, Any]) -> str:
        """Assemble a connection URL from individual config fields."""
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

    def __enter__(self) -> "DatabaseExtractor":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- contract --------------------------------------------------------------

    @with_retry(attempts=3, base_delay=1.0, exceptions=(SQLAlchemyError,))
    def validate(self) -> None:
        """Open a connection and run a trivial query to confirm reachability."""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        except SQLAlchemyError as exc:
            raise SourceConnectionError(
                f"Cannot connect to database '{self.name}': {exc}"
            ) from exc

    def _extract(
        self,
        *,
        query: str | None = None,
        table: str | None = None,
        schema: str | None = None,
        params: dict[str, Any] | None = None,
        chunksize: int | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        if (query is None) == (table is None):
            raise ValidationError("Provide exactly one of 'query' or 'table'.")

        with self.engine.connect() as conn:
            if table is not None:
                self.logger.debug("Reading table %s (schema=%s)", table, schema)
                if chunksize:
                    return self._concat_chunks(
                        pd.read_sql_table(
                            table, conn, schema=schema, chunksize=chunksize, **kwargs
                        )
                    )
                return pd.read_sql_table(table, conn, schema=schema, **kwargs)

            self.logger.debug("Running query (params=%s)", params)
            sql = text(query)  # type: ignore[arg-type]
            if chunksize:
                return self._concat_chunks(
                    pd.read_sql_query(
                        sql, conn, params=params, chunksize=chunksize, **kwargs
                    )
                )
            return pd.read_sql_query(sql, conn, params=params, **kwargs)

    @staticmethod
    def _concat_chunks(chunks: Iterator[pd.DataFrame]) -> pd.DataFrame:
        frames = list(chunks)
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def _build_metadata(self, df: pd.DataFrame, **kwargs: Any) -> dict[str, Any]:
        meta: dict[str, Any] = {"connection": self.name}
        if kwargs.get("table"):
            meta["table"] = kwargs["table"]
            meta["schema"] = kwargs.get("schema")
        if kwargs.get("query"):
            meta["query"] = " ".join(str(kwargs["query"]).split())[:500]
        return meta
