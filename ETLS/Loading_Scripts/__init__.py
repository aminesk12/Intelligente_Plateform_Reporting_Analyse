"""Data loaders for the ETL platform.

Every loader subclasses :class:`ETLS.core.base_loader.BaseLoader` and returns
a :class:`ETLS.core.base_loader.LoadResult`.

Quick reference
---------------
* :class:`DatabaseLoader`    — write to any SQLAlchemy-supported database
  (PostgreSQL, MySQL, SQL Server, SQLite, Oracle, ...).
* :class:`FileLoader`        — write CSV, Excel, JSON, or Parquet to local disk.
* :class:`APILoader`         — POST/PUT rows to a REST API endpoint as JSON.
* :class:`DataSphereLoader`  — write to an SAP DataSphere Open SQL Schema table
  via sqlalchemy-hana (preferred) or hdbcli (fallback).
"""

from ETLS.Loading_Scripts.api_loader import APILoader
from ETLS.Loading_Scripts.database_loader import DatabaseLoader
from ETLS.Loading_Scripts.datasphere_loader import DataSphereLoader
from ETLS.Loading_Scripts.file_loader import FileLoader

__all__ = [
    "DatabaseLoader",
    "FileLoader",
    "APILoader",
    "DataSphereLoader",
]
