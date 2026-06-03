"""Tests for DatabaseExtractor using a file-backed SQLite database.

SQLite ships with Python, so no external service is needed. We use a file (not
``:memory:``) so the seeding connection and the extractor's own connections see
the same data.
"""

from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy import create_engine

from ETLS.Extraction_Scripts.DataBase_Extractor import DatabaseExtractor
from ETLS.core.exceptions import SourceConnectionError, ValidationError


@pytest.fixture
def db_url(tmp_path, sample_frame):
    """Create a SQLite db file seeded with `sample_frame` and return its URL."""
    db_file = tmp_path / "test.db"
    url = f"sqlite:///{db_file}"
    engine = create_engine(url)
    sample_frame.to_sql("people", engine, index=False, if_exists="replace")
    engine.dispose()
    return url


def test_extract_full_table(db_url):
    with DatabaseExtractor(db_url) as ext:
        result = ext.extract(table="people")
    assert result.source_type == "database"
    assert result.row_count == 3
    assert set(result.data.columns) == {"id", "name", "amount"}


def test_extract_query(db_url):
    with DatabaseExtractor(db_url) as ext:
        result = ext.extract(query="SELECT name FROM people ORDER BY id")
    assert list(result.data["name"]) == ["alice", "bob", "carol"]


def test_query_with_params(db_url):
    with DatabaseExtractor(db_url) as ext:
        result = ext.extract(
            query="SELECT * FROM people WHERE amount > :threshold",
            params={"threshold": 15},
        )
    assert result.row_count == 2


def test_table_with_chunksize(db_url):
    with DatabaseExtractor(db_url) as ext:
        result = ext.extract(table="people", chunksize=2)
    # Chunks are concatenated back into one frame.
    assert result.row_count == 3


def test_query_chunksize_empty_result(db_url):
    with DatabaseExtractor(db_url) as ext:
        result = ext.extract(
            query="SELECT * FROM people WHERE id < 0", chunksize=2
        )
    assert result.is_empty


def test_requires_exactly_one_of_query_or_table(db_url):
    with DatabaseExtractor(db_url) as ext:
        with pytest.raises(ValidationError, match="exactly one"):
            ext.extract()
        with pytest.raises(ValidationError, match="exactly one"):
            ext.extract(query="SELECT 1", table="people")


def test_table_metadata(db_url):
    with DatabaseExtractor(db_url) as ext:
        meta = ext.extract(table="people", schema=None).metadata
    assert meta["table"] == "people"
    assert "connection" in meta


def test_query_metadata_is_normalised(db_url):
    with DatabaseExtractor(db_url) as ext:
        meta = ext.extract(query="SELECT   *\n  FROM people").metadata
    assert meta["query"] == "SELECT * FROM people"


def test_safe_name_hides_credentials():
    url = "postgresql+psycopg2://user:secret@dbhost:5432/sales"
    name = DatabaseExtractor(url).name
    assert "secret" not in name
    assert "postgresql" in name
    assert "dbhost" in name
    assert "sales" in name


def test_validate_connection_failure():
    # Nonexistent SQLite path under a missing directory -> cannot connect.
    ext = DatabaseExtractor("sqlite:////nonexistent_dir/should_fail.db")
    with pytest.raises(SourceConnectionError):
        ext.validate()


def test_engine_is_lazy_and_cached(db_url):
    ext = DatabaseExtractor(db_url)
    assert ext._engine is None
    eng1 = ext.engine
    eng2 = ext.engine
    assert eng1 is eng2
    ext.close()
    assert ext._engine is None


def test_from_config_with_connection_url(tmp_path, db_url):
    cfg = tmp_path / "db.yaml"
    cfg.write_text(
        "database:\n"
        f"  connection_url: {db_url}\n"
        "  name: configured-db\n",
        encoding="utf-8",
    )
    ext = DatabaseExtractor.from_config(cfg)
    assert ext.name == "configured-db"
    assert ext.extract(table="people").row_count == 3


def test_from_config_builds_url_from_parts(tmp_path):
    cfg = tmp_path / "db.yaml"
    cfg.write_text(
        "database:\n"
        "  drivername: postgresql+psycopg2\n"
        "  username: u\n"
        "  password: p\n"
        "  host: h\n"
        "  port: 5432\n"
        "  database: d\n",
        encoding="utf-8",
    )
    ext = DatabaseExtractor.from_config(cfg)
    assert ext.connection_url.startswith("postgresql+psycopg2://u:p@h:5432/d")
