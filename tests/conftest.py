from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "fakestore"


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    """Point LANDING_DIR and DUCKDB_PATH at an isolated temp directory."""
    landing = tmp_path / "landing"
    db = tmp_path / "warehouse" / "test.duckdb"
    monkeypatch.setenv("LANDING_DIR", str(landing))
    monkeypatch.setenv("DUCKDB_PATH", str(db))
    return {"landing": landing, "db": db, "root": tmp_path}


@pytest.fixture
def fixtures_dir():
    return FIXTURES_DIR
