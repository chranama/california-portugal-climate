from __future__ import annotations

import os
from pathlib import Path
from typing import Iterator

import duckdb
import pytest


# ======================================================
# Core paths
# ======================================================

@pytest.fixture(scope="session")
def project_root() -> Path:
    """
    Resolve the project root as the repo root (one level above tests/).
    """
    return Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def duckdb_path(project_root: Path) -> Path:
    """
    Path to the main DuckDB warehouse used by tests.
    """
    return project_root / "data" / "warehouse" / "climate.duckdb"


# ======================================================
# Environment wiring
# ======================================================

@pytest.fixture(autouse=True)
def configure_test_env(monkeypatch: pytest.MonkeyPatch, project_root: Path, duckdb_path: Path) -> None:
    """
    Automatically configure environment variables for all tests so that
    code relying on CLIMATE_DATA_ROOT / CLIMATE_LOG_ROOT / DUCKDB_PATH
    behaves consistently.
    """
    data_root = project_root / "data"
    log_root = project_root / "logs"
    dbt_dir = project_root / "dbt"

    monkeypatch.setenv("CLIMATE_ENV", "test")
    monkeypatch.setenv("CLIMATE_DATA_ROOT", str(data_root))
    monkeypatch.setenv("CLIMATE_LOG_ROOT", str(log_root))
    monkeypatch.setenv("DUCKDB_PATH", str(duckdb_path))
    monkeypatch.setenv("DBT_PROFILES_DIR", str(dbt_dir))


# ======================================================
# DuckDB connection fixture (used by many tests)
# ======================================================

@pytest.fixture
def con(duckdb_path: Path) -> Iterator[duckdb.DuckDBPyConnection]:
    """
    Provide a read-only DuckDB connection for tests that need to query
    the warehouse directly.

    Any test that requires this fixture will be skipped if the DuckDB
    file does not exist yet (e.g., before the pipeline / dbt has run).
    """
    if not duckdb_path.exists():
        pytest.skip(f"DuckDB file not found at {duckdb_path} – run the pipeline/dbt first.")

    conn = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        yield conn
    finally:
        conn.close()


# ======================================================
# Alias for tests expecting `warehouse_path`
# ======================================================

@pytest.fixture
def warehouse_path(duckdb_path: Path) -> Path:
    """
    Some tests expect a fixture called `warehouse_path`. Reuse duckdb_path.
    """
    return duckdb_path