# tests/test_utils_paths.py

from pathlib import Path

from climate_pipeline.utils.get_paths import (
    PROJECT_ROOT,
    get_data_root,
    get_log_root,
    get_duckdb_path,
)


def test_project_root_matches_repo_root(project_root: Path) -> None:
    """
    Sanity check: PROJECT_ROOT in get_paths should be the same as the
    project_root fixture (repo root).
    """
    assert PROJECT_ROOT == project_root


def test_default_paths_under_project_root(project_root: Path, monkeypatch) -> None:
    """
    With no env overrides, paths should resolve under the project root:
      - data -> <PROJECT_ROOT>/data
      - logs -> <PROJECT_ROOT>/logs
      - duckdb -> <PROJECT_ROOT>/data/warehouse/climate.duckdb
    """
    # Ensure we don't inherit any env overrides from CI/local
    monkeypatch.delenv("CLIMATE_DATA_ROOT", raising=False)
    monkeypatch.delenv("CLIMATE_LOG_ROOT", raising=False)
    monkeypatch.delenv("DUCKDB_PATH", raising=False)

    data_root = get_data_root()
    log_root = get_log_root()
    duckdb_path = get_duckdb_path()

    assert data_root == project_root / "data"
    assert log_root == project_root / "logs"
    assert duckdb_path == project_root / "data" / "warehouse" / "climate.duckdb"


def test_env_overrides_relative_paths(project_root: Path, monkeypatch) -> None:
    """
    If env vars are set to *relative* paths, they should be resolved
    relative to PROJECT_ROOT.
    """
    monkeypatch.setenv("CLIMATE_DATA_ROOT", "custom_data")
    monkeypatch.setenv("CLIMATE_LOG_ROOT", "custom_logs")
    monkeypatch.setenv("DUCKDB_PATH", "custom_warehouse/custom.duckdb")

    data_root = get_data_root()
    log_root = get_log_root()
    duckdb_path = get_duckdb_path()

    assert data_root == project_root / "custom_data"
    assert log_root == project_root / "custom_logs"
    assert duckdb_path == project_root / "custom_warehouse" / "custom.duckdb"


def test_env_overrides_absolute_paths(monkeypatch, tmp_path: Path) -> None:
    """
    If env vars are set to *absolute* paths, they should be used as-is.
    """
    abs_data = tmp_path / "data_root"
    abs_logs = tmp_path / "log_root"
    abs_duckdb = tmp_path / "warehouse" / "file.duckdb"

    monkeypatch.setenv("CLIMATE_DATA_ROOT", str(abs_data))
    monkeypatch.setenv("CLIMATE_LOG_ROOT", str(abs_logs))
    monkeypatch.setenv("DUCKDB_PATH", str(abs_duckdb))

    data_root = get_data_root()
    log_root = get_log_root()
    duckdb_path = get_duckdb_path()

    assert data_root == abs_data
    assert log_root == abs_logs
    assert duckdb_path == abs_duckdb