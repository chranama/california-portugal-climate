from __future__ import annotations

import os
from pathlib import Path


# This file lives at: <project_root>/src/climate_pipeline/get_paths.py
# So project root is two levels up from here.
PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _resolve_under_project(path_str: str) -> Path:
    """
    Resolve a path that may be absolute or relative.

    - If absolute, return as-is.
    - If relative, treat it as relative to PROJECT_ROOT.
    """
    p = Path(path_str)
    if p.is_absolute():
        return p
    return PROJECT_ROOT / p


def get_data_root() -> Path:
    """
    Root folder where raw + processed data lives.

    Defaults:
      - local:  "data"
      - docker: whatever CLIMATE_DATA_ROOT is set to (e.g. "/app/data")
    """
    env_value = os.getenv("CLIMATE_DATA_ROOT", "data")
    return _resolve_under_project(env_value)


def get_log_root() -> Path:
    """
    Root folder for logs.

    Defaults:
      - local:  "logs"
      - docker: whatever CLIMATE_LOG_ROOT is set to (e.g. "/app/logs")
    """
    env_value = os.getenv("CLIMATE_LOG_ROOT", "logs")
    return _resolve_under_project(env_value)


def get_duckdb_path() -> Path:
    """
    Path to the main DuckDB warehouse.

    Resolution order:
      1. If DUCKDB_PATH is set, use it (relative to project root if not absolute).
      2. Otherwise: <DATA_ROOT>/warehouse/climate.duckdb
    """
    env_value = os.getenv("DUCKDB_PATH")
    if env_value:
        return _resolve_under_project(env_value)

    data_root = get_data_root()
    return data_root / "warehouse" / "climate.duckdb"