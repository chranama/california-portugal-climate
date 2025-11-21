# tests/conftest.py
from pathlib import Path
import pytest
import duckdb


@pytest.fixture(scope="session")
def project_root() -> Path:
    # .../california-portugal-climate/tests/conftest.py
    # parents[0] = tests
    # parents[1] = project root
    return Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def db_path(project_root: Path) -> Path:
    return project_root / "data" / "warehouse" / "climate.duckdb"


@pytest.fixture(scope="session")
def con(db_path: Path):
    """Shared DuckDB connection for tests.

    Skips tests gracefully if the warehouse doesn't exist yet.
    """
    if not db_path.exists():
        pytest.skip(f"Warehouse DB not found at {db_path}. Run `uv run climate-run-all` first.")
    con = duckdb.connect(str(db_path), read_only=True)
    yield con
    con.close()