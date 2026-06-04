from __future__ import annotations

import argparse
import csv
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from climate_pipeline.observability.run_logger import (
    PipelineRunRecord,
    compute_run_stats,
    ensure_observability_schema,
    log_pipeline_run,
)
from climate_pipeline.utils.get_paths import (
    PROJECT_ROOT,
    get_data_root,
    get_duckdb_path,
    get_log_root,
)

DBT_DIR = PROJECT_ROOT / "dbt"
PROOF_DIR = PROJECT_ROOT / "proof"
WORKFLOW_STATE_PATH = PROOF_DIR / "workflow_state.latest.json"
TEST_SUMMARY_PATH = PROOF_DIR / "test_summary.latest.json"
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
SUPPRESSED_LINES = (
    "WARN Broken interpreter cache entry",
)


@dataclass
class CommandResult:
    name: str
    command: list[str]
    cwd: str
    return_code: int
    duration_seconds: float
    started_at: str
    finished_at: str
    stdout_tail: list[str]
    stderr_tail: list[str]

    @property
    def status(self) -> str:
        return "pass" if self.return_code == 0 else "fail"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _git_text(args: list[str]) -> str:
    try:
        return (
            subprocess.check_output(["git", *args], cwd=PROJECT_ROOT)
            .decode("utf-8")
            .strip()
        )
    except Exception:
        return "UNKNOWN"


def _tail(text: str, limit: int = 30) -> list[str]:
    lines = [_sanitize_line(line) for line in text.splitlines()]
    lines = [
        line
        for line in lines
        if line and not any(line.startswith(prefix) for prefix in SUPPRESSED_LINES)
    ]
    return lines[-limit:]


def _relative_path(path: Path | str) -> str:
    resolved = Path(path).resolve()
    try:
        return str(resolved.relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)


def _sanitize_line(line: str) -> str:
    clean = ANSI_RE.sub("", line)
    return clean.replace(str(PROJECT_ROOT), ".")


def _build_env(extra: dict[str, str] | None = None) -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("CLIMATE_ENV", "local")
    env["CLIMATE_DATA_ROOT"] = str(get_data_root())
    env["CLIMATE_LOG_ROOT"] = str(get_log_root())
    env["DUCKDB_PATH"] = str(get_duckdb_path())
    env["DBT_PROFILES_DIR"] = str(DBT_DIR)
    env.setdefault(
        "OPEN_METEO_GEOCODING_BASE_URL",
        "https://geocoding-api.open-meteo.com/v1/search",
    )
    env.setdefault(
        "OPEN_METEO_HISTORICAL_BASE_URL",
        "https://archive-api.open-meteo.com/v1/archive",
    )
    if extra:
        env.update(extra)
    return env


def _run_command(
    *,
    name: str,
    command: list[str],
    cwd: Path = PROJECT_ROOT,
    extra_env: dict[str, str] | None = None,
) -> CommandResult:
    started = datetime.now(timezone.utc)
    start = time.monotonic()
    print(f"\n== {name} ==")
    print(f"$ {' '.join(command)}")

    result = subprocess.run(
        command,
        cwd=str(cwd),
        env=_build_env(extra_env),
        text=True,
        capture_output=True,
    )

    finished = datetime.now(timezone.utc)
    duration = time.monotonic() - start

    if result.stdout:
        print(result.stdout.rstrip())
    if result.stderr:
        print(result.stderr.rstrip(), file=sys.stderr)

    return CommandResult(
        name=name,
        command=command,
        cwd=_relative_path(cwd),
        return_code=result.returncode,
        duration_seconds=round(duration, 3),
        started_at=started.isoformat(),
        finished_at=finished.isoformat(),
        stdout_tail=_tail(result.stdout),
        stderr_tail=_tail(result.stderr),
    )


def _run_or_fail(
    results: list[CommandResult],
    *,
    name: str,
    command: list[str],
    cwd: Path = PROJECT_ROOT,
    extra_env: dict[str, str] | None = None,
) -> CommandResult:
    result = _run_command(name=name, command=command, cwd=cwd, extra_env=extra_env)
    results.append(result)
    if result.return_code != 0:
        raise RuntimeError(f"{name} failed with exit code {result.return_code}")
    return result


def _parse_pytest_summary(result: CommandResult) -> dict[str, Any]:
    combined = "\n".join([*result.stdout_tail, *result.stderr_tail])
    summary_line = ""
    for line in reversed(combined.splitlines()):
        if " passed" in line or " failed" in line or " error" in line:
            summary_line = line.strip("= ").strip()
            break

    counts: dict[str, int] = {}
    for key in ("passed", "failed", "errors", "skipped", "deselected", "xfailed", "xpassed"):
        match = re.search(rf"(\d+)\s+{key}", summary_line)
        if match:
            counts[key] = int(match.group(1))

    return {
        "name": result.name,
        "command": result.command,
        "status": result.status,
        "return_code": result.return_code,
        "summary_line": summary_line,
        "counts": counts,
    }


def _write_test_summary(results: list[CommandResult]) -> None:
    test_results = [
        _parse_pytest_summary(result)
        for result in results
        if result.name in {"fast_tests", "integration_tests"}
    ]
    status = "pass" if test_results and all(item["status"] == "pass" for item in test_results) else "fail"
    payload = {
        "source": "climate-verify-local",
        "generated_at": _now_iso(),
        "status": status,
        "test_commands": test_results,
        "required_test_files": [
            "tests/test_anomaly_layer.py",
            "tests/test_ml_features.py",
            "tests/test_observability_models.py",
        ],
    }
    TEST_SUMMARY_PATH.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _count_rows(conn: duckdb.DuckDBPyConnection, table_name: str) -> int | None:
    try:
        return int(conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0])
    except Exception:
        return None


def _warehouse_state(run_mode: str) -> dict[str, Any]:
    db_path = get_duckdb_path()
    if not db_path.exists():
        return {"warehouse_path": _relative_path(db_path), "exists": False}

    tables = [
        "landing_daily_weather",
        "clean_daily_weather_features",
        "clean_monthly_climate",
        "anomaly_city_month",
        "anomaly_city_events",
        "anomaly_city_lags",
        "anomaly_city_correlations",
        "ml_features",
        "pipeline_run_log",
        "pipeline_run_daily_summary",
        "pipeline_ml_metrics",
        "pipeline_ml_daily_summary",
        "ml_predictions",
    ]
    state: dict[str, Any] = {
        "warehouse_path": _relative_path(db_path),
        "exists": True,
        "row_counts": {},
    }
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        state["row_counts"] = {table: _count_rows(conn, table) for table in tables}
        state["latest_pipeline_run"] = (
            conn.execute(
                """
                SELECT *
                FROM pipeline_run_daily_summary
                WHERE run_mode = ?
                ORDER BY run_date DESC, run_mode
                LIMIT 5
                """,
                [run_mode],
            )
            .df()
            .to_dict(orient="records")
        )
        state["latest_ml_summary"] = (
            conn.execute(
                """
                SELECT *
                FROM pipeline_ml_daily_summary
                WHERE run_mode = ?
                ORDER BY run_date DESC, run_mode
                LIMIT 5
                """,
                [run_mode],
            )
            .df()
            .to_dict(orient="records")
        )
    except Exception as exc:
        state["error"] = str(exc)
    finally:
        conn.close()
    return state


def _model_metrics_state() -> dict[str, Any]:
    path = PROJECT_ROOT / "models" / "baseline_rf_metrics.json"
    if not path.exists():
        return {"path": _relative_path(path), "exists": False}
    data = json.loads(path.read_text(encoding="utf-8"))
    return {
        "path": _relative_path(path),
        "exists": True,
        "accuracy": data.get("accuracy"),
        "roc_auc": data.get("roc_auc"),
        "f1_1": data.get("f1_1"),
        "recall_1": data.get("recall_1"),
        "n_train": data.get("n_train"),
        "n_test": data.get("n_test"),
        "class_distribution_test": data.get("class_distribution_test"),
        "target_column": data.get("target_column"),
    }


def _prediction_state() -> dict[str, Any]:
    path = PROJECT_ROOT / "data" / "mart" / "predictions" / "baseline_rf_predictions.csv"
    if not path.exists():
        return {"path": _relative_path(path), "exists": False}
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        row_count = max(sum(1 for _ in reader) - 1, 0)
    return {"path": _relative_path(path), "exists": True, "row_count": row_count}


def _write_workflow_state(
    *,
    status: str,
    run_mode: str,
    started_at: datetime,
    finished_at: datetime,
    command_results: list[CommandResult],
    notes: list[str],
) -> None:
    payload = {
        "source": "climate-verify-local",
        "generated_at": _now_iso(),
        "status": status,
        "run_mode": run_mode,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "repo_commit": _git_text(["rev-parse", "--short", "HEAD"]),
        "git_dirty": bool(_git_text(["status", "--porcelain"])),
        "command_results": [asdict(result) | {"status": result.status} for result in command_results],
        "warehouse": _warehouse_state(run_mode),
        "model_metrics": _model_metrics_state(),
        "predictions": _prediction_state(),
        "notes": notes,
    }
    WORKFLOW_STATE_PATH.write_text(json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8")


def _log_manual_pipeline_run(run_mode: str, status: str, started_at: datetime, finished_at: datetime) -> str:
    try:
        ensure_observability_schema()
        stats = compute_run_stats(get_duckdb_path())
        log_pipeline_run(
            PipelineRunRecord(
                flow_name="local-verification-workflow",
                run_mode=run_mode,
                status=status,
                started_at=started_at,
                finished_at=finished_at,
                rows_bronze=stats.rows_bronze,
                rows_gold_ml=stats.rows_gold_ml,
                rows_bronze_delta=stats.rows_bronze_delta,
                rows_gold_ml_delta=stats.rows_gold_ml_delta,
                bronze_max_date=stats.bronze_max_date,
                gold_ml_max_date=stats.gold_ml_max_date,
                freshness_status=stats.freshness_status,
            )
        )
        return "manual pipeline run logged"
    except Exception as exc:
        return f"manual pipeline run logging failed: {exc}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the local verification workflow and refresh proof artifacts."
    )
    parser.add_argument(
        "--ingestion-mode",
        choices=["recent", "backfill"],
        default="recent",
        help="Weather ingestion mode to run before building the warehouse.",
    )
    parser.add_argument(
        "--run-mode",
        default="verify",
        help="Run mode label written to observability tables.",
    )
    parser.add_argument(
        "--skip-ingestion",
        action="store_true",
        help="Use existing raw data and skip Open-Meteo ingestion.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    started_at = datetime.now(timezone.utc)
    results: list[CommandResult] = []
    notes: list[str] = []
    status = "pass"

    try:
        if not args.skip_ingestion:
            _run_or_fail(
                results,
                name=f"ingestion_{args.ingestion_mode}",
                command=["uv", "run", "fetch-daily-weather", "--mode", args.ingestion_mode],
            )

        _run_or_fail(
            results,
            name="dbt_build",
            command=["uv", "run", "climate-dbt-build"],
        )
        _run_or_fail(
            results,
            name="train_baseline",
            command=["uv", "run", "climate-train-baseline"],
            extra_env={
                "PIPELINE_FLOW_NAME": "local-verification-workflow",
                "PIPELINE_RUN_MODE": args.run_mode,
            },
        )
        _run_or_fail(
            results,
            name="predict_baseline",
            command=["uv", "run", "climate-predict-baseline"],
        )

        notes.append(_log_manual_pipeline_run(args.run_mode, "success", started_at, datetime.now(timezone.utc)))

        _run_or_fail(
            results,
            name="rebuild_observability",
            command=[
                "uv",
                "run",
                "dbt",
                "build",
                "--project-dir",
                ".",
                "--profiles-dir",
                ".",
                "--select",
                "observability",
            ],
            cwd=DBT_DIR,
        )
        _run_or_fail(
            results,
            name="fast_tests",
            command=["uv", "run", "python", "-m", "pytest", "-m", "not integration"],
        )
        _run_or_fail(
            results,
            name="integration_tests",
            command=["uv", "run", "python", "-m", "pytest", "-m", "integration"],
        )
    except Exception as exc:
        status = "fail"
        notes.append(str(exc))
    finally:
        finished_at = datetime.now(timezone.utc)
        if status == "fail":
            notes.append(_log_manual_pipeline_run(args.run_mode, "fail", started_at, finished_at))
        _write_test_summary(results)
        _write_workflow_state(
            status=status,
            run_mode=args.run_mode,
            started_at=started_at,
            finished_at=finished_at,
            command_results=results,
            notes=notes,
        )

    if status == "fail":
        print(f"Local verification failed. Wrote {WORKFLOW_STATE_PATH}")
        raise SystemExit(1)

    _run_or_fail(
        results,
        name="generate_evidence_manifest_initial",
        command=["python", "proof/generate_canonical_manifest.py"],
    )
    _run_or_fail(
        results,
        name="validate_evidence_manifest_initial",
        command=["python", "proof/validate_evidence_manifest.py"],
    )
    notes.append("initial evidence manifest validation passed")
    _write_workflow_state(
        status=status,
        run_mode=args.run_mode,
        started_at=started_at,
        finished_at=finished_at,
        command_results=results,
        notes=notes,
    )
    _run_or_fail(
        results,
        name="generate_evidence_manifest_final",
        command=["python", "proof/generate_canonical_manifest.py"],
    )
    _run_or_fail(
        results,
        name="validate_evidence_manifest_final",
        command=["python", "proof/validate_evidence_manifest.py"],
    )
    print(f"Local verification completed. Wrote {WORKFLOW_STATE_PATH}")


if __name__ == "__main__":
    main()
