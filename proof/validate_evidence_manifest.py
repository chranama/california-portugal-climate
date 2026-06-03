#!/usr/bin/env python3
from __future__ import annotations

import json
import hashlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "proof" / "evidence_manifest.latest.json"

REQUIRED_TOP = ["proof_id", "run_id", "generated_at", "repo_commit", "status", "claims", "diagnostics"]
REQUIRED_CLAIM = ["claim_text", "verification_command", "artifact_paths", "expected_signal"]


def fail(msg: str) -> None:
    print(f"ERROR: {msg}")
    sys.exit(1)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> None:
    if not MANIFEST.exists():
        fail(f"missing manifest: {MANIFEST}")

    data = json.loads(MANIFEST.read_text(encoding="utf-8"))
    for key in REQUIRED_TOP:
        if key not in data:
            fail(f"missing top-level key: {key}")

    if data["status"] not in {"pass", "fail"}:
        fail("status must be pass|fail")

    claims = data["claims"]
    if not isinstance(claims, list) or not claims:
        fail("claims must be non-empty list")

    for idx, claim in enumerate(claims, start=1):
        for key in REQUIRED_CLAIM:
            if key not in claim:
                fail(f"claim[{idx}] missing key: {key}")

        artifact_paths = claim["artifact_paths"]
        artifact_hashes = claim.get("artifact_sha256", {})
        if artifact_hashes and not isinstance(artifact_hashes, dict):
            fail(f"claim[{idx}] artifact_sha256 must be an object")
        unexpected_hashes = set(artifact_hashes) - set(artifact_paths)
        if unexpected_hashes:
            fail(f"claim[{idx}] hashes reference non-artifact paths: {sorted(unexpected_hashes)}")

        for raw in artifact_paths:
            p = ROOT / raw
            if not p.exists():
                fail(f"claim[{idx}] missing artifact path: {raw}")
            if p.is_file() and p.stat().st_size == 0:
                fail(f"claim[{idx}] artifact is empty: {raw}")
            expected_hash = artifact_hashes.get(raw)
            if expected_hash and p.is_file():
                actual_hash = sha256_file(p)
                if actual_hash != expected_hash:
                    fail(
                        f"claim[{idx}] SHA-256 mismatch for {raw}: "
                        f"expected {expected_hash}, got {actual_hash}"
                    )

    diagnostics = data.get("diagnostics", {})
    if not diagnostics.get("orchestration_entrypoint"):
        fail("diagnostics.orchestration_entrypoint is required")
    if not diagnostics.get("test_commands"):
        fail("diagnostics.test_commands is required")

    print("OK: manifest checks, artifact presence, and artifact hashes validated")


if __name__ == "__main__":
    main()
