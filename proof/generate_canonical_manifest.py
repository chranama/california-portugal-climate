#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
import hashlib
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "proof" / "evidence_manifest.latest.json"


def git_commit() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT).decode().strip()
    except Exception:
        return "UNKNOWN"


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> None:
    data = json.loads(MANIFEST.read_text(encoding="utf-8"))
    data["generated_at"] = datetime.now(timezone.utc).isoformat()
    data["repo_commit"] = git_commit()
    for claim in data.get("claims", []):
        hashes = {}
        for raw in claim.get("artifact_paths", []):
            path = ROOT / raw
            if path.is_file():
                hashes[raw] = sha256_file(path)
        claim["artifact_sha256"] = hashes
    MANIFEST.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    print(f"Updated {MANIFEST}")


if __name__ == "__main__":
    main()
