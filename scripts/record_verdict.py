#!/usr/bin/env python3
"""Record Lisa's verdict deterministically."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
VALIDATION_FILE = REPO_ROOT / "docs" / "validation.json"


def git(*args: str) -> str:
    result = subprocess.run(["git", "-C", str(REPO_ROOT), *args], capture_output=True, text=True)
    if result.returncode != 0:
        print(result.stderr.strip(), file=sys.stderr)
        sys.exit(result.returncode)
    return result.stdout.strip()


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] not in {"PASS", "REJECT", "PENDING"}:
        print("Usage: python3 scripts/record_verdict.py <PASS|REJECT|PENDING>", file=sys.stderr)
        sys.exit(2)
    verdict = sys.argv[1]
    head = git("rev-parse", "--short", "HEAD")
    if not VALIDATION_FILE.exists():
        validation = {}
    else:
        validation = json.loads(VALIDATION_FILE.read_text())
    validation["verdict"] = verdict
    validation["last_reviewed_commit"] = head
    VALIDATION_FILE.write_text(json.dumps(validation, indent=2) + "\n")
    git("add", "docs/validation.json")
    result = subprocess.run(["git", "-C", str(REPO_ROOT), "diff", "--staged", "--quiet"])
    if result.returncode != 0:
        git("commit", "-m", f"lisa: review verdict {verdict}")


if __name__ == "__main__":
    main()
