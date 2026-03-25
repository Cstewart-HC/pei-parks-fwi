#!/usr/bin/env python3
"""Record Lisa's verdict deterministically.

MissHoover V2: Enhanced with structured failure nodes and lineage tracking.

Usage:
    python3 scripts/record_verdict.py PASS
    python3 scripts/record_verdict.py REJECT
    python3 scripts/record_verdict.py REJECT --failing-nodes '[{"file": "src/foo.py", "line": 42, "message": "Missing import"}]'
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
VALIDATION_FILE = REPO_ROOT / "docs" / "validation.json"
LINEAGE_FILE = REPO_ROOT / "docs" / "lineage.jsonl"


def git(*args: str) -> str:
    result = subprocess.run(["git", "-C", str(REPO_ROOT), *args], capture_output=True, text=True)
    if result.returncode != 0:
        print(result.stderr.strip(), file=sys.stderr)
        sys.exit(result.returncode)
    return result.stdout.strip()


def emit_lineage_event(verdict: str, failing_nodes: list[dict] | None = None) -> None:
    """Emit a lineage event for the verdict."""
    if not LINEAGE_FILE.parent.exists():
        LINEAGE_FILE.parent.mkdir(parents=True, exist_ok=True)

    # Read state for phase info
    state_file = REPO_ROOT / "docs" / "ralph-state.json"
    phase = "?"
    if state_file.exists():
        try:
            state = json.loads(state_file.read_text())
            phase = str(state.get("phase", "?"))
        except json.JSONDecodeError:
            pass

    event = {
        "eventType": "LISA_VERDICT",
        "eventTime": datetime.now(timezone.utc).astimezone().isoformat(),
        "run": {
            "runId": f"lisa-{datetime.now().strftime('%Y%m%d%H%M%S')}",
            "facets": {
                "verdict": verdict,
                "failing_nodes": failing_nodes or [],
            },
        },
        "job": {
            "namespace": "misshoover://pea-met-network",
            "name": f"phase-{phase}-lisa-review",
        },
        "producer": "record_verdict.py",
    }

    with open(LINEAGE_FILE, "a") as f:
        f.write(json.dumps(event) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Record Lisa's verdict")
    parser.add_argument("verdict", choices=["PASS", "REJECT", "PENDING"])
    parser.add_argument(
        "--failing-nodes",
        type=str,
        help="JSON array of failing nodes: [{\"file\": \"path\", \"line\": 42, \"message\": \"error\"}]",
    )
    parser.add_argument(
        "--summary",
        type=str,
        help="Optional summary text",
    )
    args = parser.parse_args()

    # Parse failing nodes if provided
    failing_nodes = None
    if args.failing_nodes:
        try:
            failing_nodes = json.loads(args.failing_nodes)
        except json.JSONDecodeError as e:
            print(f"Error parsing --failing-nodes JSON: {e}", file=sys.stderr)
            sys.exit(2)

    head = git("rev-parse", "--short", "HEAD")

    # Load or create validation
    if not VALIDATION_FILE.exists():
        validation = {}
    else:
        validation = json.loads(VALIDATION_FILE.read_text())

    # Update validation
    validation["verdict"] = args.verdict
    validation["last_reviewed_commit"] = head
    validation["reviewed_at"] = datetime.now(timezone.utc).astimezone().isoformat()

    # Add structured failing nodes if provided
    if failing_nodes:
        validation["failing_nodes"] = failing_nodes

    # Add summary if provided
    if args.summary:
        validation["summary"] = args.summary

    # Write validation
    VALIDATION_FILE.write_text(json.dumps(validation, indent=2) + "\n")

    # Emit lineage event
    emit_lineage_event(args.verdict, failing_nodes)

    # Git commit
    git("add", "docs/validation.json")

    # Also add lineage file if it exists
    if LINEAGE_FILE.exists():
        git("add", "docs/lineage.jsonl")

    result = subprocess.run(["git", "-C", str(REPO_ROOT), "diff", "--staged", "--quiet"])
    if result.returncode != 0:
        commit_msg = f"lisa: review verdict {args.verdict}"
        if failing_nodes:
            commit_msg += f" ({len(failing_nodes)} failing nodes)"
        git("commit", "-m", commit_msg)

    print(f"VERDICT_RECORDED={args.verdict}")
    if failing_nodes:
        print(f"FAILING_NODES_COUNT={len(failing_nodes)}")


if __name__ == "__main__":
    main()
