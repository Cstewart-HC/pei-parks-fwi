#!/usr/bin/env python3
"""
Orchestrator wrapper with deterministic teardown.

1. Triggers the MissHoover agent via Moltis CLI
2. After agent completes, advances last_reviewed_commit to current HEAD
3. Commits if validation.json changed

Usage:
    python3 scripts/run_orchestrator.py
"""

import json
import subprocess
import sys
from pathlib import Path

REPO = Path("/mnt/fast_data/workspaces/pea-met-network")
VALIDATION = REPO / "docs" / "validation.json"


def advance_pointer():
    """Capture current HEAD and update last_reviewed_commit in validation.json."""
    # Capture HEAD before any changes
    result = subprocess.run(
        ["git", "-C", str(REPO), "rev-parse", "--short", "HEAD"],
        capture_output=True, text=True, check=True,
    )
    head = result.stdout.strip()

    # Read current validation
    if not VALIDATION.exists():
        print("WARNING: validation.json does not exist, skipping pointer advance")
        return False

    with open(VALIDATION) as f:
        validation = json.load(f)

    old_reviewed = validation.get("last_reviewed_commit", "")
    validation["last_reviewed_commit"] = head

    # Write updated validation
    with open(VALIDATION, "w") as f:
        json.dump(validation, f, indent=2)
        f.write("\n")

    if old_reviewed != head:
        print(f"ADVANCE: {old_reviewed} -> {head}")
    else:
        print(f"POINTER: already at {head} (no change)")

    return old_reviewed != head


def commit_if_dirty():
    """Stage and commit validation.json if it changed."""
    result = subprocess.run(
        ["git", "-C", str(REPO), "diff", "--quiet", str(VALIDATION)],
        capture_output=True, text=True,
    )

    if result.returncode != 0:
        # File changed — stage and commit
        subprocess.run(
            ["git", "-C", str(REPO), "add", str(VALIDATION)],
            check=True,
        )
        subprocess.run(
            ["git", "-C", str(REPO), "commit", "-m", "chore: advance orchestrator review pointer"],
            check=True,
        )
        print("COMMITTED: pointer advance")
        return True
    else:
        print("CLEAN: no pointer commit needed")
        return False


def run_agent():
    """Trigger the orchestrator agent via Moltis CLI."""
    msg = (
        "Working directory: /mnt/fast_data/workspaces/pea-met-network\n\n"
        "Read docs/orchestrator-prompt.md and follow it exactly."
    )
    print("AGENT: starting orchestrator...")
    try:
        subprocess.run(
            ["moltis", "agent", "--message", msg],
            check=True,
            cwd=str(REPO),
        )
        print("AGENT: completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"AGENT: failed with exit code {e.returncode}")
        return False


def main():
    print("=== ORCHESTRATOR WRAPPER START ===")

    # Step 1: Run the agent
    agent_ok = run_agent()

    # Step 2: Advance pointer (always runs, even if agent failed)
    print("TEARDOWN: advancing review pointer...")
    advance_pointer()
    commit_if_dirty()

    if not agent_ok:
        print("=== ORCHESTRATOR WRAPPER END (agent failed) ===")
        sys.exit(1)

    print("=== ORCHESTRATOR WRAPPER END ===")
    sys.exit(0)


if __name__ == "__main__":
    main()
