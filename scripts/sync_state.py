#!/usr/bin/env python3
"""
sync_state.py — Ralph loop state synchronizer.

Runs at the START of every loop iteration. Derives ground truth from
git history and the implementation plan, then updates ralph-state.json.

Usage:
    python scripts/sync_state.py [--check-only]

    --check-only   Print derived state without writing JSON.
                   Exit 0 if in sync, exit 1 if drift detected.

The loop MUST run this before doing any work. It returns:
  1. Current phase (derived from git + plan)
  2. Next task with its gate command
  3. Whether any drift was detected and corrected
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
STATE_FILE = REPO_ROOT / "docs" / "ralph-state.json"
PLAN_FILE = REPO_ROOT / "IMPLEMENTATION_PLAN.md"
DIARY_DIR = REPO_ROOT / "docs" / "diary"


def git(*args: str) -> str:
    """Run a git command and return stripped stdout."""
    result = subprocess.run(
        ["git", "-C", str(REPO_ROOT), *args],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"git error: {result.stderr.strip()}", file=sys.stderr)
        sys.exit(1)
    return result.stdout.strip()


def git_log(oneline: bool = True, n: int = 20) -> list[str]:
    """Get recent commit messages."""
    flag = "--oneline" if oneline else "--format=%H %s"
    output = git("log", flag, f"-{n}")
    return output.splitlines() if output else []


def derive_phase_from_commits(commits: list[str]) -> tuple[int, str]:
    """
    Derive the current phase from commit message prefixes.

    Convention: commits use scope prefixes like:
      audit:    → Phase 1 (Obtain)
      scrub:    → Phase 2 (Scrub)
      explore:  → Phase 3 (Explore)
      model:    → Phase 4 (Model: Reference + FWI)
      redund:   → Phase 5 (Model: Redundancy)
      interp:   → Phase 6 (Interpret)
      chore/scaffold/data → Phase 1

    Returns (phase_number, phase_name).
    """
    phase_keywords = {
        1: {"audit", "data", "scaffold", "chore"},
        2: {"scrub", "clean", "normali", "resamp", "imput"},
        3: {"explore", "visual", "inspect"},
        4: {"model", "stanhope", "fwi", "moisture", "ref"},
        5: {"redund", "pca", "cluster", "benchmark"},
        6: {"interp", "uncertain", "recommend", "report"},
    }
    phase_names = {
        1: "Obtain",
        2: "Scrub",
        3: "Explore",
        4: "Model: Reference + FWI",
        5: "Model: Redundancy",
        6: "Interpret",
    }

    latest_phase = 1
    for line in commits:
        scope = line.split(":", 1)[-1].lower() if ":" in line else line.lower()
        for phase, keywords in phase_keywords.items():
            if any(kw in scope for kw in keywords):
                latest_phase = max(latest_phase, phase)

    return latest_phase, phase_names[latest_phase]


def parse_plan_tasks(plan_text: str) -> tuple[list[str], list[str]]:
    """
    Parse IMPLEMENTATION_PLAN.md to find completed and remaining tasks.

    Returns (completed_task_ids, remaining_task_ids).
    """
    completed = []
    remaining = []
    current_task_id = None

    for line in plan_text.splitlines():
        # Match task lines: "- [x] ..." or "- [ ] ..."
        m = re.match(r"- \[([ x])\] (.+)", line)
        if m:
            status, desc = m.group(1), m.group(2)
            task_id = _slugify(desc)
            if status == "x":
                completed.append(task_id)
            else:
                remaining.append(task_id)
            current_task_id = task_id
        elif current_task_id and line.strip().startswith("gate:"):
            pass  # gate lives on the task, already captured

    return completed, remaining


def _slugify(text: str) -> str:
    """Create a short task identifier from description."""
    words = re.findall(r"[a-zA-Z0-9]+", text.lower())
    return "-".join(words[:5]) if words else "unknown"


def find_next_task(plan_text: str) -> dict | None:
    """
    Find the first unchecked task in the implementation plan.

    Returns dict with 'description', 'gate', 'depends' or None.
    """
    lines = plan_text.splitlines()
    for i, line in enumerate(lines):
        if re.match(r"- \[ \] ", line):
            desc = re.sub(r"^- \[ \] ", "", line).strip()
            gate = None
            depends = []

            # Look ahead for gate and depends lines
            for j in range(i + 1, min(i + 4, len(lines))):
                ahead = lines[j].strip()
                if ahead.startswith("gate:"):
                    gate = ahead.split(":", 1)[1].strip()
                elif ahead.startswith("depends:"):
                    raw = ahead.split(":", 1)[1].strip().strip("[]")
                    depends = [
                        d.strip().strip("'\"")
                        for d in raw.split(",")
                        if d.strip()
                    ]
                elif ahead.startswith("- ["):
                    break  # next task

            return {
                "description": desc,
                "gate": gate,
                "depends": depends,
            }
    return None


def run_gate(gate_cmd: str | None) -> bool:
    """Run a verification gate command. Returns True if it passes."""
    if not gate_cmd:
        return False
    try:
        result = subprocess.run(
            gate_cmd,
            shell=True,
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=30,
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, Exception):
        return False


def load_state() -> dict:
    """Load current ralph-state.json."""
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            return json.load(f)
    return {}


def save_state(state: dict) -> None:
    """Write ralph-state.json atomically."""
    state["updated_at"] = datetime.now(
        timezone.utc
    ).astimezone().isoformat()
    STATE_FILE.write_text(json.dumps(state, indent=2) + "\n")


def main() -> None:
    check_only = "--check-only" in sys.argv

    # --- Derive ground truth from git ---
    commits = git_log()
    if not commits:
        print("No commits found", file=sys.stderr)
        sys.exit(1)

    latest_sha_short = commits[0].split()[0]
    latest_msg = " ".join(commits[0].split()[1:]) if len(commits[0].split()) > 1 else ""

    phase, phase_name = derive_phase_from_commits(commits)

    # --- Parse implementation plan ---
    plan_text = PLAN_FILE.read_text() if PLAN_FILE.exists() else ""
    completed, remaining = parse_plan_tasks(plan_text)
    next_task = find_next_task(plan_text)

    # --- Build derived state ---
    today = datetime.now().astimezone().strftime("%Y-%m-%T")
    derived = {
        "phase": phase,
        "phase_name": phase_name,
        "last_commit": latest_sha_short,
        "last_commit_msg": latest_msg,
        "tasks_completed": completed,
        "tasks_remaining": remaining,
        "next_task": next_task,
    }

    # --- Load existing state and detect drift ---
    existing = load_state()
    drift = []

    if existing.get("last_commit") != latest_sha_short:
        drift.append(
            f"commit: stored={existing.get('last_commit')}, "
            f"actual={latest_sha_short}"
        )
    if existing.get("phase") != phase:
        drift.append(
            f"phase: stored={existing.get('phase')}, "
            f"derived={phase}"
        )

    # --- Output ---
    if check_only:
        if drift:
            print("DRIFT DETECTED:")
            for d in drift:
                print(f"  - {d}")
            sys.exit(1)
        else:
            print("State is in sync.")
            print(f"  Phase: {phase} — {phase_name}")
            print(f"  Last commit: {latest_sha_short}")
            if next_task:
                print(f"  Next task: {next_task['description']}")
            sys.exit(0)

    # --- Update state ---
    existing.update(derived)
    save_state(existing)

    # --- Print summary for the loop to consume ---
    print(f"PHASE={phase}")
    print(f"PHASE_NAME={phase_name}")
    print(f"COMMIT={latest_sha_short}")
    print(f"TASKS_DONE={len(completed)}")
    print(f"TASKS_LEFT={len(remaining)}")

    if drift:
        print("DRIFT_CORRECTED:")
        for d in drift:
            print(f"  {d}")

    if next_task:
        print(f"NEXT_TASK={next_task['description']}")
        if next_task["gate"]:
            # Check if the gate already passes (task may be done
            # but not marked)
            gate_ok = run_gate(next_task["gate"])
            if gate_ok:
                print("GATE_STATUS=ALREADY_PASSES")
            else:
                print("GATE_STATUS=NOT_YET")
            print(f"GATE_CMD={next_task['gate']}")
        if next_task["depends"]:
            print(f"DEPENDS={','.join(next_task['depends'])}")
    else:
        print("NEXT_TASK=NONE")
        print("ALL_TASKS_COMPLETE=true")


if __name__ == "__main__":
    main()
