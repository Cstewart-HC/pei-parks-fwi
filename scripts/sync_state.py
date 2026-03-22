#!/usr/bin/env python3
"""
sync_state.py — Ralph loop state synchronizer.

Runs at the START of every loop iteration. Reads ralph-state.json,
checks gates, reports current state. The JSON is the single source
of truth — this script never derives state from git messages.

Usage:
    python scripts/sync_state.py              Full sync: check gates, report state, write JSON
    python scripts/sync_state.py --check-only Print state, exit 1 if gate drift detected
    python scripts/sync_state.py --run-gates  Check all gates, report pass/fail
    python scripts/sync_state.py --write      Write JSON only (for pre-commit hook)
    python scripts/sync_state.py --view plan  Generate plan summary from JSON
    python scripts/sync_state.py --view decisions  Generate decisions log from JSON
    python scripts/sync_state.py --view status    Generate status summary from JSON
"""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
STATE_FILE = REPO_ROOT / "docs" / "ralph-state.json"


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


def get_head_sha() -> str:
    """Get current HEAD commit short hash."""
    return git("rev-parse", "--short", "HEAD")


def run_gate(gate_cmd: str | None) -> tuple[bool, str]:
    """Run a verification gate command. Returns (passed, output)."""
    if not gate_cmd:
        return False, "no gate defined"
    try:
        result = subprocess.run(
            gate_cmd,
            shell=True,
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=60,
        )
        output = result.stdout.strip()[:200]
        if result.returncode != 0 and result.stderr.strip():
            output = result.stderr.strip()[:200]
        return result.returncode == 0, output
    except subprocess.TimeoutExpired:
        return False, "gate timed out (60s)"
    except Exception as e:
        return False, str(e)[:200]


def load_state() -> dict:
    """Load current ralph-state.json."""
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            return json.load(f)
    print("ralph-state.json not found", file=sys.stderr)
    sys.exit(1)


def save_state(state: dict) -> None:
    """Write ralph-state.json atomically."""
    state["updated_at"] = datetime.now(
        timezone.utc
    ).astimezone().isoformat()
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2) + "\n")
    tmp.replace(STATE_FILE)


def get_next_task(state: dict) -> dict | None:
    """Find the first pending task whose dependencies are all done."""
    done_ids = {
        t["id"]
        for t in state.get("tasks", [])
        if t.get("status") == "done"
    }
    for task in state.get("tasks", []):
        if task.get("status") != "pending":
            continue
        deps = task.get("depends", [])
        if all(d in done_ids for d in deps):
            return task
    return None


def check_all_gates(state: dict) -> list[dict]:
    """Run all gates, return list of results."""
    results = []
    for task in state.get("tasks", []):
        gate_cmd = task.get("gate", "")
        gate_ok, gate_out = run_gate(gate_cmd)
        results.append({
            "id": task["id"],
            "status": task.get("status", "?"),
            "gate_passes": gate_ok,
            "gate_output": gate_out,
            "gate_cmd": gate_cmd,
        })
    return results


def detect_drift(state: dict) -> list[str]:
    """Check for gate drift: done tasks with failing gates, pending with passing."""
    drift = []
    results = check_all_gates(state)
    for r in results:
        if r["status"] == "done" and not r["gate_passes"]:
            drift.append(f"DRIFT: {r['id']} marked done but gate FAILS")
            drift.append(f"  gate: {r['gate_cmd']}")
            drift.append(f"  output: {r['gate_output']}")
        elif r["status"] == "pending" and r["gate_passes"]:
            drift.append(f"UNMARKED: {r['id']} marked pending but gate PASSES")
    return drift


def auto_correct_drift(state: dict) -> bool:
    """Correct drift: mark done tasks with failing gates back to pending,
    and pending tasks with passing gates to done. Returns True if changes made."""
    results = check_all_gates(state)
    changed = False
    for r in results:
        task = next(
            t for t in state["tasks"] if t["id"] == r["id"]
        )
        if r["status"] == "done" and not r["gate_passes"]:
            task["status"] = "pending"
            changed = True
        elif r["status"] == "pending" and r["gate_passes"]:
            task["status"] = "done"
            changed = True
    return changed


def render_plan(state: dict) -> str:
    """Generate a plan summary from JSON state."""
    lines = []
    phase = state.get("phase", "?")
    phases = state.get("phases", {})
    phase_info = phases.get(str(phase), {})
    phase_name = phase_info.get("name", "unknown")
    phase_exit = phase_info.get("exit", "")

    lines.append(f"# Implementation Plan (generated)")
    lines.append("")
    lines.append(f"## Current Phase: {phase} — {phase_name}")
    lines.append(f"Exit criteria: {phase_exit}")
    lines.append("")

    lines.append("## Phase Roadmap")
    lines.append("")
    lines.append("| Phase | Name | Status |")
    lines.append("|---|---|---|")
    for p_num, p_info in phases.items():
        p_int = int(p_num)
        if p_int < phase:
            p_status = "done"
        elif p_int == phase:
            p_status = "in progress"
        else:
            p_status = "not started"
        lines.append(f"| {p_num} | {p_info.get('name', '?')} | {p_status} |")
    lines.append("")

    lines.append("## Tasks")
    lines.append("")

    done = [t for t in state.get("tasks", []) if t.get("status") == "done"]
    pending = [t for t in state.get("tasks", []) if t.get("status") == "pending"]

    if done:
        lines.append("### Completed")
        for t in done:
            lines.append(f"- [x] {t.get('description', t['id'])}")
            lines.append(f"  gate: `{t.get('gate', '')}`")
        lines.append("")

    if pending:
        lines.append("### Pending")
        for t in pending:
            deps = t.get("depends", [])
            dep_str = f" (depends: {', '.join(deps)})" if deps else ""
            lines.append(f"- [ ] {t.get('description', t['id'])}{dep_str}")
            lines.append(f"  gate: `{t.get('gate', '')}`")
        lines.append("")

    decisions = state.get("decisions", [])
    if decisions:
        lines.append("## Decisions")
        lines.append("")
        for d in decisions:
            lines.append(f"- {d.get('summary', '?')} ({d.get('date', '?')})")
        lines.append("")

    blocker = state.get("blocker")
    if blocker:
        lines.append("## Blocker")
        lines.append("")
        lines.append(f"**{blocker}**")
        lines.append("")

    return "\n".join(lines)


def render_decisions(state: dict) -> str:
    """Generate a decisions log from JSON state."""
    lines = []
    lines.append("# Decisions Log (generated)")
    lines.append("")

    decisions = state.get("decisions", [])
    if not decisions:
        lines.append("No decisions recorded.")
    else:
        for d in decisions:
            lines.append(f"- **{d.get('summary', '?')}** ({d.get('date', '?')})")
            if d.get("rationale"):
                lines.append(f"  Rationale: {d['rationale']}")
            lines.append("")

    return "\n".join(lines)


def render_status(state: dict) -> str:
    """Generate a status summary from JSON state."""
    lines = []
    phase = state.get("phase", "?")
    phases = state.get("phases", {})
    phase_info = phases.get(str(phase), {})
    phase_name = phase_info.get("name", "unknown")

    done = sum(1 for t in state.get("tasks", []) if t.get("status") == "done")
    pending = sum(1 for t in state.get("tasks", []) if t.get("status") == "pending")
    total = done + pending

    next_task = get_next_task(state)

    lines.append(f"Phase: {phase} — {phase_name}")
    lines.append(f"Tasks: {done}/{total} done, {pending} remaining")
    lines.append(f"Iteration: {state.get('iteration', '?')}/{state.get('max_per_day', '?')}")
    lines.append(f"Status: {state.get('status', '?')}")

    blocker = state.get("blocker")
    if blocker:
        lines.append(f"Blocker: {blocker}")
    else:
        lines.append("Blocker: none")

    lines.append("")
    if next_task:
        lines.append(f"Next: {next_task['id']}")
        lines.append(f"  {next_task.get('description', '')}")
        lines.append(f"  gate: {next_task.get('gate', '')}")
    else:
        lines.append("Next: no eligible tasks")

    return "\n".join(lines)


def main() -> None:
    args = set(sys.argv[1:])
    check_only = "--check-only" in args
    run_gates_only = "--run-gates" in args
    write_only = "--write" in args

    # Parse --view flag
    view_arg = None
    for a in sys.argv[1:]:
        if a.startswith("--view="):
            view_arg = a.split("=", 1)[1]
        elif a == "--view" and len(sys.argv) > sys.argv.index(a) + 1:
            view_arg = sys.argv[sys.argv.index(a) + 1]

    state = load_state()

    # --- View modes (human-readable, stdout only) ---
    if view_arg == "plan":
        print(render_plan(state))
        return
    elif view_arg == "decisions":
        print(render_decisions(state))
        return
    elif view_arg == "status":
        print(render_status(state))
        return

    # --- Write-only mode (pre-commit hook) ---
    if write_only:
        changed = auto_correct_drift(state)
        if changed:
            save_state(state)
        return

    # --- Get git context ---
    head_sha = get_head_sha()

    # --- Check-only mode ---
    if check_only:
        drift = detect_drift(state)
        if drift:
            print("DRIFT DETECTED:")
            for d in drift:
                print(f"  {d}")
            sys.exit(1)
        else:
            print("No drift detected.")
            sys.exit(0)

    # --- Run-gates mode ---
    if run_gates_only:
        print(f"COMMIT={head_sha}")
        for r in check_all_gates(state):
            label = f"{r['id']} [{r['status']}]"
            gate_label = "PASS" if r["gate_passes"] else "FAIL"
            if r["status"] == "done" and not r["gate_passes"]:
                print(f"  DRIFT: {label} gate={gate_label}")
                print(f"    gate: {r['gate_cmd']}")
                print(f"    output: {r['gate_output']}")
            elif r["status"] == "pending" and r["gate_passes"]:
                print(f"  UNMARKED: {label} gate={gate_label}")
            else:
                print(f"  OK: {label} gate={gate_label}")
        return

    # --- Full sync (normal loop startup) ---
    # Auto-correct drift
    changed = auto_correct_drift(state)
    if changed:
        print("DRIFT_CORRECTED: state updated to match gate results")

    # Update iteration
    state["iteration"] = state.get("iteration", 0) + 1
    state["status"] = "running"
    save_state(state)

    # Report state
    phase = state.get("phase", "?")
    phases = state.get("phases", {})
    phase_name = phases.get(str(phase), {}).get("name", "unknown")
    done = sum(1 for t in state["tasks"] if t.get("status") == "done")
    pending = sum(1 for t in state["tasks"] if t.get("status") == "pending")

    print(f"PHASE={phase}")
    print(f"PHASE_NAME={phase_name}")
    print(f"COMMIT={head_sha}")
    print(f"TASKS_DONE={done}")
    print(f"TASKS_LEFT={pending}")

    next_task = get_next_task(state)
    if next_task:
        print(f"NEXT_TASK={next_task['id']}")
        print(f"NEXT_TASK_DESC={next_task['description']}")
        gate_cmd = next_task.get("gate", "")
        print(f"GATE_CMD={gate_cmd}")
        if gate_cmd:
            gate_ok, gate_out = run_gate(gate_cmd)
            if gate_ok:
                print("GATE_STATUS=ALREADY_PASSES")
            else:
                print("GATE_STATUS=NOT_YET")
                if gate_out:
                    print(f"GATE_OUTPUT={gate_out}")
        deps = next_task.get("depends", [])
        if deps:
            print(f"DEPENDS={','.join(deps)}")
    else:
        print("NEXT_TASK=NONE")
        if pending == 0:
            print("ALL_TASKS_COMPLETE=true")

    blocker = state.get("blocker")
    if blocker:
        print(f"BLOCKER={blocker}")


if __name__ == "__main__":
    main()
