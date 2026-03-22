#!/usr/bin/env python3
"""
sync_state.py — Ralph loop state synchronizer (spec-driven).

Discovers tests, runs them, reports pass/fail. The test suite IS
the task list. Ralph reads specs, writes tests, implements.

Usage:
    python scripts/sync_state.py
        Full sync: discover tests, report state, write JSON
    python scripts/sync_state.py --check-only
        Print state, exit 1 if phase exit fails unexpectedly
    python scripts/sync_state.py --write
        Write JSON only (for pre-commit hook)
    python scripts/sync_state.py --view plan
    python scripts/sync_state.py --view decisions
    python scripts/sync_state.py --view status
"""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
STATE_FILE = REPO_ROOT / "docs" / "ralph-state.json"
TESTS_DIR = REPO_ROOT / "tests"


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


def run_cmd(cmd: str, timeout: int = 60) -> tuple[bool, str]:
    """Run a shell command. Returns (passed, output)."""
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        output = result.stdout.strip()[:300]
        if result.returncode != 0 and result.stderr.strip():
            output = result.stderr.strip()[:300]
        return result.returncode == 0, output
    except subprocess.TimeoutExpired:
        return False, f"timed out ({timeout}s)"
    except Exception as e:
        return False, str(e)[:300]


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
    tmp = STATE_FILE.with_suffix("..tmp")
    tmp.write_text(json.dumps(state, indent=2) + "\n")
    tmp.replace(STATE_FILE)


def discover_tests() -> list[str]:
    """Find all test files in tests/ directory."""
    if not TESTS_DIR.exists():
        return []
    return sorted(
        str(p.relative_to(REPO_ROOT))
        for p in TESTS_DIR.glob("test_*.py")
    )


def run_test_file(test_file: str) -> tuple[bool, str]:
    """Run a single test file with pytest. Returns (passed, output)."""
    cmd = f".venv/bin/pytest {test_file} -q --tb=short 2>&1"
    return run_cmd(cmd, timeout=120)


def check_phase_exit(state: dict) -> tuple[bool, str, str]:
    """Run the current phase's exit criteria.
    Returns (passes, gate_cmd, output)."""
    phases = state.get("phases", {})
    phase = str(state.get("phase", "?"))
    phase_info = phases.get(phase, {})
    exit_gate = phase_info.get("exit", "")

    if not exit_gate:
        return True, "(no exit gate defined)", ""

    passes, output = run_cmd(exit_gate, timeout=120)
    return passes, exit_gate, output


VALIDATION_FILE = REPO_ROOT / "docs" / "validation.json"


def load_validation() -> dict | None:
    """Load validation.json if it exists."""
    if VALIDATION_FILE.exists():
        with open(VALIDATION_FILE) as f:
            return json.load(f)
    return None


def get_validation_verdict(validation: dict | None) -> str:
    """Return verdict string: PASS, REJECT, or NONE."""
    if validation is None:
        return "NONE"
    return validation.get("verdict", "NONE").upper()


def revert_phase_on_reject(state: dict) -> bool:
    """If current phase is 'done' but validation says REJECT, revert to active.
    Returns True if reverted."""
    validation = load_validation()
    verdict = get_validation_verdict(validation)

    phases = state.get("phases", {})
    current = str(state.get("phase", "?"))
    phase_info = phases.get(current, {})

    # Only revert if validation explicitly rejects
    if verdict != "REJECT":
        return False

    # Check if current phase is marked done
    if phase_info.get("status") == "done":
        phase_info["status"] = "active"
        # Also revert phase number if it advanced past the rejected phase
        # Find the last phase that was marked done
        for p_num in sorted(phases.keys(), key=int, reverse=True):
            p_info = phases[p_num]
            if p_info.get("status") == "done":
                # Check if there's a rejected phase before or at this one
                break
        # Reset current phase to the one that was rejected
        # The rejected commit's phase should be the one we go back to
        state["phase"] = int(current)
        phase_info["status"] = "active"
        return True

    return False


def advance_phase_if_done(state: dict) -> bool:
    """Check if current phase exit passes AND validation is not REJECT.
    Returns True if phase advanced.
    
    2x2 grid:
        PP (phase exit PASS + validation PASS) → advance (only true pass)
        PF (phase exit PASS + validation FAIL) → BLOCK, do not advance
        FF (phase exit FAIL + validation FAIL) → continue working
        FP (phase exit FAIL + validation PASS) → IMPOSSIBLE, log anomaly
    """
    phases = state.get("phases", {})
    current = str(state.get("phase", "?"))
    phase_info = phases.get(current, {})

    # Check phase exit criteria
    exit_passes, exit_cmd, exit_output = check_phase_exit(state)

    # Check validation
    validation = load_validation()
    verdict = get_validation_verdict(validation)

    # FF: phase exit fails — keep working
    if not exit_passes:
        if verdict == "PASS":
            # FP: impossible state — tests fail but validation passes
            print(f"ANOMALY: FP state detected — phase exit fails but validation is PASS", file=sys.stderr)
            print(f"  Phase exit: {exit_cmd}", file=sys.stderr)
        return False

    # Phase exit passes — now check validation
    if verdict == "REJECT":
        # PF: false positive — tests pass but spec compliance fails
        # Do NOT advance. Revert phase status if needed.
        if phase_info.get("status") == "done":
            phase_info["status"] = "active"
        return False

    if verdict == "NONE":
        # No validation yet — allow advancement (no UnRalph has reviewed)
        phase_info["status"] = "done"
        for p_num in sorted(phases.keys(), key=int):
            if phases[p_num].get("status") == "not_started":
                state["phase"] = int(p_num)
                phases[p_num]["status"] = "active"
                return True
        phase_info["status"] = "done"
        return True

    # PP: both pass — true pass, advance
    phase_info["status"] = "done"
    for p_num in sorted(phases.keys(), key=int):
        if phases[p_num].get("status") == "not_started":
            state["phase"] = int(p_num)
            phases[p_num]["status"] = "active"
            return True
    phase_info["status"] = "done"
    return True


def report_working_tree() -> list[str]:
    """Report uncommitted changes without cleaning."""
    result = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "status", "--porcelain"],
        capture_output=True,
        text=True,
    )
    lines = []
    if result.stdout.strip():
        lines.append("WORKING_TREE=DIRTY")
        for entry in result.stdout.strip().split("\n"):
            lines.append(f"  {entry}")
    else:
        lines.append("WORKING_TREE=CLEAN")
    return lines


def render_plan(state: dict) -> str:
    """Generate a plan summary from JSON state."""
    lines = []
    phase = state.get("phase", "?")
    phases = state.get("phases", {})
    phase_info = phases.get(str(phase), {})
    phase_name = phase_info.get("name", "unknown")
    phase_exit = phase_info.get("exit", "")

    lines.append("# Implementation Plan (generated)")
    lines.append("")
    lines.append(f"## Current Phase: {phase} — {phase_name}")
    lines.append(f"Exit criteria: `{phase_exit}`")
    lines.append("")

    lines.append("## Phase Roadmap")
    lines.append("")
    lines.append("| Phase | Name | Status | Exit Criteria |")
    lines.append("|---|---|---|---|")
    for p_num, p_info in phases.items():
        name = p_info.get("name", "?")
        status = p_info.get("status", "?")
        exit_c = p_info.get("exit", "")
        lines.append(f"| {p_num} | {name} | {status} | `{exit_c}` |")
    lines.append("")

    decisions = state.get("decisions", [])
    if decisions:
        lines.append("## Decisions")
        lines.append("")
        for d in decisions:
            summary = d.get("summary", "?")
            date = d.get("date", "?")
            lines.append(f"- {summary} ({date})")
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
            summary = d.get("summary", "?")
            date = d.get("date", "?")
            lines.append(f"- **{summary}** ({date})")
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

    lines.append(f"Phase: {phase} — {phase_name}")
    lines.append(f"Exit: `{phase_info.get('exit', '')}`")
    iteration = state.get("iteration", "?")
    max_per_day = state.get("max_per_day", "?")
    lines.append(f"Iteration: {iteration}/{max_per_day}")
    lines.append(f"Status: {state.get('status', '?')}")

    blocker = state.get("blocker")
    if blocker:
        lines.append(f"Blocker: {blocker}")
    else:
        lines.append("Blocker: none")

    lines.append("")
    lines.append("## Test Suite")
    lines.append("")
    test_files = discover_tests()
    for tf in test_files:
        passes, output = run_test_file(tf)
        label = "PASS" if passes else "FAIL"
        lines.append(f"  {label}: {tf}")
        if not passes:
            lines.append(f"    {output[:100]}")

    return "\n".join(lines)


def parse_view_arg(argv: list[str]) -> str | None:
    """Extract a --view argument from argv."""
    for index, arg in enumerate(argv):
        if arg.startswith("--view="):
            return arg.split("=", 1)[1]
        if arg == "--view" and len(argv) > index + 1:
            return argv[index + 1]
    return None


def handle_view_mode(view_arg: str | None, state: dict) -> bool:
    """Render a requested view. Return True when handled."""
    if view_arg == "plan":
        print(render_plan(state))
        return True
    if view_arg == "decisions":
        print(render_decisions(state))
        return True
    if view_arg == "status":
        print(render_status(state))
        return True
    return False


def handle_check_only(state: dict) -> None:
    """Run phase exit check and exit."""
    passes, gate_cmd, output = check_phase_exit(state)
    if not passes:
        print(f"Phase exit FAILS: {gate_cmd}")
        print(f"  {output}")
        sys.exit(1)
    print("Phase exit passes.")
    sys.exit(0)


def print_sync_report(
    state: dict, head_sha: str, phase_advanced: bool
) -> None:
    """Print the standard sync_state summary fields."""
    # Working tree status
    for line in report_working_tree():
        print(line)

    phases = state.get("phases", {})
    phase = state.get("phase", "?")
    phase_info = phases.get(str(phase), {})
    phase_name = phase_info.get("name", "unknown")

    print(f"PHASE={phase}")
    print(f"PHASE_NAME={phase_name}")
    print(f"COMMIT={head_sha}")

    if phase_advanced:
        print("PHASE_ADVANCED=true")

    # Validation state (2x2 grid)
    validation = load_validation()
    verdict = get_validation_verdict(validation)
    if verdict != "NONE":
        print(f"VALIDATION={verdict}")
        if verdict == "REJECT":
            # Summarize failed criteria
            failed = [c for c in validation.get("criteria", []) if c.get("status") == "FAIL"]
            print(f"VALIDATION_FAIL_COUNT={len(failed)}")
            for c in failed:
                print(f"  FAIL: {c.get('id', '?')} — {c.get('name', '?')}")
                evidence = c.get("evidence", "")
                if evidence:
                    print(f"    {evidence[:200]}")
    else:
        print("VALIDATION=NONE")

    # Phase exit check
    exit_passes, exit_cmd, exit_output = check_phase_exit(state)
    print(f"PHASE_EXIT={'PASS' if exit_passes else 'FAIL'}")
    print(f"PHASE_EXIT_CMD={exit_cmd}")
    if not exit_passes and exit_output:
        print(f"PHASE_EXIT_OUTPUT={exit_output[:200]}")

    # Compute 2x2 state
    if verdict == "PASS" and exit_passes:
        print("VALIDATION_STATE=PP")
    elif verdict == "REJECT" and exit_passes:
        print("VALIDATION_STATE=PF")
    elif verdict == "REJECT" and not exit_passes:
        print("VALIDATION_STATE=FF")
    elif verdict == "PASS" and not exit_passes:
        print("VALIDATION_STATE=FP")
    else:
        print(f"VALIDATION_STATE=INDETERMINATE (validation={verdict}, exit={'PASS' if exit_passes else 'FAIL'})")

    # Test discovery — what test files exist for this phase
    print("")
    print("## Test Discovery")
    test_files = discover_tests()
    if not test_files:
        print("  No test files found in tests/")
    else:
        for tf in test_files:
            print(f"  {tf}")

    blocker = state.get("blocker")
    if blocker:
        print(f"BLOCKER={blocker}")


def main() -> None:
    argv = sys.argv[1:]
    args = set(argv)
    check_only = "--check-only" in args
    write_only = "--write" in args
    view_arg = parse_view_arg(argv)

    state = load_state()

    if handle_view_mode(view_arg, state):
        return

    if write_only:
        # Pre-commit hook: just check if phase should advance
        advance_phase_if_done(state)
        save_state(state)
        return

    head_sha = get_head_sha()

    if check_only:
        handle_check_only(state)

    # Revert phase if validation says REJECT (fixes PF false positives)
    reverted = revert_phase_on_reject(state)
    if reverted:
        save_state(state)
        print("PHASE_REVERTED=true (validation REJECT)", file=sys.stderr)

    phase_advanced = advance_phase_if_done(state)

    state["iteration"] = state.get("iteration", 0) + 1
    state["status"] = "running"
    save_state(state)
    print_sync_report(state, head_sha, phase_advanced)


if __name__ == "__main__":
    main()
