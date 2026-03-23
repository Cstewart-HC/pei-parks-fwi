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
import re
import subprocess
import sys
from datetime import datetime, timezone
from hashlib import sha256
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
            print(
                "ANOMALY: FP state — exit fails but validation PASS",
                file=sys.stderr,
            )
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
            if phases[p_num].get("status") in ("not_started", "pending"):
                state["phase"] = int(p_num)
                phases[p_num]["status"] = "active"
                return True
        phase_info["status"] = "done"
        return True

    # PP: both pass — true pass, advance
    phase_info["status"] = "done"
    for p_num in sorted(phases.keys(), key=int):
        if phases[p_num].get("status") in ("not_started", "pending"):
            state["phase"] = int(p_num)
            phases[p_num]["status"] = "active"
            return True
    phase_info["status"] = "done"
    return True


def reset_state_files_if_dirty() -> list[str]:
    """Reset ralph-state.json and validation.json from HEAD if dirty.

    These files are owned by sync_state.py / UnRalph / pre-commit hook.
    Ralph should never modify them. If they're dirty, the working tree
    has stale state that will cause incorrect decisions.
    """
    messages = []
    for state_file in [STATE_FILE, VALIDATION_FILE]:
        if not state_file.exists():
            continue
        rel = str(state_file.relative_to(REPO_ROOT))
        result = subprocess.run(
            ["git", "-C", str(REPO_ROOT), "diff", "--quiet", rel],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            subprocess.run(
                ["git", "-C", str(REPO_ROOT), "checkout", "--", rel],
                capture_output=True,
                text=True,
            )
            messages.append(f"RESET: {rel} reverted from HEAD")
    return messages


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


def validate_blocker(state: dict) -> tuple[bool, str | None]:
    """Check if a set blocker is still valid.

    Runs the relevant check scoped to the file mentioned in the
    blocker text. Returns (still_valid, reason).
    """
    blocker = state.get("blocker")
    if not blocker:
        return False, None

    blocker_lower = blocker.lower()
    files_mentioned = re.findall(r"[\w/\-]+\.py", blocker)

    if "ruff" in blocker_lower:
        if files_mentioned:
            targets = " ".join(files_mentioned)
        else:
            targets = "."
        passed, output = run_cmd(f".venv/bin/ruff check {targets}")
        if passed:
            return False, f"ruff now passes on {targets}"

    if "pytest" in blocker_lower or "test" in blocker_lower:
        if files_mentioned:
            test_files = [f for f in files_mentioned if "test_" in f]
            if test_files:
                targets = " ".join(test_files)
            else:
                targets = "."
        else:
            targets = "."
        passed, output = run_cmd(
            f".venv/bin/pytest {targets} -q --tb=short 2>&1"
        )
        if passed:
            return False, f"pytest now passes on {targets}"

    return True, blocker


def compute_fail_criteria_hash(validation: dict | None) -> str | None:
    """Hash the set of failing criterion IDs for stall detection."""
    if validation is None:
        return None
    failed = sorted(
        c.get("id", "")
        for c in validation.get("criteria", [])
        if c.get("status") == "FAIL"
    )
    if not failed:
        return None
    return sha256(
        ",".join(failed).encode()
    ).hexdigest()[:16]


def update_circuit_breaker(state: dict) -> bool:
    """Check for stalls and update the circuit_breaker block.

    Returns True if the breaker has tripped (should stop).

    Stall = same commit + same REJECT verdict + same failing criteria
    across consecutive sync_state runs.
    """
    cb = state.setdefault("circuit_breaker", {
        "consecutive_stalls": 0,
        "last_commit": None,
        "last_verdict": None,
        "last_fail_criteria_hash": None,
        "consecutive_errors": 0,
        "tripped": False,
        "trip_reason": None,
        "trip_at": None,
    })

    # If already tripped, stay tripped (manual reset)
    if cb.get("tripped"):
        return True

    head_sha = get_head_sha()
    validation = load_validation()
    verdict = get_validation_verdict(validation)
    fail_hash = compute_fail_criteria_hash(validation)

    # Detect stall: same commit + same verdict + same failing criteria
    is_stall = (
        cb.get("last_commit") == head_sha
        and cb.get("last_verdict") == verdict
        and cb.get("last_fail_criteria_hash") == fail_hash
        and verdict == "REJECT"
    )

    if is_stall:
        cb["consecutive_stalls"] = cb.get("consecutive_stalls", 0) + 1
    else:
        cb["consecutive_stalls"] = 0

    # Update tracking fields
    cb["last_commit"] = head_sha
    cb["last_verdict"] = verdict
    cb["last_fail_criteria_hash"] = fail_hash

    # Trip at 3 consecutive stalls
    if cb["consecutive_stalls"] >= 3:
        cb["tripped"] = True
        cb["trip_reason"] = (
            f"3 consecutive stalls on commit {head_sha}: "
            f"verdict={verdict}, "
            f"fail_criteria={fail_hash}"
        )
        cb["trip_at"] = datetime.now(
            timezone.utc
        ).astimezone().isoformat()
        print(
            f"CIRCUIT_BREAKER_TRIPPED: {cb['trip_reason']}",
            file=sys.stderr,
        )
        return True

    if cb["consecutive_stalls"] > 0:
        print(
            f"CIRCUIT_BREAKER_WARNING: "
            f"stall {cb['consecutive_stalls']}/3 "
            f"on commit {head_sha}",
            file=sys.stderr,
        )

    return False


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
            failed = [
                c
                for c in validation.get("criteria", [])
                if c.get("status") == "FAIL"
            ]
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
        v_exit = "PASS" if exit_passes else "FAIL"
        print(
            f"VALIDATION_STATE=INDETERMINATE"
            f" (validation={verdict}, exit={v_exit})"
        )

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

    # Step 0: Reset state files from HEAD if dirty
    reset_messages = reset_state_files_if_dirty()
    for msg in reset_messages:
        print(msg, file=sys.stderr)

    state = load_state()

    # Auto-clear stale blockers (scoped to mentioned files)
    still_valid, reason = validate_blocker(state)
    if not still_valid and state.get("blocker"):
        print(
            f"BLOCKER_CLEARED reason=\"{reason}\"",
            file=sys.stderr,
        )
        state["blocker"] = None
        save_state(state)

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

    # Circuit breaker check — before any work happens
    tripped = update_circuit_breaker(state)
    if tripped:
        state["status"] = "circuit_breaked"
        save_state(state)
        cb = state.get("circuit_breaker", {})
        print("CIRCUIT_BREAKER=TRIPPED")
        print(f"CIRCUIT_BREAKER_REASON={cb.get('trip_reason', '')}")
        print(f"CIRCUIT_BREAKER_AT={cb.get('trip_at', '')}")
        sys.exit(0)

    # Print circuit breaker status if approaching threshold
    cb = state.get("circuit_breaker", {})
    stalls = cb.get("consecutive_stalls", 0)
    if stalls > 0:
        print(
            f"CIRCUIT_BREAKER={stalls}/3 stalls",
            file=sys.stderr,
        )

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
