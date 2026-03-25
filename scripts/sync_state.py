#!/usr/bin/env python3
"""sync_state.py — Ralph loop state synchronizer (spec-driven)."""

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
VALIDATION_FILE = REPO_ROOT / "docs" / "validation.json"
LOOP_LOG_FILE = REPO_ROOT / "docs" / "loop-log.jsonl"


def git(*args: str) -> str:
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
    return git("rev-parse", "--short", "HEAD")


def run_cmd(cmd: str, timeout: int = 60) -> tuple[bool, str]:
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


def _normalize_state(state: dict) -> dict:
    """Normalize state to the expected internal format.

    v2 state files use a list of phase dicts keyed by 'id' and
    'current_phase' as an int.  Convert those to the dict-of-dicts
    layout that the rest of sync_state.py expects.
    """
    phases = state.get("phases")
    if isinstance(phases, list):
        # v2 format: list of {"id": N, ...} -> {"1": {...}, ...}
        phase_map = {}
        for p in phases:
            key = str(p["id"])
            # v2 uses "exit_gate"; v1 uses "exit" — normalise to "exit"
            if "exit_gate" in p and "exit" not in p:
                p["exit"] = p["exit_gate"]
            phase_map[key] = p
        state["phases"] = phase_map

    # Normalise the current-phase key
    if "phase" not in state and "current_phase" in state:
        state["phase"] = str(state["current_phase"])
    elif "phase" in state:
        state["phase"] = str(state["phase"])

    return state


# Remember whether the original file used a list so we can round-trip.
_PHASES_WAS_LIST = False


def load_state() -> dict:
    global _PHASES_WAS_LIST
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            raw = json.load(f)
        _PHASES_WAS_LIST = isinstance(raw.get("phases"), list)
        return _normalize_state(raw)
    print("ralph-state.json not found", file=sys.stderr)
    sys.exit(1)


def _denormalize_state(state: dict) -> dict:
    """Convert internal dict-of-dicts back to list format if needed."""
    out = dict(state)
    if _PHASES_WAS_LIST and isinstance(out.get("phases"), dict):
        out["phases"] = sorted(
            out["phases"].values(), key=lambda p: p["id"]
        )
        try:
            out["current_phase"] = int(out.get("phase", out.get("current_phase", 1)))
        except (ValueError, TypeError):
            pass
    return out


def save_state(state: dict) -> None:
    state["updated_at"] = datetime.now(timezone.utc).astimezone().isoformat()
    out = _denormalize_state(state)
    tmp = STATE_FILE.with_suffix("..tmp")
    tmp.write_text(json.dumps(out, indent=2) + "\n")
    tmp.replace(STATE_FILE)


def load_validation() -> dict | None:
    if VALIDATION_FILE.exists():
        with open(VALIDATION_FILE) as f:
            return json.load(f)
    return None


def save_validation(validation: dict) -> None:
    tmp = VALIDATION_FILE.with_suffix("..tmp")
    tmp.write_text(json.dumps(validation, indent=2) + "\n")
    tmp.replace(VALIDATION_FILE)


def discover_tests() -> list[str]:
    if not TESTS_DIR.exists():
        return []
    return sorted(str(p.relative_to(REPO_ROOT)) for p in TESTS_DIR.glob("test_*.py"))


def run_test_file(test_file: str) -> tuple[bool, str]:
    cmd = f".venv/bin/pytest {test_file} -q --tb=short 2>&1"
    return run_cmd(cmd, timeout=120)


def _ensure_venv_path(cmd: str) -> str:
    """Prepend .venv/bin/ to pytest/ruff if not already present."""
    for tool in ("pytest", "ruff"):
        if cmd.startswith(tool + " ") or cmd == tool:
            return f".venv/bin/{cmd}"
    return cmd


def check_phase_exit(state: dict) -> tuple[bool, str, str]:
    phases = state.get("phases", {})
    phase = str(state.get("phase", "?"))
    phase_info = phases.get(phase, {})
    exit_gate = phase_info.get("exit", "")
    if not exit_gate:
        return True, "(no exit gate defined)", ""
    exit_gate = _ensure_venv_path(exit_gate)
    passes, output = run_cmd(exit_gate, timeout=120)
    return passes, exit_gate, output


def get_validation_verdict(validation: dict | None) -> str:
    if validation is None:
        return "NONE"
    return validation.get("verdict", "NONE").upper()


def revert_phase_on_reject(state: dict) -> bool:
    validation = load_validation()
    verdict = get_validation_verdict(validation)
    phases = state.get("phases", {})
    current = str(state.get("phase", "?"))
    phase_info = phases.get(current, {})
    if verdict != "REJECT":
        return False
    if phase_info.get("status") == "done":
        state["phase"] = current
        phase_info["status"] = "active"
        return True
    return False


def _phase_sort_key(key: str) -> tuple[int, str]:
    """Sort phase keys like: 1, 2, ..., 9, 10, 10b, 10c, 10d, 11."""
    m = re.match(r"^(\d+)(.*)", key)
    if m:
        return (int(m.group(1)), m.group(2))
    return (999, key)


def advance_phase_if_done(state: dict) -> bool:
    phases = state.get("phases", {})
    current = str(state.get("phase", "?"))
    phase_info = phases.get(current, {})
    exit_passes, exit_cmd, exit_output = check_phase_exit(state)
    validation = load_validation()
    verdict = get_validation_verdict(validation)

    if not exit_passes:
        if verdict == "PASS":
            print("ANOMALY: FP state — exit fails but validation PASS", file=sys.stderr)
            print(f"  Phase exit: {exit_cmd}", file=sys.stderr)
        return False

    if verdict == "REJECT":
        if phase_info.get("status") == "done":
            phase_info["status"] = "active"
        return False

    if verdict in {"NONE", "PASS"}:
        phase_info["status"] = "done"
        next_phase = None
        for p_num in sorted(phases.keys(), key=_phase_sort_key):
            if phases[p_num].get("status") in ("not_started", "pending"):
                next_phase = p_num
                break
        if next_phase is not None:
            state["phase"] = next_phase
            phases[next_phase]["status"] = "active"
            if validation is not None:
                validation["verdict"] = "PENDING"
                validation["criteria"] = []
                validation["summary"] = f"Phase {next_phase} activated; awaiting Lisa review."
                validation["reviewed_at"] = datetime.now(timezone.utc).astimezone().isoformat()
                save_validation(validation)
            return True
        return True

    return False


def report_working_tree() -> list[str]:
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
    lines = ["# Implementation Plan (generated)", ""]
    phase = state.get("phase", "?")
    phases = state.get("phases", {})
    phase_info = phases.get(str(phase), {})
    lines.append(f"## Current Phase: {phase} — {phase_info.get('name', 'unknown')}")
    lines.append(f"Exit criteria: `{phase_info.get('exit', '')}`")
    lines.append("")
    lines.append("## Phase Roadmap")
    lines.append("")
    lines.append("| Phase | Name | Status | Exit Criteria |")
    lines.append("|---|---|---|---|")
    for p_num, p_info in phases.items():
        lines.append(f"| {p_num} | {p_info.get('name', '?')} | {p_info.get('status', '?')} | `{p_info.get('exit', '')}` |")
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
    lines = ["# Decisions Log (generated)", ""]
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
    lines = []
    phase = state.get("phase", "?")
    phases = state.get("phases", {})
    phase_info = phases.get(str(phase), {})
    lines.append(f"Phase: {phase} — {phase_info.get('name', 'unknown')}")
    lines.append(f"Exit: `{phase_info.get('exit', '')}`")
    lines.append(f"Iteration: {state.get('iteration', '?')}/{state.get('max_per_day', '?')}")
    lines.append(f"Status: {state.get('status', '?')}")
    blocker = state.get("blocker")
    lines.append(f"Blocker: {blocker if blocker else 'none'}")
    lines.append("")
    lines.append("## Test Suite")
    lines.append("")
    for tf in discover_tests():
        passes, output = run_test_file(tf)
        lines.append(f"  {'PASS' if passes else 'FAIL'}: {tf}")
        if not passes:
            lines.append(f"    {output[:100]}")
    return "\n".join(lines)


def parse_view_arg(argv: list[str]) -> str | None:
    for index, arg in enumerate(argv):
        if arg.startswith("--view="):
            return arg.split("=", 1)[1]
        if arg == "--view" and len(argv) > index + 1:
            return argv[index + 1]
    return None


def handle_view_mode(view_arg: str | None, state: dict) -> bool:
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
    passes, gate_cmd, output = check_phase_exit(state)
    if not passes:
        print(f"Phase exit FAILS: {gate_cmd}")
        print(f"  {output}")
        sys.exit(1)
    print("Phase exit passes.")
    sys.exit(0)


def validate_blocker(state: dict) -> tuple[bool, str | None]:
    blocker = state.get("blocker")
    if not blocker:
        return False, None
    blocker_lower = blocker.lower()
    files_mentioned = re.findall(r"[\w/\-]+\.py", blocker)
    if "ruff" in blocker_lower:
        targets = " ".join(files_mentioned) if files_mentioned else "."
        passed, _ = run_cmd(f".venv/bin/ruff check {targets}")
        if passed:
            return False, f"ruff now passes on {targets}"
    if "pytest" in blocker_lower or "test" in blocker_lower:
        if files_mentioned:
            test_files = [f for f in files_mentioned if "test_" in f]
            targets = " ".join(test_files) if test_files else "."
        else:
            targets = "."
        passed, _ = run_cmd(f".venv/bin/pytest {targets} -q --tb=short 2>&1")
        if passed:
            return False, f"pytest now passes on {targets}"
    return True, blocker


def compute_fail_criteria_hash(validation: dict | None) -> str | None:
    if validation is None:
        return None
    failed = sorted(c.get("id", "") for c in validation.get("criteria", []) if c.get("status") == "FAIL")
    if not failed:
        return None
    return sha256(",".join(failed).encode()).hexdigest()[:16]


def update_circuit_breaker(state: dict) -> bool:
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
    if cb.get("tripped"):
        return True
    head_sha = get_head_sha()
    validation = load_validation()
    verdict = get_validation_verdict(validation)
    fail_hash = compute_fail_criteria_hash(validation)
    is_stall = (
        cb.get("last_commit") == head_sha and
        cb.get("last_verdict") == verdict and
        cb.get("last_fail_criteria_hash") == fail_hash and
        verdict == "REJECT"
    )
    cb["consecutive_stalls"] = cb.get("consecutive_stalls", 0) + 1 if is_stall else 0
    cb["last_commit"] = head_sha
    cb["last_verdict"] = verdict
    cb["last_fail_criteria_hash"] = fail_hash
    if cb["consecutive_stalls"] >= 3:
        cb["tripped"] = True
        cb["trip_reason"] = f"3 consecutive stalls on commit {head_sha}: verdict={verdict}, fail_criteria={fail_hash}"
        cb["trip_at"] = datetime.now(timezone.utc).astimezone().isoformat()
        print(f"CIRCUIT_BREAKER_TRIPPED: {cb['trip_reason']}", file=sys.stderr)
        return True
    if cb["consecutive_stalls"] > 0:
        print(f"CIRCUIT_BREAKER_WARNING: stall {cb['consecutive_stalls']}/3 on commit {head_sha}", file=sys.stderr)
    return False


def print_sync_report(state: dict, head_sha: str, phase_advanced: bool) -> None:
    for line in report_working_tree():
        print(line)
    phases = state.get("phases", {})
    phase = state.get("phase", "?")
    phase_info = phases.get(str(phase), {})
    print(f"PHASE={phase}")
    print(f"PHASE_NAME={phase_info.get('name', 'unknown')}")
    print(f"COMMIT={head_sha}")
    if phase_advanced:
        print("PHASE_ADVANCED=true")
    validation = load_validation()
    verdict = get_validation_verdict(validation)
    if verdict != "NONE":
        print(f"VALIDATION={verdict}")
        if verdict == "REJECT":
            failed = [c for c in validation.get("criteria", []) if c.get("status") == "FAIL"]
            print(f"VALIDATION_FAIL_COUNT={len(failed)}")
            for c in failed:
                print(f"  FAIL: {c.get('id', '?')} — {c.get('name', '?')}")
                evidence = c.get("evidence", "")
                if evidence:
                    print(f"    {evidence[:200]}")
    else:
        print("VALIDATION=NONE")
    exit_passes, exit_cmd, exit_output = check_phase_exit(state)
    print(f"PHASE_EXIT={'PASS' if exit_passes else 'FAIL'}")
    print(f"PHASE_EXIT_CMD={exit_cmd}")
    if not exit_passes and exit_output:
        print(f"PHASE_EXIT_OUTPUT={exit_output[:200]}")
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


def append_loop_log(state: dict, head_sha: str, phase_advanced: bool, auto_committed: bool) -> None:
    """Append one JSON line to docs/loop-log.jsonl for deterministic tick tracking."""
    validation = load_validation()
    verdict = get_validation_verdict(validation)
    phases = state.get("phases", {})
    phase = str(state.get("phase", "?"))
    phase_info = phases.get(phase, {})
    # Discover commits since last log entry
    last_logged_head = None
    if LOOP_LOG_FILE.exists():
        try:
            last_line = LOOP_LOG_FILE.read_text().strip().split("\n")[-1]
            if last_line:
                last_logged_head = json.loads(last_line).get("head_sha")
        except (json.JSONDecodeError, KeyError, IndexError):
            pass
    new_commits = []
    if last_logged_head:
        log_output = git("log", "--oneline", f"{last_logged_head}..HEAD")
        if log_output:
            new_commits = [line.strip() for line in log_output.split("\n") if line.strip()]
    else:
        log_output = git("log", "--oneline", "-10")
        if log_output:
            new_commits = [line.strip() for line in log_output.split("\n") if line.strip()]
    # Discover files changed in working tree
    files_changed = []
    result = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "diff", "--name-only", "HEAD"],
        capture_output=True, text=True,
    )
    if result.stdout.strip():
        files_changed = [f.strip() for f in result.stdout.strip().split("\n") if f.strip()]
    entry = {
        "ts": datetime.now(timezone.utc).astimezone().isoformat(),
        "iteration": state.get("iteration", 0),
        "phase": int(phase) if phase.isdigit() else phase,
        "phase_name": phase_info.get("name", "unknown"),
        "phase_status": phase_info.get("status", "unknown"),
        "verdict": verdict,
        "head_sha": head_sha,
        "phase_advanced": phase_advanced,
        "auto_committed": auto_committed,
        "new_commits": new_commits[:10],
        "files_changed": files_changed[:20],
        "working_tree_clean": len(files_changed) == 0,
    }
    # Append to log file
    with open(LOOP_LOG_FILE, "a") as f:
        f.write(json.dumps(entry) + "\n")


def auto_commit_if_changed() -> bool:
    pre_sync_head = get_head_sha()
    validation = load_validation() or {}
    validation["last_reviewed_commit"] = pre_sync_head
    save_validation(validation)
    changed = False
    for state_file in [STATE_FILE, VALIDATION_FILE]:
        if not state_file.exists():
            continue
        rel = str(state_file.relative_to(REPO_ROOT))
        result = subprocess.run(["git", "-C", str(REPO_ROOT), "diff", "--quiet", rel], capture_output=True, text=True)
        if result.returncode != 0:
            subprocess.run(["git", "-C", str(REPO_ROOT), "add", rel], capture_output=True, text=True)
            changed = True
    # Always stage the loop log if it exists (new or updated)
    if LOOP_LOG_FILE.exists():
        rel = str(LOOP_LOG_FILE.relative_to(REPO_ROOT))
        subprocess.run(["git", "-C", str(REPO_ROOT), "add", rel], capture_output=True, text=True)
        result = subprocess.run(["git", "-C", str(REPO_ROOT), "diff", "--cached", "--quiet", rel], capture_output=True, text=True)
        if result.returncode != 0:
            changed = True
    if changed:
        subprocess.run(["git", "-C", str(REPO_ROOT), "commit", "-m", "orchestrator: sync state after PASS"], capture_output=True, text=True)
        print("AUTO_COMMIT=true")
        return True
    print("AUTO_COMMIT=false")
    return False


def main() -> None:
    argv = sys.argv[1:]
    args = set(argv)
    check_only = "--check-only" in args
    write_only = "--write" in args
    auto_commit = "--auto-commit" in args
    view_arg = parse_view_arg(argv)
    state = load_state()
    still_valid, reason = validate_blocker(state)
    if not still_valid and state.get("blocker"):
        print(f"BLOCKER_CLEARED reason=\"{reason}\"", file=sys.stderr)
        state["blocker"] = None
        save_state(state)
    if handle_view_mode(view_arg, state):
        return
    if write_only:
        advance_phase_if_done(state)
        save_state(state)
        return
    head_sha = get_head_sha()
    if check_only:
        handle_check_only(state)
    tripped = update_circuit_breaker(state)
    if tripped:
        state["status"] = "circuit_breaked"
        save_state(state)
        cb = state.get("circuit_breaker", {})
        print("CIRCUIT_BREAKER=TRIPPED")
        print(f"CIRCUIT_BREAKER_REASON={cb.get('trip_reason', '')}")
        print(f"CIRCUIT_BREAKER_AT={cb.get('trip_at', '')}")
        sys.exit(0)
    cb = state.get("circuit_breaker", {})
    stalls = cb.get("consecutive_stalls", 0)
    if stalls > 0:
        print(f"CIRCUIT_BREAKER={stalls}/3 stalls", file=sys.stderr)
    reverted = revert_phase_on_reject(state)
    if reverted:
        save_state(state)
        print("PHASE_REVERTED=true (validation REJECT)", file=sys.stderr)
    phase_advanced = advance_phase_if_done(state)
    state["iteration"] = state.get("iteration", 0) + 1
    state["status"] = "running"
    save_state(state)
    print_sync_report(state, head_sha, phase_advanced)
    # Fix B: FP anomaly handling
    # If phase exit fails but validation says PASS, the PASS is stale
    # (from a previous phase). Reset verdict to PENDING and do NOT commit.
    validation = load_validation()
    verdict = get_validation_verdict(validation)
    exit_passes, exit_cmd, exit_output = check_phase_exit(state)
    if verdict == "PASS" and not exit_passes:
        print("VALIDATION_STATE=FP", file=sys.stderr)
        print("ACTION=RESET_STALE_PASS", file=sys.stderr)
        validation["verdict"] = "PENDING"
        validation["criteria"] = []
        validation["summary"] = f"Phase {state.get('phase', '?')} activated; stale PASS reset. Awaiting Lisa review."
        validation["reviewed_at"] = datetime.now(timezone.utc).astimezone().isoformat()
        save_validation(validation)
        # Stage the reset validation file but do NOT commit
        subprocess.run(["git", "-C", str(REPO_ROOT), "add", str(VALIDATION_FILE.relative_to(REPO_ROOT))], capture_output=True, text=True)
        subprocess.run(["git", "-C", str(REPO_ROOT), "commit", "-m", "orchestrator: reset stale PASS to PENDING (FP state)"], capture_output=True, text=True)
        print("AUTO_COMMIT=false")
        print("NEXT_ACTION=RUN_RALPH")
        return
    if auto_commit:
        committed = auto_commit_if_changed()
        append_loop_log(state, head_sha, phase_advanced, committed)
    else:
        append_loop_log(state, head_sha, phase_advanced, False)


if __name__ == "__main__":
    main()
