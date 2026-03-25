#!/usr/bin/env python3
"""sync_state.py — Ralph loop state synchronizer (spec-driven).

MissHoover V2: Data-Centric Determinism + OpenLineage
- Calls pre_flight.py before phase exit gate (structural lint)
- Calls validate_artifacts.py after pytest but before Lisa (Hard Gate)
- Emits OpenLineage events for data provenance tracking
- Generates STALL_REPORT.md on circuit breaker trip
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path

# Import OpenLineage client
try:
    from scripts.utils.lineage_client import LineageClient
    LINEAGE_AVAILABLE = True
except ImportError:
    LINEAGE_AVAILABLE = False
    LineageClient = None

REPO_ROOT = Path(__file__).resolve().parent.parent
STATE_FILE = REPO_ROOT / "docs" / "ralph-state.json"
TESTS_DIR = REPO_ROOT / "tests"
VALIDATION_FILE = REPO_ROOT / "docs" / "validation.json"
LOOP_LOG_FILE = REPO_ROOT / "docs" / "loop-log.jsonl"
ARTIFACT_VALIDATION_FILE = REPO_ROOT / "docs" / "artifact-validation.json"
PRE_FLIGHT_FILE = REPO_ROOT / "docs" / "pre-flight.json"
LINEAGE_FILE = REPO_ROOT / "docs" / "lineage.jsonl"
STALL_REPORT_FILE = REPO_ROOT / "docs" / "STALL_REPORT.md"

# Global lineage client (initialized lazily)
_lineage_client = None


def get_lineage_client() -> LineageClient | None:
    """Get or create the lineage client."""
    global _lineage_client
    if not LINEAGE_AVAILABLE:
        return None
    if _lineage_client is None:
        _lineage_client = LineageClient()
    return _lineage_client


def run_artifact_validation() -> tuple[bool, dict]:
    """Run validate_artifacts.py and return (passes, result_dict).
    
    This is the Hard Gate - artifact validation runs BEFORE Lisa review.
    If artifacts fail validation, Lisa is skipped and verdict is REJECT.
    """
    script_path = REPO_ROOT / "scripts" / "validate_artifacts.py"
    if not script_path.exists():
        print("ARTIFACT_VALIDATION=SKIP (script not found)", file=sys.stderr)
        return True, {"verdict": "SKIP", "reason": "script not found"}
    
    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            cwd=str(REPO_ROOT),
            timeout=60,
        )
        
        # Print output for visibility
        if result.stdout:
            for line in result.stdout.strip().split("\n"):
                print(f"  {line}")
        
        # Load detailed result
        artifact_result = {}
        if ARTIFACT_VALIDATION_FILE.exists():
            try:
                artifact_result = json.loads(ARTIFACT_VALIDATION_FILE.read_text())
            except json.JSONDecodeError:
                pass
        
        passes = result.returncode == 0
        return passes, artifact_result
        
    except subprocess.TimeoutExpired:
        print("ARTIFACT_VALIDATION=TIMEOUT", file=sys.stderr)
        return False, {"verdict": "TIMEOUT", "reason": "validation timed out"}
    except Exception as e:
        print(f"ARTIFACT_VALIDATION=ERROR: {e}", file=sys.stderr)
        return False, {"verdict": "ERROR", "reason": str(e)}


def run_pre_flight() -> tuple[bool, dict]:
    """Run pre_flight.py and return (passes, result_dict).
    
    This is the Structural Lint - runs BEFORE phase exit gate.
    Verifies required classes/functions exist in src/.
    """
    script_path = REPO_ROOT / "scripts" / "pre_flight.py"
    if not script_path.exists():
        print("PRE_FLIGHT=SKIP (script not found)", file=sys.stderr)
        return True, {"verdict": "SKIP", "reason": "script not found"}
    
    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            cwd=str(REPO_ROOT),
            timeout=60,
        )
        
        # Print output for visibility
        if result.stdout:
            for line in result.stdout.strip().split("\n"):
                print(f"  {line}")
        
        # Load detailed result
        pre_flight_result = {}
        if PRE_FLIGHT_FILE.exists():
            try:
                pre_flight_result = json.loads(PRE_FLIGHT_FILE.read_text())
            except json.JSONDecodeError:
                pass
        
        passes = result.returncode == 0
        return passes, pre_flight_result
        
    except subprocess.TimeoutExpired:
        print("PRE_FLIGHT=TIMEOUT", file=sys.stderr)
        return False, {"verdict": "TIMEOUT", "reason": "pre-flight timed out"}
    except Exception as e:
        print(f"PRE_FLIGHT=ERROR: {e}", file=sys.stderr)
        return False, {"verdict": "ERROR", "reason": str(e)}


def generate_stall_report(state: dict, validation: dict | None) -> None:
    """Generate STALL_REPORT.md when circuit breaker trips.
    
    Concatenates last 3 test failures and Lisa's last 3 REJECT summaries
    to provide a clear hand-off for human intervention.
    """
    cb = state.get("circuit_breaker", {})
    
    lines = [
        "# ⚠️ STALL REPORT — Circuit Breaker Tripped",
        "",
        f"**Generated:** {datetime.now(timezone.utc).astimezone().isoformat()}",
        f"**Commit:** {cb.get('last_commit', 'unknown')}",
        f"**Trip Reason:** {cb.get('trip_reason', 'unknown')}",
        f"**Trip Time:** {cb.get('trip_at', 'unknown')}",
        "",
        "---",
        "",
        "## Recent Loop History",
        "",
    ]
    
    # Read last 3 entries from loop-log.jsonl
    if LOOP_LOG_FILE.exists():
        try:
            log_lines = LOOP_LOG_FILE.read_text().strip().split("\n")[-3:]
            lines.append("| Timestamp | Phase | Verdict | Commits | Files Changed |")
            lines.append("|---|---|---|---|---|")
            for line in reversed(log_lines):
                try:
                    entry = json.loads(line)
                    lines.append(
                        f"| {entry.get('ts', '?')[:19]} | "
                        f"{entry.get('phase', '?')} | "
                        f"{entry.get('verdict', '?')} | "
                        f"{len(entry.get('new_commits', []))} | "
                        f"{len(entry.get('files_changed', []))} |"
                    )
                except json.JSONDecodeError:
                    continue
            lines.append("")
        except Exception as e:
            lines.append(f"*Could not read loop log: {e}*\n")
    
    # Add validation failures
    if validation and validation.get("criteria"):
        lines.extend([
            "## Last Review Criteria (FAIL)",
            "",
        ])
        for c in validation.get("criteria", []):
            if c.get("status") == "FAIL":
                lines.append(f"### {c.get('id', '?')}: {c.get('name', '?')}")
                lines.append(f"- **Status:** {c.get('status', '?')}")
                evidence = c.get("evidence", "")
                if evidence:
                    lines.append(f"- **Evidence:** {evidence}")
                lines.append("")
    
    # Add artifact validation failures
    if ARTIFACT_VALIDATION_FILE.exists():
        try:
            artifact = json.loads(ARTIFACT_VALIDATION_FILE.read_text())
            if artifact.get("errors"):
                lines.extend([
                    "## Artifact Validation Errors",
                    "",
                ])
                for err in artifact.get("errors", []):
                    lines.append(f"- {err}")
                lines.append("")
        except json.JSONDecodeError:
            pass
    
    # Add current phase info
    phases = state.get("phases", {})
    phase = str(state.get("phase", "?"))
    phase_info = phases.get(phase, {})
    
    lines.extend([
        "## Current Phase Details",
        "",
        f"- **Phase:** {phase} — {phase_info.get('name', 'unknown')}",
        f"- **Status:** {phase_info.get('status', '?')}",
        f"- **Exit Command:** `{phase_info.get('exit', 'not defined')}`",
        f"- **Iteration:** {state.get('iteration', '?')}",
        "",
        "---",
        "",
        "## Recommended Actions",
        "",
        "1. **Review the errors above** — identify if this is a code issue, data issue, or spec ambiguity.",
        "2. **Check the test suite** — run `.venv/bin/pytest tests/ -v` to see full failure details.",
        "3. **Inspect data artifacts** — check `data/processed/` for missing or malformed outputs.",
        "4. **Update specs or code** — fix the root cause, not just the symptom.",
        "5. **Reset the circuit breaker** — set `circuit_breaker.tripped = false` in `docs/ralph-state.json`.",
        "",
        "### To Reset and Resume",
        "",
        "```bash",
        "# After fixing the issue, reset the circuit breaker:",
        "python scripts/sync_state.py  # verify state is clean",
        "# Edit docs/ralph-state.json: set circuit_breaker.tripped = false",
        "git add docs/ralph-state.json && git commit -m 'fix: reset circuit breaker after stall'",
        "```",
        "",
        "---",
        "*This report was auto-generated by sync_state.py (MissHoover 2.0)*",
    ])
    
    STALL_REPORT_FILE.write_text("\n".join(lines))
    print(f"STALL_REPORT_GENERATED={STALL_REPORT_FILE}", file=sys.stderr)


class StateManager:
    """Encapsulates state IO to prevent global variable mutation and race conditions."""
    def __init__(self):
        self.phases_was_list = False

    def load_state(self) -> dict:
        if not STATE_FILE.exists():
            print("ralph-state.json not found", file=sys.stderr)
            sys.exit(1)

        with open(STATE_FILE) as f:
            raw = json.load(f)

        self.phases_was_list = isinstance(raw.get("phases"), list)
        return self._normalize_state(raw)

    def save_state(self, state: dict) -> None:
        state["updated_at"] = datetime.now(timezone.utc).astimezone().isoformat()
        out = self._denormalize_state(state)
        self._atomic_write(STATE_FILE, out)

    def load_validation(self) -> dict | None:
        if VALIDATION_FILE.exists():
            with open(VALIDATION_FILE) as f:
                return json.load(f)
        return None

    def save_validation(self, validation: dict) -> None:
        self._atomic_write(VALIDATION_FILE, validation)

    def _normalize_state(self, state: dict) -> dict:
        phases = state.get("phases")
        if isinstance(phases, list):
            phase_map = {}
            for p in phases:
                key = str(p["id"])
                # FIX 3: Use truthiness check to catch empty string, not just key absence
                if "exit_gate" in p and not p.get("exit"):
                    p["exit"] = p["exit_gate"]
                phase_map[key] = p
            state["phases"] = phase_map

        if "phase" not in state and "current_phase" in state:
            state["phase"] = str(state["current_phase"])
        elif "phase" in state:
            state["phase"] = str(state["phase"])
        return state

    def _denormalize_state(self, state: dict) -> dict:
        out = dict(state)
        if self.phases_was_list and isinstance(out.get("phases"), dict):
            out["phases"] = sorted(out["phases"].values(), key=lambda p: p["id"])
            try:
                out["current_phase"] = int(out.get("phase", out.get("current_phase", 1)))
            except (ValueError, TypeError):
                pass
        return out

    @staticmethod
    def _atomic_write(filepath: Path, data: dict) -> None:
        # FIX 2: Avoid with_suffix("..tmp") which raises ValueError on Python ≥ 3.13
        tmp = filepath.parent / (filepath.name + ".tmp")
        tmp.write_text(json.dumps(data, indent=2) + "\n")
        tmp.replace(filepath)


def git(*args: str, check: bool = True, capture: bool = True) -> subprocess.CompletedProcess:
    """Wrapper for git commands with safe default error checking."""
    return subprocess.run(
        ["git", "-C", str(REPO_ROOT), *args],
        capture_output=capture,
        text=True,
        check=check
    )


def get_head_sha() -> str:
    try:
        result = git("rev-parse", "--short", "HEAD")
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"git error: {e.stderr.strip()}", file=sys.stderr)
        sys.exit(1)


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


def discover_tests() -> list[str]:
    if not TESTS_DIR.exists():
        return []
    return sorted(str(p.relative_to(REPO_ROOT)) for p in TESTS_DIR.glob("test_*.py"))


def run_test_file(test_file: str) -> tuple[bool, str]:
    cmd = f"{sys.executable} -m pytest {test_file} -q --tb=short 2>&1"
    return run_cmd(cmd, timeout=120)


def _ensure_portable_execution(cmd: str) -> str:
    """Ensure standard tools execute within the current Python environment."""
    for tool in ("pytest", "ruff"):
        if cmd.startswith(tool + " ") or cmd == tool:
            return cmd.replace(tool, f"{sys.executable} -m {tool}", 1)
    return cmd


def check_phase_exit(state: dict) -> tuple[bool, str, str]:
    phases = state.get("phases", {})
    phase = str(state.get("phase", "?"))
    phase_info = phases.get(phase, {})
    exit_gate = phase_info.get("exit", "")

    if not exit_gate:
        return True, "(no exit gate defined)", ""

    exit_gate = _ensure_portable_execution(exit_gate)
    passes, output = run_cmd(exit_gate, timeout=120)
    return passes, exit_gate, output


def get_validation_verdict(validation: dict | None) -> str:
    if validation is None:
        return "NONE"
    return validation.get("verdict", "NONE").upper()


def revert_phase_on_reject(state: dict, verdict: str) -> bool:
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
    m = re.match(r"^(\d+)(.*)", key)
    if m:
        return (int(m.group(1)), m.group(2))
    return (999, key)


def advance_phase_if_done(state: dict, validation: dict | None, verdict: str, exit_passes: bool, exit_cmd: str) -> bool:
    phases = state.get("phases", {})
    current = str(state.get("phase", "?"))
    phase_info = phases.get(current, {})

    if not exit_passes:
        if verdict == "PASS":
            print("ANOMALY: FP state — exit fails but validation PASS", file=sys.stderr)
            print(f"  Phase exit: {exit_cmd}", file=sys.stderr)
        return False

    if verdict == "REJECT":
        if phase_info.get("status") == "done":
            phase_info["status"] = "active"
        return False

    # CORE BUG FIX: Only advance on explicit PASS, not NONE
    if verdict == "PASS":
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
            return True
        return True

    return False


def report_working_tree() -> list[str]:
    result = git("status", "--porcelain", check=False)
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
    lines.append(f"Exit criteria: `{phase_info.get('exit', '')}`\n")
    lines.append("## Phase Roadmap\n")
    lines.append("| Phase | Name | Status | Exit Criteria |")
    lines.append("|---|---|---|---|")

    for p_num, p_info in phases.items():
        lines.append(f"| {p_num} | {p_info.get('name', '?')} | {p_info.get('status', '?')} | `{p_info.get('exit', '')}` |")
    lines.append("")

    decisions = state.get("decisions", [])
    if decisions:
        lines.append("## Decisions\n")
        for d in decisions:
            lines.append(f"- {d.get('summary', '?')} ({d.get('date', '?')})")
        lines.append("")

    blocker = state.get("blocker")
    if blocker:
        lines.append("## Blocker\n")
        lines.append(f"**{blocker}**\n")
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
    lines.append(f"Blocker: {blocker if blocker else 'none'}\n")
    lines.append("## Test Suite\n")

    for tf in discover_tests():
        passes, output = run_test_file(tf)
        lines.append(f"  {'PASS' if passes else 'FAIL'}: {tf}")
        if not passes:
            lines.append(f"    {output[:100]}")
    return "\n".join(lines)


def parse_view_arg(argv: list[str]) -> str | None:
    for index, arg in enumerate(argv):
        if arg.startswith("--view="):
            return arg.split("=", 1)[1]  # FIX 1: extract the value, not the list
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


# FIX 3: Accept precomputed results instead of re-running check_phase_exit
def handle_check_only(exit_passes: bool, exit_cmd: str, exit_output: str) -> None:
    if not exit_passes:
        print(f"Phase exit FAILS: {exit_cmd}")
        print(f"  {exit_output}")
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
        passed, _ = run_cmd(f"{sys.executable} -m ruff check {targets}")
        if passed:
            return False, f"ruff now passes on {targets}"

    if "pytest" in blocker_lower or "test" in blocker_lower:
        if files_mentioned:
            test_files = [f for f in files_mentioned if "test_" in f]
            targets = " ".join(test_files) if test_files else "."
        else:
            targets = "."
        passed, _ = run_cmd(f"{sys.executable} -m pytest {targets} -q --tb=short 2>&1")
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


def update_circuit_breaker(state: dict, head_sha: str, validation: dict | None, verdict: str) -> bool:
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


def print_sync_report(state: dict, head_sha: str, phase_advanced: bool, validation: dict | None, verdict: str, exit_passes: bool, exit_cmd: str, exit_output: str) -> None:
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

    if verdict != "NONE":
        print(f"VALIDATION={verdict}")
        if verdict == "REJECT" and validation:
            failed = [c for c in validation.get("criteria", []) if c.get("status") == "FAIL"]
            print(f"VALIDATION_FAIL_COUNT={len(failed)}")
            for c in failed:
                print(f"  FAIL: {c.get('id', '?')} — {c.get('name', '?')}")
                evidence = c.get("evidence", "")
                if evidence:
                    print(f"    {evidence[:200]}")
    else:
        print("VALIDATION=NONE")

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

    print("\n## Test Discovery")
    test_files = discover_tests()
    if not test_files:
        print("  No test files found in tests/")
    else:
        for tf in test_files:
            print(f"  {tf}")

    blocker = state.get("blocker")
    if blocker:
        print(f"BLOCKER={blocker}")


def append_loop_log(state: dict, head_sha: str, phase_advanced: bool, auto_committed: bool, verdict: str) -> None:
    phases = state.get("phases", {})
    phase = str(state.get("phase", "?"))
    phase_info = phases.get(phase, {})

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
        try:
            log_output = git("log", "--oneline", f"{last_logged_head}..HEAD").stdout
            if log_output:
                new_commits = [line.strip() for line in log_output.split("\n") if line.strip()]
        except subprocess.CalledProcessError:
            pass
    else:
        log_output = git("log", "--oneline", "-10", check=False).stdout
        if log_output:
            new_commits = [line.strip() for line in log_output.split("\n") if line.strip()]

    files_changed = []
    diff_result = git("diff", "--name-only", "HEAD", check=False).stdout
    if diff_result.strip():
        files_changed = [f.strip() for f in diff_result.strip().split("\n") if f.strip()]

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

    with open(LOOP_LOG_FILE, "a") as f:
        f.write(json.dumps(entry) + "\n")


# Pure git operations — no validation mutation or file writes
def auto_commit_if_changed() -> bool:
    changed = False

    for state_file in [STATE_FILE, VALIDATION_FILE]:
        if not state_file.exists():
            continue
        rel = str(state_file.relative_to(REPO_ROOT))
        # CAVEAT 1: check=False for resilience on non-critical git operations
        if git("diff", "--quiet", rel, check=False).returncode != 0:
            git("add", rel, check=False)
            changed = True

    if LOOP_LOG_FILE.exists():
        rel = str(LOOP_LOG_FILE.relative_to(REPO_ROOT))
        git("add", rel, check=False)
        if git("diff", "--cached", "--quiet", rel, check=False).returncode != 0:
            changed = True

    if changed:
        git("commit", "-m", "orchestrator: sync state after PASS", check=False)
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

    state_mgr = StateManager()
    state = state_mgr.load_state()

    still_valid, reason = validate_blocker(state)
    if not still_valid and state.get("blocker"):
        print(f"BLOCKER_CLEARED reason=\"{reason}\"", file=sys.stderr)
        state["blocker"] = None
        state_mgr.save_state(state)

    # Emit START lineage event
    lineage_client = get_lineage_client()
    if lineage_client:
        phase = state.get("phase", "?")
        lineage_client.emit_start(
            f"phase-{phase}-sync",
            description=f"MissHoover V2 sync for phase {phase}",
        )

    if handle_view_mode(view_arg, state):
        return

    # Centralize reads to prevent race conditions during the execution loop
    validation = state_mgr.load_validation()
    verdict = get_validation_verdict(validation)
    exit_passes, exit_cmd, exit_output = check_phase_exit(state)
    head_sha = get_head_sha()

    if write_only:
        phase_advanced = advance_phase_if_done(state, validation, verdict, exit_passes, exit_cmd)
        state_mgr.save_state(state)
        # CAVEAT 2: Only save validation if phase advanced and validation exists
        if phase_advanced and validation is not None:
            validation["last_reviewed_commit"] = head_sha
            state_mgr.save_validation(validation)
        return

    if check_only:
        handle_check_only(exit_passes, exit_cmd, exit_output)  # FIX 3
        return                                                  # FIX 5

    # 1. FP Anomaly Handling (Stale PASS Reset)
    if verdict == "PASS" and not exit_passes:
        print("VALIDATION_STATE=FP", file=sys.stderr)
        print("ACTION=RESET_STALE_PASS", file=sys.stderr)
        if validation is not None:
            validation["verdict"] = "PENDING"
            validation["criteria"] = []
            validation["summary"] = (
                f"Phase {state.get('phase', '?')} activated; "
                "stale PASS reset. Awaiting Lisa review."
            )
            validation["reviewed_at"] = datetime.now(timezone.utc).astimezone().isoformat()
            state_mgr.save_validation(validation)

        git("add", str(VALIDATION_FILE.relative_to(REPO_ROOT)), check=False)
        print("AUTO_COMMIT=false")
        print("NEXT_ACTION=RUN_RALPH")
        return

    # 1.5. MissHoover 2.0: Hard Gate — Artifact Validation
    # Run BEFORE Lisa review. If artifacts fail, skip Lisa and REJECT immediately.
    artifact_passes, artifact_result = run_artifact_validation()
    if not artifact_passes:
        print("HARD_GATE=ARTIFACT_VALIDATION_FAIL", file=sys.stderr)
        print("ACTION=SKIP_LISA_AUTO_REJECT", file=sys.stderr)
        # Auto-set validation to REJECT with artifact errors
        if validation is None:
            validation = {}
        validation["verdict"] = "REJECT"
        validation["last_reviewed_commit"] = head_sha
        validation["reviewed_at"] = datetime.now(timezone.utc).astimezone().isoformat()
        artifact_summary = artifact_result.get("summary", "Unknown artifact validation failure")
        validation["summary"] = f"Artifact validation failed: {artifact_summary}"
        validation["criteria"] = [
            {
                "id": f"ARTIFACT-{i}",
                "name": (err[:50] + "...") if len(err) > 50 else err,
                "status": "FAIL",
                "evidence": err,
            }
            for i, err in enumerate(artifact_result.get("errors", ["Unknown artifact error"]), 1)
        ]
        state_mgr.save_validation(validation)
        verdict = "REJECT"  # Update local verdict for subsequent logic
        
        # Emit FAIL lineage event
        lineage_client = get_lineage_client()
        if lineage_client:
            lineage_client.emit_fail(
                f"phase-{state.get('phase', '?')}-pipeline",
                error=artifact_summary,
                failing_nodes=[
                    {"file": c.get("path", "unknown"), "error": c.get("message", "")}
                    for c in artifact_result.get("checks", [])
                    if c.get("status") == "FAIL"
                ],
            )

    # 1.6. MissHoover V2: Pre-Flight Structural Lint
    # Run BEFORE phase exit gate. If structural requirements fail, REJECT.
    pre_flight_passes, pre_flight_result = run_pre_flight()
    if not pre_flight_passes:
        print("HARD_GATE=PRE_FLIGHT_FAIL", file=sys.stderr)
        print("ACTION=SKIP_PHASE_EXIT_AUTO_REJECT", file=sys.stderr)
        # Auto-set validation to REJECT with structural errors
        if validation is None:
            validation = {}
        validation["verdict"] = "REJECT"
        validation["last_reviewed_commit"] = head_sha
        validation["reviewed_at"] = datetime.now(timezone.utc).astimezone().isoformat()
        pre_flight_summary = pre_flight_result.get("summary", "Unknown pre-flight failure")
        validation["summary"] = f"Pre-flight structural lint failed: {pre_flight_summary}"
        validation["criteria"] = [
            {
                "id": f"STRUCT-{m.get('type', '?').upper()}-{i}",
                "name": f"Missing {m.get('type', 'requirement')}: {m.get('name', '?')}",
                "status": "FAIL",
                "evidence": f"Expected in {m.get('file_pattern', '?')}",
            }
            for i, m in enumerate(pre_flight_result.get("missing", []), 1)
        ]
        state_mgr.save_validation(validation)
        verdict = "REJECT"  # Update local verdict for subsequent logic
        
        # Emit FAIL lineage event
        lineage_client = get_lineage_client()
        if lineage_client:
            lineage_client.emit_fail(
                f"phase-{state.get('phase', '?')}-pipeline",
                error=pre_flight_summary,
                failing_nodes=[
                    {"type": m.get("type"), "name": m.get("name"), "file_pattern": m.get("file_pattern")}
                    for m in pre_flight_result.get("missing", [])
                ],
            )

    # 2. Pipeline Execution
    tripped = update_circuit_breaker(state, head_sha, validation, verdict)
    if tripped:
        state["status"] = "circuit_breaked"
        state_mgr.save_state(state)
        cb = state.get("circuit_breaker", {})
        print("CIRCUIT_BREAKER=TRIPPED")
        print(f"CIRCUIT_BREAKER_REASON={cb.get('trip_reason', '')}")
        print(f"CIRCUIT_BREAKER_AT={cb.get('trip_at', '')}")
        # MissHoover 2.0: Generate stall report for human hand-off
        generate_stall_report(state, validation)
        sys.exit(0)

    cb = state.get("circuit_breaker", {})
    stalls = cb.get("consecutive_stalls", 0)
    if stalls > 0:
        print(f"CIRCUIT_BREAKER={stalls}/3 stalls", file=sys.stderr)

    reverted = revert_phase_on_reject(state, verdict)
    if reverted:
        print("PHASE_REVERTED=true (validation REJECT)", file=sys.stderr)

    phase_advanced = advance_phase_if_done(state, validation, verdict, exit_passes, exit_cmd)

    state["iteration"] = state.get("iteration", 0) + 1
    state["status"] = "running"

    # 3. Single IO write point — all mutations complete
    state_mgr.save_state(state)

    # CAVEAT 2: Only save validation when phase advanced OR auto_commit is set
    if phase_advanced and validation is not None:
        validation["last_reviewed_commit"] = head_sha
        state_mgr.save_validation(validation)
    elif auto_commit and validation is not None:
        validation["last_reviewed_commit"] = head_sha
        state_mgr.save_validation(validation)

    # 4. Reporting
    print_sync_report(
        state, head_sha, phase_advanced, validation,
        verdict, exit_passes, exit_cmd, exit_output,
    )

    # 5. Git Logistics
    committed = auto_commit_if_changed() if auto_commit else False
    append_loop_log(state, head_sha, phase_advanced, committed, verdict)

    # 6. Emit COMPLETE lineage event
    lineage_client = get_lineage_client()
    if lineage_client:
        phase = state.get("phase", "?")
        lineage_client.emit_complete(
            f"phase-{phase}-sync",
            outputs=[
                {"name": str(STATE_FILE.relative_to(REPO_ROOT))},
                {"name": str(VALIDATION_FILE.relative_to(REPO_ROOT))},
                {"name": str(LOOP_LOG_FILE.relative_to(REPO_ROOT))},
            ]
        )


if __name__ == "__main__":
    main()
