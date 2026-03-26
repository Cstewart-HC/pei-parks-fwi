#!/usr/bin/env python3
"""martin-lint.py — Deterministic test quality linter for Martin.

Reads docs/martin-rules.*.json (latest by CalVer filename) and scans test files.
Outputs structured JSON for the back-pressure gate (same pattern as Lisa's validation.json).

Usage:
    python scripts/martin-lint.py tests/                    # lint all tests
    python scripts/martin-lint.py tests/ -o docs/martin-lint.json  # custom output
    python scripts/martin-lint.py tests/ --json             # stdout as JSON
    python scripts/martin-lint.py tests/ --category safety  # only safety rules
    python scripts/martin-lint.py tests/ --severity critical,high  # only critical+high

Exit codes:
  0 = PASS (no violations)
  1 = FAIL (violations found)
  2 = ERROR (script error)
"""
from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
RULES_DIR = REPO_ROOT / "docs"
DEFAULT_OUTPUT = REPO_ROOT / "docs" / "martin-lint.json"

SEVERITY_ORDER = {"critical": 0, "high": 1, "medium": 2, "low": 3}


# --- CalVer rules loading ---

def find_latest_rules(rules_dir: Path) -> Path:
    """Find the latest martin-rules.*.json by CalVer filename prefix."""
    pattern = "martin-rules."
    candidates = sorted(
        (f for f in rules_dir.iterdir() if f.name.startswith(pattern) and f.suffix == ".json"),
        key=lambda f: f.name,
        reverse=True,
    )
    if not candidates:
        print(f"ERROR: No {pattern}*.json found in {rules_dir}", file=sys.stderr)
        sys.exit(2)
    return candidates[0]


def load_rules(rules_path: Path) -> dict:
    """Load and validate the rules config."""
    try:
        data = json.loads(rules_path.read_text())
    except (json.JSONDecodeError, OSError) as e:
        print(f"ERROR: Cannot read rules: {e}", file=sys.stderr)
        sys.exit(2)

    if "anti_patterns" not in data:
        print("ERROR: Rules file missing 'anti_patterns' key", file=sys.stderr)
        sys.exit(2)

    return data


# --- AST-based extractors ---

def extract_test_functions(source: str) -> list[dict]:
    """Extract test function defs with line ranges, markers, and metrics using AST."""
    tree = ast.parse(source)
    tests = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name.startswith("test_"):
            markers = set()
            for dec in node.decorator_list:
                if isinstance(dec, ast.Attribute) and dec.attr:
                    markers.add(dec.attr)
                elif isinstance(dec, ast.Name) and dec.id:
                    markers.add(dec.id)
                elif isinstance(dec, ast.Call):
                    if isinstance(dec.func, ast.Attribute):
                        markers.add(dec.func.attr)
                    elif isinstance(dec.func, ast.Name):
                        markers.add(dec.func.id)

            # Count branches (if/for/while/try/with) for complexity check
            branch_count = 0
            for child in ast.walk(node):
                if isinstance(child, (ast.If, ast.For, ast.While, ast.Try)):
                    branch_count += 1

            end_lineno = getattr(node, "end_lineno", node.lineno)
            line_count = end_lineno - node.lineno + 1

            # Extract parameter names (for fixture detection)
            param_names = {arg.arg for arg in node.args.args}

            tests.append({
                "name": node.name,
                "lineno": node.lineno,
                "end_lineno": end_lineno,
                "line_count": line_count,
                "branch_count": branch_count,
                "markers": markers,
                "params": param_names,
                "source_lines": source.splitlines()[node.lineno - 1 : end_lineno],
            })
    return tests


def extract_test_classes(source: str) -> list[dict]:
    """Extract test class defs to check naming convention."""
    tree = ast.parse(source)
    classes = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            classes.append({
                "name": node.name,
                "lineno": node.lineno,
                "has_test_prefix": node.name.startswith("Test"),
            })
    return classes


def extract_subprocess_calls(source: str) -> list[dict]:
    """Find subprocess.* calls and check for timeout kwarg."""
    tree = ast.parse(source)
    calls = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Attribute) and func.attr in (
                "run", "call", "Popen", "check_output", "check_call"
            ):
                is_subprocess = False
                if isinstance(func.value, ast.Name) and func.value.id == "subprocess":
                    is_subprocess = True
                elif isinstance(func.value, ast.Attribute) and func.value.attr == "run":
                    is_subprocess = True

                if is_subprocess:
                    has_timeout = any(
                        (isinstance(kw.arg, str) and kw.arg == "timeout")
                        for kw in node.keywords
                        if kw.arg
                    )
                    calls.append({
                        "lineno": node.lineno,
                        "func": f"subprocess.{func.attr}",
                        "has_timeout": has_timeout,
                    })
    return calls


def extract_unused_imports(source: str, source_lines: list[str]) -> list[tuple[int, str]]:
    """Find imports that appear unused (basic heuristic)."""
    tree = ast.parse(source)
    import_names = set()
    used_names = set()

    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                import_names.add(alias.asname or alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if alias.name != "*":
                    import_names.add(alias.asname or alias.name.split(".")[0])

    # Walk all names used in the file
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            used_names.add(node.id)

    unused = []
    for i, line in enumerate(source_lines, 1):
        stripped = line.strip()
        if stripped.startswith(("import ", "from ")):
            # Extract imported names from this line
            for name in import_names:
                if re.search(rf"\b{name}\b", line):
                    if name not in used_names:
                        unused.append((i, stripped))
    return unused


# --- Line-based scanner ---

def scan_lines(source: str, pattern: str) -> list[tuple[int, str]]:
    """Scan source lines for a regex pattern. Returns [(lineno, line_text)]."""
    regex = re.compile(pattern)
    results = []
    for i, line in enumerate(source.splitlines(), 1):
        if regex.search(line):
            results.append((i, line.strip()))
    return results


# --- Generic rule checkers ---

def check_pattern_in_test_function(tests: list[dict], rule: dict) -> list[dict]:
    """Generic: scan test function bodies for a regex pattern.
    Used by: AP001 (ouroboros), AP002 (unmarked_subprocess), AP003 (unmarked_notebook),
             AP008 (unmarked_network_call), AP009 (unmarked_database_access),
             AP010 (sleep_in_test), AP011 (hardcoded_path), AP013 (broad_except),
             AP017 (unmarked_disk_write), AP018 (environment_mutation),
             AP019 (non_tmp_path_file_creation), AP023 (print_in_test),
             AP024 (hardcoded_timestamp), AP025 (unmarked_io_operation),
             AP030 (test_depends_on_cwd)
    """
    violations = []
    detect = rule.get("detect")
    if not detect:
        return violations

    requires_marker = rule.get("requires_marker")

    for test in tests:
        for line_no, line_text in scan_lines("\n".join(test["source_lines"]), detect):
            if requires_marker and requires_marker in test["markers"]:
                continue  # already marked correctly
            violations.append({
                "rule_id": rule["id"],
                "severity": rule["severity"],
                "file": None,
                "line": test["lineno"] + (line_no - 1),
                "test": test["name"],
                "message": f"[{rule['name']}] {line_text[:100]}",
                "auto_fix": rule.get("auto_fix"),
            })
            if requires_marker:
                violations[-1]["suggested_marker"] = requires_marker
    return violations


def check_pattern_in_file(source: str, rule: dict) -> list[dict]:
    """Generic: scan entire file for a regex pattern.
    Used by: AP005 (tautological_assertion), AP012 (bare_assert),
             AP014 (float_assertion), AP022 (import_side_effects), AP028 (pytest_raises_broad)
    """
    violations = []
    detect = rule.get("detect")
    if not detect:
        return violations

    for line_no, line_text in scan_lines(source, detect):
        stripped = line_text.split("#")[0].strip()
        # Skip lines that are clearly comments
        if not stripped or stripped.startswith("#"):
            continue
        violations.append({
            "rule_id": rule["id"],
            "severity": rule["severity"],
            "file": None,
            "line": line_no,
            "test": None,
            "message": f"[{rule['name']}] {line_text[:100]}",
            "auto_fix": rule.get("auto_fix"),
        })
    return violations


def check_duplicate_tests(tests: list[dict], rule: dict) -> list[dict]:
    """AP007: duplicate test function names in same file."""
    violations = []
    seen = {}
    for test in tests:
        name = test["name"]
        if name in seen:
            violations.append({
                "rule_id": rule["id"],
                "severity": rule["severity"],
                "file": None,
                "line": test["lineno"],
                "test": name,
                "message": f"[{rule['name']}] '{name}' also defined at line {seen[name]}",
                "auto_fix": rule.get("auto_fix"),
            })
        else:
            seen[name] = test["lineno"]
    return violations


def check_missing_timeout(subprocess_calls: list[dict], rule: dict) -> list[dict]:
    """AP004: subprocess call without timeout kwarg."""
    violations = []
    for call in subprocess_calls:
        if not call["has_timeout"]:
            violations.append({
                "rule_id": rule["id"],
                "severity": rule["severity"],
                "file": None,
                "line": call["lineno"],
                "test": None,
                "message": f"[{rule['name']}] {call['func']}() missing 'timeout' kwarg",
                "auto_fix": rule.get("auto_fix"),
            })
    return violations


def check_test_too_long(tests: list[dict], rule: dict) -> list[dict]:
    """AP015: test function exceeds line count threshold."""
    violations = []
    threshold = rule.get("threshold", {})
    max_lines = threshold.get("max", 50)
    for test in tests:
        if test["line_count"] > max_lines:
            violations.append({
                "rule_id": rule["id"],
                "severity": rule["severity"],
                "file": None,
                "line": test["lineno"],
                "test": test["name"],
                "message": f"[{rule['name']}] '{test['name']}' is {test['line_count']} lines (max {max_lines})",
                "auto_fix": rule.get("auto_fix"),
            })
    return violations


def check_test_name_quality(tests: list[dict], rule: dict) -> list[dict]:
    """AP016: test name too short or generic."""
    violations = []
    threshold = rule.get("threshold", {})
    min_len = threshold.get("min", 10)
    exclude = [re.compile(p) for p in threshold.get("exclude_patterns", [])]
    for test in tests:
        if any(p.match(test["name"]) for p in exclude):
            continue
        if len(test["name"]) < min_len:
            violations.append({
                "rule_id": rule["id"],
                "severity": rule["severity"],
                "file": None,
                "line": test["lineno"],
                "test": test["name"],
                "message": f"[{rule['name']}] '{test['name']}' is {len(test['name'])} chars (min {min_len})",
                "auto_fix": rule.get("auto_fix"),
            })
    return violations


def check_complex_test_logic(tests: list[dict], rule: dict) -> list[dict]:
    """AP027: test has too many branches."""
    violations = []
    threshold = rule.get("threshold", {})
    max_branches = threshold.get("max", 3)
    for test in tests:
        if test["branch_count"] > max_branches:
            violations.append({
                "rule_id": rule["id"],
                "severity": rule["severity"],
                "file": None,
                "line": test["lineno"],
                "test": test["name"],
                "message": f"[{rule['name']}] '{test['name']}' has {test['branch_count']} branches (max {max_branches})",
                "auto_fix": rule.get("auto_fix"),
            })
    return violations


def check_test_class_prefix(classes: list[dict], rule: dict) -> list[dict]:
    """AP026: test class doesn't start with 'Test'."""
    violations = []
    for cls in classes:
        if not cls["has_test_prefix"]:
            violations.append({
                "rule_id": rule["id"],
                "severity": rule["severity"],
                "file": None,
                "line": cls["lineno"],
                "test": cls["name"],
                "message": f"[{rule['name']}] '{cls['name']}' doesn't start with 'Test'",
                "auto_fix": rule.get("auto_fix"),
            })
    return violations


def check_unused_imports(source: str, source_lines: list[str], rule: dict) -> list[dict]:
    """AP021: unused imports in test files."""
    violations = []
    unused = extract_unused_imports(source, source_lines)
    for line_no, line_text in unused:
        violations.append({
            "rule_id": rule["id"],
            "severity": rule["severity"],
            "file": None,
            "line": line_no,
            "test": None,
            "message": f"[{rule['name']}] Possibly unused import: {line_text[:100]}",
            "auto_fix": rule.get("auto_fix"),
        })
    return violations


# --- Rule dispatch table ---

def get_checker(rule: dict):
    """Return the appropriate checker function for a rule based on its properties."""
    rid = rule["id"]
    scope = rule.get("scope", "line")

    # AP004: special case — needs subprocess call list
    if rid == "AP004":
        return "subprocess_calls"
    # AP007: duplicate test names
    elif rid == "AP007":
        return "duplicate_tests"
    # AP015: test too long (threshold-based, no detect pattern)
    elif rid == "AP015":
        return "test_too_long"
    # AP016: test name quality (threshold-based, no detect pattern)
    elif rid == "AP016":
        return "test_name_quality"
    # AP020: file-level check (test_ prefix) — handled separately
    elif rid == "AP020":
        return "file_prefix"
    # AP021: unused imports
    elif rid == "AP021":
        return "unused_imports"
    # AP026: test class prefix
    elif rid == "AP026":
        return "test_class_prefix"
    # AP027: complex test logic (threshold-based)
    elif rid == "AP027":
        return "complex_test_logic"
    # Scope-based dispatch
    elif scope == "test_function" and rule.get("detect"):
        return "pattern_in_test"
    elif scope == "line" and rule.get("detect"):
        return "pattern_in_file"
    elif scope == "call_site" and rule.get("detect"):
        return "subprocess_calls"
    elif scope == "file" and rule.get("detect"):
        return "pattern_in_file"

    return None


# --- Main lint logic ---

def lint_file(file_path: Path, rules_config: dict, filter_severity: set[str] | None = None,
              filter_category: set[str] | None = None) -> list[dict]:
    """Run all applicable rules against a single test file."""
    try:
        source = file_path.read_text(errors="replace")
    except OSError as e:
        return [{
            "rule_id": "FILE_ERROR",
            "severity": "high",
            "file": str(file_path),
            "line": None,
            "test": None,
            "message": f"Cannot read file: {e}",
            "auto_fix": None,
        }]

    source_lines = source.splitlines()

    # Parse AST once
    try:
        tests = extract_test_functions(source)
        classes = extract_test_classes(source)
        subprocess_calls = extract_subprocess_calls(source)
    except SyntaxError as e:
        return [{
            "rule_id": "PARSE_ERROR",
            "severity": "high",
            "file": str(file_path.relative_to(REPO_ROOT) if file_path.is_relative_to(REPO_ROOT) else file_path),
            "line": getattr(e, "lineno", None),
            "test": None,
            "message": f"Cannot parse file: {e}",
            "auto_fix": None,
        }]

    violations = []

    # File-level check: AP020 (test_ prefix)
    if file_path.name.startswith("test_") is False:
        # Check if it's a test file by being in tests/ directory
        rel = file_path.relative_to(REPO_ROOT) if file_path.is_relative_to(REPO_ROOT) else file_path
        if "test" in str(rel).split("/"):
            for rule in rules_config["anti_patterns"]:
                if rule["id"] == "AP020":
                    violations.append({
                        "rule_id": rule["id"],
                        "severity": rule["severity"],
                        "file": None,
                        "line": 1,
                        "test": None,
                        "message": f"[{rule['name']}] File '{file_path.name}' doesn't start with 'test_'",
                        "auto_fix": rule.get("auto_fix"),
                    })

    for rule in rules_config["anti_patterns"]:
        # Apply filters
        if filter_severity and rule["severity"] not in filter_severity:
            continue
        if filter_category and rule.get("category") not in filter_category:
            continue

        checker_type = get_checker(rule)
        if checker_type is None:
            continue

        if checker_type == "pattern_in_test":
            violations.extend(check_pattern_in_test_function(tests, rule))
        elif checker_type == "pattern_in_file":
            violations.extend(check_pattern_in_file(source, rule))
        elif checker_type == "subprocess_calls":
            violations.extend(check_missing_timeout(subprocess_calls, rule))
        elif checker_type == "duplicate_tests":
            violations.extend(check_duplicate_tests(tests, rule))
        elif checker_type == "test_too_long":
            violations.extend(check_test_too_long(tests, rule))
        elif checker_type == "test_name_quality":
            violations.extend(check_test_name_quality(tests, rule))
        elif checker_type == "complex_test_logic":
            violations.extend(check_complex_test_logic(tests, rule))
        elif checker_type == "test_class_prefix":
            violations.extend(check_test_class_prefix(classes, rule))
        elif checker_type == "unused_imports":
            violations.extend(check_unused_imports(source, source_lines, rule))

    # Fill in file path for all violations
    try:
        rel_path = file_path.relative_to(REPO_ROOT)
    except ValueError:
        rel_path = file_path
    for v in violations:
        v["file"] = str(rel_path)

    return violations


def lint_directory(test_dir: Path, rules_config: dict, filter_severity: set[str] | None = None,
                  filter_category: set[str] | None = None) -> dict:
    """Lint all test_*.py files in a directory tree."""
    test_files = sorted(test_dir.rglob("test_*.py"))

    all_violations = []
    files_scanned = 0

    for tf in test_files:
        file_violations = lint_file(tf, rules_config, filter_severity, filter_category)
        all_violations.extend(file_violations)
        files_scanned += 1

    # Sort by severity then file then line
    all_violations.sort(key=lambda v: (SEVERITY_ORDER.get(v["severity"], 99), v["file"], v["line"] or 0))

    # Summary counts
    by_severity = {}
    by_rule = {}
    by_category = {}
    for v in all_violations:
        sev = v["severity"]
        rid = v["rule_id"]
        by_severity[sev] = by_severity.get(sev, 0) + 1
        by_rule[rid] = by_rule.get(rid, 0) + 1
        # Look up category from rules
        for rule in rules_config["anti_patterns"]:
            if rule["id"] == rid:
                cat = rule.get("category", "unknown")
                by_category[cat] = by_category.get(cat, 0) + 1
                break

    result = {
        "linted_at": datetime.now(timezone.utc).astimezone().isoformat(),
        "rules_version": rules_config.get("version", "unknown"),
        "verdict": "FAIL" if all_violations else "PASS",
        "files_scanned": files_scanned,
        "total_violations": len(all_violations),
        "by_severity": by_severity,
        "by_rule": by_rule,
        "by_category": by_category,
        "violations": all_violations,
        "summary": (
            f"{len(all_violations)} violation(s) across {files_scanned} file(s): "
            + ", ".join(f"{c} {s}" for s, c in sorted(by_severity.items(), key=lambda x: SEVERITY_ORDER.get(x[0], 99)))
            if all_violations
            else f"All {files_scanned} files clean"
        ),
    }

    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Martin's deterministic test linter")
    parser.add_argument("test_dir", type=Path, help="Root directory to scan (e.g., tests/)")
    parser.add_argument("-o", "--output", type=Path, default=None, help="Output JSON path")
    parser.add_argument("--rules", type=Path, default=None, help="Custom rules file path")
    parser.add_argument("--json", action="store_true", dest="json_stdout", help="Output JSON to stdout")
    parser.add_argument("--severity", type=str, default=None,
                        help="Only show violations at this severity (comma-sep, e.g., critical,high)")
    parser.add_argument("--category", type=str, default=None,
                        help="Only show violations in this category (comma-sep, e.g., safety,isolation)")
    args = parser.parse_args()

    test_dir = args.test_dir.resolve()
    if not test_dir.is_dir():
        print(f"ERROR: {test_dir} is not a directory", file=sys.stderr)
        return 2

    # Parse filters
    filter_severity = set(args.severity.split(",")) if args.severity else None
    filter_category = set(args.category.split(",")) if args.category else None

    # Find rules
    rules_path = args.rules or find_latest_rules(RULES_DIR)
    rules_config = load_rules(rules_path)

    # Lint
    result = lint_directory(test_dir, rules_config, filter_severity, filter_category)

    # Output
    output_path = args.output or DEFAULT_OUTPUT
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2) + "\n")

    # Human-readable summary to stderr
    print(f"RULES:    {rules_path.name}", file=sys.stderr)
    print(f"FILES:    {result['files_scanned']} scanned", file=sys.stderr)
    print(f"VERDICT:  {result['verdict']}", file=sys.stderr)
    print(f"SUMMARY:  {result['summary']}", file=sys.stderr)
    if result.get("by_category"):
        print(f"CATEGORY: {', '.join(f'{c}: {n}' for c, n in sorted(result['by_category'].items()))}", file=sys.stderr)

    if args.json_stdout:
        print(json.dumps(result, indent=2))

    # Violation details to stderr
    if result["violations"]:
        print("\nViolations:", file=sys.stderr)
        for v in result["violations"]:
            loc = f"{v['file']}:{v['line']}" if v["line"] else v["file"]
            test_info = f" [{v['test']}]" if v["test"] else ""
            fix_info = f" (auto-fix: {v['auto_fix']})" if v["auto_fix"] else ""
            print(f"  {v['severity'].upper():8s} {v['rule_id']} {loc}{test_info}{fix_info}", file=sys.stderr)
            print(f"           {v['message']}", file=sys.stderr)

    print(f"\nFull report: {output_path}", file=sys.stderr)

    return 1 if result["violations"] else 0


if __name__ == "__main__":
    sys.exit(main())