#!/usr/bin/env python3
"""pre_flight.py — Pre-flight structural linter for MissHoover V2.

This script verifies that required classes/functions defined in the active
phase's specs/*.md actually exist in src/ before the phase exit gate is tested.

Uses AST (Abstract Syntax Tree) parsing to verify structural requirements
without importing the modules (which could fail if dependencies are missing).

Exit codes:
  0 = PASS (all structural requirements found)
  1 = FAIL (missing structural requirements)
  2 = ERROR (script error, not a validation failure)
"""
from __future__ import annotations

import ast
import json
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
STATE_FILE = REPO_ROOT / "docs" / "ralph-state.json"
SPECS_DIR = REPO_ROOT / "specs"
SRC_DIR = REPO_ROOT / "src"
PRE_FLIGHT_OUTPUT = REPO_ROOT / "docs" / "pre-flight.json"


@dataclass
class StructuralRequirement:
    """A structural requirement to check."""
    type: str  # "class", "function", "constant", "import"
    name: str
    file_pattern: str  # glob pattern for src/
    must_inherit: str | None = None  # for classes, optional base class
    found: bool = False
    location: str | None = None
    error: str | None = None


# Phase → structural requirements mapping
# These define what classes/functions must exist for each phase to pass
PHASE_STRUCTURAL_REQUIREMENTS: dict[str, list[StructuralRequirement]] = {
    "1": [  # Adapter Architecture
        StructuralRequirement("class", "BaseAdapter", "**/adapters/*.py"),
        StructuralRequirement("class", "AdapterRegistry", "**/adapters/*.py"),
        StructuralRequirement("constant", "CANONICAL_COLUMNS", "**/adapters/*.py"),
        StructuralRequirement("function", "route_by_extension", "**/*.py"),
    ],
    "2": [  # Format Adapters
        StructuralRequirement("class", "CSVAdapter", "**/adapters/*.py", must_inherit="BaseAdapter"),
        StructuralRequirement("class", "XLSXAdapter", "**/adapters/*.py", must_inherit="BaseAdapter"),
        StructuralRequirement("class", "XLEAdapter", "**/adapters/*.py", must_inherit="BaseAdapter"),
        StructuralRequirement("class", "JSONAdapter", "**/adapters/*.py", must_inherit="BaseAdapter"),
    ],
    "3": [  # Pipeline Integration
        StructuralRequirement("function", "build_raw_manifest", "**/*.py"),
        StructuralRequirement("function", "resample_hourly", "**/*.py"),
        StructuralRequirement("function", "resample_daily", "**/*.py"),
        StructuralRequirement("function", "impute_frame", "**/*.py"),
        StructuralRequirement("function", "materialize_resampled_outputs", "**/*.py"),
    ],
    "4": [  # Stanhope Validation
        StructuralRequirement("function", "validate_against_reference", "**/*.py"),
        StructuralRequirement("function", "compare_station_data", "**/*.py"),
    ],
    "5": [  # QA/QC Reporting
        StructuralRequirement("function", "generate_qa_qc_report", "**/*.py"),
        StructuralRequirement("function", "calculate_completeness", "**/*.py"),
    ],
    "6": [  # Determinism
        StructuralRequirement("function", "compute_checksum", "**/*.py"),
        StructuralRequirement("function", "verify_determinism", "**/*.py"),
    ],
    "7": [  # E2E Validation
        # Phase 7 is about running the full suite, no new structural requirements
    ],
}


def load_json(path: Path) -> dict | None:
    """Load JSON file, return None if not found or invalid."""
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return None


def save_json(path: Path, data: dict) -> None:
    """Atomically write JSON file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / (path.name + ".tmp")
    tmp.write_text(json.dumps(data, indent=2) + "\n")
    tmp.replace(path)


def find_files(pattern: str, base_dir: Path = SRC_DIR) -> list[Path]:
    """Find files matching glob pattern."""
    return list(base_dir.glob(pattern))


def extract_classes_from_ast(tree: ast.AST) -> dict[str, ast.ClassDef]:
    """Extract all class definitions from AST."""
    classes = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            classes[node.name] = node
    return classes


def extract_functions_from_ast(tree: ast.AST) -> dict[str, ast.FunctionDef]:
    """Extract all top-level function definitions from AST."""
    functions = {}
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.FunctionDef):
            functions[node.name] = node
    return functions


def extract_constants_from_ast(tree: ast.AST) -> dict[str, ast.Assign]:
    """Extract all top-level constant assignments from AST."""
    constants = {}
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    constants[target.id] = node
    return constants


def get_base_classes(class_node: ast.ClassDef) -> list[str]:
    """Get list of base class names for a class."""
    bases = []
    for base in class_node.bases:
        if isinstance(base, ast.Name):
            bases.append(base.id)
        elif isinstance(base, ast.Attribute):
            bases.append(f"{base.value.id}.{base.attr}" if isinstance(base.value, ast.Name) else base.attr)
    return bases


def check_requirement(req: StructuralRequirement) -> StructuralRequirement:
    """Check if a single structural requirement is satisfied."""
    files = find_files(req.file_pattern)
    
    if not files:
        req.error = f"No files match pattern: {req.file_pattern}"
        return req
    
    for file_path in files:
        try:
            source = file_path.read_text()
            tree = ast.parse(source)
            
            if req.type == "class":
                classes = extract_classes_from_ast(tree)
                if req.name in classes:
                    class_node = classes[req.name]
                    if req.must_inherit:
                        bases = get_base_classes(class_node)
                        if req.must_inherit in bases:
                            req.found = True
                            req.location = f"{file_path.relative_to(REPO_ROOT)}:{class_node.lineno}"
                            return req
                        else:
                            # Found the class but wrong inheritance
                            req.error = f"Class {req.name} found but doesn't inherit from {req.must_inherit}"
                            continue
                    else:
                        req.found = True
                        req.location = f"{file_path.relative_to(REPO_ROOT)}:{class_node.lineno}"
                        return req
            
            elif req.type == "function":
                functions = extract_functions_from_ast(tree)
                if req.name in functions:
                    func_node = functions[req.name]
                    req.found = True
                    req.location = f"{file_path.relative_to(REPO_ROOT)}:{func_node.lineno}"
                    return req
            
            elif req.type == "constant":
                constants = extract_constants_from_ast(tree)
                if req.name in constants:
                    const_node = constants[req.name]
                    req.found = True
                    req.location = f"{file_path.relative_to(REPO_ROOT)}:{const_node.lineno}"
                    return req
        
        except SyntaxError as e:
            req.error = f"Syntax error in {file_path}: {e}"
            continue
        except Exception as e:
            req.error = f"Error parsing {file_path}: {e}"
            continue
    
    if not req.error:
        req.error = f"{req.type.title()} '{req.name}' not found in {req.file_pattern}"
    
    return req


def run_pre_flight(phase: str) -> dict:
    """Run pre-flight checks for a phase."""
    result = {
        "phase": phase,
        "timestamp": datetime.now(timezone.utc).astimezone().isoformat(),
        "verdict": "PASS",
        "checks": [],
        "missing": [],
        "errors": [],
    }
    
    requirements = PHASE_STRUCTURAL_REQUIREMENTS.get(phase, [])
    
    if not requirements:
        result["verdict"] = "SKIP"
        result["summary"] = f"Phase {phase} has no structural requirements"
        return result
    
    for req in requirements:
        checked = check_requirement(req)
        check_result = {
            "type": req.type,
            "name": req.name,
            "found": checked.found,
            "location": checked.location,
            "error": checked.error,
        }
        result["checks"].append(check_result)
        
        if checked.found:
            print(f"  ✓ {req.type.title()} '{req.name}' found at {checked.location}")
        else:
            result["verdict"] = "FAIL"
            result["missing"].append({
                "type": req.type,
                "name": req.name,
                "file_pattern": req.file_pattern,
                "must_inherit": req.must_inherit,
            })
            result["errors"].append(checked.error)
            print(f"  ✗ {req.type.title()} '{req.name}' NOT FOUND: {checked.error}")
    
    result["summary"] = (
        f"Checked {len(requirements)} structural requirements for phase {phase}: "
        f"{result['verdict']} ({len(result['missing'])} missing)"
    )
    
    return result


def main() -> int:
    """Main entry point. Returns exit code."""
    # Load state
    state = load_json(STATE_FILE)
    if not state:
        print("ERROR: Cannot read docs/ralph-state.json", file=sys.stderr)
        return 2
    
    # Get current phase
    phase = str(state.get("phase", state.get("current_phase", "?")))
    
    print(f"PRE_FLIGHT_CHECK for Phase {phase}")
    print("=" * 50)
    
    # Run pre-flight
    result = run_pre_flight(phase)
    
    # Save result
    save_json(PRE_FLIGHT_OUTPUT, result)
    
    # Output summary
    print()
    print(f"PRE_FLIGHT_VERDICT={result['verdict']}")
    print(f"PHASE={phase}")
    print(f"CHECKS_COUNT={len(result['checks'])}")
    print(f"MISSING_COUNT={len(result['missing'])}")
    print(f"Full report: docs/pre-flight.json")
    
    # Exit code
    if result["verdict"] == "PASS":
        return 0
    elif result["verdict"] == "SKIP":
        return 0  # Skip is not a failure
    else:
        return 1


if __name__ == "__main__":
    sys.exit(main())
