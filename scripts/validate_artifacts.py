#!/usr/bin/env python3
"""validate_artifacts.py — Deterministic data artifact validation for MissHoover V2.

This script enforces the "Hard Gate" before Lisa review:
1. Reads docs/ralph-state.json to identify active phase
2. Verifies expected data outputs exist, are not empty
3. Performs basic schema checks (no NaNs in primary keys)
4. Uses pandas for robust CSV validation
5. Computes SHA256 fingerprint for validated outputs
6. Outputs structured JSON for sync_state.py to consume

Exit codes:
  0 = PASS (all artifacts valid)
  1 = FAIL (validation failed)
  2 = ERROR (script error, not a validation failure)
"""
from __future__ import annotations

import csv
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

# Optional pandas import for enhanced validation
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    pd = None

REPO_ROOT = Path(__file__).resolve().parent.parent
STATE_FILE = REPO_ROOT / "docs" / "ralph-state.json"
DATA_MANIFEST = REPO_ROOT / "docs" / "data-manifest.json"
PIPELINE_MANIFEST = REPO_ROOT / "data" / "processed" / "pipeline_manifest.json"
VALIDATION_OUTPUT = REPO_ROOT / "docs" / "artifact-validation.json"

# Phase → expected artifact patterns
# These define what data outputs each phase should produce
PHASE_ARTIFACT_EXPECTATIONS = {
    "3": {  # Pipeline Integration
        "required_dirs": ["data/processed/cavendish", "data/processed/greenwich"],
        "required_files": [
            "data/processed/cavendish/station_hourly.csv",
            "data/processed/cavendish/station_daily.csv",
            "data/processed/greenwich/station_hourly.csv",
            "data/processed/greenwich/station_daily.csv",
        ],
        "schema_checks": {
            "station_hourly.csv": {
                "required_columns": ["station", "timestamp_utc", "air_temperature_c", "relative_humidity_pct"],
                "no_nan_columns": ["station", "timestamp_utc"],
            },
            "station_daily.csv": {
                "required_columns": ["station", "timestamp_utc", "air_temperature_c", "relative_humidity_pct"],
                "no_nan_columns": ["station", "timestamp_utc"],
            },
        },
    },
    "4": {  # Stanhope Validation
        "required_dirs": ["data/processed/stanhope"],
        "required_files": [
            "data/processed/stanhope/station_hourly.csv",
            "data/processed/stanhope/station_daily.csv",
        ],
    },
    "5": {  # QA/QC Reporting
        "required_files": ["data/processed/imputation_report.csv"],
    },
}

# Minimum rows for valid output (catch empty files)
MIN_ROWS_PER_ARTIFACT = 10

# Maximum NaN ratio allowed in non-key columns (0.0 = none, 1.0 = any)
MAX_NAN_RATIO = 0.1  # 10% NaNs allowed in data columns


def compute_file_hash(path: Path) -> str:
    """Compute SHA256 hash of a file for fingerprinting."""
    sha256 = hashlib.sha256()
    try:
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        return sha256.hexdigest()[:16]  # First 16 chars for brevity
    except Exception as e:
        return f"ERROR: {e}"


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


def check_file_exists(path: Path) -> tuple[bool, str]:
    """Check if file exists and is not empty."""
    if not path.exists():
        return False, f"File not found: {path}"
    if path.stat().st_size == 0:
        return False, f"File is empty: {path}"
    return True, "OK"


def check_row_count(path: Path, min_rows: int) -> tuple[bool, str, int]:
    """Check CSV has at least min_rows (excluding header). Returns row count."""
    try:
        with open(path, newline="") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if not header:
                return False, f"No header in {path}", 0
            row_count = sum(1 for _ in reader)
            if row_count < min_rows:
                return False, f"Only {row_count} rows in {path}, need >= {min_rows}", row_count
            return True, f"{row_count} rows", row_count
    except Exception as e:
        return False, f"Error reading {path}: {e}", 0


def check_schema(path: Path, schema: dict) -> tuple[bool, str]:
    """Validate CSV schema: required columns and no NaNs in key columns.

    Uses pandas if available for more robust validation.
    Falls back to csv module if pandas is not installed.
    """
    if PANDAS_AVAILABLE:
        return check_schema_pandas(path, schema)
    else:
        return check_schema_csv(path, schema)


def check_schema_pandas(path: Path, schema: dict) -> tuple[bool, str]:
    """Validate CSV schema using pandas for robust checking."""
    try:
        df = pd.read_csv(path, nrows=1000)  # Sample first 1000 rows

        # Check required columns
        required = schema.get("required_columns", [])
        missing = [c for c in required if c not in df.columns]
        if missing:
            return False, f"Missing columns in {path}: {missing}"

        # Check for NaNs in specified columns
        no_nan_cols = schema.get("no_nan_columns", [])
        nan_issues = []
        for col in no_nan_cols:
            if col in df.columns:
                nan_count = df[col].isna().sum()
                if nan_count > 0:
                    nan_issues.append(f"'{col}' has {nan_count} NaN values")

        if nan_issues:
            return False, f"NaN values in primary keys in {path}: {', '.join(nan_issues)}"

        # Check NaN ratio in data columns
        data_cols = [c for c in df.columns if c not in no_nan_cols]
        high_nan_cols = []
        for col in data_cols:
            nan_ratio = df[col].isna().sum() / len(df) if len(df) > 0 else 0
            if nan_ratio > MAX_NAN_RATIO:
                high_nan_cols.append(f"'{col}' has {nan_ratio:.1%} NaN (max {MAX_NAN_RATIO:.0%})")

        if high_nan_cols:
            return False, f"High NaN ratio in {path}: {', '.join(high_nan_cols)}"

        return True, f"Schema OK (pandas, {len(df)} rows sampled)"

    except Exception as e:
        return False, f"Pandas schema check error for {path}: {e}"


def check_schema_csv(path: Path, schema: dict) -> tuple[bool, str]:
    """Validate CSV schema using csv module (fallback)."""
    try:
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            header = reader.fieldnames or []

            # Check required columns
            required = schema.get("required_columns", [])
            missing = [c for c in required if c not in header]
            if missing:
                return False, f"Missing columns in {path}: {missing}"

            # Check for NaNs in specified columns (sample first 100 rows)
            no_nan_cols = schema.get("no_nan_columns", [])
            nan_counts = {col: 0 for col in no_nan_cols}
            rows_checked = 0

            for row in reader:
                rows_checked += 1
                if rows_checked > 100:
                    break
                for col in no_nan_cols:
                    val = row.get(col, "")
                    if val == "" or val.lower() in ("nan", "null", "none", "na"):
                        nan_counts[col] += 1

            # Any NaN in primary key columns is a failure
            for col, count in nan_counts.items():
                if count > 0:
                    return False, f"Found {count} NaN/null values in primary key '{col}' in {path}"

            return True, f"Schema OK ({rows_checked} rows checked)"
    except Exception as e:
        return False, f"Schema check error for {path}: {e}"


def validate_phase_artifacts(phase: str) -> dict:
    """Validate all expected artifacts for a phase."""
    result = {
        "phase": phase,
        "timestamp": datetime.now(timezone.utc).astimezone().isoformat(),
        "verdict": "PASS",
        "checks": [],
        "errors": [],
        "fingerprints": {},  # SHA256 hashes of validated files
    }

    expectations = PHASE_ARTIFACT_EXPECTATIONS.get(phase)
    if not expectations:
        # Phase has no data artifact requirements
        result["verdict"] = "SKIP"
        result["summary"] = f"Phase {phase} has no data artifact expectations"
        return result

    # Check required directories exist
    for dir_path in expectations.get("required_dirs", []):
        full_path = REPO_ROOT / dir_path
        if not full_path.exists() or not full_path.is_dir():
            result["verdict"] = "FAIL"
            result["errors"].append(f"Missing directory: {dir_path}")
            result["checks"].append({
                "type": "directory",
                "path": dir_path,
                "status": "FAIL",
                "message": "Directory not found",
            })
        else:
            result["checks"].append({
                "type": "directory",
                "path": dir_path,
                "status": "PASS",
            })

    # Check required files exist and have content
    for file_path in expectations.get("required_files", []):
        full_path = REPO_ROOT / file_path
        exists, msg = check_file_exists(full_path)
        if not exists:
            result["verdict"] = "FAIL"
            result["errors"].append(msg)
            result["checks"].append({
                "type": "file_exists",
                "path": file_path,
                "status": "FAIL",
                "message": msg,
            })
            continue

        # Compute fingerprint for validated file
        file_hash = compute_file_hash(full_path)
        result["fingerprints"][file_path] = file_hash

        result["checks"].append({
            "type": "file_exists",
            "path": file_path,
            "status": "PASS",
            "fingerprint": file_hash,
        })

        # Check row count
        row_ok, row_msg, row_count = check_row_count(full_path, MIN_ROWS_PER_ARTIFACT)
        if not row_ok:
            result["verdict"] = "FAIL"
            result["errors"].append(row_msg)
            result["checks"].append({
                "type": "row_count",
                "path": file_path,
                "status": "FAIL",
                "message": row_msg,
            })
        else:
            result["checks"].append({
                "type": "row_count",
                "path": file_path,
                "status": "PASS",
                "message": row_msg,
                "row_count": row_count,
            })

        # Schema checks based on filename pattern
        for pattern, schema in expectations.get("schema_checks", {}).items():
            if file_path.endswith(pattern):
                schema_ok, schema_msg = check_schema(full_path, schema)
                if not schema_ok:
                    result["verdict"] = "FAIL"
                    result["errors"].append(schema_msg)
                    result["checks"].append({
                        "type": "schema",
                        "path": file_path,
                        "status": "FAIL",
                        "message": schema_msg,
                    })
                else:
                    result["checks"].append({
                        "type": "schema",
                        "path": file_path,
                        "status": "PASS",
                        "message": schema_msg,
                    })

    # Check data-manifest.json exists (if any data artifacts were expected)
    if expectations.get("required_files") and not DATA_MANIFEST.exists():
        result["checks"].append({
            "type": "data_manifest",
            "path": "docs/data-manifest.json",
            "status": "WARN",
            "message": "Data manifest not found (Ralph should create/update it)",
        })

    # Check pipeline_manifest.json exists for data phases
    if expectations.get("required_files"):
        if PIPELINE_MANIFEST.exists():
            manifest = load_json(PIPELINE_MANIFEST)
            if manifest and "artifacts" in manifest:
                result["checks"].append({
                    "type": "pipeline_manifest",
                    "path": "data/processed/pipeline_manifest.json",
                    "status": "PASS",
                    "message": f"Found {len(manifest.get('artifacts', []))} artifacts",
                })
            else:
                result["checks"].append({
                    "type": "pipeline_manifest",
                    "path": "data/processed/pipeline_manifest.json",
                    "status": "WARN",
                    "message": "Pipeline manifest exists but has no artifacts list",
                })
        else:
            result["checks"].append({
                "type": "pipeline_manifest",
                "path": "data/processed/pipeline_manifest.json",
                "status": "FAIL",
                "message": "Pipeline manifest not found",
            })
            result["verdict"] = "FAIL"
            result["errors"].append("Missing pipeline_manifest.json")

    result["summary"] = (
        f"Validated {len(result['checks'])} checks for phase {phase}: "
        f"{result['verdict']} ({len(result['errors'])} errors)"
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

    # Run validation
    result = validate_phase_artifacts(phase)

    # Save result
    save_json(VALIDATION_OUTPUT, result)

    # Output for consumption by sync_state.py
    print(f"ARTIFACT_VALIDATION={result['verdict']}")
    print(f"PHASE={phase}")
    print(f"CHECKS_COUNT={len(result['checks'])}")
    print(f"ERRORS_COUNT={len(result['errors'])}")
    print(f"FINGERPRINTS_COUNT={len(result.get('fingerprints', {}))}")

    if result["errors"]:
        print("\n## Validation Errors")
        for err in result["errors"]:
            print(f"  - {err}")

    if result.get("fingerprints"):
        print("\n## File Fingerprints (SHA256)")
        for path, fp in result["fingerprints"].items():
            print(f"  {fp[:8]}... {path}")

    print("\nFull report: docs/artifact-validation.json")

    # Exit code
    if result["verdict"] == "PASS":
        return 0
    elif result["verdict"] == "SKIP":
        return 0  # Skip is not a failure
    else:
        return 1


if __name__ == "__main__":
    sys.exit(main())
