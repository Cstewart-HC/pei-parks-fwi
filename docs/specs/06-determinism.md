# Spec 06: Determinism + Reproducibility

**Phase:** 6  
**Status:** Pending  
**Depends on:** Phase 5 (QA/QC Reporting)

---

## Goal

Pipeline produces byte-identical outputs on re-run from the same raw data.

---

## Deliverables

### 1. Sorted Output

All CSVs sorted by (station, timestamp_utc) before writing:

```python
def write_station_outputs(station: str, hourly: pd.DataFrame, daily: pd.DataFrame):
    station_hourly = hourly[hourly["station"] == station].sort_values("timestamp_utc")
    station_daily = daily[daily["station"] == station].sort_values("timestamp_utc")
    
    # Deterministic column order
    columns = sorted(station_hourly.columns)
    station_hourly = station_hourly[columns]
    
    station_hourly.to_csv(f"data/processed/{station}/station_hourly.csv", index=False)
```

### 2. Checksums in Manifest

```python
def write_pipeline_manifest():
    manifest = {
        "run_timestamp": datetime.utcnow().isoformat(),
        "files": [],
        "checksums": {},
    }
    
    for station in STATIONS:
        for filename in ["station_hourly.csv", "station_daily.csv"]:
            path = Path(f"data/processed/{station}/{filename}")
            if path.exists():
                checksum = hashlib.sha256(path.read_bytes()).hexdigest()[:16]
                manifest["checksums"][f"{station}_{filename}"] = checksum
    
    # Write manifest
    with open("data/processed/pipeline_manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)
```

### 3. --force Flag

```python
def parse_args():
    parser.add_argument("--force", action="store_true",
                        help="Overwrite existing outputs (default: skip if newer)")
    return parser.parse_args()

def should_process(station: str, force: bool) -> bool:
    output_path = Path(f"data/processed/{station}/station_hourly.csv")
    if not output_path.exists():
        return True
    if force:
        return True
    # Skip if output is newer than all raw inputs
    raw_files = get_raw_files_for_station(station)
    newest_raw = max(f.stat().st_mtime for f in raw_files)
    if output_path.stat().st_mtime > newest_raw:
        return False
    return True
```

### 4. Race Condition Fix

The `test_cleaning_py_runs` test has a race condition with pytest-xdist. Fix:

```python
# In tests/test_data_refresh.py

@pytest.mark.serial  # Run without xdist parallelization
def test_cleaning_py_runs():
    """Test that cleaning.py runs without error."""
    result = subprocess.run(
        ["python", "cleaning.py", "--stations", "greenwich"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"cleaning.py failed: {result.stderr}"
```

And in `pytest.ini`:

```ini
[pytest]
addopts = -n auto
markers =
    serial: run without xdist parallelization
```

---

## Acceptance Criteria

| ID | Criterion |
|----|-----------|
| AC-DET-1 | Two consecutive runs of `cleaning.py` produce byte-identical CSV outputs |
| AC-DET-2 | All outputs sorted by (station, timestamp_utc) |
| AC-DET-3 | Column order is deterministic (alphabetical) |
| AC-DET-4 | Pipeline manifest includes SHA256 checksums (first 16 chars) for all output files |
| AC-DET-5 | `--force` flag overwrites existing outputs; without it, skips if output newer than inputs |
| AC-DET-6 | `test_cleaning_py_runs` no longer has race condition (marked serial) |
| AC-DET-7 | Checksum comparison between runs shows no differences |

---

## Exit Gate

```bash
pytest tests/test_v2_pipeline.py::TestAC_PIPE_6_Determinism -v
```

All tests must pass.
