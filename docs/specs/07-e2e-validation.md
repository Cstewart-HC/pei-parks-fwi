# Spec 07: End-to-End Validation

**Phase:** 7  
**Status:** Pending  
**Depends on:** Phase 6 (Determinism)

---

## Goal

Full pipeline runs clean, all tests pass, all stations have complete outputs, zero unprocessed files.

---

## Deliverables

### 1. Full Pipeline Run

```bash
python cleaning.py
```

Must complete without error.

### 2. Complete Output Verification

All 6 stations have:
- `data/processed/<station>/station_hourly.csv`
- `data/processed/<station>/station_daily.csv`
- Both with FWI columns populated (ffmc, dmc, dc, isi, bui, fwi)

### 3. Report Artifacts

All exist:
- `data/processed/pipeline_manifest.json`
- `data/processed/imputation_report.csv`
- `data/processed/qa_qc_report.csv`
- `data/processed/stanhope_validation.csv`

### 4. Coverage Verification

```python
def verify_coverage():
    """Verify continuous coverage for all stations."""
    for station in STATIONS:
        hourly = pd.read_csv(f"data/processed/{station}/station_hourly.csv")
        daily = pd.read_csv(f"data/processed/{station}/station_daily.csv")
        
        # No large gaps (>7 days) in hourly data
        timestamps = pd.to_datetime(hourly["timestamp_utc"])
        gaps = timestamps.diff().dropna()
        max_gap = gaps.max()
        assert max_gap < pd.Timedelta(days=7), f"{station} has gap > 7 days: {max_gap}"
```

### 5. Zero Unprocessed Files

```python
def verify_no_unprocessed():
    """Verify all raw files were processed."""
    raw_files = discover_raw_files(RAW_DIR)
    manifest = json.load(open("data/processed/pipeline_manifest.json"))
    
    for raw_file in raw_files:
        # Each raw file should be represented in processed output
        station = infer_station_from_path(raw_file)
        assert station in manifest["stations"], f"Unprocessed file: {raw_file}"
```

### 6. Test Suite Passes

```bash
pytest tests/ -v
```

All tests pass, no failures, no xfail.

---

## Acceptance Criteria

| ID | Criterion |
|----|-----------|
| AC-E2E-1 | `python cleaning.py` completes with exit code 0 |
| AC-E2E-2 | All 6 stations have hourly + daily CSVs |
| AC-E2E-3 | All stations with RH data have populated FWI columns |
| AC-E2E-4 | All 4 report artifacts exist (manifest, imputation, qa_qc, stanhope_validation) |
| AC-E2E-5 | No gaps > 7 days in hourly data for any station |
| AC-E2E-6 | Continuous coverage from earliest PEINP record through latest Licor record |
| AC-E2E-7 | Pipeline manifest reports 0 unprocessed files |
| AC-E2E-8 | Full test suite passes: `pytest tests/ -v` |

---

## Exit Gate

```bash
pytest tests/test_v2_pipeline.py::TestAC_PIPE_7_E2EValidation -v
```

All tests must pass.

---

## Project Complete

When this phase passes, the pipeline rebuild is complete. The SSOT processed data is ready for analysis.
